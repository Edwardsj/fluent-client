package fluent

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	pdebug "github.com/lestrrat/go-pdebug"
	"github.com/pkg/errors"
)

// Architecture:
//
// The Client passes raw payload to be sent to fluentd to a channel where
// the minion reader is reading from.
//
//    User (payload) -> fluent.Client -> ch
//
// In the default asynchronous mode, this is the end of interaction between
// the library user and the library.
//
// The minion reader loop, which runs on a separate goroutine, reads the
// payload, and encodes it to bytes using the designated marshaler, which
// is then appended to a buffer that is shared between the minion reader
// and writer.
//
//    payload -> marshaler (default: msgpac) -> bytes
//
// The minion reader is responsible for accepting the payload and encoding
// it as soon as possible, as the Client is being blocked while this is
// happening.
//
// Once the buffer is appended, the reader immediately goes back to waiting
// for new payload coming in from the client.
//
// Meanwhile, the minion writer is woken up by the reader via a sync.Cond.
// (The minion writer waits for this condition, so if there's nothing to
// write, the writer does nothing)
//
// The minion writer checks to see if there are any pending bytes to write
// to the server. If there's anything, we start the write process.
//
// The writer is responsible for connecting to the fluentd host, and reusing
// that connection.
//
// Once connected, the writer tries to write everything it can, for as long
// as it can. If the buffer is empty, or the connection is dropped, we
// start over the write process (without waiting for the wake-up call)

type minion struct {
	address         string
	bufferLimit     int
	cond            *sync.Cond
	dialTimeout     time.Duration
	done            chan struct{}
	flush           bool
	incoming        chan *Message
	marshaler       marshaler
	maxConnAttempts uint64
	muFlush         sync.RWMutex
	muPending       sync.RWMutex
	network         string
	pending         []byte
	tagPrefix       string
	writeThreshold  int
	writeTimeout    time.Duration
}

func newMinion(options ...Option) (*minion, error) {
	m := &minion{
		address:         "127.0.0.1:24224",
		bufferLimit:     8 * 1024 * 1024,
		cond:            sync.NewCond(&sync.Mutex{}),
		dialTimeout:     3 * time.Second,
		done:            make(chan struct{}),
		incoming:        make(chan *Message),
		maxConnAttempts: 64,
		marshaler:       marshalFunc(msgpackMarshal),
		network:         "tcp",
		writeThreshold:  8 * 1028,
		writeTimeout:    3 * time.Second,
	}

	for _, opt := range options {
		switch opt.Name() {
		case optkeyNetwork:
			v := opt.Value().(string)
			switch v {
			case "tcp", "unix":
			default:
				return nil, errors.Errorf(`invalid network type: %s`, v)
			}
			m.network = v
		case optkeyAddress:
			m.address = opt.Value().(string)
		case optkeyBufferLimit:
			m.bufferLimit = opt.Value().(int)
		case optkeyDialTimeout:
			m.dialTimeout = opt.Value().(time.Duration)
		case optkeyMarshaler:
			m.marshaler = opt.Value().(marshaler)
		case optkeyMaxConnAttempts:
			m.maxConnAttempts = opt.Value().(uint64)
		case optkeyWriteThreshold:
			m.writeThreshold = opt.Value().(int)
		case optkeyTagPrefix:
			m.tagPrefix = opt.Value().(string)
		}
	}
	m.pending = make([]byte, 0, m.bufferLimit)

	return m, nil
}

// This is the reader loop. The only thing we're responsible for
// is to accept incoming messages from the client as soon as possible
func (m *minion) runReader(ctx context.Context) {
	if pdebug.Enabled {
		pdebug.Printf("background reader: starting")
		defer pdebug.Printf("background reader: exiting")
	}
	// This goroutine receives the incoming data as fast as
	// possible, so that the caller to enqueue does not block
	for {
		select {
		case <-ctx.Done():
			// Wake up the writer goroutine so that it can detect
			// cancelation. m.flush is used to tell the write that
			// it should try really hard to write everything in the
			// buffer before the program exits
			m.muFlush.Lock()
			m.flush = true
			m.muFlush.Unlock()

			m.cond.Broadcast()
			if pdebug.Enabled {
				pdebug.Printf("background reader: cancel detected")
			}
			return
		case msg := <-m.incoming:
			m.appendMessage(msg)
		}
	}
}

// appends a message to the pending buffer
func (m *minion) appendMessage(msg *Message) {
	defer releaseMessage(msg)

	if p := m.tagPrefix; len(p) > 0 {
		msg.Tag = p + "." + msg.Tag
	}

	if pdebug.Enabled {
		if msg.replyCh != nil {
			pdebug.Printf("background reader: message expects reply")
		}
	}

	buf, err := m.marshaler.Marshal(msg)
	if err != nil {
		if pdebug.Enabled {
			pdebug.Printf("background reader: failed to marshal message: %s", err)
		}
		if msg.replyCh != nil {
			msg.replyCh <- errors.Wrap(err, `failed to marshal payload`)
		}
		return
	}

	// Wake up the writer goroutine. This is implemented in terms of a
	// condition variable, because we do not want to block trying to
	// write to a channel. With a condition variable, the blocking is
	// contained to the scope of the condition variable's surrounding
	// locker, so we save precious little time we have until we receive
	// our next Post() requests
	//
	// This is implemented in terms of a defer(), because we want to
	// wake up the writer regardless of if the buffer is full or not
	defer m.cond.Broadcast()

	m.muPending.Lock()
	defer m.muPending.Unlock()
	isFull := len(m.pending)+len(buf) > m.bufferLimit

	if isFull {
		if pdebug.Enabled {
			pdebug.Printf("background reader: buffer is full")
		}
		if msg.replyCh != nil {
			msg.replyCh <- errors.New("buffer full")
		}
		return
	}

	if pdebug.Enabled {
		pdebug.Printf("background reader: received %d more bytes, appending", len(buf))
	}
	m.pending = append(m.pending, buf...)
}

// This goroutine waits for the receiver goroutine to wake
// it up. When it's awake, we know that there's at least one
// piece of data to send to the fluentd server.
func (m *minion) runWriter(ctx context.Context) {
	if pdebug.Enabled {
		defer pdebug.Printf("background writer: exiting")
	}
	defer close(m.done)

	var conn net.Conn
	defer func() {
		// Make sure that this connection is closed.
		if conn != nil {
			if pdebug.Enabled {
				pdebug.Printf("background writer: closing connection (in cleanup)")
			}
			conn.Close()
		}
	}()

	expbackoff := backoff.NewExponentialBackOff()

	for {
		// Wait for the reader to notify us
		if err := m.waitPending(ctx); err != nil {
			return
		}

		// if we're not connected, we should do that now.
		// there are two cases where we can get to this point.
		// 1. reader got something, want us to write
		// 2. reader got notified of cancel, want us to exit
		// case 1 is simple. in case 2, we need to at least attempt to
		// flush the remaining buffer, without checking the context cancelation
		// status, otherwise we exit immediately

		var connAttempts uint64
		flush := m.isFlushMode()
		for conn == nil {
			if pdebug.Enabled {
				if flush {
					pdebug.Printf("background writer: attempting to connect in flush mode")
				} else {
					pdebug.Printf("background writer: attempting to connect")
				}
			}

			parentCtx := ctx
			if flush {
				// In flush mode, we don't let a parent context to cancel us.
				// we connect, or we die trying
				parentCtx = context.Background()
			}

			connCtx, cancel := context.WithTimeout(parentCtx, m.dialTimeout)
			b := backoff.WithContext(expbackoff, connCtx)
			var dialer net.Dialer
			backoff.Retry(func() error {
				var err error
				conn, err = dialer.DialContext(connCtx, m.network, m.address)
				return err
			}, b)
			cancel()

			if pdebug.Enabled {
				if conn == nil {
					pdebug.Printf("background writer: failed to connect to %s:%s", m.network, m.address)
				} else {
					pdebug.Printf("background writer: connected to %s:%s", m.network, m.address)
				}
			}

			if conn != nil {
				break
			}

			// The flush mode may have changed while we were trying to
			// connect, so update it.
			if m.isFlushMode() {
				flush = true
			}
			if flush {
				connAttempts++
				if connAttempts > m.maxConnAttempts {
					if pdebug.Enabled {
						pdebug.Printf("background writer: bailing out after failed to connect to %s:%s (%d attempts) under flush mode", m.network, m.address, connAttempts)
					}
					return
				}
			}
		}

		if flush {
			if pdebug.Enabled {
				pdebug.Printf("background writer: in flush mode, no deadline set")
			}
			conn.SetWriteDeadline(time.Time{})
		} else {
			conn.SetWriteDeadline(time.Now().Add(m.writeTimeout))
		}

		if err := m.flushPending(conn); err != nil {
			conn.Close()
			conn = nil
		}

		if flush {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

func (m *minion) waitPending(ctx context.Context) error {
	// We need to check for ctx.Done() here before getting into
	// the cond loop, because otherwise we might never be woken
	// up again
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	for {
		if m.pendingAvailable(m.writeThreshold) {
			break
		}

		select {
		case <-ctx.Done():
			if pdebug.Enabled {
				pdebug.Printf("background writer: cancel detected")
			}
			return nil
		default:
		}

		m.cond.Wait()
	}
	return nil
}

func (m *minion) flushPending(conn net.Conn) error {
	var writeiters int
	var wrotebytes int
	if pdebug.Enabled {
		defer func() {
			pdebug.Printf("background writer: wrote %d bytes in %d iterations", wrotebytes, writeiters)
		}()
	}
	for {
		if pdebug.Enabled {
			writeiters++
		}
		n, err := m.writePending(conn)
		if pdebug.Enabled {
			wrotebytes += n
		}

		if err != nil {
			return err
		}

		if !m.pendingAvailable(0) {
			break
		}
	}
	return nil
}

func (m *minion) isFlushMode() bool {
	m.muFlush.RLock()
	defer m.muFlush.RUnlock()
	return m.flush
}

func (m *minion) writePending(conn net.Conn) (int, error) {
	m.muPending.Lock()
	defer m.muPending.Unlock()
	if pdebug.Enabled {
		pdebug.Printf("background writer: attempting to write %d bytes", len(m.pending))
	}

	n, err := conn.Write(m.pending)
	if err != nil {
		if pdebug.Enabled {
			pdebug.Printf("background writer: error while writing: %s", err)
		}
		return 0, errors.Wrap(err, `failed to write data to conn`)
	}
	m.pending = m.pending[n:]
	return n, nil
}

func (m *minion) pendingAvailable(threshold int) bool {
	m.muPending.RLock()
	defer m.muPending.RUnlock()

	if l := len(m.pending); l > threshold {
		if pdebug.Enabled {
			pdebug.Printf("background writer: %d bytes to write", l)
		}
		return true
	}
	return false
}
