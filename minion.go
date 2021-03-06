package fluent

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	backoff "github.com/lestrrat-go/backoff"
	pdebug "github.com/lestrrat-go/pdebug"
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
	address            string
	backoffPolicy      backoff.Policy
	buffer             []byte
	bufferLimit        int
	cond               *sync.Cond
	dialTimeout        time.Duration
	done               chan struct{}
	incoming           chan *Message
	marshaler          marshaler
	maxConnAttempts    uint64
	maxHttpPackageSize int
	httpPackageGzip    bool
	httpRetries        int
	muPending          sync.RWMutex
	network            string
	method             string
	pending            []byte
	pingCh             chan *Message
	httpCh             chan *Message
	readerDone         chan struct{}
	tagPrefix          string
	writeThreshold     int
	writeTimeout       time.Duration
	tlsConf            TLSConfig
}

func newMinion(options ...Option) (*minion, error) {
	m := &minion{
		address:            "127.0.0.1:24224",
		backoffPolicy:      backoff.NewExponential(),
		bufferLimit:        8 * 1024 * 1024,
		cond:               sync.NewCond(&sync.Mutex{}),
		dialTimeout:        3 * time.Second,
		done:               make(chan struct{}),
		maxConnAttempts:    64,
		marshaler:          marshalFunc(msgpackMarshal),
		network:            "tcp",
		method:             "forward",
		pingCh:             make(chan *Message),
		readerDone:         make(chan struct{}),
		writeThreshold:     8 * 1028,
		writeTimeout:       3 * time.Second,
		tlsConf:            TLSConfig{Enable: false},
		maxHttpPackageSize: 10,
		httpPackageGzip:    false,
		httpRetries:        5,
	}

	var writeQueueSize = 6
	var connectOnStart bool
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
		case optkeyTagPrefix:
			m.tagPrefix = opt.Value().(string)
		case optkeyWriteQueueSize:
			writeQueueSize = opt.Value().(int)
		case optkeyWriteThreshold:
			m.writeThreshold = opt.Value().(int)
		case optkeyConnectOnStart:
			connectOnStart = opt.Value().(bool)
		case optkeyWithTLS:
			m.tlsConf = TLSConfig{Enable: true, Conf: opt.Value().(tls.Config)}
		case optkeyMethod:
			m.method = opt.Value().(string)
		case optkeyMaxHttpPackageSize:
			m.maxHttpPackageSize = opt.Value().(int)
		case optkeyHttpPackageGzip:
			m.httpPackageGzip = opt.Value().(bool)
		case optkeyHttpRetries:
			m.httpRetries = opt.Value().(int)
		}
	}

	// if requested, connect to the server
	if connectOnStart {
		conn, err := dial(context.Background(), m.network, m.address, m.dialTimeout, m.tlsConf)
		if err != nil {
			return nil, errors.Wrap(err, `failed to connect on start`)
		}
		defer conn.Close()
	}

	//if method is http marshaler must by raw json
	if m.method == "http" {
		m.marshaler = marshalFunc(rawJsonMarshal)
		//TODO we need use go-disruptor instead
		m.httpCh = make(chan *Message, m.bufferLimit)
		if pdebug.Enabled {
			pdebug.Printf("m.httpCh cap %d", cap(m.httpCh))
		}
	} else {
		m.buffer = make([]byte, 0, m.bufferLimit)
		m.pending = m.buffer
		if pdebug.Enabled {
			pdebug.Printf("m.pending cap %d", cap(m.pending))
			pdebug.Printf("m.pending len %d", len(m.pending))
		}
		m.incoming = make(chan *Message, writeQueueSize)
	}

	return m, nil
}

// This is the reader loop. The only thing we're responsible for
// is to accept incoming messages from the client as soon as possible
func (m *minion) runReader(ctx context.Context) {
	if pdebug.Enabled {
		pdebug.Printf("background reader: starting")
		defer pdebug.Printf("background reader: exiting")
	}

	defer close(m.readerDone)
	// Wake up the writer goroutine so that it can detect
	// cancelation.
	defer m.cond.Broadcast()

	// This goroutine receives the incoming data as fast as
	// possible, so that the caller to enqueue does not block
	for loop := true; loop; {
		select {
		case <-ctx.Done():
			if pdebug.Enabled {
				pdebug.Printf("background reader: cancel detected")
			}
			loop = false
		case msg, ok := <-m.incoming:
			// m.incoming could have been closed already, so we should
			// check if msg is legit
			if msg != nil {
				m.appendMessage(msg)
			}
			if !ok {
				loop = false
			}
		case msg, ok := <-m.pingCh:
			if msg != nil {
				m.ping(msg)
			}
			if !ok {
				loop = false
			}
		}
	}

	// if we have more messages in the channel, we should try to flush them
	for len(m.pingCh) > 0 {
		if pdebug.Enabled {
			pdebug.Printf("background reader: flushing incoming pings (%d left)", len(m.pingCh))
		}
		m.ping(<-m.pingCh)
	}

	for len(m.incoming) > 0 {
		if pdebug.Enabled {
			pdebug.Printf("background reader: flushing incoming buffer (%d left)", len(m.incoming))
		}
		m.appendMessage(<-m.incoming)
	}

}

// ping is a one-shot deal. we connect, we send, we bail out.
// if anything fails, oh well...
func (m *minion) ping(msg *Message) (err error) {
	if pdebug.Enabled {
		g := pdebug.Marker("minion.ping").BindError(&err)
		defer g.End()
	}
	defer releaseMessage(msg)
	defer func() {
		if err == nil {
			return
		}

		if pdebug.Enabled {
			pdebug.Printf("Replying back with an error message (%s)", err)
		}
		msg.replyCh <- err
	}()

	// Ping messages MUST have a return channel
	if msg.replyCh == nil {
		return nil
	}

	if pdebug.Enabled {
		pdebug.Printf("Connecting to server for ping...")
	}
	conn, err := dial(context.Background(), m.network, m.address, m.dialTimeout, m.tlsConf)
	if err != nil {
		return errors.Wrap(err, `failed to connect server for ping`)
	}

	if pdebug.Enabled {
		pdebug.Printf("Serializing ping message...")
	}
	buf, err := m.serialize(msg)
	if err != nil {
		return errors.Wrap(err, `failed to serialize ping message`)
	}

	if pdebug.Enabled {
		pdebug.Printf("Writing ping message...")
	}
	for len(buf) > 0 {
		n, err := conn.Write(buf)
		if err != nil {
			return errors.Wrap(err, `failed to write ping message to connection`)
		}
		buf = buf[n:]
	}

	// releaseMessage automatically closes msg.replyCh
	return nil
}

// all messages in one http post have the same tag.
// if got error in the post action, we will sent the message back to http chan.
func (m *minion) http_post(msg *Message) (err error) {
	if pdebug.Enabled {
		g := pdebug.Marker("minion.http_post").BindError(&err)
		defer g.End()
	}
	defer func() {
		if err == nil {
			releaseMessage(msg)
			return
		}

		if msg.retries > m.httpRetries {
			if pdebug.Enabled {
				pdebug.Printf("message (%v) retry too many times , drop it.", msg.Record)
			}
			releaseMessage(msg)
			return
		}

		if pdebug.Enabled {
			pdebug.Printf("Sending back the error message (%v)", msg.Record)
		}

		msg.retries++
		select {
		case m.httpCh <- msg:
		default:
			releaseMessage(msg)
			if pdebug.Enabled {
				pdebug.Printf("http queue is full, drop msg")
			}
			return
		}
	}()

	defer func() {
		if err == nil {
			return
		}

		if pdebug.Enabled {
			pdebug.Printf("Replying back with an error message (%s)", err)
		}
		// make sure we won't block by replyCh
		if msg.replyCh != nil {
			select {
			case msg.replyCh <- err:
			default:
				return
			}
		}

		if pdebug.Enabled {
			pdebug.Printf("End Reply back with an error message (%s)", err)
		}
	}()

	//combine msgs
	m.combineMsg(msg)

	if pdebug.Enabled {
		pdebug.Printf("Serializing http message...")
	}
	buf, err := m.serialize(msg)
	if err != nil {
		return errors.Wrap(err, `failed to serialize http message`)
	}

	if pdebug.Enabled {
		pdebug.Printf("Writing http message...")
	}

	address := bytes.NewBufferString(m.address)
	_, err = address.WriteString("/")
	if err != nil {
		return errors.Wrap(err, `failed to make http address`)
	}
	_, err = address.WriteString(msg.Tag)
	if err != nil {
		return errors.Wrap(err, `failed to make http address`)
	}

	if pdebug.Enabled {
		pdebug.Printf("Posting http message...")
	}

	client := http.Client{
		Timeout: 5 * time.Second,
	}

	if m.httpPackageGzip {
		gzipBuf := new(bytes.Buffer)
		gzipWriter := gzip.NewWriter(gzipBuf)
		_, err := gzipWriter.Write(buf)
		if err != nil {
			return errors.Wrap(err, `failed to gzip post http body`)
		}
		gzipWriter.Flush()
		gzipWriter.Close()

		req, err := http.NewRequest("POST", address.String(), gzipBuf)
		if err != nil {
			return errors.Wrap(err, `failed to new http gzip request`)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")

		resp, err := client.Do(req)
		if err != nil {
			return errors.Wrap(err, `failed to post http gzip request`)
		}
		if resp.StatusCode != 200 {
			return errors.New("return code is not 200")
		}

	} else {
		resp, err := client.Post(address.String(), "application/json", bytes.NewReader(buf))
		if err != nil {
			return errors.Wrap(err, `failed to post http request`)
		}
		if resp.StatusCode != 200 {
			return errors.New("return code is not 200")
		}
	}

	// releaseMessage automatically closes msg.replyCh
	return nil
}

func (m *minion) serialize(msg *Message) ([]byte, error) {
	if p := m.tagPrefix; len(p) > 0 {
		msg.Tag = p + "." + msg.Tag
	}

	return m.marshaler.Marshal(msg)
}

// appends a message to the pending buffer
func (m *minion) appendMessage(msg *Message) {
	defer releaseMessage(msg)

	if pdebug.Enabled {
		if msg.replyCh != nil {
			pdebug.Printf("background reader: message expects reply")
		}
	}

	buf, err := m.serialize(msg)
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
			if pdebug.Enabled {
				pdebug.Printf("background reader: replying error to client")
			}
			msg.replyCh <- &bufferFullErrInstance
		}
		return
	}

	if pdebug.Enabled {
		pdebug.Printf("background reader: received %d more bytes, appending", len(buf))
	}
	m.pending = append(m.pending, buf...)
}

func (m *minion) isReaderDone() bool {
	select {
	case <-m.readerDone:
		return true
	default:
	}
	return false
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
	defer func(conn net.Conn) {
		// Make sure that this connection is closed.
		if conn != nil {
			if pdebug.Enabled {
				pdebug.Printf("background writer: closing connection (in cleanup)")
			}
			conn.Close()
		}
	}(conn)

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
		for conn == nil {
			if pdebug.Enabled {
				if m.isReaderDone() {
					pdebug.Printf("background writer: attempting to connect in flush mode")
				} else {
					pdebug.Printf("background writer: attempting to connect")
				}
			}

			parentCtx := ctx
			if m.isReaderDone() {
				// In flush mode, we don't let a parent context to cancel us.
				// we connect, or we die trying
				parentCtx = context.Background()
			}

			conn = m.connect(parentCtx)
			if pdebug.Enabled {
				if conn == nil {
					pdebug.Printf("background writer: failed to connect to %s:%s", m.network, m.address)
				} else {
					pdebug.Printf("background writer: connected to %s:%s", m.network, m.address)
				}
			}

			if conn != nil {
				go func() {
					defer func() {
						if err := recover(); err != nil {
							pdebug.Dump(err)
						}
					}()
					one := make([]byte, 1)
					if pdebug.Enabled {
						pdebug.Printf("connection monitor start: connected to %s:%s", m.network, m.address)
					}
					for {
						if _, err := conn.Read(one); err == io.EOF {
							if pdebug.Enabled {
								pdebug.Printf("connection closed: error %s connected to %s:%s", err.Error(), m.network, m.address)
							}
							conn.SetDeadline(time.Now().Add(-time.Second))
							conn.Close()
							return
						}
						select {
						case <-ctx.Done():
							return
						default:
						}
					}
				}()
				break
			}

			if m.isReaderDone() {
				connAttempts++
				if m.maxConnAttempts > 0 && connAttempts > m.maxConnAttempts {
					if pdebug.Enabled {
						pdebug.Printf("background writer: bailing out after failed to connect to %s:%s (%d attempts) under flush mode", m.network, m.address, connAttempts)
					}
					return
				}
			}
		}

		if m.isReaderDone() {
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

		if m.isReaderDone() {
			if !m.pendingAvailable(0) {
				if pdebug.Enabled {
					pdebug.Printf("background writer: pending buffer is empty, bailing out")
				}
				return
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

func (m *minion) writePending(conn net.Conn) (int, error) {
	m.muPending.Lock()
	defer m.muPending.Unlock()
	if pdebug.Enabled {
		pdebug.Printf("background writer: attempting to write %d bytes", len(m.pending))
	}

	if conn == nil {
		return 0, errors.New(`conn is nil failed to write data to conn`)
	}

	n, err := conn.Write(m.pending)
	if err != nil {
		if pdebug.Enabled {
			pdebug.Printf("background writer: error while writing: %s", err)
		}
		return 0, errors.Wrap(err, `failed to write data to conn`)
	}
	m.pending = m.pending[n:]
	if len(m.pending) == 0 {
		m.pending = m.buffer[0:0]
	}

	if pdebug.Enabled {
		pdebug.Printf("m.pending cap %d", cap(m.pending))
		pdebug.Printf("m.pending len %d", len(m.pending))
	}
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

func (m *minion) connect(ctx context.Context) net.Conn {
	retryCtx, cancel := context.WithTimeout(ctx, m.dialTimeout)
	defer cancel()

	b, backoffCancel := m.backoffPolicy.Start(retryCtx)
	defer backoffCancel()

	for {
		conn, err := dial(ctx, m.network, m.address, m.dialTimeout, m.tlsConf)
		if err == nil {
			if pdebug.Enabled {
				pdebug.Printf("connected to server!")
			}
			return conn
		}

		if pdebug.Enabled {
			pdebug.Printf("failed to connect to server, backing off...")
		}
		select {
		case <-b.Done():
			return nil
		case <-b.Next():
		}

		if pdebug.Enabled {
			pdebug.Printf("connected to server!")
		}
		return conn
	}
	return nil
}

func (m *minion) runHTTPWriter(ctx context.Context) {
	if pdebug.Enabled {
		defer pdebug.Printf("background http writer: exiting")
	}

	for {
		msg := <-m.httpCh
		if msg.Len >= m.maxHttpPackageSize {
			m.http_post(msg)
			continue
		}

		//we should make sure all messages in same bundle have the same tag
		msgBundles := make(map[string]*Message)
		msgBundles[msg.Tag] = msg

		//try to flush all the msg in httpCH
		for len(m.httpCh) > 0 {
			if pdebug.Enabled {
				pdebug.Printf("background reader: flushing incoming httpCh (%d left)", len(m.httpCh))
			}

			select {
			case nextMsg := <-m.httpCh:
				tag := nextMsg.Tag
				if _, ok := msgBundles[tag]; !ok {
					msgBundles[tag] = nextMsg
				} else {
					if (msgBundles[tag].Len + nextMsg.Len) >= m.maxHttpPackageSize {
						m.http_post(nextMsg)
						continue
					}

					//head msg'len is sum of all the line msgs
					msgBundles[tag].Len += nextMsg.Len
					msgBundles[tag].End.Next = nextMsg
					msgBundles[tag].End = nextMsg.End
				}
			default:
			}
		}

		for _, s := range msgBundles {
			m.http_post(s)
		}
	}
}

func (m *minion) combineMsg(msg *Message) {
	if msg.Next == nil {
		return
	}

	var records []interface{}

	if _, ok := (msg.Record).([]interface{}); msg.combined && ok {
		records = (msg.Record).([]interface{})
	} else {
		records = make([]interface{}, 0)
		records = append(records, msg.Record)
	}

	nextMsg := msg.Next
	for nextMsg != nil {
		if _, ok := (nextMsg.Record).([]interface{}); nextMsg.combined && ok {
			records = append(records, (nextMsg.Record).([]interface{})...)
		} else {
			records = append(records, nextMsg.Record)
		}
		nextMsg = nextMsg.Next
	}

	msg.Record = records
	msg.combined = true
	releaseMessage(msg.Next)
	msg.Next = nil
	msg.Len = len(records)

	if pdebug.Enabled {
		pdebug.Printf("Combining http messages...  len is %d", len(records))
	}
	return
}
