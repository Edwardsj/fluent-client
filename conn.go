package fluent

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/pkg/errors"
)

func dial(ctx context.Context, network, address string, timeout time.Duration, tlsConfig TLSConfig) (conn net.Conn, err error) {
	connCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var dialer net.Dialer
	dialer.Timeout = timeout
	dialer.KeepAlive = time.Second * 30

	if tlsConfig.Enable {
		conn, err = tls.DialWithDialer(&dialer, network, address, &tlsConfig.Conf)
		if err != nil {
			fmt.Println(err, `, fluent forward failed to connect to server`)
			return nil, errors.Wrap(err, `failed to connect to server`)
		}
	} else {
		conn, err = dialer.DialContext(connCtx, network, address)
		if err != nil {
			fmt.Println(err, `, fluent forward failed to connect to server`)
			return nil, errors.Wrap(err, `failed to connect to server`)
		}
	}

	return conn, nil
}
