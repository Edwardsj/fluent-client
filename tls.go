package fluent

import "crypto/tls"

type TLSConfig struct {
	Enable bool
	Conf   tls.Config
}

// is tls means if the connection is with tls
func WithTLS(conf tls.Config) Option {
	return &option{
		name:  optkeyWithTLS,
		value: conf,
	}
}
