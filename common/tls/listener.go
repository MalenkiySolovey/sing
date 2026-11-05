package tls

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Listener struct {
	net.Listener
	config ServerConfig
}

func NewListener(inner net.Listener, config ServerConfig) net.Listener {
	return &Listener{
		Listener: inner,
		config:   config,
	}
}

func (l *Listener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return NewLazyConn(conn, l.config), nil
}

type LazyConn struct {
	net.Conn
	tlsConfig ServerConfig
	access    sync.Mutex
	tlsConn   atomic.Pointer[Conn]
}

func NewLazyConn(conn net.Conn, config ServerConfig) *LazyConn {
	return &LazyConn{
		Conn:      conn,
		tlsConfig: config,
	}
}

func (c *LazyConn) HandshakeContext(ctx context.Context) error {
	if c.tlsConn.Load() != nil {
		return nil
	}
	c.access.Lock()
	defer c.access.Unlock()
	if c.tlsConn.Load() == nil {
		tlsConn, err := ServerHandshake(ctx, c.Conn, c.tlsConfig)
		if err != nil {
			return err
		}
		// Keep the raw connection stable: promoted net.Conn methods and Close
		// may run while the handshake is in flight. Publish only its result.
		c.tlsConn.Store(&tlsConn)
	}
	return nil
}

func (c *LazyConn) Read(p []byte) (n int, err error) {
	err = c.HandshakeContext(context.Background())
	if err != nil {
		return
	}
	return c.upstream().Read(p)
}

func (c *LazyConn) Write(p []byte) (n int, err error) {
	err = c.HandshakeContext(context.Background())
	if err != nil {
		return
	}
	return c.upstream().Write(p)
}

func (c *LazyConn) Close() error {
	// Never wait for the handshake mutex: closing the raw connection must
	// interrupt a blocked handshake. Established TLS still owns normal Close.
	return c.upstream().Close()
}

func (c *LazyConn) LocalAddr() net.Addr                { return c.upstream().LocalAddr() }
func (c *LazyConn) RemoteAddr() net.Addr               { return c.upstream().RemoteAddr() }
func (c *LazyConn) SetDeadline(t time.Time) error      { return c.upstream().SetDeadline(t) }
func (c *LazyConn) SetReadDeadline(t time.Time) error  { return c.upstream().SetReadDeadline(t) }
func (c *LazyConn) SetWriteDeadline(t time.Time) error { return c.upstream().SetWriteDeadline(t) }

func (c *LazyConn) NeedHandshake() bool {
	return c.tlsConn.Load() == nil
}

func (c *LazyConn) ReaderReplaceable() bool {
	return c.tlsConn.Load() != nil
}

func (c *LazyConn) WriterReplaceable() bool {
	return c.tlsConn.Load() != nil
}

func (c *LazyConn) Upstream() any {
	return c.upstream()
}

func (c *LazyConn) upstream() net.Conn {
	if tlsConn := c.tlsConn.Load(); tlsConn != nil {
		return *tlsConn
	}
	return c.Conn
}
