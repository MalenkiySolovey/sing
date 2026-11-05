package tls

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type lazyServerConfig struct {
	ServerConfig
	handshake func(context.Context, net.Conn) (Conn, error)
}

func (c lazyServerConfig) HandshakeTimeout() time.Duration { return 0 }
func (c lazyServerConfig) ServerHandshake(ctx context.Context, conn net.Conn) (Conn, error) {
	return c.handshake(ctx, conn)
}

type lazyTestConn struct {
	net.Conn
	closes atomic.Int32
}

func (c *lazyTestConn) NetConn() net.Conn                      { return c.Conn }
func (c *lazyTestConn) HandshakeContext(context.Context) error { return nil }
func (c *lazyTestConn) ConnectionState() ConnectionState       { return ConnectionState{} }
func (c *lazyTestConn) Close() error {
	c.closes.Add(1)
	return c.Conn.Close()
}

func TestLazyConnConcurrentHandshakeStateAndClose(t *testing.T) {
	raw, peer := net.Pipe()
	t.Cleanup(func() { _ = raw.Close(); _ = peer.Close() })
	entered, release := make(chan struct{}), make(chan struct{})
	wrapped := &lazyTestConn{Conn: raw}
	lazy := NewLazyConn(raw, lazyServerConfig{handshake: func(context.Context, net.Conn) (Conn, error) {
		close(entered)
		<-release
		return wrapped, nil
	}})
	handshakeDone := make(chan error, 1)
	go func() { handshakeDone <- lazy.HandshakeContext(t.Context()) }()
	<-entered
	observing, stop, observed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(observed)
		close(observing)
		for {
			select {
			case <-stop:
				return
			default:
				_ = lazy.NeedHandshake()
				_ = lazy.ReaderReplaceable()
				_ = lazy.WriterReplaceable()
				_ = lazy.Upstream()
				_ = lazy.LocalAddr()
				_ = lazy.Close()
			}
		}
	}()
	<-observing
	close(release)
	if err := <-handshakeDone; err != nil {
		t.Fatal(err)
	}
	close(stop)
	<-observed
	if lazy.NeedHandshake() || !lazy.ReaderReplaceable() || !lazy.WriterReplaceable() || lazy.Upstream() != wrapped {
		t.Fatal("successful handshake did not publish the TLS connection")
	}
	before := wrapped.closes.Load()
	_ = lazy.Close()
	if wrapped.closes.Load() != before+1 {
		t.Fatal("established TLS connection did not retain Close ownership")
	}
}

func TestLazyConnCloseInterruptsHandshake(t *testing.T) {
	raw, peer := net.Pipe()
	t.Cleanup(func() { _ = raw.Close(); _ = peer.Close() })
	entered := make(chan struct{})
	lazy := NewLazyConn(raw, lazyServerConfig{handshake: func(_ context.Context, conn net.Conn) (Conn, error) {
		close(entered)
		_, err := conn.Read(make([]byte, 1))
		return nil, err
	}})
	handshakeDone := make(chan error, 1)
	go func() { handshakeDone <- lazy.HandshakeContext(t.Context()) }()
	<-entered
	closeDone := make(chan struct{})
	go func() { _ = lazy.Close(); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		_ = raw.Close()
		t.Fatal("Close waited for the blocked handshake")
	}
	select {
	case err := <-handshakeDone:
		if err == nil {
			t.Fatal("closed handshake unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt the handshake")
	}
}
