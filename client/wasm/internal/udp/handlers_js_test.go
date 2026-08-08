//go:build js

package udp

import (
	"net"
	"sync"
	"syscall/js"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These tests drive the UDP JS interface (CreateJSInterface) under
// GOOS=js/GOARCH=wasm through an in-memory net.PacketConn, covering the
// send/onmessage wiring and the js.Func release on teardown.
//
// Run with: PATH="$(go env GOROOT)/lib/wasm:$PATH" GOOS=js GOARCH=wasm go test ./...

type datagram struct {
	data []byte
	addr string
}

// fakePacketConn is an in-memory net.PacketConn. deliver() simulates an
// inbound datagram; WriteTo captures outbound datagrams on the sent channel.
type fakePacketConn struct {
	readCh    chan datagram
	sent      chan datagram
	done      chan struct{}
	closeOnce sync.Once
}

func newFakePacketConn() *fakePacketConn {
	return &fakePacketConn{
		readCh: make(chan datagram, 8),
		sent:   make(chan datagram, 16),
		done:   make(chan struct{}),
	}
}

func (c *fakePacketConn) deliver(d datagram) { c.readCh <- d }

func (c *fakePacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	select {
	case d := <-c.readCh:
		return copy(b, d.data), udpAddr(d.addr), nil
	case <-c.done:
		return 0, nil, net.ErrClosed
	}
}

func (c *fakePacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	select {
	case <-c.done:
		return 0, net.ErrClosed
	default:
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	c.sent <- datagram{data: cp, addr: addr.String()}
	return len(b), nil
}

func (c *fakePacketConn) Close() error {
	c.closeOnce.Do(func() { close(c.done) })
	return nil
}

func (c *fakePacketConn) LocalAddr() net.Addr              { return udpAddr("100.64.0.5:53") }
func (c *fakePacketConn) SetDeadline(time.Time) error      { return nil }
func (c *fakePacketConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakePacketConn) SetWriteDeadline(time.Time) error { return nil }

type udpAddr string

func (a udpAddr) Network() string { return "udp" }
func (a udpAddr) String() string  { return string(a) }

const settle = 2 * time.Second

func TestUDPSocket_SendForwardsDatagram(t *testing.T) {
	conn := newFakePacketConn()
	defer conn.Close()
	iface := CreateJSInterface(conn, 53)

	require.True(t, iface.Call("send", "payload", "10.0.0.1:53").Bool(), "send() returned false")

	select {
	case d := <-conn.sent:
		require.Equal(t, "payload", string(d.data))
		require.Equal(t, "10.0.0.1:53", d.addr)
	case <-time.After(settle):
		t.Fatal("datagram was not forwarded to the socket")
	}
}

func TestUDPSocket_SendRejectsBadRemoteAddr(t *testing.T) {
	conn := newFakePacketConn()
	defer conn.Close()
	iface := CreateJSInterface(conn, 53)

	// Not an ip:port — ResolveUDPAddr fails, so send returns false.
	require.False(t, iface.Call("send", "payload", "not-an-address").Bool(),
		"send() to an invalid address should return false")
}

func TestUDPSocket_OnMessageDeliversIncoming(t *testing.T) {
	conn := newFakePacketConn()
	defer conn.Close()
	iface := CreateJSInterface(conn, 53)

	type msg struct {
		data []byte
		from string
	}
	ch := make(chan msg, 1)
	cb := js.FuncOf(func(_ js.Value, args []js.Value) any {
		b := make([]byte, args[0].Get("length").Int())
		js.CopyBytesToGo(b, args[0])
		ch <- msg{data: b, from: args[1].String()}
		return nil
	})
	defer cb.Release()
	iface.Set("onmessage", cb)

	conn.deliver(datagram{data: []byte("reply"), addr: "10.0.0.2:5000"})
	select {
	case m := <-ch:
		require.Equal(t, "reply", string(m.data))
		require.Equal(t, "10.0.0.2:5000", m.from)
	case <-time.After(settle):
		t.Fatal("onmessage callback was not invoked")
	}
}

func TestUDPSocket_CloseReleasesFuncs(t *testing.T) {
	conn := newFakePacketConn()
	iface := CreateJSInterface(conn, 53)

	iface.Call("close") // closes the socket; read loop exits and releases funcs

	for _, name := range []string{"send", "close"} {
		name := name
		require.Eventually(t, func() bool {
			return iface.Get(name).IsUndefined()
		}, settle, 5*time.Millisecond, "%s() was not released after close", name)
	}
}
