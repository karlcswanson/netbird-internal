//go:build js

package tcp

import (
	"io"
	"net"
	"sync"
	"syscall/js"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These tests exercise the JS-facing interfaces (CreateJSInterface,
// CreateListenerJSInterface, and the flow-control bindings) under
// GOOS=js/GOARCH=wasm, driving them through in-memory fakes so the callback
// wiring — and the js.Func release on teardown — is covered by automated
// tests rather than only the manual browser page.
//
// Run with: PATH="$(go env GOROOT)/lib/wasm:$PATH" GOOS=js GOARCH=wasm go test ./...

// fakeConn is an in-memory net.Conn. deliver() simulates bytes arriving from
// the remote; writes are captured on the written channel.
type fakeConn struct {
	readCh    chan []byte
	written   chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

func newFakeConn() *fakeConn {
	return &fakeConn{
		readCh:  make(chan []byte, 8),
		written: make(chan []byte, 16),
		done:    make(chan struct{}),
	}
}

func (c *fakeConn) deliver(b []byte) { c.readCh <- b }

func (c *fakeConn) Read(b []byte) (int, error) {
	select {
	case data := <-c.readCh:
		return copy(b, data), nil
	case <-c.done:
		return 0, io.EOF
	}
}

func (c *fakeConn) Write(b []byte) (int, error) {
	select {
	case <-c.done:
		return 0, net.ErrClosed
	default:
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	c.written <- cp
	return len(b), nil
}

func (c *fakeConn) Close() error {
	c.closeOnce.Do(func() { close(c.done) })
	return nil
}

func (c *fakeConn) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *fakeConn) LocalAddr() net.Addr              { return fakeAddr("local") }
func (c *fakeConn) RemoteAddr() net.Addr             { return fakeAddr("remote") }
func (c *fakeConn) SetDeadline(time.Time) error      { return nil }
func (c *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(time.Time) error { return nil }

type fakeAddr string

func (a fakeAddr) Network() string { return "fake" }
func (a fakeAddr) String() string  { return string(a) }

// fakeListener hands connections to Accept from its accept channel and makes
// Accept return net.ErrClosed once closed.
type fakeListener struct {
	accept    chan net.Conn
	done      chan struct{}
	closeOnce sync.Once
}

func newFakeListener() *fakeListener {
	return &fakeListener{accept: make(chan net.Conn, 4), done: make(chan struct{})}
}

func (l *fakeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.accept:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *fakeListener) Close() error {
	l.closeOnce.Do(func() { close(l.done) })
	return nil
}

func (l *fakeListener) Addr() net.Addr { return fakeAddr("listener") }

// settle bounds how long an assertion waits for a callback or teardown.
const settle = 2 * time.Second

func jsBytes(arr js.Value) []byte {
	b := make([]byte, arr.Get("length").Int())
	js.CopyBytesToGo(b, arr)
	return b
}

// requireReleased asserts every named js.Func property becomes undefined once
// the read/accept loop tears the interface down (the js.Func leak fix).
func requireReleased(t *testing.T, iface js.Value, names ...string) {
	t.Helper()
	for _, name := range names {
		name := name
		require.Eventually(t, func() bool {
			return iface.Get(name).IsUndefined()
		}, settle, 5*time.Millisecond, "%s() was not released after teardown", name)
	}
}

func TestConnection_WriteForwardsToConn(t *testing.T) {
	conn := newFakeConn()
	defer conn.Close()
	iface := CreateJSInterface(NewConnection(conn))

	require.True(t, iface.Call("write", "hello").Bool(), "write() returned false")

	select {
	case got := <-conn.written:
		require.Equal(t, "hello", string(got))
	case <-time.After(settle):
		t.Fatal("write was not forwarded to the connection")
	}
}

func TestConnection_OnDataDeliversIncoming(t *testing.T) {
	conn := newFakeConn()
	defer conn.Close()
	iface := CreateJSInterface(NewConnection(conn))

	got := make(chan []byte, 1)
	cb := js.FuncOf(func(_ js.Value, args []js.Value) any {
		got <- jsBytes(args[0])
		return nil
	})
	defer cb.Release()
	iface.Set("ondata", cb)

	conn.deliver([]byte("ping"))
	select {
	case b := <-got:
		require.Equal(t, "ping", string(b))
	case <-time.After(settle):
		t.Fatal("ondata callback was not invoked")
	}
}

func TestConnection_CloseFiresOncloseAndReleasesFuncs(t *testing.T) {
	conn := newFakeConn()
	iface := CreateJSInterface(NewConnection(conn))

	closed := make(chan struct{}, 1)
	cb := js.FuncOf(func(_ js.Value, _ []js.Value) any {
		closed <- struct{}{}
		return nil
	})
	defer cb.Release()
	iface.Set("onclose", cb)

	conn.Close() // read loop hits EOF

	select {
	case <-closed:
	case <-time.After(settle):
		t.Fatal("onclose callback was not invoked")
	}
	requireReleased(t, iface, "write", "close", "writeQueued", "queued", "pause", "resume")
}

func TestListener_OnConnectionFires(t *testing.T) {
	l := newFakeListener()
	defer l.Close()
	iface := CreateListenerJSInterface(l, 8080)

	type accepted struct {
		conn js.Value
		addr string
	}
	ch := make(chan accepted, 1)
	cb := js.FuncOf(func(_ js.Value, args []js.Value) any {
		ch <- accepted{conn: args[0], addr: args[1].String()}
		return nil
	})
	defer cb.Release()
	iface.Set("onconnection", cb)

	l.accept <- newFakeConn()
	select {
	case a := <-ch:
		require.Equal(t, js.TypeObject, a.conn.Type(), "onconnection should pass a connection object")
		require.Equal(t, "remote", a.addr, "remoteAddr")
		require.Equal(t, js.TypeFunction, a.conn.Get("write").Type(), "accepted connection is missing write()")
	case <-time.After(settle):
		t.Fatal("onconnection callback was not invoked")
	}
}

func TestListener_CloseFiresOncloseAndReleasesFunc(t *testing.T) {
	l := newFakeListener()
	iface := CreateListenerJSInterface(l, 8080)

	closed := make(chan struct{}, 1)
	cb := js.FuncOf(func(_ js.Value, _ []js.Value) any {
		closed <- struct{}{}
		return nil
	})
	defer cb.Release()
	iface.Set("onclose", cb)

	l.Close() // accept loop sees ErrClosed

	select {
	case <-closed:
	case <-time.After(settle):
		t.Fatal("onclose callback was not invoked")
	}
	requireReleased(t, iface, "close")
}

func TestListener_DropsConnectionWithoutHandler(t *testing.T) {
	l := newFakeListener()
	defer l.Close()
	CreateListenerJSInterface(l, 8080)

	orphan := newFakeConn()
	l.accept <- orphan // no onconnection handler set

	require.Eventually(t, orphan.isClosed, settle, 5*time.Millisecond,
		"a connection accepted with no onconnection handler should be closed")
}

func TestFlowControl_WriteQueuedDrainsAndFiresOnDrain(t *testing.T) {
	conn := newFakeConn()
	defer conn.Close()
	iface := CreateJSInterface(NewConnection(conn))

	drained := make(chan struct{}, 1)
	cb := js.FuncOf(func(_ js.Value, _ []js.Value) any {
		drained <- struct{}{}
		return nil
	})
	defer cb.Release()
	iface.Set("ondrain", cb)

	require.GreaterOrEqual(t, iface.Call("writeQueued", "abc").Int(), 0, "writeQueued rejected the write")

	select {
	case got := <-conn.written:
		require.Equal(t, "abc", string(got))
	case <-time.After(settle):
		t.Fatal("queued write was not drained to the connection")
	}

	select {
	case <-drained:
	case <-time.After(settle):
		t.Fatal("ondrain was not fired when the queue emptied")
	}
}
