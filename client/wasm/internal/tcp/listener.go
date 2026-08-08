//go:build js

package tcp

import (
	"errors"
	"net"
	"syscall/js"

	"github.com/sirupsen/logrus"
)

// CreateListenerJSInterface creates a JavaScript interface for a TCP listener.
// The interface provides:
//   - addr: The listen address (e.g. "100.64.0.5:8080")
//   - port: The listen port
//   - onconnection: Callback for accepted connections (set by JS), invoked
//     with (connection, remoteAddr). The callback must attach the
//     connection's ondata handler synchronously, before returning, or early
//     data may be delivered with no handler attached.
//   - close(): Stop listening. Already-accepted connections stay open.
func CreateListenerJSInterface(listener net.Listener, port int) js.Value {
	jsInterface := js.Global().Get("Object").Call("create", js.Null())

	jsInterface.Set("addr", listener.Addr().String())
	jsInterface.Set("port", port)

	closeFunc := js.FuncOf(func(this js.Value, args []js.Value) any {
		if err := listener.Close(); err != nil {
			logrus.Debugf("TCP listener close error: %v", err)
		}
		return js.Undefined()
	})
	jsInterface.Set("close", closeFunc)

	go func() {
		acceptLoop(listener, jsInterface)

		// Undefining before Release turns post-close JS calls into a
		// TypeError instead of a silent "call to released function".
		jsInterface.Set("close", js.Undefined())
		closeFunc.Release()
	}()

	return jsInterface
}

func acceptLoop(listener net.Listener, jsInterface js.Value) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				logrus.Debugf("TCP accept error: %v", err)
			}
			if onclose := jsInterface.Get("onclose"); !onclose.IsUndefined() && !onclose.IsNull() {
				onclose.Invoke()
			}
			return
		}

		onconnection := jsInterface.Get("onconnection")
		if onconnection.IsUndefined() || onconnection.IsNull() {
			logrus.Warnf("TCP connection from %s dropped: no onconnection handler", conn.RemoteAddr())
			if err := conn.Close(); err != nil {
				logrus.Debugf("TCP close error: %v", err)
			}
			continue
		}

		onconnection.Invoke(CreateJSInterface(NewConnection(conn)), conn.RemoteAddr().String())
	}
}
