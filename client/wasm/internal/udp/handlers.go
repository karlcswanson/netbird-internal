//go:build js

// Package udp exposes a netstack UDP socket to JavaScript.
package udp

import (
	"errors"
	"net"
	"syscall/js"

	"github.com/sirupsen/logrus"
)

const readBufferSize = 65535

// CreateJSInterface creates a JavaScript interface for a UDP socket.
// The interface provides:
//   - addr: The bound address (e.g. "100.64.0.5:53")
//   - port: The bound port
//   - send(data, remoteAddr): Send a datagram (string or Uint8Array) to
//     "ip:port". Returns false if the address is invalid or the send fails.
//   - close(): Close the socket
//   - onmessage: Callback for received datagrams (set by JS), invoked with
//     (data: Uint8Array, remoteAddr: string)
//   - onclose: Callback for socket close (set by JS)
func CreateJSInterface(conn net.PacketConn, port int) js.Value {
	jsInterface := js.Global().Get("Object").Call("create", js.Null())

	jsInterface.Set("addr", conn.LocalAddr().String())
	jsInterface.Set("port", port)

	sendFunc := js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) < 2 || args[1].Type() != js.TypeString {
			logrus.Debug("UDP send requires data and a remote address")
			return js.ValueOf(false)
		}

		remoteAddr, err := net.ResolveUDPAddr("udp", args[1].String())
		if err != nil {
			logrus.Debugf("UDP resolve %s: %v", args[1].String(), err)
			return js.ValueOf(false)
		}

		var payload []byte
		if data := args[0]; data.Type() == js.TypeString {
			payload = []byte(data.String())
		} else {
			uint8Array := js.Global().Get("Uint8Array").New(data)
			payload = make([]byte, uint8Array.Get("length").Int())
			js.CopyBytesToGo(payload, uint8Array)
		}

		if _, err := conn.WriteTo(payload, remoteAddr); err != nil {
			logrus.Debugf("UDP write to %s: %v", remoteAddr, err)
			return js.ValueOf(false)
		}
		return js.ValueOf(true)
	})
	jsInterface.Set("send", sendFunc)

	closeFunc := js.FuncOf(func(_ js.Value, _ []js.Value) any {
		if err := conn.Close(); err != nil {
			logrus.Debugf("UDP close error: %v", err)
		}
		return js.Undefined()
	})
	jsInterface.Set("close", closeFunc)

	go func() {
		readLoop(conn, jsInterface)

		// Undefining before Release turns post-close JS calls into a
		// TypeError instead of a silent "call to released function".
		jsInterface.Set("send", js.Undefined())
		jsInterface.Set("close", js.Undefined())
		sendFunc.Release()
		closeFunc.Release()
	}()

	return jsInterface
}

func readLoop(conn net.PacketConn, jsInterface js.Value) {
	defer func() {
		if onclose := jsInterface.Get("onclose"); !onclose.IsUndefined() && !onclose.IsNull() {
			onclose.Invoke()
		}
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			logrus.Debugf("UDP close on readLoop exit: %v", err)
		}
	}()

	buffer := make([]byte, readBufferSize)
	for {
		n, remoteAddr, err := conn.ReadFrom(buffer)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				logrus.Debugf("UDP read error: %v", err)
			}
			return
		}
		if n == 0 {
			continue
		}

		onmessage := jsInterface.Get("onmessage")
		if onmessage.IsUndefined() || onmessage.IsNull() {
			continue
		}

		uint8Array := js.Global().Get("Uint8Array").New(n)
		js.CopyBytesToJS(uint8Array, buffer[:n])
		onmessage.Invoke(uint8Array, remoteAddr.String())
	}
}
