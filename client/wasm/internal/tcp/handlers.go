//go:build js

package tcp

import (
	"io"
	"net"
	"syscall/js"

	"github.com/sirupsen/logrus"
)

// Connection wraps a net.Conn for JavaScript
type Connection struct {
	conn net.Conn
}

// NewConnection creates a new TCP connection wrapper
func NewConnection(conn net.Conn) *Connection {
	return &Connection{conn: conn}
}

// Write writes data to the connection
func (c *Connection) Write(data []byte) (int, error) {
	return c.conn.Write(data)
}

// Read reads data from the connection
func (c *Connection) Read(buffer []byte) (int, error) {
	return c.conn.Read(buffer)
}

// Close closes the connection
func (c *Connection) Close() error {
	return c.conn.Close()
}

// namedFunc pairs a js.Func with the interface property it is set on, so
// the property can be undefined before the func is released.
type namedFunc struct {
	name string
	fn   js.Func
}

// CreateJSInterface creates a JavaScript interface for the TCP connection
// The interface provides:
//   - write(data): Write data (string or Uint8Array) to the connection
//   - close(): Close the connection
//   - ondata: Callback for received data (set by JS)
//   - onclose: Callback for connection close (set by JS)
func CreateJSInterface(conn *Connection) js.Value {
	jsInterface := js.Global().Get("Object").Call("create", js.Null())

	// write(data) - write data to the connection
	writeFunc := js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) < 1 {
			return js.ValueOf(false)
		}

		data := args[0]
		var bytes []byte

		if data.Type() == js.TypeString {
			bytes = []byte(data.String())
		} else {
			// Assume it's an ArrayBuffer or Uint8Array
			uint8Array := js.Global().Get("Uint8Array").New(data)
			length := uint8Array.Get("length").Int()
			bytes = make([]byte, length)
			js.CopyBytesToGo(bytes, uint8Array)
		}

		_, err := conn.Write(bytes)
		return js.ValueOf(err == nil)
	})
	jsInterface.Set("write", writeFunc)

	// close() - close the connection
	closeFunc := js.FuncOf(func(this js.Value, args []js.Value) any {
		if err := conn.Close(); err != nil {
			logrus.Debugf("TCP close error: %v", err)
		}
		return js.Undefined()
	})
	jsInterface.Set("close", closeFunc)

	// Optional flow control: writeQueued/queued/pause/resume/ondrain.
	// The write/close/ondata/onclose contract above is unchanged.
	gate, flowFuncs, cleanup := attachFlowControl(conn, jsInterface)

	funcs := append([]namedFunc{{"write", writeFunc}, {"close", closeFunc}}, flowFuncs...)

	go func() {
		readLoop(conn, jsInterface, gate, cleanup)

		// Undefining before Release turns post-close JS calls into a
		// TypeError instead of a silent "call to released function".
		for _, nf := range funcs {
			jsInterface.Set(nf.name, js.Undefined())
			nf.fn.Release()
		}
	}()

	return jsInterface
}

func readLoop(conn *Connection, jsInterface js.Value, gate *readGate, cleanup func()) {
	defer cleanup()
	buffer := make([]byte, 32768) // 32KB buffer for VNC/RDP data
	for {
		gate.wait()
		n, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				logrus.Debugf("TCP read error: %v", err)
			}
			// Call onclose callback
			if onclose := jsInterface.Get("onclose"); !onclose.IsUndefined() && !onclose.IsNull() {
				onclose.Invoke()
			}
			conn.Close()
			return
		}

		if n > 0 {
			// Call ondata callback with received data
			if ondata := jsInterface.Get("ondata"); !ondata.IsUndefined() && !ondata.IsNull() {
				uint8Array := js.Global().Get("Uint8Array").New(n)
				js.CopyBytesToJS(uint8Array, buffer[:n])
				ondata.Invoke(uint8Array)
			}
		}
	}
}
