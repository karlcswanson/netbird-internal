//go:build js

package tcp

import (
	"syscall/js"

	"github.com/sirupsen/logrus"
)

// The writeQueue and readGate types this file uses live in the untagged
// flowcontrol_core.go so their logic can be unit tested on the host.

// drainLoop writes queued chunks to the connection. When the queue empties
// it invokes the JS ondrain callback, if set.
func (q *writeQueue) drainLoop(conn *Connection, jsInterface js.Value) {
	for {
		q.mu.Lock()
		for len(q.chunks) == 0 && !q.closed {
			q.mu.Unlock()
			<-q.notify
			q.mu.Lock()
		}
		if q.closed {
			q.mu.Unlock()
			return
		}
		chunk := q.chunks[0]
		q.chunks = q.chunks[1:]
		q.total -= len(chunk)
		empty := len(q.chunks) == 0
		q.mu.Unlock()

		if _, err := conn.Write(chunk); err != nil {
			logrus.Debugf("TCP queued write error: %v", err)
			q.close()
			if err := conn.Close(); err != nil {
				logrus.Debugf("TCP close error: %v", err)
			}
			return
		}

		if empty {
			if ondrain := jsInterface.Get("ondrain"); !ondrain.IsUndefined() && !ondrain.IsNull() {
				ondrain.Invoke()
			}
		}
	}
}

// attachFlowControl adds optional flow-control methods to a connection's JS
// interface: writeQueued(data) -> queuedBytes|-1, queued() -> bytes,
// pause(), resume(), and the ondrain callback property. The existing
// write/close/ondata/onclose contract is unchanged. The returned cleanup
// must be called when the connection ends so the writer goroutine exits;
// the returned funcs must then be undefined on the interface and released.
func attachFlowControl(conn *Connection, jsInterface js.Value) (*readGate, []namedFunc, func()) {
	queue := newWriteQueue()
	gate := newReadGate()

	writeQueuedFunc := js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) < 1 {
			return js.ValueOf(-1)
		}

		data := args[0]
		var bytes []byte
		if data.Type() == js.TypeString {
			bytes = []byte(data.String())
		} else {
			uint8Array := js.Global().Get("Uint8Array").New(data)
			bytes = make([]byte, uint8Array.Get("length").Int())
			js.CopyBytesToGo(bytes, uint8Array)
		}

		queue.mu.Lock()
		started := queue.started
		queue.started = true
		queue.mu.Unlock()
		if !started {
			go queue.drainLoop(conn, jsInterface)
		}

		return js.ValueOf(queue.push(bytes))
	})
	jsInterface.Set("writeQueued", writeQueuedFunc)

	queuedFunc := js.FuncOf(func(this js.Value, args []js.Value) any {
		queue.mu.Lock()
		defer queue.mu.Unlock()
		return js.ValueOf(queue.total)
	})
	jsInterface.Set("queued", queuedFunc)

	pauseFunc := js.FuncOf(func(this js.Value, args []js.Value) any {
		gate.pause()
		return js.Undefined()
	})
	jsInterface.Set("pause", pauseFunc)

	resumeFunc := js.FuncOf(func(this js.Value, args []js.Value) any {
		gate.unpause()
		return js.Undefined()
	})
	jsInterface.Set("resume", resumeFunc)

	// Release queued writes when the connection goes away.
	origClose := jsInterface.Get("close")
	closeWrapperFunc := js.FuncOf(func(this js.Value, args []js.Value) any {
		queue.close()
		gate.unpause()
		if origClose.Type() == js.TypeFunction {
			origClose.Invoke()
		}
		return js.Undefined()
	})
	jsInterface.Set("close", closeWrapperFunc)

	funcs := []namedFunc{
		{"writeQueued", writeQueuedFunc},
		{"queued", queuedFunc},
		{"pause", pauseFunc},
		{"resume", resumeFunc},
		{"close", closeWrapperFunc},
	}

	cleanup := func() {
		queue.close()
		gate.unpause()
	}
	return gate, funcs, cleanup
}
