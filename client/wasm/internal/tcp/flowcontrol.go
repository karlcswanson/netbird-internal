//go:build js

package tcp

import (
	"sync"
	"syscall/js"

	"github.com/sirupsen/logrus"
)

const maxWriteQueue = 8 << 20 // 8 MiB per connection

// writeQueue decouples JS write calls from netstack writes so a full TCP
// send buffer never blocks a JS callback (which would stall the event loop
// the relay transport depends on). A single writer goroutine drains it.
type writeQueue struct {
	mu      sync.Mutex
	chunks  [][]byte
	total   int
	closed  bool
	started bool
	notify  chan struct{}
}

func newWriteQueue() *writeQueue {
	return &writeQueue{notify: make(chan struct{}, 1)}
}

func (q *writeQueue) wake() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// push enqueues data. Returns the queued byte count, or -1 if the
// connection is closed or the queue limit is exceeded.
func (q *writeQueue) push(data []byte) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.total+len(data) > maxWriteQueue {
		return -1
	}
	q.chunks = append(q.chunks, data)
	q.total += len(data)
	q.wake()
	return q.total
}

func (q *writeQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.chunks = nil
	q.total = 0
	q.mu.Unlock()
	q.wake()
}

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

// readGate lets JS pause inbound delivery so netstack's TCP receive window
// applies backpressure to the remote sender.
type readGate struct {
	mu     sync.Mutex
	paused bool
	resume chan struct{}
}

func newReadGate() *readGate {
	return &readGate{resume: make(chan struct{})}
}

func (g *readGate) pause() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.paused {
		g.paused = true
		g.resume = make(chan struct{})
	}
}

func (g *readGate) unpause() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.paused {
		g.paused = false
		close(g.resume)
	}
}

// wait blocks while paused.
func (g *readGate) wait() {
	g.mu.Lock()
	paused, ch := g.paused, g.resume
	g.mu.Unlock()
	if paused {
		<-ch
	}
}

// attachFlowControl adds optional flow-control methods to a connection's JS
// interface: writeQueued(data) -> queuedBytes|-1, queued() -> bytes,
// pause(), resume(), and the ondrain callback property. The existing
// write/close/ondata/onclose contract is unchanged. The returned cleanup
// must be called when the connection ends so the writer goroutine exits.
func attachFlowControl(conn *Connection, jsInterface js.Value) (*readGate, func()) {
	queue := newWriteQueue()
	gate := newReadGate()

	jsInterface.Set("writeQueued", js.FuncOf(func(this js.Value, args []js.Value) any {
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
	}))

	jsInterface.Set("queued", js.FuncOf(func(this js.Value, args []js.Value) any {
		queue.mu.Lock()
		defer queue.mu.Unlock()
		return js.ValueOf(queue.total)
	}))

	jsInterface.Set("pause", js.FuncOf(func(this js.Value, args []js.Value) any {
		gate.pause()
		return js.Undefined()
	}))

	jsInterface.Set("resume", js.FuncOf(func(this js.Value, args []js.Value) any {
		gate.unpause()
		return js.Undefined()
	}))

	// Release queued writes when the connection goes away.
	origClose := jsInterface.Get("close")
	jsInterface.Set("close", js.FuncOf(func(this js.Value, args []js.Value) any {
		queue.close()
		gate.unpause()
		if origClose.Type() == js.TypeFunction {
			origClose.Invoke()
		}
		return js.Undefined()
	}))

	cleanup := func() {
		queue.close()
		gate.unpause()
	}
	return gate, cleanup
}
