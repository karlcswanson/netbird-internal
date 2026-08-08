package tcp

import "sync"

const maxWriteQueue = 8 << 20 // 8 MiB per connection

// writeQueue decouples JS write calls from netstack writes so a full TCP
// send buffer never blocks a JS callback (which would stall the event loop
// the relay transport depends on). A single writer goroutine drains it.
//
// This file carries no build constraint so the queue/gate logic is unit
// tested on the host; the JS-facing drain loop and bindings live in the
// js-tagged flowcontrol.go.
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
