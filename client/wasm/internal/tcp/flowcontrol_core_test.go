package tcp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteQueue_PushReturnsRunningTotal(t *testing.T) {
	q := newWriteQueue()

	require.Equal(t, 5, q.push([]byte("hello")))
	require.Equal(t, 11, q.push([]byte("world!")))
}

func TestWriteQueue_Limit(t *testing.T) {
	tests := []struct {
		name  string
		first int
		next  int
		want  int
	}{
		{"at limit is accepted", maxWriteQueue, 0, maxWriteQueue},
		{"one past limit is rejected", maxWriteQueue, 1, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := newWriteQueue()
			require.Equal(t, tt.first, q.push(make([]byte, tt.first)))
			require.Equal(t, tt.want, q.push(make([]byte, tt.next)))

			q.mu.Lock()
			total := q.total
			q.mu.Unlock()
			// A rejected push must not change the running total.
			if tt.want == -1 {
				require.Equal(t, tt.first, total)
			}
		})
	}
}

func TestWriteQueue_RejectsAfterClose(t *testing.T) {
	q := newWriteQueue()
	q.push([]byte("data"))
	q.close()

	require.Equal(t, -1, q.push([]byte("more")))

	q.mu.Lock()
	total, chunks := q.total, len(q.chunks)
	q.mu.Unlock()
	require.Zero(t, total, "close should drop the queued byte count")
	require.Zero(t, chunks, "close should drop queued chunks")
}

func TestWriteQueue_WakeIsNonBlocking(t *testing.T) {
	q := newWriteQueue()
	// notify has capacity 1; repeated wakes must never block even with no
	// drain goroutine draining them.
	require.NotPanics(t, func() {
		for i := 0; i < 100; i++ {
			q.wake()
		}
	})
}

func TestReadGate_WaitReturnsWhenNotPaused(t *testing.T) {
	g := newReadGate()
	done := make(chan struct{})
	go func() {
		g.wait() // must return immediately: gate starts unpaused
		close(done)
	}()
	requireClosed(t, done, "wait blocked while not paused")
}

func TestReadGate_PauseThenResumeReleasesWaiter(t *testing.T) {
	g := newReadGate()
	g.pause()

	released := make(chan struct{})
	go func() {
		g.wait()
		close(released)
	}()

	requireStillOpen(t, released, "wait returned while paused")
	g.unpause()
	requireClosed(t, released, "wait did not return after unpause")
}

func TestReadGate_UnpauseIsIdempotent(t *testing.T) {
	g := newReadGate()
	g.pause()
	g.unpause()
	// A second unpause must not panic (a double close of the resume channel).
	require.NotPanics(t, g.unpause)
}

func TestReadGate_DoublePauseKeepsSingleGate(t *testing.T) {
	g := newReadGate()
	g.pause()
	g.pause() // second pause must not replace the channel and strand a waiter

	released := make(chan struct{})
	go func() {
		g.wait()
		close(released)
	}()
	g.unpause()
	requireClosed(t, released, "wait did not return after unpause following a double pause")
}

// requireClosed fails unless ch closes within a generous deadline.
func requireClosed(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal(msg)
	}
}

// requireStillOpen fails if ch closes within a short window.
func requireStillOpen(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(msg)
	case <-time.After(20 * time.Millisecond):
	}
}
