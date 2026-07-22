package actor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeProcesser is a Processer used by the inbox tests. It records every
// envelope it sees in arrival order under a mutex and exposes a plain
// (non-atomic, non-mutex-protected) counter that the single-consumer test
// relies on: any concurrent invocation of Invoke would race on that
// counter and the race detector would fail the test, which is exactly
// the point.
//
// notify is a buffered channel that receives one signal per envelope
// delivered; tests use it to wait until the processor has caught up
// without polling.
type fakeProcesser struct {
	mu       sync.Mutex
	received []Envelope
	cnt      int
	notify   chan struct{}
}

func newFakeProcesser() *fakeProcesser {
	return &fakeProcesser{
		notify: make(chan struct{}, 1<<20),
	}
}

func (f *fakeProcesser) Invoke(batch []Envelope) {
	// Deliberately racy: the inbox contract is that Invoke is called by
	// at most one goroutine at a time, so the single-consumer test
	// passing under -race is what proves the contract holds.
	f.cnt += len(batch)

	f.mu.Lock()
	f.received = append(f.received, batch...)
	f.mu.Unlock()

	for range batch {
		f.notify <- struct{}{}
	}
}

// waitForCount blocks until the processor has delivered exactly n
// envelopes or the deadline elapses. Returns true on success.
func (f *fakeProcesser) waitForCount(t testing.TB, n int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.After(timeout)
	for i := 0; i < n; i++ {
		select {
		case <-f.notify:
		case <-deadline:
			t.Errorf("timed out after %s waiting for envelope %d/%d", timeout, i+1, n)
			return false
		}
	}
	return true
}

// TestInboxDeliversAll sends n envelopes in order, waits for the
// processor to catch up, and asserts every message was delivered in
// FIFO order. The total count is the simplest possible end-to-end
// check; the per-index check catches reordering bugs the count check
// would miss.
func TestInboxDeliversAll(t *testing.T) {
	const n = 1000
	fake := newFakeProcesser()
	in := NewInbox(64)
	in.Start(fake)

	for i := 0; i < n; i++ {
		in.Send(Envelope{Msg: i})
	}

	require.True(t, fake.waitForCount(t, n, 5*time.Second), "processor did not deliver all envelopes")
	assert.Equal(t, n, fake.cnt, "delivered count mismatch")
	require.Len(t, fake.received, n)
	for i := 0; i < n; i++ {
		assert.Equal(t, i, fake.received[i].Msg, "envelope %d arrived out of order", i)
	}
}

// TestInboxIdleUntilFirstSend asserts the two states the inbox
// contract puts on the idle/park story: after construction and Start
// the inbox is idle and no processor goroutine has been spawned; the
// first Send wakes it up and the message is delivered; after delivery
// the processor parks and procStatus returns to idle.
func TestInboxIdleUntilFirstSend(t *testing.T) {
	fake := newFakeProcesser()
	in := NewInbox(64)
	in.Start(fake)

	require.Equal(t, int32(stateIdle), atomic.LoadInt32(&in.procStatus),
		"inbox must be idle before the first Send")

	in.Send(Envelope{Msg: "hello"})
	require.True(t, fake.waitForCount(t, 1, 5*time.Second), "first envelope not delivered")
	assert.Equal(t, 1, fake.cnt)

	// After the only envelope is drained, the processor must park
	// itself. Allow a short window for the goroutine to finish its
	// current run and re-check the buffer before asserting.
	assert.Eventually(t, func() bool {
		return atomic.LoadInt32(&in.procStatus) == stateIdle
	}, 2*time.Second, time.Millisecond, "inbox must return to idle after draining")
}

// TestInboxSingleConsumer is the load-bearing race test. Sixteen
// goroutines send a thousand envelopes each into the same inbox, and
// the fake's plain (non-atomic, non-locked) counter is incremented
// inside Invoke. If the inbox ever spawned two processor goroutines
// concurrently, two of them would increment the counter at the same
// time and the race detector would fail the test. A passing run under
// -race proves the inbox's "exactly one processor per inbox" rule.
func TestInboxSingleConsumer(t *testing.T) {
	const goroutines = 16
	const perGoroutine = 1000
	const total = goroutines * perGoroutine

	fake := newFakeProcesser()
	in := NewInbox(1024)
	in.Start(fake)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				in.Send(Envelope{Msg: gid*perGoroutine + i})
			}
		}(g)
	}
	wg.Wait()

	require.True(t, fake.waitForCount(t, total, 10*time.Second), "processor dropped envelopes")
	assert.Equal(t, total, fake.cnt, "final count must equal total sent")
}

// TestInboxNoLostWakeup hammers the park/wake edge. Each iteration
// sends exactly one envelope and waits for it to be delivered before
// sending the next, which forces the processor to park after every
// single message and wake up again on the next Send. Doing this
// ten thousand times catches a lost wakeup in the schedule/process
// handshake: if a Send ever landed in the window between the
// empty-buffer check and the park CAS without being picked up, the
// notify would never fire and the test would time out.
func TestInboxNoLostWakeup(t *testing.T) {
	const iters = 10000
	fake := newFakeProcesser()
	// Tiny starting size forces the ring buffer to grow mid-test,
	// which adds extra traffic through the wake path.
	in := NewInbox(2)
	in.Start(fake)

	for i := 0; i < iters; i++ {
		in.Send(Envelope{Msg: i})
		select {
		case <-fake.notify:
		case <-time.After(2 * time.Second):
			t.Fatalf("lost wakeup at iteration %d: processor never delivered the message", i)
		}
	}

	assert.Equal(t, iters, fake.cnt)
}

// TestInboxStopIdempotent checks the stop contract: calling Stop twice
// in a row must not panic, and calling Stop concurrently with Sends
// must not panic either. The processor goroutine is allowed to bail
// out at any point and any in-flight batch may be dropped; the only
// requirement is no panic and no use-after-free of the inbox.
func TestInboxStopIdempotent(t *testing.T) {
	t.Run("stop twice", func(t *testing.T) {
		fake := newFakeProcesser()
		in := NewInbox(64)
		in.Start(fake)

		assert.NoError(t, in.Stop())
		assert.NoError(t, in.Stop(), "second Stop must be a no-op, not a panic")
	})

	t.Run("stop concurrent with sends", func(t *testing.T) {
		fake := newFakeProcesser()
		in := NewInbox(64)
		in.Start(fake)

		const senders = 8
		const perSender = 1000
		var wg sync.WaitGroup
		wg.Add(senders + 1)
		for s := 0; s < senders; s++ {
			go func() {
				defer wg.Done()
				for i := 0; i < perSender; i++ {
					in.Send(Envelope{Msg: i})
				}
			}()
		}
		go func() {
			defer wg.Done()
			_ = in.Stop()
		}()
		wg.Wait()
		// A second Stop after the race is still allowed.
		assert.NoError(t, in.Stop())
	})
}

// BenchmarkInboxSend measures the cost of handing an envelope to an
// inbox under contention. The processor is kept busy draining so the
// Send path always races against the consumer; that's the steady
// state the inbox is designed for.
func BenchmarkInboxSend(b *testing.B) {
	fake := newFakeProcesser()
	in := NewInbox(1024)
	in.Start(fake)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			in.Send(Envelope{Msg: i})
			i++
		}
	})
}
