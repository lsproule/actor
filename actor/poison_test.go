package actor

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// counterMsg is the user message these tests queue ahead of a poison pill. It
// carries its sequence number so the actor's log proves not just how many
// messages arrived but that they arrived in order.
type counterMsg struct{ n int }

// pingMsg is a bare user message for tests that only need to prove a send
// lands (or does not panic) without caring about ordering.
type pingMsg struct{}

// poisonActor records every delivery it sees. The mutex guards the fields
// against the test goroutine, which reads them once Done has fired: the actor
// itself is single-threaded, but the handover to the test is a real
// cross-goroutine read that -race is entitled to flag.
type poisonActor struct {
	mu       sync.Mutex
	received []int
	kinds    []string
	stops    int

	// started closes when Started has been delivered, so tests can wait for
	// the actor to be alive without sleeping.
	started chan struct{}
	// alive answers a pingMsg, so a test can prove the actor is still running
	// at a given moment rather than inferring it from elapsed time.
	alive chan struct{}
}

var _ Receiver = (*poisonActor)(nil)

func newPoisonActor() *poisonActor {
	return &poisonActor{
		started: make(chan struct{}),
		alive:   make(chan struct{}, 1),
	}
}

func (a *poisonActor) Receive(c *Context) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch msg := c.Message().(type) {
	case Initialized:
		a.kinds = append(a.kinds, "Initialized")
	case Started:
		a.kinds = append(a.kinds, "Started")
		close(a.started)
	case Stopped:
		a.kinds = append(a.kinds, "Stopped")
		a.stops++
	case counterMsg:
		a.kinds = append(a.kinds, "counterMsg")
		a.received = append(a.received, msg.n)
	case pingMsg:
		a.kinds = append(a.kinds, "pingMsg")
		select {
		case a.alive <- struct{}{}:
		default:
		}
	}
}

func (a *poisonActor) snapshot() (received []int, kinds []string, stops int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	received = append([]int(nil), a.received...)
	kinds = append([]string(nil), a.kinds...)
	return received, kinds, a.stops
}

// spawnPoisonActor spawns a poisonActor and waits until it is fully started,
// so every test begins from the same known-live state.
func spawnPoisonActor(t *testing.T) (*Engine, *PID, *poisonActor) {
	t.Helper()

	e, err := NewEngine(NewEngineConfig())
	require.NoError(t, err)

	a := newPoisonActor()
	pid := e.Spawn(func() Receiver { return a }, "poison")

	waitClosed(t, a.started, time.Second, "actor never reached Started")
	return e, pid, a
}

// waitClosed blocks until ch is closed or timeout elapses, failing the test in
// the latter case. These tests never use time.Sleep to synchronise; every wait
// goes through this helper or a context's Done channel.
func waitClosed(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}

// TestPoisonProcessesQueuedMessages is the central guarantee: a poison pill
// queues behind work already sent, so nothing in flight is lost and Stopped is
// strictly last.
func TestPoisonProcessesQueuedMessages(t *testing.T) {
	e, pid, a := spawnPoisonActor(t)

	const n = 1000
	for i := 0; i < n; i++ {
		e.Send(pid, counterMsg{n: i})
	}

	waitClosed(t, e.Poison(pid).Done(), 5*time.Second, "poison never completed")

	received, kinds, stops := a.snapshot()
	require.Len(t, received, n, "every queued message must be processed before the pill")
	for i := 0; i < n; i++ {
		require.Equal(t, i, received[i], "messages must be processed in send order")
	}
	require.Equal(t, 1, stops, "Stopped must be delivered exactly once")
	assert.Equal(t, "Stopped", kinds[len(kinds)-1], "Stopped must be the last message")
}

// TestPoisonDoneCloses pins the basic contract: the returned context is
// actually cancelled, and reasonably fast.
func TestPoisonDoneCloses(t *testing.T) {
	e, pid, _ := spawnPoisonActor(t)

	select {
	case <-e.Poison(pid).Done():
	case <-time.After(time.Second):
		t.Fatal("Poison context was never cancelled")
	}
}

// TestPoisonDeregisters asserts the ordering that makes Done meaningful: by
// the time a waiter is woken the PID no longer resolves, and sending to it is
// a harmless no-op rather than a panic.
func TestPoisonDeregisters(t *testing.T) {
	e, pid, _ := spawnPoisonActor(t)

	waitClosed(t, e.Poison(pid).Done(), time.Second, "poison never completed")

	assert.Nil(t, e.Registry().get(pid), "PID must be gone from the registry once Done fires")
	assert.NotPanics(t, func() { e.Send(pid, pingMsg{}) }, "sending to a stopped actor must not panic")
}

// TestPoisonUnknownPID covers the registry miss. A caller who poisons a PID
// that was never alive must not wait forever for a stop that cannot happen.
func TestPoisonUnknownPID(t *testing.T) {
	e, err := NewEngine(NewEngineConfig())
	require.NoError(t, err)

	ctx := e.Poison(NewPID(LocalLookupAddr, "poison/nope"))

	select {
	case <-ctx.Done():
	default:
		t.Fatal("poisoning an unknown PID must return an already-cancelled context")
	}
}

// TestDoublePoison covers the second caller. Both contexts must be cancelled
// even though only one teardown runs, and the actor must not see Stopped twice.
func TestDoublePoison(t *testing.T) {
	e, pid, a := spawnPoisonActor(t)

	first := e.Poison(pid)
	second := e.Poison(pid)

	waitClosed(t, first.Done(), time.Second, "first poison never completed")
	waitClosed(t, second.Done(), time.Second, "second poison never completed")

	_, _, stops := a.snapshot()
	assert.Equal(t, 1, stops, "Stopped must be delivered exactly once across both poisons")

	// A third poison, long after the actor is gone, is a pure registry miss.
	third := e.Poison(pid)
	select {
	case <-third.Done():
	default:
		t.Fatal("poisoning an already-stopped actor must return an already-cancelled context")
	}
}

// TestConcurrentPoisonWaiters is the reason the process keeps a slice of
// cancels rather than a single one: many callers can queue a pill before the
// first is handled, and every one of them holds a context that must be
// cancelled by the single teardown that actually runs.
func TestConcurrentPoisonWaiters(t *testing.T) {
	e, pid, a := spawnPoisonActor(t)

	const waiters = 50
	var wg sync.WaitGroup
	wg.Add(waiters)

	for i := 0; i < waiters; i++ {
		go func() {
			defer wg.Done()
			<-e.Poison(pid).Done()
		}()
	}

	allDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(allDone)
	}()

	waitClosed(t, allDone, 5*time.Second, "not every concurrent poison waiter was released")

	_, _, stops := a.snapshot()
	assert.Equal(t, 1, stops, "Stopped must be delivered exactly once regardless of waiter count")
}

// TestPoisonAfter checks that the delayed variant returns immediately, leaves
// the actor serving messages until the timer fires, and tears it down after.
func TestPoisonAfter(t *testing.T) {
	e, pid, a := spawnPoisonActor(t)

	ctx := e.PoisonAfter(pid, 50*time.Millisecond)

	// Returning immediately is the point: the caller is not blocked for the
	// delay, so the context cannot already be done here.
	select {
	case <-ctx.Done():
		t.Fatal("PoisonAfter must return before the delay elapses")
	default:
	}

	// The actor is still alive: it answers a ping. This is a positive proof of
	// liveness rather than a sleep-and-hope.
	e.Send(pid, pingMsg{})
	waitClosed(t, a.alive, time.Second, "actor should still be serving messages before the timer fires")

	waitClosed(t, ctx.Done(), 5*time.Second, "PoisonAfter never completed")
	assert.Nil(t, e.Registry().get(pid), "actor must be gone once the delayed poison completes")
}

// TestPoisonAfterAlreadyStopped fires a delayed poison at an actor that is
// already gone. The timer's callback runs against an empty registry and must
// cancel its context instead of panicking.
func TestPoisonAfterAlreadyStopped(t *testing.T) {
	e, pid, _ := spawnPoisonActor(t)

	late := e.PoisonAfter(pid, 50*time.Millisecond)

	waitClosed(t, e.Poison(pid).Done(), time.Second, "immediate poison never completed")

	assert.NotPanics(t, func() {
		waitClosed(t, late.Done(), 5*time.Second, "late PoisonAfter never released its waiter")
	}, "a PoisonAfter timer firing after the actor is gone must not panic")
}
