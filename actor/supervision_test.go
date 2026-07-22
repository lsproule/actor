package actor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// poisonThenRecordActor panics on the first user message it receives and
// records every user message after that (including the one it panicked on,
// so the test can see it was never appended). A fresh instance is produced by
// the Producer on every (re)start, so panicked tracks only this incarnation.
type poisonThenRecordActor struct {
	mu       sync.Mutex
	panicked bool
	got      []any
}

func (a *poisonThenRecordActor) Receive(c *Context) {
	switch c.Message().(type) {
	case Initialized, Started, Stopped:
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.panicked {
		a.panicked = true
		panic("boom")
	}
	a.got = append(a.got, c.Message())
}

// TestActorRecoversFromPanic sends a poison message followed by a normal one
// and asserts the normal message is handled by the restarted incarnation —
// and, implicitly, that the test binary survives the panic at all.
func TestActorRecoversFromPanic(t *testing.T) {
	e := newTestEngine(t)
	a := &poisonThenRecordActor{}
	pid := e.Spawn(func() Receiver { return a }, "poison", WithRestartDelay(time.Millisecond))

	e.Send(pid, "boom-trigger")
	require.Eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.panicked
	}, time.Second, time.Millisecond, "actor never observed the poison message")

	e.Send(pid, "hello")
	require.Eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return len(a.got) == 1
	}, time.Second, time.Millisecond, "normal message after poison was never handled")

	a.mu.Lock()
	defer a.mu.Unlock()
	assert.Equal(t, []any{"hello"}, a.got)
}

// freshStateActor increments counter on every user message and panics once
// counter reaches 2, so a second incarnation observing counter == 1 on its
// first message proves the Producer built a genuinely fresh instance rather
// than reusing state across the restart.
type freshStateActor struct {
	counter int
	seen    chan int
}

func (a *freshStateActor) Receive(c *Context) {
	switch c.Message().(type) {
	case Initialized, Started, Stopped:
		return
	}
	a.counter++
	a.seen <- a.counter
	if a.counter == 2 {
		panic("counter hit its limit")
	}
}

// TestRestartProducesFreshState sends three "tick" messages. The first
// incarnation reports counter 1, then 2 and panics; the restarted incarnation
// must report counter 1 again — not 3 — because its counter field started at
// the Producer's zero value, not the crashed instance's.
func TestRestartProducesFreshState(t *testing.T) {
	e := newTestEngine(t)
	seen := make(chan int, 10)
	pid := e.Spawn(func() Receiver { return &freshStateActor{seen: seen} }, "fresh",
		WithRestartDelay(time.Millisecond))

	e.Send(pid, "tick")
	assert.Equal(t, 1, mustReceiveInt(t, seen))

	e.Send(pid, "tick") // counter becomes 2 and panics
	assert.Equal(t, 2, mustReceiveInt(t, seen))

	e.Send(pid, "tick") // delivered to the restarted, fresh incarnation
	assert.Equal(t, 1, mustReceiveInt(t, seen), "restarted incarnation must start from a fresh zero-valued counter")
}

func mustReceiveInt(t *testing.T, ch <-chan int) int {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(deliverTimeout):
		t.Fatal("timed out waiting for value")
		return -1
	}
}

// TestRestartKeepsSamePID panics once and asserts the PID returned by Spawn
// is still reachable and identical afterwards — senders holding the old PID
// must keep working against the restarted incarnation.
func TestRestartKeepsSamePID(t *testing.T) {
	e := newTestEngine(t)
	a := &poisonThenRecordActor{}
	pid := e.Spawn(func() Receiver { return a }, "samepid", WithRestartDelay(time.Millisecond))
	before := NewPID(pid.Address, pid.ID)

	e.Send(pid, "boom-trigger")
	require.Eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.panicked
	}, time.Second, time.Millisecond, "actor never observed the poison message")

	// Give the restart a moment to complete, then confirm the PID still
	// resolves to a live, registered process and is byte-for-byte the same.
	require.Eventually(t, func() bool {
		return e.Registry().get(pid) != nil
	}, time.Second, time.Millisecond, "restarted actor never re-registered under its PID")

	assert.True(t, before.Equals(pid), "PID value must be unchanged")

	e.Send(pid, "hello")
	require.Eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return len(a.got) == 1
	}, time.Second, time.Millisecond, "message via the original PID never reached the restarted actor")
}

// queueActor panics on "boom" and forwards every other message to ch. It is
// used to prove that messages queued behind a poison message in the same (or
// a closely following) batch survive the restart.
type queueActor struct {
	ch chan any
}

func (a *queueActor) Receive(c *Context) {
	switch c.Message().(type) {
	case Initialized, Started, Stopped:
		return
	}
	if c.Message() == "boom" {
		panic("boom")
	}
	a.ch <- c.Message()
}

// TestQueuedMessagesSurviveRestart enqueues a poison message immediately
// followed by three normal ones without waiting between sends, so they are
// likely to land in the inbox together, and asserts all three are eventually
// delivered to the restarted incarnation.
func TestQueuedMessagesSurviveRestart(t *testing.T) {
	e := newTestEngine(t)
	ch := make(chan any, 3)
	pid := e.Spawn(func() Receiver { return &queueActor{ch: ch} }, "queued",
		WithRestartDelay(time.Millisecond))

	e.Send(pid, "boom")
	e.Send(pid, "m1")
	e.Send(pid, "m2")
	e.Send(pid, "m3")

	got := make([]any, 0, 3)
	for i := 0; i < 3; i++ {
		got = append(got, mustReceive(t, ch))
	}
	assert.ElementsMatch(t, []any{"m1", "m2", "m3"}, got)
}

// alwaysPanicActor panics on every user message and counts how many it saw,
// so a test can assert that sends after the actor gives up never reach it.
type alwaysPanicActor struct {
	hits int32
}

func (a *alwaysPanicActor) Receive(c *Context) {
	switch c.Message().(type) {
	case Initialized, Started, Stopped:
		return
	}
	atomic.AddInt32(&a.hits, 1)
	panic("always boom")
}

// TestMaxRestartsExhausted configures WithMaxRestarts(2) against an actor
// that always panics, and asserts it stops for good once the 3rd panic
// exceeds the restart budget: the PID is no longer registered, and messages
// sent afterwards are silently dropped rather than triggering another
// restart. "Let it crash" never retries the message that caused a panic, so
// exhausting a budget of 2 restarts takes 3 poisoned messages: one to cause
// restart 1, one against that fresh incarnation to cause restart 2, and one
// more against the next incarnation to push the count past the budget.
func TestMaxRestartsExhausted(t *testing.T) {
	e := newTestEngine(t)
	a := &alwaysPanicActor{}
	pid := e.Spawn(func() Receiver { return a }, "exhausted",
		WithMaxRestarts(2), WithRestartDelay(time.Millisecond))

	e.Send(pid, "trigger-1")
	e.Send(pid, "trigger-2")
	e.Send(pid, "trigger-3")

	require.Eventually(t, func() bool {
		return e.Registry().get(pid) == nil
	}, time.Second, time.Millisecond, "actor should be unregistered once restarts are exhausted")

	hitsAfterGivingUp := atomic.LoadInt32(&a.hits)

	// Further sends must be dropped (registry miss), not trigger a restart.
	e.Send(pid, "trigger-4")
	e.Send(pid, "trigger-5")
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, hitsAfterGivingUp, atomic.LoadInt32(&a.hits),
		"a shut-down actor must not receive further messages or restart again")
	assert.Nil(t, e.Registry().get(pid))
}

// TestNeverRestart sets WithMaxRestarts(0) and asserts the actor is shut down
// permanently on its very first panic — no restart is attempted at all.
func TestNeverRestart(t *testing.T) {
	e := newTestEngine(t)
	a := &alwaysPanicActor{}
	pid := e.Spawn(func() Receiver { return a }, "never", WithMaxRestarts(0))

	e.Send(pid, "trigger")

	require.Eventually(t, func() bool {
		return e.Registry().get(pid) == nil
	}, time.Second, time.Millisecond, "MaxRestarts == 0 must shut the actor down on first panic")

	assert.Equal(t, int32(1), atomic.LoadInt32(&a.hits), "actor must have seen exactly the one panicking message")
}

// echoActor replies to every user message by forwarding it to ch, used as the
// well-behaved neighbor in the non-blocking restart-delay test.
type echoActor struct {
	ch chan any
}

func (a *echoActor) Receive(c *Context) {
	switch c.Message().(type) {
	case Initialized, Started, Stopped:
		return
	}
	a.ch <- c.Message()
}

// oneShotPanicActor panics exactly once (on its first user message) and is
// silent afterwards; used to hold actor A in its RestartDelay sleep.
type oneShotPanicActor struct {
	panicked int32
}

func (a *oneShotPanicActor) Receive(c *Context) {
	switch c.Message().(type) {
	case Initialized, Started, Stopped:
		return
	}
	if atomic.CompareAndSwapInt32(&a.panicked, 0, 1) {
		panic("boom")
	}
}

// TestRestartDoesNotBlockOtherActors gives actor A a real 300ms RestartDelay
// and panics it, then proves actor B keeps echoing messages promptly during
// that whole window — the sleep in tryRestart runs on A's own goroutine and
// must not stall B's inbox processor.
func TestRestartDoesNotBlockOtherActors(t *testing.T) {
	e := newTestEngine(t)

	pidA := e.Spawn(func() Receiver { return &oneShotPanicActor{} }, "slow-restart",
		WithRestartDelay(300*time.Millisecond))

	chB := make(chan any, 8)
	pidB := e.Spawn(func() Receiver { return &echoActor{ch: chB} }, "responsive")

	e.Send(pidA, "boom-trigger")

	// A is now asleep inside tryRestart for 300ms. Round-trip messages
	// through B repeatedly during that window, each bounded by a timeout far
	// shorter than A's delay: if A's sleep were blocking a shared scheduler,
	// these round-trips would stall until A wakes up and one would time out.
	deadline := time.Now().Add(280 * time.Millisecond)
	round := 0
	for time.Now().Before(deadline) {
		e.Send(pidB, round)
		select {
		case got := <-chB:
			assert.Equal(t, round, got, "actor B must keep responding promptly while A sleeps in its restart delay")
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("actor B did not respond within 100ms while actor A was restarting (round %d)", round)
		}
		round++
	}
	require.Greater(t, round, 0, "test should have exercised at least one round-trip during A's restart delay")
}
