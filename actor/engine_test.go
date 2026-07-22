package actor

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// deliverTimeout bounds every "did the message arrive" wait so a lost message
// fails fast instead of hanging the suite. Waits are always on a channel, never
// a sleep: time is a deadline here, not a synchronization primitive.
const deliverTimeout = time.Second

// noop is a Receiver that ignores every message. It is used where only spawn
// bookkeeping (IDs, registry size) matters.
type noop struct{}

func (noop) Receive(*Context) {}

// recorder forwards every user message to ch, skipping the lifecycle messages
// so tests assert only on what they sent.
type recorder struct {
	ch chan any
}

func (r *recorder) Receive(ctx *Context) {
	switch ctx.Message().(type) {
	case Initialized, Started, Stopped:
		return
	}
	r.ch <- ctx.Message()
}

// counter counts user messages and closes done exactly once, when got reaches
// want. Receive runs on the actor's single goroutine, so got needs no lock.
type counter struct {
	got  int
	want int
	done chan struct{}
}

func (c *counter) Receive(ctx *Context) {
	switch ctx.Message().(type) {
	case Initialized, Started, Stopped:
		return
	}
	c.got++
	if c.got == c.want {
		close(c.done)
	}
}

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := NewEngine(NewEngineConfig())
	require.NoError(t, err)
	require.Equal(t, LocalLookupAddr, e.Address())
	return e
}

// mustReceive fails the test if nothing arrives on ch before deliverTimeout.
func mustReceive(t *testing.T, ch <-chan any) any {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(deliverTimeout):
		t.Fatal("timed out waiting for message")
		return nil
	}
}

func TestEngineSpawnAndSend(t *testing.T) {
	tests := []struct {
		name string
		msg  any
	}{
		{"string", "hello"},
		{"int", 42},
		{"struct", struct{ N int }{N: 7}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEngine(t)
			ch := make(chan any, 1)
			pid := e.Spawn(func() Receiver { return &recorder{ch: ch} }, "rec")

			e.Send(pid, tc.msg)

			require.Equal(t, tc.msg, mustReceive(t, ch))
		})
	}
}

// TestSpawnThenImmediateSend proves the "never lost" guarantee: a Send on the
// line right after Spawn always lands. Running it in a loop stresses the
// spawn/register/deliver ordering rather than getting lucky once.
func TestSpawnThenImmediateSend(t *testing.T) {
	e := newTestEngine(t)
	for i := 0; i < 100; i++ {
		ch := make(chan any, 1)
		pid := e.Spawn(func() Receiver { return &recorder{ch: ch} }, "imm")
		e.Send(pid, i)
		select {
		case got := <-ch:
			require.Equal(t, i, got)
		case <-time.After(deliverTimeout):
			t.Fatalf("iteration %d: message lost after spawn", i)
		}
	}
}

// TestSpawnUniqueIDs spawns the same kind many times with no explicit ID and
// asserts every PID is distinct and every actor is registered.
func TestSpawnUniqueIDs(t *testing.T) {
	e := newTestEngine(t)
	const n = 100
	seen := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		pid := e.Spawn(func() Receiver { return noop{} }, "uniq")
		_, dup := seen[pid.String()]
		require.Falsef(t, dup, "duplicate PID %s", pid.String())
		seen[pid.String()] = struct{}{}
	}
	require.Len(t, seen, n)
	require.Equal(t, n, e.Registry().len())
}

// TestSpawnFunc checks that a func-based actor sees Started (delivered during
// Spawn) and then a user message, in order.
func TestSpawnFunc(t *testing.T) {
	e := newTestEngine(t)
	got := make(chan string, 2)
	pid := e.SpawnFunc(func(ctx *Context) {
		switch m := ctx.Message().(type) {
		case Started:
			got <- "started"
		case string:
			got <- m
		}
	}, "fn")

	e.Send(pid, "hello")

	require.Equal(t, "started", waitString(t, got))
	require.Equal(t, "hello", waitString(t, got))
}

func waitString(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(deliverTimeout):
		t.Fatal("timed out waiting for string")
		return ""
	}
}

// TestSendConcurrent fans 10000 sends across four goroutines into one actor and
// asserts all of them are received. The actor's single processor goroutine is
// the only reader of its counter, so the count is race-free by construction;
// the WaitGroup and the done channel provide real synchronization instead of a
// sleep.
func TestSendConcurrent(t *testing.T) {
	e := newTestEngine(t)
	const (
		senders = 4
		perGoro = 2500
		total   = senders * perGoro
	)
	done := make(chan struct{})
	pid := e.Spawn(func() Receiver {
		return &counter{want: total, done: done}
	}, "counter")

	var wg sync.WaitGroup
	wg.Add(senders)
	for g := 0; g < senders; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoro; i++ {
				e.Send(pid, 1)
			}
		}()
	}
	wg.Wait()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive all messages")
	}
}

// TestSendToUnknownPID sends to a PID that was never spawned and asserts the
// call neither panics nor blocks.
func TestSendToUnknownPID(t *testing.T) {
	e := newTestEngine(t)
	returned := make(chan struct{})
	go func() {
		e.Send(NewPID(LocalLookupAddr, "nope"), "into the void")
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(deliverTimeout):
		t.Fatal("Send to unknown PID blocked")
	}
	require.Equal(t, 0, e.Registry().len())
}

// TestSpawnExplicitIDCollisionKeepsIncumbent documents the chosen collision
// behaviour: a duplicate explicit ID returns the incumbent's PID and does not
// grow the registry, so the live actor is never orphaned.
func TestSpawnExplicitIDCollisionKeepsIncumbent(t *testing.T) {
	e := newTestEngine(t)
	first := e.Spawn(func() Receiver { return noop{} }, "svc", WithID("only"))
	second := e.Spawn(func() Receiver { return noop{} }, "svc", WithID("only"))

	require.True(t, first.Equals(second))
	require.Equal(t, "local/svc/only", first.String())
	require.Equal(t, 1, e.Registry().len())
}

// TestSendWithSender checks the sender PID is delivered through the Context.
func TestSendWithSender(t *testing.T) {
	e := newTestEngine(t)
	senders := make(chan *PID, 1)
	pid := e.SpawnFunc(func(ctx *Context) {
		if _, ok := ctx.Message().(string); ok {
			senders <- ctx.Sender()
		}
	}, "rec")

	from := NewPID(LocalLookupAddr, "caller")
	e.SendWithSender(pid, "ping", from)

	select {
	case got := <-senders:
		require.True(t, from.Equals(got))
	case <-time.After(deliverTimeout):
		t.Fatal("timed out waiting for sender")
	}
}
