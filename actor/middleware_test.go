package actor

import (
	"bytes"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- test helpers for middleware tests ---

// sequenceRecorder is a Receiver that appends "recv" to a shared sequence on
// every delivery. It is used to assert middleware ordering and short-circuiting.
type sequenceRecorder struct {
	seq *[]string
}

func (r *sequenceRecorder) Receive(c *Context) {
	*r.seq = append(*r.seq, "recv")
}

// appendMiddleware returns a Middleware that appends tag+"-in" before and
// tag+"-out" after the next handler.
func appendMiddleware(seq *[]string, tag string) Middleware {
	return func(next ReceiveFunc) ReceiveFunc {
		return func(c *Context) {
			*seq = append(*seq, tag+"-in")
			next(c)
			*seq = append(*seq, tag+"-out")
		}
	}
}

// filterUserMessages returns a new slice with only user messages (not
// lifecycle ones). Used to isolate the ordering of a single delivery.
func filterUserMessages(seq []string) []string {
	var out []string
	for _, s := range seq {
		if s == "a-in" || s == "b-in" || s == "recv" || s == "b-out" || s == "a-out" {
			out = append(out, s)
		}
	}
	return out
}

// --- acceptance tests ---

// TestMiddlewareOrder asserts outermost-first execution: WithMiddleware(a, b)
// yields [a-in, b-in, recv, b-out, a-out] for each message (including
// lifecycle messages — Spawn delivers Initialized and Started through the
// same chain). We filter to user messages only for the final assertion.
func TestMiddlewareOrder(t *testing.T) {
	var seq []string

	a := appendMiddleware(&seq, "a")
	b := appendMiddleware(&seq, "b")

	e := newTestEngine(t)
	pid := e.Spawn(
		func() Receiver { return &sequenceRecorder{seq: &seq} },
		"order",
		WithMiddleware(a, b),
	)

	e.Send(pid, "msg")

	require.Eventually(t, func() bool {
		// After Spawn we already have 10 entries (2 lifecycle × 5).
		// After Send we have 15. Wait for at least 15.
		return len(seq) >= 15
	}, deliverTimeout, time.Millisecond, "sequence not fully recorded")

	// The last 5 entries correspond to the user message "msg".
	user := filterUserMessages(seq)
	require.GreaterOrEqual(t, len(user), 5, "need at least one full user-message cycle")
	last5 := user[len(user)-5:]
	assert.Equal(t, []string{"a-in", "b-in", "recv", "b-out", "a-out"}, last5,
		"outermost middleware must run first and unwind last")
}

// TestMiddlewareSeesLifecycle asserts that middleware observes Initialized,
// Started, and Stopped — they flow through the same invokeMsg path.
func TestMiddlewareSeesLifecycle(t *testing.T) {
	var lifecycle []string

	mw := func(next ReceiveFunc) ReceiveFunc {
		return func(c *Context) {
			switch c.Message().(type) {
			case Initialized:
				lifecycle = append(lifecycle, "Initialized")
			case Started:
				lifecycle = append(lifecycle, "Started")
			case Stopped:
				lifecycle = append(lifecycle, "Stopped")
			}
			next(c)
		}
	}

	e := newTestEngine(t)
	pid := e.Spawn(
		func() Receiver { return &noopActor{} },
		"lifecycle",
		WithMiddleware(mw),
	)

	// Initialized + Started are delivered synchronously by Spawn.
	require.Equal(t, []string{"Initialized", "Started"}, lifecycle)

	// Stopped arrives after Poison drains the inbox.
	<-e.Poison(pid).Done()

	require.Equal(t, []string{"Initialized", "Started", "Stopped"}, lifecycle)
}

// TestMiddlewareShortCircuit verifies that a middleware can return without
// calling next, preventing the receiver from running. Lifecycle messages
// still pass through (they are not "skip"), so the receiver sees Initialized
// and Started; the assertion checks that "skip" was intercepted.
func TestMiddlewareShortCircuit(t *testing.T) {
	var got []string

	shortCircuit := func(next ReceiveFunc) ReceiveFunc {
		return func(c *Context) {
			if s, ok := c.Message().(string); ok && s == "skip" {
				got = append(got, "skipped")
				return // do not call next — receiver must not run for this msg
			}
			next(c)
		}
	}

	e := newTestEngine(t)
	pid := e.Spawn(
		func() Receiver { return &sequenceRecorder{seq: &got} },
		"short",
		WithMiddleware(shortCircuit),
	)

	// After Spawn: Initialized and Started passed through → 2 × "recv".
	// "skip" is short-circuited → "skipped".
	// "deliver" passes through → "recv".
	e.Send(pid, "skip")
	e.Send(pid, "deliver")

	require.Eventually(t, func() bool {
		return len(got) >= 4 // recv, recv, skipped, recv
	}, deliverTimeout, time.Millisecond)

	// Lifecycle messages are at the front; user messages at the back.
	assert.Contains(t, got, "skipped", "short-circuit middleware must record its action")
	assert.Equal(t, "recv", got[len(got)-1],
		"deliver message must reach the receiver")
	// Verify "skip" did NOT reach the receiver: the only "recv" entries
	// should come from lifecycle messages and the "deliver" message.
	recvCount := 0
	for _, s := range got {
		if s == "recv" {
			recvCount++
		}
	}
	assert.Equal(t, 3, recvCount,
		"exactly 3 recv entries: Initialized + Started + deliver (skip must not reach receiver)")
}

// TestMiddlewareChainBuiltOnce asserts the middleware constructor body runs
// exactly once per actor incarnation, not per message. The build counter is
// incremented in the Middleware function body (outside the returned closure),
// so it increments only when the chain is composed in Start.
func TestMiddlewareChainBuiltOnce(t *testing.T) {
	var buildCount atomic.Int32

	buildCounter := func(next ReceiveFunc) ReceiveFunc {
		buildCount.Add(1)
		return func(c *Context) { next(c) }
	}

	e := newTestEngine(t)
	pid := e.Spawn(
		func() Receiver { return &noopActor{} },
		"once",
		WithMiddleware(buildCounter),
	)

	for i := 0; i < 100; i++ {
		e.Send(pid, i)
	}

	// Give messages time to drain.
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int32(1), buildCount.Load(),
		"middleware constructor must run exactly once (at chain-build time), not per message")
}

// TestMiddlewareNoAllocPerMessage asserts zero allocations per invokeMsg call
// when a no-op middleware is configured. This mirrors TestProcessNoAllocPerMessage
// from process_test.go but with the middleware path exercised.
func TestMiddlewareNoAllocPerMessage(t *testing.T) {
	p := newProcess(&Engine{}, Opts{
		Producer:  func() Receiver { return noopActor{} },
		ID:        "bench",
		InboxSize: 8,
		Middleware: []Middleware{
			func(next ReceiveFunc) ReceiveFunc {
				return func(c *Context) { next(c) }
			},
		},
	})
	p.Start()

	allocs := testing.AllocsPerRun(1000, func() {
		p.invokeMsg(Envelope{Msg: "test"})
	})

	assert.Zero(t, allocs,
		"invokeMsg with middleware must not allocate per message")
}

// TestMiddlewarePanicRestarts verifies that a panic inside middleware is caught
// by the same recover in invokeMsg and routed through the normal supervision
// restart path. The chain is rebuilt in Start(), so the restarted incarnation
// gets a fresh chain.
func TestMiddlewarePanicRestarts(t *testing.T) {
	var panics atomic.Int32

	panicking := func(next ReceiveFunc) ReceiveFunc {
		return func(c *Context) {
			switch c.Message().(type) {
			case Initialized, Started, Stopped:
				next(c)
				return
			}
			if panics.CompareAndSwap(0, 1) {
				panic("middleware panic")
			}
			next(c)
		}
	}

	ch := make(chan any, 1)
	e := newTestEngine(t)
	pid := e.Spawn(
		func() Receiver { return &recorder{ch: ch} },
		"panic",
		WithMiddleware(panicking),
		WithRestartDelay(time.Millisecond),
	)

	// First user message panics — actor restarts.
	e.Send(pid, "boom")
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(1), panics.Load(), "first message must have panicked")

	// Second user message should be handled by the restarted incarnation.
	e.Send(pid, "after-restart")
	assert.Equal(t, "after-restart", mustReceive(t, ch),
		"restarted actor must handle messages normally")
}

// TestNoMiddlewareUnchanged is a full end-to-end regression with zero
// middleware configured: spawn, send, receive, verify.
func TestNoMiddlewareUnchanged(t *testing.T) {
	e := newTestEngine(t)
	ch := make(chan any, 1)
	pid := e.Spawn(func() Receiver { return &recorder{ch: ch} }, "nomw")

	e.Send(pid, "hello")
	require.Equal(t, "hello", mustReceive(t, ch))
}

// TestWithMiddlewareCopies verifies that the middleware slice is copied on the
// way in, so caller mutations afterwards cannot affect the actor's Opts.
func TestWithMiddlewareCopies(t *testing.T) {
	noop := func(next ReceiveFunc) ReceiveFunc {
		return func(c *Context) { next(c) }
	}
	extra := func(next ReceiveFunc) ReceiveFunc {
		return func(c *Context) { next(c) }
	}

	mws := []Middleware{noop}
	apply := WithMiddleware(mws...)

	opts := DefaultOpts(func() Receiver { return noopActor{} })
	apply(&opts)

	require.Len(t, opts.Middleware, 1, "must have exactly one middleware")

	// Mutate the caller's original slice.
	mws = append(mws, extra)

	require.Len(t, opts.Middleware, 1,
		"caller mutation must not leak into Opts.Middleware")
}

// TestWithMiddlewareEmpty clears previously set middleware when called with no
// arguments.
func TestWithMiddlewareEmpty(t *testing.T) {
	noop := func(next ReceiveFunc) ReceiveFunc {
		return func(c *Context) { next(c) }
	}

	opts := DefaultOpts(func() Receiver { return noopActor{} })
	WithMiddleware(noop)(&opts)
	require.Len(t, opts.Middleware, 1)

	WithMiddleware()(&opts)
	require.Nil(t, opts.Middleware, "WithMiddleware() must clear the slice")
}

// TestWithLoggingDefault verifies WithLogging(nil) uses slog.Default and does
// not panic.
func TestWithLoggingDefault(t *testing.T) {
	e := newTestEngine(t)
	pid := e.Spawn(
		func() Receiver { return &noopActor{} },
		"lognil",
		WithMiddleware(WithLogging(nil)),
	)

	e.Send(pid, "msg")
	// No panic = pass. The log output goes to stderr via slog.Default.
}

// TestWithLoggingCustom verifies WithLogging writes to the provided logger
// and includes the expected fields.
func TestWithLoggingCustom(t *testing.T) {
	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	mw := WithLogging(logger)
	wrapped := mw(func(c *Context) {
		// no-op receiver
	})

	// Build a context to exercise the middleware.
	pid := NewPID("local", "test-actor")
	ctx := newContext(&Engine{}, pid)
	ctx.withEnvelope(Envelope{Msg: "hello"})

	wrapped(ctx)

	out := buf.String()
	assert.True(t, strings.Contains(out, "actor receive"),
		"logged output must contain 'actor receive', got: %s", out)
	assert.True(t, strings.Contains(out, "local/test-actor"),
		"logged output must contain PID, got: %s", out)
}

// --- benchmarks ---

// BenchmarkReceiveNoMiddleware measures the hot path with zero middleware.
func BenchmarkReceiveNoMiddleware(b *testing.B) {
	p := newProcess(&Engine{}, Opts{
		Producer:  func() Receiver { return noopActor{} },
		ID:        "bench",
		InboxSize: 8,
	})
	p.Start()

	env := Envelope{Msg: "msg"}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.invokeMsg(env)
	}
}

// BenchmarkReceiveWithMiddleware measures the hot path with one no-op middleware.
func BenchmarkReceiveWithMiddleware(b *testing.B) {
	p := newProcess(&Engine{}, Opts{
		Producer:  func() Receiver { return noopActor{} },
		ID:        "bench",
		InboxSize: 8,
		Middleware: []Middleware{
			func(next ReceiveFunc) ReceiveFunc {
				return func(c *Context) { next(c) }
			},
		},
	})
	p.Start()

	env := Envelope{Msg: "msg"}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.invokeMsg(env)
	}
}
