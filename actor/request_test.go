package actor

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// echoReply is what echoResponder answers with, wrapping the original
// message so tests can assert the round trip rather than just "any reply".
type echoReply struct{ value any }

// echoResponder answers every user message with an echoReply via
// Context.Respond, exercising the ordinary Send-back-to-Sender path.
type echoResponder struct{}

func (echoResponder) Receive(c *Context) {
	switch c.Message().(type) {
	case Initialized, Started, Stopped:
		return
	}
	c.Respond(echoReply{value: c.Message()})
}

func spawnEchoResponder(e *Engine) *PID {
	return e.Spawn(func() Receiver { return echoResponder{} }, "echo")
}

// spawnSilentActor spawns an actor that never replies to anything, so tests
// can exercise the timeout path.
func spawnSilentActor(e *Engine) *PID {
	return e.Spawn(func() Receiver { return noop{} }, "silent")
}

// TestRequestReceivesReply is the golden path: a reply lands and Result
// returns it with no error.
func TestRequestReceivesReply(t *testing.T) {
	e := newTestEngine(t)
	pid := spawnEchoResponder(e)

	resp := e.Request(pid, "ping", time.Second)
	msg, err := resp.Result()

	require.NoError(t, err)
	assert.Equal(t, echoReply{value: "ping"}, msg)
}

// TestRequestReturnsImmediately pins the non-blocking contract: Request
// itself must not wait on the reply, only Result does.
func TestRequestReturnsImmediately(t *testing.T) {
	e := newTestEngine(t)
	pid := spawnEchoResponder(e)

	start := time.Now()
	resp := e.Request(pid, "ping", time.Second)
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 100*time.Millisecond, "Request must return before any reply can arrive")

	_, err := resp.Result()
	require.NoError(t, err)
}

// TestRequestTimeout covers a responder that never answers: Result must
// unblock with an error identifiable via errors.Is, not hang forever.
func TestRequestTimeout(t *testing.T) {
	e := newTestEngine(t)
	pid := spawnSilentActor(e)

	resp := e.Request(pid, "ping", 20*time.Millisecond)
	msg, err := resp.Result()

	assert.Nil(t, msg)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrRequestTimeout), "timeout error must satisfy errors.Is(err, ErrRequestTimeout)")
}

// TestRequestReplyCleansUpResponder asserts the temporary reply actor is
// gone from the registry once a reply has landed and Result has returned.
func TestRequestReplyCleansUpResponder(t *testing.T) {
	e := newTestEngine(t)
	pid := spawnEchoResponder(e)
	baseline := e.Registry().len()

	resp := e.Request(pid, "ping", time.Second)
	_, err := resp.Result()
	require.NoError(t, err)

	assert.Equal(t, baseline, e.Registry().len(), "the temporary responder must be gone once a reply lands")
}

// TestRequestTimeoutCleansUpResponder is the same assertion on the timeout
// path: a reply that never comes must not leave the responder registered.
func TestRequestTimeoutCleansUpResponder(t *testing.T) {
	e := newTestEngine(t)
	pid := spawnSilentActor(e)
	baseline := e.Registry().len()

	resp := e.Request(pid, "ping", 20*time.Millisecond)
	_, err := resp.Result()
	require.Error(t, err)

	assert.Equal(t, baseline, e.Registry().len(), "the temporary responder must be gone once the request times out")
}

// TestResultSecondCall asserts Result is safe and idempotent to call more
// than once, on both the reply and the timeout path.
func TestResultSecondCall(t *testing.T) {
	t.Run("reply", func(t *testing.T) {
		e := newTestEngine(t)
		pid := spawnEchoResponder(e)

		resp := e.Request(pid, "ping", time.Second)
		firstMsg, firstErr := resp.Result()
		secondMsg, secondErr := resp.Result()

		assert.Equal(t, firstMsg, secondMsg)
		assert.Equal(t, firstErr, secondErr)
	})

	t.Run("timeout", func(t *testing.T) {
		e := newTestEngine(t)
		pid := spawnSilentActor(e)

		resp := e.Request(pid, "ping", 20*time.Millisecond)
		_, firstErr := resp.Result()
		_, secondErr := resp.Result()

		assert.True(t, errors.Is(firstErr, ErrRequestTimeout))
		assert.True(t, errors.Is(secondErr, ErrRequestTimeout))
	})
}

// TestContextRespondNoSenderIsNoop covers the fire-and-forget case: a
// message with no sender must make Respond a harmless no-op, never a panic.
func TestContextRespondNoSenderIsNoop(t *testing.T) {
	e := newTestEngine(t)
	ctx := newContext(e, NewPID(LocalLookupAddr, "probe"))
	ctx.withEnvelope(Envelope{Msg: "hello"}) // no Sender

	assert.NotPanics(t, func() { ctx.Respond("reply") })
}

// TestContextRespondSendsToSender exercises Respond end to end through the
// engine: a real sender actor should receive exactly what was Respond-ed.
func TestContextRespondSendsToSender(t *testing.T) {
	e := newTestEngine(t)
	rec := &recorder{ch: make(chan any, 1)}
	senderPID := e.Spawn(func() Receiver { return rec }, "sender")

	responderPID := e.SpawnFunc(func(c *Context) {
		switch c.Message().(type) {
		case Initialized, Started, Stopped:
			return
		}
		c.Respond("pong")
	}, "responder")

	e.SendWithSender(responderPID, "ping", senderPID)

	assert.Equal(t, "pong", mustReceive(t, rec.ch))
}

// TestRequestFromInsideReceive is the deadlock-avoidance guarantee: calling
// Request from inside an actor's own Receive must not block that actor,
// because Request itself never waits on the reply.
func TestRequestFromInsideReceive(t *testing.T) {
	e := newTestEngine(t)
	echoPID := spawnEchoResponder(e)

	replies := make(chan any, 1)
	caller := e.SpawnFunc(func(c *Context) {
		switch c.Message().(type) {
		case Initialized, Started, Stopped:
			return
		}
		// Fire the request and immediately hand the Response off to a
		// goroutine instead of blocking here — this is the pattern the
		// package documents as safe from inside Receive.
		resp := c.Engine().Request(echoPID, "hi", time.Second)
		go func() {
			msg, err := resp.Result()
			if err == nil {
				replies <- msg
			}
		}()
	}, "caller")

	e.Send(caller, "go")

	assert.Equal(t, echoReply{value: "hi"}, mustReceive(t, replies))
}

// TestRequestLeak10k is the acceptance criterion's leak test: 10k requests,
// half of them timing out, must leave the registry at its starting size
// once every Result has returned.
func TestRequestLeak10k(t *testing.T) {
	e := newTestEngine(t)
	echoPID := spawnEchoResponder(e)
	silentPID := spawnSilentActor(e)
	baseline := e.Registry().len()

	const n = 10_000
	responses := make([]*Response, n)
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			responses[i] = e.Request(echoPID, "ping", time.Second)
		} else {
			responses[i] = e.Request(silentPID, "ping", 10*time.Millisecond)
		}
	}

	for _, resp := range responses {
		resp.Result()
	}

	assert.Equal(t, baseline, e.Registry().len(), "registry must return to its starting size after 10k requests, half of them timing out")
}
