package actor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContextAccessors(t *testing.T) {
	pid := NewPID("local", "worker-1")
	engine := &Engine{}
	receiver := &testContextReceiver{}
	ctx := newContext(engine, pid)
	ctx.parent = NewPID("local", "parent-1")
	ctx.receiver = receiver

	msg := "hello"
	sender := NewPID("local", "sender-1")
	env := Envelope{Msg: msg, Sender: sender}

	result := ctx.withEnvelope(env)

	// Same pointer — no per-message allocation
	assert.Same(t, ctx, result, "withEnvelope must return the same pointer")

	assert.Equal(t, msg, ctx.Message())
	assert.Equal(t, sender, ctx.Sender())
	assert.Equal(t, pid, ctx.PID())
	assert.Equal(t, "parent-1", ctx.Parent().ID)
	assert.Equal(t, engine, ctx.Engine())
	assert.Equal(t, receiver, ctx.Receiver())
}

func TestContextNoSender(t *testing.T) {
	pid := NewPID("local", "worker-1")
	engine := &Engine{}
	ctx := newContext(engine, pid)
	ctx.receiver = &testContextReceiver{}

	// Envelope with nil sender (fire-and-forget)
	ctx.withEnvelope(Envelope{Msg: "fire-and-forget"})

	// Sender() must return nil, NOT panic
	assert.Nil(t, ctx.Sender(), "Sender() must be nil for fire-and-forget messages")
}

func TestContextReuse(t *testing.T) {
	pid := NewPID("local", "worker-1")
	engine := &Engine{}
	ctx := newContext(engine, pid)
	ctx.receiver = &testContextReceiver{}

	// First message: withEnvelope returns the same pointer each time
	first := ctx.withEnvelope(Envelope{
		Msg:    "first",
		Sender: NewPID("local", "alpha"),
	})
	assert.Equal(t, "first", ctx.Message())
	assert.Equal(t, "alpha", ctx.Sender().ID)

	// Second message on the same *Context — fields are overwritten
	second := ctx.withEnvelope(Envelope{
		Msg:    "second",
		Sender: NewPID("local", "beta"),
	})
	assert.Equal(t, "second", ctx.Message(), "second message must be visible")
	assert.Equal(t, "beta", ctx.Sender().ID)

	// Same pointer both times — zero allocations per message
	assert.Same(t, first, second, "both withEnvelope calls must return the same pointer")
}

func TestContextParentNil(t *testing.T) {
	pid := NewPID("local", "root")
	engine := &Engine{}
	ctx := newContext(engine, pid)
	ctx.receiver = &testContextReceiver{}

	// Root actor — no parent set
	ctx.withEnvelope(Envelope{Msg: "root-msg"})
	assert.Nil(t, ctx.Parent(), "root actor must have nil Parent")
}

// --- helpers ---

type testContextReceiver struct{}

func (r *testContextReceiver) Receive(c *Context) {}
