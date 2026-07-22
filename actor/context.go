package actor

// Context is the view an actor sees while handling a single message. It
// exposes the message itself, who sent it, and the actor's own identity,
// parent, engine, and receiver.
//
// A Context is allocated once per actor — not once per message — and
// re-targeted before each Receive call via withEnvelope. This keeps
// allocations off the hot path and reduces GC pressure.
//
// A *Context must not be retained past the Receive call that produced it,
// and must not be shared across goroutines. The engine reuses the same
// pointer for subsequent deliveries; keeping a reference after Receive
// returns means observing stale data.
type Context struct {
	pid      *PID
	parent   *PID
	engine   *Engine
	receiver Receiver
	message  any
	sender   *PID
	// children maps string names to child PIDs. It is declared now so that
	// the struct layout is stable; it will be populated by issue #12.
	children map[string]*PID
}

// newContext allocates a Context for the given engine and actor identity.
// It is called once per actor, at spawn time. The returned Context is
// re-used for every message the actor receives.
func newContext(engine *Engine, pid *PID) *Context {
	return &Context{
		pid:    pid,
		engine: engine,
	}
}

// withEnvelope re-targets this Context to describe a new delivery. It sets
// message and sender from e and returns the same pointer so the caller can
// prove no per-message allocation occurred.
//
// The process loop calls this before each Receive invocation.
func (c *Context) withEnvelope(e Envelope) *Context {
	c.message = e.Msg
	c.sender = e.Sender
	return c
}

// Message returns the message being delivered. It is never nil for a
// delivery produced by the engine.
func (c *Context) Message() any { return c.message }

// Sender returns the PID of the actor that sent this message, or nil when
// the message was sent without a sender (fire-and-forget from outside the
// engine). It never panics.
func (c *Context) Sender() *PID { return c.sender }

// PID returns this actor's own address.
func (c *Context) PID() *PID { return c.pid }

// Parent returns this actor's parent PID, or nil if this is a root actor
// spawned by the engine directly.
func (c *Context) Parent() *PID { return c.parent }

// Engine returns the engine that owns this actor.
func (c *Context) Engine() *Engine { return c.engine }

// Receiver returns the Receiver value this actor was spawned with.
func (c *Context) Receiver() Receiver { return c.receiver }
