package actor

import (
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

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
	// proc is the process this Context belongs to. It gives the child
	// helpers direct access to the process's child set without a registry
	// lookup on every call. It is set once, in newProcess, and never
	// changes; a Context built without a process (lower-level unit tests)
	// leaves it nil and the child helpers report no children.
	proc *process
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

// Respond sends msg back to whoever sent the message currently being
// handled — an ordinary Send to Context.Sender(), nothing more. It is a
// safe no-op when there is no sender, which happens whenever the current
// message was sent via Engine.Send instead of Engine.Request or
// Engine.SendWithSender.
func (c *Context) Respond(msg any) {
	if c.sender == nil {
		return
	}
	c.engine.Send(c.sender, msg)
}

// childIDCounter provides unique suffixes for auto-generated child IDs
// within a single parent. It is per-process, not global, so two parents
// never collide on auto-generated child IDs.
var childIDCounter uint64

// SpawnChild spawns a child actor under this actor. The child's PID is
// derived from the parent's PID via PID.Child, producing a nested ID of the
// form "parentKind/parentID/kind/id". The child's Context.Parent() returns
// this actor's PID.
func (c *Context) SpawnChild(p Producer, kind string, opts ...OptFunc) *PID {
	o := DefaultOpts(p)
	o.Kind = kind
	for _, opt := range opts {
		opt(&o)
	}
	if o.ID == "" {
		n := atomic.AddUint64(&childIDCounter, 1)
		o.ID = strconv.FormatUint(n, 10)
	}
	childID := kind + "/" + o.ID
	o.ID = c.pid.Child(childID).ID
	o.parent = c.pid
	proc := newProcess(c.engine, o)
	pid := c.engine.spawnProc(proc)
	// Register the child PID with the parent process so cascade shutdown works.
	if parentProc, ok := c.engine.registry.get(c.pid).(*process); ok && parentProc != nil {
		parentProc.addChild(pid)
	}
	return pid
}

// SpawnChildFunc spawns a child actor from a function. It is the child variant
// of Engine.SpawnFunc.
func (c *Context) SpawnChildFunc(f func(*Context), kind string, opts ...OptFunc) *PID {
	return c.SpawnChild(func() Receiver { return &funcReceiver{f: f} }, kind, opts...)
}

// Send routes msg to the actor addressed by to with this actor as the
// sender. That is the entire difference from Engine.Send, and it is what
// makes the receiving side's Respond work: the receiver sees c.Sender() as
// this actor and can answer it directly.
//
// It is safe to call during Initialized and Stopped as well as during
// normal message handling — the send goes through the engine, which handles
// unknown or already-stopped targets via the dead-letter path, never a
// panic.
func (c *Context) Send(to *PID, msg any) {
	c.engine.SendWithSender(to, msg, c.pid)
}

// Forward re-sends the message currently being handled to the actor
// addressed by to, preserving the ORIGINAL sender — it does not substitute
// this actor's PID. That is the difference between forwarding and
// re-sending: after a router Forwards a request to a worker, the worker's
// Respond reaches the original requester directly, not the router. Use
// c.Send(to, c.Message()) instead when the reply should come back to this
// actor.
func (c *Context) Forward(to *PID) {
	c.engine.SendWithSender(to, c.message, c.sender)
}

// Child returns the PID of a direct child by the short name it was spawned
// under — the ID passed to SpawnChild (via WithID, or the auto-generated
// suffix), not the fully qualified PID string. "kind/id" is also accepted
// to disambiguate two children with the same ID under different kinds. It
// returns nil when no such child exists — never a panic.
//
// During Initialized the actor has not spawned anything yet, so Child
// always returns nil there.
func (c *Context) Child(id string) *PID {
	if c.proc == nil {
		return nil
	}
	qualified := c.pid.Child(id).ID
	for _, pid := range c.proc.childPIDs() {
		if pid.ID == qualified || shortName(pid.ID) == id {
			return pid
		}
	}
	return nil
}

// Children returns a snapshot of the PIDs of every direct child, in no
// particular order. The slice is a fresh copy taken under the process's
// lock: mutating it cannot corrupt the actor's child set. During
// Initialized and Stopped it is simply empty.
func (c *Context) Children() []*PID {
	if c.proc == nil {
		return nil
	}
	return c.proc.childPIDs()
}

// SendRepeat sends msg to the actor addressed by to immediately and then
// again every interval, until the returned SendRepeater is stopped. It is
// sugar over Engine.SendRepeat, tied to this actor only in that the actor
// typically stores the repeater and stops it on Stopped.
func (c *Context) SendRepeat(to *PID, msg any, interval time.Duration) *SendRepeater {
	return c.engine.SendRepeat(to, msg, interval)
}

// shortName returns the last path segment of a registry ID: "kind/id"
// yields "id". It is the name a parent uses to find a child via Child.
func shortName(id string) string {
	if i := strings.LastIndexByte(id, '/'); i >= 0 {
		return id[i+1:]
	}
	return id
}
