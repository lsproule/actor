package actor

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"
)

// poisonPill is the stop request in message form. It is deliberately
// unexported: it travels an actor's ordinary inbox but is engine plumbing, and
// process.Invoke intercepts it before the user's Receive is ever reached.
//
// cancel is the waiter's context.CancelFunc, fired once teardown is complete.
// graceful records that the queued work ahead of the pill was allowed to
// finish; it is always true today and exists as the seam for the abrupt-stop
// variant that a later issue may add.
type poisonPill struct {
	cancel   context.CancelFunc
	graceful bool
}

// LocalLookupAddr is the address every actor in this in-process engine is
// reachable under. Remote addressing is out of scope for the proof of concept,
// so a single well-known address keeps PID.String stable and lets the registry
// key on the ID alone.
const LocalLookupAddr = "local"

// pidCounter hands out a process-unique, monotonically increasing suffix for
// auto-generated actor IDs. It is read and written only through sync/atomic so
// concurrent Spawns neither collide nor race.
var pidCounter uint64

// EngineConfig configures a new Engine. It is intentionally empty today:
// NewEngine already returns an error so that future configuration — a logger,
// middleware, a custom registry — can be added and fail without touching a
// single existing call site.
type EngineConfig struct{}

// NewEngineConfig returns the default EngineConfig. Callers build on top of the
// returned value so that fields added later inherit sensible zero defaults.
func NewEngineConfig() EngineConfig {
	return EngineConfig{}
}

// Engine is the core runtime: it hands out PIDs, builds a process per actor,
// keeps them in a registry, and routes Send. Everything it owns is safe for
// concurrent use; Spawn and Send may be called from any goroutine, including
// from inside an actor's Receive.
type Engine struct {
	registry *registry
	address  string
	events   *eventStream
}

// NewEngine builds an Engine from cfg. It returns an error even though nothing
// can fail yet: later configuration (logger, middleware) will be fallible and
// the callers are already written to check the error.
func NewEngine(cfg EngineConfig) (*Engine, error) {
	e := &Engine{
		address: LocalLookupAddr,
		events:  newEventStream(),
	}
	e.registry = newRegistry(e)
	return e, nil
}

// Spawn builds an actor from p, brings it fully to life, and returns its PID.
// It is synchronous with respect to reachability: by the time Spawn returns the
// actor is registered and has already processed Initialized and Started, so a
// Send on the very next line is guaranteed to land.
func (e *Engine) Spawn(p Producer, kind string, opts ...OptFunc) *PID {
	o := DefaultOpts(p)
	o.Kind = kind
	for _, opt := range opts {
		opt(&o)
	}
	o.ID = e.buildID(kind, o.ID)
	return e.spawnProc(newProcess(e, o))
}

// SpawnFunc spawns an actor whose behaviour is a single function, so a
// three-line actor needs no named type. The function is wrapped in an internal
// Receiver on every (re)start, matching the Producer contract.
func (e *Engine) SpawnFunc(f func(*Context), kind string, opts ...OptFunc) *PID {
	return e.Spawn(func() Receiver { return &funcReceiver{f: f} }, kind, opts...)
}

// SpawnProc registers and starts an already-built Processer. It is the escape
// hatch for custom process implementations; Spawn and SpawnFunc are the common
// path. The Processer owns its own mailbox and lifecycle.
func (e *Engine) SpawnProc(p Processer) *PID {
	if pr, ok := p.(*process); ok {
		pr.inbox.Start(pr)
	}
	p.Start()
	// A *process self-registers inside Start, so add is then a no-op; a custom
	// Processer that does not self-register is added to the registry here.
	e.registry.add(p)
	return p.PID()
}

// spawnProc binds a freshly built process to its inbox, starts it
// (Initialized -> register -> Started, all synchronously), and returns its PID.
// If an actor with the same ID already exists it keeps the incumbent and
// returns its PID, because silently orphaning a live inbox is worse than
// rejecting a duplicate spawn. Auto-generated IDs never collide, so this only
// affects callers that force a duplicate explicit ID via WithID.
func (e *Engine) spawnProc(proc *process) *PID {
	if existing := e.registry.getByID(proc.pid.ID); existing != nil {
		return existing.PID()
	}
	proc.inbox.Start(proc)
	proc.Start()
	return proc.pid
}

// Send routes msg to the actor addressed by pid with no sender. It is safe to
// call from any goroutine, including from inside a Receive, and never blocks:
// the target inbox grows rather than blocking on a full buffer. A send to an
// unknown PID is dropped through sendMiss without panicking.
func (e *Engine) Send(pid *PID, msg any) {
	e.sendMessage(pid, msg, nil)
}

// SendWithSender is Send with an explicit sender PID, so the receiver can reply
// via Context.Sender.
func (e *Engine) SendWithSender(pid *PID, msg any, sender *PID) {
	e.sendMessage(pid, msg, sender)
}

// Request sends msg to pid and returns a *Response immediately, without
// blocking. It gives callers ask-semantics on top of the fire-and-forget
// Send: a tiny throwaway actor is spawned to be the reply address, msg is
// sent with that actor as the sender (via SendWithSender), and a goroutine
// is started that waits for the reply — or the timeout — and then tears the
// throwaway actor down.
//
// The responding actor answers with an ordinary Context.Respond(x), which is
// just a Send back to Context.Sender(). Result on the returned Response
// blocks until that reply arrives or timeout elapses.
func (e *Engine) Request(pid *PID, msg any, timeout time.Duration) *Response {
	replyCh := make(chan any, 1)
	respPID := e.SpawnFunc(func(c *Context) {
		switch c.Message().(type) {
		case Initialized, Started, Stopped:
			return
		}
		select {
		case replyCh <- c.Message():
		default:
			// A second delivery to a one-shot reply address is unexpected;
			// drop it rather than block the responder's own inbox.
		}
	}, "response")

	e.SendWithSender(pid, msg, respPID)

	resp := &Response{
		engine: e,
		pid:    respPID,
		done:   make(chan struct{}),
	}
	go resp.await(replyCh, timeout)
	return resp
}

// Poison asks the actor at pid to stop once it has finished everything already
// in its mailbox, and returns a context that is cancelled when the actor is
// gone. It never blocks.
//
// The trick is that the stop request is itself a message: Poison builds a
// cancellable context and sends a poisonPill through the very same inbox as
// every other message, so ordering comes for free. Anything sent before the
// pill is processed first; the actor is torn down only when the pill is
// finally reached. There is no separate control channel to keep in step with
// the mailbox, because there is no separate control channel.
//
// Messages sent *after* the pill is queued are never processed. They may be
// pushed into an inbox that is about to stop and are silently abandoned there.
// That is accepted in this proof of concept; issue #15 may route them to dead
// letters instead.
//
// Poisoning an unknown PID, or an actor that has already stopped, returns an
// already-cancelled context, so <-e.Poison(pid).Done() returns immediately
// rather than hanging on a stop that will never happen.
func (e *Engine) Poison(pid *PID) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	e.poison(pid, cancel)
	return ctx
}

// PoisonAfter schedules a Poison of pid to be sent d from now and returns the
// waiter's context immediately — the caller is never blocked, and may wait on
// Done or ignore it entirely. If the actor has already stopped by the time the
// timer fires, the delayed Poison finds a registry miss and simply cancels the
// context; it never panics.
func (e *Engine) PoisonAfter(pid *PID, d time.Duration) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(d, func() { e.poison(pid, cancel) })
	return ctx
}

// Stop is an alias of Poison, for callers who find the name plainer. The
// semantics are identical: queued work runs, then the actor stops.
func (e *Engine) Stop(pid *PID) context.Context {
	return e.Poison(pid)
}

// poison is the shared body of Poison and PoisonAfter. It registers cancel
// with the target process and queues the pill, or — if there is no live actor
// to receive it — cancels straight away so the caller is never left waiting.
//
// Registering the cancel with the process, rather than relying only on the
// copy carried inside the pill, is what makes concurrent poisons safe: many
// callers may queue a pill before the first one is handled, and only one
// teardown will ever run. That teardown cancels every waiter it was handed.
func (e *Engine) poison(pid *PID, cancel context.CancelFunc) {
	if e.registry == nil {
		cancel()
		return
	}
	proc, ok := e.registry.get(pid).(*process)
	if !ok || proc == nil {
		cancel()
		return
	}
	if !proc.addCancel(cancel) {
		// The actor tore down between the lookup and here; nobody else will
		// ever call this cancel, so it is ours to fire.
		cancel()
		return
	}
	proc.deliver(Envelope{Msg: poisonPill{cancel: cancel, graceful: true}})
}

// Subscribe adds pid to the event stream. From here on the actor receives
// every BroadcastEvent as an ordinary message in its inbox, handled by its
// Receive like anything else. Subscribing an already-subscribed PID changes
// nothing: events are still delivered exactly once per subscriber.
func (e *Engine) Subscribe(pid *PID) {
	e.events.subscribe(pid)
}

// Unsubscribe removes pid from the event stream. Events broadcast after it
// returns are not delivered to pid; an event already in the actor's inbox
// still is. Unsubscribing a PID that never subscribed is a no-op.
//
// It is safe to call from inside a Receive, including while handling a
// broadcast event.
func (e *Engine) Unsubscribe(pid *PID) {
	e.events.unsubscribe(pid)
}

// BroadcastEvent sends msg to every current subscriber and returns without
// waiting for any of them: delivery is an ordinary Send per subscriber, so a
// slow subscriber never holds up the broadcaster or the other subscribers.
//
// Subscribers that have stopped are skipped and dropped from the stream, so
// a dead subscriber neither breaks the broadcast nor lingers.
func (e *Engine) BroadcastEvent(msg any) {
	e.events.broadcast(e, msg)
}

// Address returns the address this engine is reachable under.
func (e *Engine) Address() string {
	return e.address
}

// Registry exposes the process registry. It is used by the test suite and by
// intra-package callers that need direct lookups.
func (e *Engine) Registry() *registry {
	return e.registry
}

// buildID produces the registry key for a new actor. An explicit id yields a
// stable "kind/id"; an empty id gets a process-unique atomic suffix so two
// Spawns of the same kind can never collide.
func (e *Engine) buildID(kind, id string) string {
	if id != "" {
		return kind + "/" + id
	}
	n := atomic.AddUint64(&pidCounter, 1)
	return kind + "/" + strconv.FormatUint(n, 10)
}

// sendMessage is the single inbound routing path. It looks the target up in the
// registry and pushes an Envelope into its inbox. A registry miss is isolated
// in sendMiss so issue #15 has one seam to turn it into a DeadLetter event.
func (e *Engine) sendMessage(to *PID, msg any, sender *PID) {
	// A registry-less engine (the bare &Engine{} used by lower-level unit
	// tests) has nothing to route to; treat every send as a miss.
	if e.registry == nil {
		e.sendMiss(to, msg, sender)
		return
	}
	proc := e.registry.get(to)
	if proc == nil {
		e.sendMiss(to, msg, sender)
		return
	}
	if p, ok := proc.(*process); ok {
		p.deliver(Envelope{Msg: msg, Sender: sender})
	}
}

// sendMiss handles a send to an unknown or already-stopped PID. Today it is a
// silent, non-blocking drop; issue #15 turns it into a DeadLetter event. It is
// deliberately the only place a miss is observed so that issue has a single
// hook to reach.
func (e *Engine) sendMiss(pid *PID, msg any, sender *PID) {
	// Intentionally empty until issue #15 wires DeadLetter emission here.
}

// registerProcess adds a process to the registry. It is the hook process.Start
// calls once the actor has processed Initialized and is ready to be reachable.
// On an ID collision the incumbent is kept: replacing a live actor would orphan
// its inbox, so add reports the collision and the running actor stays put.
func (e *Engine) registerProcess(p Processer) {
	// The bare &Engine{} used by lower-level unit tests has no registry; those
	// tests drive a process directly and never rely on reachability, so skip.
	if e.registry == nil {
		return
	}
	e.registry.add(p)
}

// unregisterProcess removes a process from the registry. It is the hook
// process.Shutdown calls after delivering Stopped.
func (e *Engine) unregisterProcess(pid *PID) {
	if e.registry == nil {
		return
	}
	e.registry.remove(pid)
}

// deliver pushes env into this process's inbox. It is the engine's inbound
// hand-off point; the inbox schedules the actor's single processor goroutine
// and grows rather than blocking when full.
func (p *process) deliver(env Envelope) {
	p.inbox.Send(env)
}

// funcReceiver adapts a plain func(*Context) to the Receiver interface so
// SpawnFunc callers need not declare a type. A fresh one is produced on every
// (re)start, matching the Producer contract that each incarnation starts clean.
type funcReceiver struct {
	f func(*Context)
}

// Receive delegates to the wrapped function.
func (r *funcReceiver) Receive(c *Context) { r.f(c) }
