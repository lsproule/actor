package actor

import (
	"bytes"
	"context"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// localAddress is the address every PID carries until issue #8 introduces
// engine identity and remote addressing.
const localAddress = "local"

// process is the default Processer: the glue between an inbox and a Receiver.
// It owns a single Context, reused for every delivery, and drives the actor's
// lifecycle. Its fields are owned by the one goroutine the inbox schedules,
// except the mutex-guarded shutdown flag which is the only cross-goroutine
// coordination point.
type process struct {
	Opts
	inbox    Inboxer
	context  *Context
	pid      *PID
	receiver Receiver
	engine   *Engine
	mu       sync.Mutex
	stopped  bool
	// cancels holds one context.CancelFunc per pending Poison waiter. It is a
	// slice and not a single func because any number of callers may poison the
	// same actor before the first pill is handled — every one of them is
	// holding a context.Context that must be cancelled by the single teardown
	// that actually runs. Guarded by mu together with stopped: claiming stopped
	// and snapshotting cancels under the same lock is what closes the window in
	// which a late Poison could register a cancel that nobody would ever call.
	cancels []context.CancelFunc
	// restarts counts restart attempts so far. It is only ever mutated via
	// sync/atomic: invokeMsg's recover can fire on the actor's own goroutine,
	// which is the only writer, but tests read it from the outside.
	restarts int32
	// children maps child PID IDs to their PIDs. Guarded by mu.
	children map[string]*PID
}

// compile-time proof that process satisfies the engine-facing Processer
var _ Processer = (*process)(nil)

// newProcess builds a process for engine e from opts. The Context is
// allocated once here — never per message — and the inbox is sized from
// opts.InboxSize.
func newProcess(e *Engine, opts Opts) *process {
	pid := NewPID(localAddress, opts.ID)
	ctx := newContext(e, pid)
	ctx.parent = opts.parent
	return &process{
		Opts:    opts,
		engine:  e,
		pid:     pid,
		inbox:   NewInbox(opts.InboxSize),
		context: ctx,
	}
}

// PID returns the address of the actor this process drives.
func (p *process) PID() *PID { return p.pid }

// addChild records pid as a child of this process. The caller must not hold
// p.mu; addChild takes it internally.
func (p *process) addChild(pid *PID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.children == nil {
		p.children = make(map[string]*PID)
	}
	p.children[pid.ID] = pid
}

// removeChild deletes pid from the child set. Safe to call even if the child
// was never added or was already removed.
func (p *process) removeChild(pid *PID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.children, pid.ID)
}

// childPIDs returns a snapshot of the current child PIDs. The caller must not
// hold p.mu when calling this method.
func (p *process) childPIDs() []*PID {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*PID, 0, len(p.children))
	for _, pid := range p.children {
		out = append(out, pid)
	}
	return out
}

// shutdownChildren poisons every child and waits for all of them to finish.
// It copies the child list before releasing the lock, then acts on the copy,
// so the parent's mutex is never held while waiting on children.
//
// This cannot deadlock: each child has its own goroutine, and the parent is
// running on its own goroutine. The parent blocks here waiting for children,
// but children process their own poison pills independently and recursively
// shut down their own children before signalling Done.
func (p *process) shutdownChildren() {
	pids := p.childPIDs()
	if len(pids) == 0 {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(pids))
	for _, child := range pids {
		child := child
		go func() {
			defer wg.Done()
			<-p.engine.Poison(child).Done()
		}()
	}
	wg.Wait()
}

// Start brings the actor to life in the exact order later issues depend on:
// produce the Receiver, deliver Initialized while still unreachable, register
// so the PID becomes addressable, then deliver Started.
func (p *process) Start() {
	p.receiver = p.Producer()
	p.context.receiver = p.receiver
	// Initialized before registration — the actor is not reachable yet
	p.invokeMsg(Envelope{Msg: Initialized{}})
	p.engine.BroadcastEvent(ActorInitializedEvent{PID: p.pid, Timestamp: time.Now()})
	// registry insertion; #7 builds it and #8 wires this hook
	p.engine.registerProcess(p)
	// Started after registration — user messages may follow immediately
	p.invokeMsg(Envelope{Msg: Started{}})
	p.engine.BroadcastEvent(ActorStartedEvent{PID: p.pid, Timestamp: time.Now()})
}

// Send is the outbound path used by Context helpers later. It routes msg from
// sender to the actor addressed by to, through the engine. #8 wires the real
// send path; today the engine hook is a stub.
func (p *process) Send(to *PID, msg any, sender *PID) {
	p.engine.sendMessage(to, msg, sender)
}

// Invoke walks the batch and delivers each envelope through the single
// Context, never allocating a Context per message.
//
// A poisonPill is intercepted here, before invokeMsg, so the user's Receive
// never sees it: it is engine plumbing, not a message the actor wrote code
// for. Everything queued ahead of the pill has already been delivered by the
// time it is reached — that is the whole point of routing the stop request
// through the mailbox — and everything behind it in this batch is abandoned
// along with the rest of the inbox.
func (p *process) Invoke(msgs []Envelope) {
	for i := range msgs {
		if pill, ok := msgs[i].Msg.(poisonPill); ok {
			p.handlePoison(pill)
			return
		}
		p.invokeMsg(msgs[i])
	}
}

// handlePoison runs the teardown a poisonPill asks for. It delegates to
// Shutdown so Stopped stays exactly-once even when several pills are queued,
// and cancels the pill's own context afterwards as a belt-and-braces measure:
// Shutdown already cancels every waiter it snapshotted, and CancelFunc is
// idempotent, so a pill whose cancel was claimed by an earlier teardown costs
// nothing here.
func (p *process) handlePoison(pill poisonPill) {
	p.shutdownChildren()
	p.Shutdown()
	if pill.cancel != nil {
		pill.cancel()
	}
}

// addCancel registers cancel to be called when this process tears down. It
// reports false if the actor has already stopped, in which case the caller
// owns the cancel and must fire it immediately — waiting on a context nobody
// will ever cancel is the one failure mode Poison must never produce.
//
// The check and the append happen under the same mutex Shutdown uses to claim
// the stopped flag, so there is no interleaving in which a cancel is both
// refused-by-nobody and never called: either it lands in the slice Shutdown
// goes on to snapshot, or it sees stopped and is handed back.
func (p *process) addCancel(cancel context.CancelFunc) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return false
	}
	p.cancels = append(p.cancels, cancel)
	return true
}

// Shutdown delivers Stopped, removes the actor from the registry, stops the
// inbox, and finally cancels every Poison waiter — exactly once. A second call
// returns without delivering Stopped again.
//
// The step order is part of the contract, not an implementation detail. The
// cancels fire last so that a caller woken by <-ctx.Done() finds an actor that
// is genuinely gone: the PID no longer resolves and the inbox is closed. Were
// cancel called first, the waiter could observe a PID that still routes.
//
// The stopped flag is claimed and the lock released *before* invokeMsg runs.
// If Receive panics while handling Stopped, invokeMsg's recover routes back
// through tryRestart, which — on a give-up path — calls Shutdown again from
// the very same goroutine. Holding the mutex across invokeMsg would make that
// re-entrant call block on its own lock forever; releasing it first lets the
// re-entrant call see stopped == true and return immediately instead.
func (p *process) Shutdown() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	// Take the waiters with the flag, under the one lock. Any Poison that
	// arrives from here on sees stopped and cancels its own context directly.
	cancels := p.cancels
	p.cancels = nil
	p.mu.Unlock()

	// If this process has a parent, remove ourselves from the parent's child
	// set so the parent does not leak map entries for dead children.
	if p.parent != nil {
		if parentProc, ok := p.engine.registry.get(p.parent).(*process); ok && parentProc != nil {
			parentProc.removeChild(p.pid)
		}
	}

	p.invokeMsg(Envelope{Msg: Stopped{}})
	p.engine.BroadcastEvent(ActorStoppedEvent{PID: p.pid, Timestamp: time.Now()})
	p.engine.unregisterProcess(p.pid)
	_ = p.inbox.Stop()

	for _, cancel := range cancels {
		cancel()
	}
}

// invokeMsg re-targets the single Context to e and calls Receive. Reusing one
// Context per actor keeps the delivery path allocation-free.
//
// The deferred recover lives here, at the single-message granularity, rather
// than around the whole batch in Invoke: a panic on message N must not cost
// messages N+1..end their chance to run against the restarted incarnation.
func (p *process) invokeMsg(e Envelope) {
	defer func() {
		if v := recover(); v != nil {
			p.tryRestart(v)
		}
	}()
	p.receiver.Receive(p.context.withEnvelope(e))
}

// tryRestart implements the "let it crash" supervision policy for a value
// recovered from a panic in Receive. It either restarts the actor with a
// fresh Receiver from the same Producer, under the same PID, or gives up and
// shuts the actor down for good — logging the decision either way via
// log/slog.
func (p *process) tryRestart(v any) {
	stack := p.cleanTrace(debug.Stack())

	if p.Opts.MaxRestarts == 0 {
		slog.Error("actor panic recovered, restarts disabled, shutting down",
			"pid", p.pid.String(),
			"panic", v,
			"stack", string(stack),
		)
		p.Shutdown()
		return
	}

	restarts := atomic.AddInt32(&p.restarts, 1)
	if restarts > p.Opts.MaxRestarts {
		slog.Error("actor restarts exhausted, shutting down",
			"pid", p.pid.String(),
			"restart", restarts,
			"max_restarts", p.Opts.MaxRestarts,
			"panic", v,
			"stack", string(stack),
		)
		p.Shutdown()
		return
	}

	slog.Error("actor panic recovered, restarting",
		"pid", p.pid.String(),
		"restart", restarts,
		"max_restarts", p.Opts.MaxRestarts,
		"panic", v,
		"stack", string(stack),
	)

	p.engine.BroadcastEvent(ActorRestartedEvent{
		PID:        p.pid,
		Timestamp:  time.Now(),
		Stacktrace: stack,
		Reason:     v,
		Restarts:   restarts,
	})

	// This sleep runs on the actor's own inbox-processor goroutine, not the
	// engine's: every actor has its own single-consumer processor (see
	// inbox.go), so blocking here only delays this actor's own next message.
	time.Sleep(p.Opts.RestartDelay)

	p.Start()
}

// cleanTrace trims the leading runtime frames from a debug.Stack() capture —
// the goroutine header noise down through the runtime.panic machinery — so
// the logged trace starts at the user's Receive call, the first frame anyone
// debugging a restart actually cares about. If the expected "panic(" frame
// marker is not found, the stack is returned unchanged rather than guessing.
func (p *process) cleanTrace(stack []byte) []byte {
	lines := bytes.Split(stack, []byte("\n"))
	if len(lines) == 0 {
		return stack
	}
	header := lines[0]

	for i, line := range lines {
		if !bytes.HasPrefix(line, []byte("panic(")) {
			continue
		}
		// line i is the "panic(...)" frame and i+1 is its file:line; the
		// user's own frame starts at i+2.
		if i+2 >= len(lines) {
			return stack
		}
		out := make([][]byte, 0, len(lines)-i-1)
		out = append(out, header)
		out = append(out, lines[i+2:]...)
		return bytes.Join(out, []byte("\n"))
	}
	return stack
}
