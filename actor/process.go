package actor

import (
	"bytes"
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
	// restarts counts restart attempts so far. It is only ever mutated via
	// sync/atomic: invokeMsg's recover can fire on the actor's own goroutine,
	// which is the only writer, but tests read it from the outside.
	restarts int32
}

// compile-time proof that process satisfies the engine-facing Processer
var _ Processer = (*process)(nil)

// newProcess builds a process for engine e from opts. The Context is
// allocated once here — never per message — and the inbox is sized from
// opts.InboxSize.
func newProcess(e *Engine, opts Opts) *process {
	pid := NewPID(localAddress, opts.ID)
	return &process{
		Opts:    opts,
		engine:  e,
		pid:     pid,
		inbox:   NewInbox(opts.InboxSize),
		context: newContext(e, pid),
	}
}

// PID returns the address of the actor this process drives.
func (p *process) PID() *PID { return p.pid }

// Start brings the actor to life in the exact order later issues depend on:
// produce the Receiver, deliver Initialized while still unreachable, register
// so the PID becomes addressable, then deliver Started.
func (p *process) Start() {
	p.receiver = p.Producer()
	p.context.receiver = p.receiver
	// Initialized before registration — the actor is not reachable yet
	p.invokeMsg(Envelope{Msg: Initialized{}})
	// registry insertion; #7 builds it and #8 wires this hook
	p.engine.registerProcess(p)
	// Started after registration — user messages may follow immediately
	p.invokeMsg(Envelope{Msg: Started{}})
}

// Send is the outbound path used by Context helpers later. It routes msg from
// sender to the actor addressed by to, through the engine. #8 wires the real
// send path; today the engine hook is a stub.
func (p *process) Send(to *PID, msg any, sender *PID) {
	p.engine.sendMessage(to, msg, sender)
}

// Invoke walks the batch and delivers each envelope through the single
// Context, never allocating a Context per message.
func (p *process) Invoke(msgs []Envelope) {
	for i := range msgs {
		p.invokeMsg(msgs[i])
	}
}

// Shutdown delivers Stopped, stops the inbox, and removes the actor from the
// registry — exactly once. A second call returns without delivering Stopped
// again.
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
	p.mu.Unlock()

	p.invokeMsg(Envelope{Msg: Stopped{}})
	_ = p.inbox.Stop()
	p.engine.unregisterProcess(p.pid)
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
		// TODO(#15): broadcast ActorStoppedEvent here
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
		// TODO(#15): broadcast ActorStoppedEvent here
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

	// This sleep runs on the actor's own inbox-processor goroutine, not the
	// engine's: every actor has its own single-consumer processor (see
	// inbox.go), so blocking here only delays this actor's own next message.
	time.Sleep(p.Opts.RestartDelay)

	// TODO(#15): broadcast ActorRestartedEvent here
	// Same *process, same PID, same inbox: Start() below produces a fresh
	// Receiver and replays Initialized -> Started. registerProcess will try
	// to re-add this exact *process under a PID already in the registry;
	// registry.add tolerates that specific case (see its comment) instead of
	// treating it as a collision.
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
