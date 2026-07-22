package actor

import "sync"

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
func (p *process) Shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	p.stopped = true
	p.invokeMsg(Envelope{Msg: Stopped{}})
	_ = p.inbox.Stop()
	p.engine.unregisterProcess(p.pid)
}

// invokeMsg re-targets the single Context to e and calls Receive. Reusing one
// Context per actor keeps the delivery path allocation-free.
func (p *process) invokeMsg(e Envelope) {
	p.receiver.Receive(p.context.withEnvelope(e))
}
