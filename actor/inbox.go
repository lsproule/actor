package actor

import (
	"runtime"
	"sync/atomic"

	"github.com/lsproule/actor/ringbuffer"
)

// defaultThroughput is the maximum number of messages a single processor
// goroutine drains from the inbox before yielding with runtime.Gosched().
// A bounded batch keeps one hot actor from starving the rest of the runtime
// while still amortising the wake-up cost over many messages.
const defaultThroughput = 300

// procStatus values for the idle/running/stopped flag on Inbox. The flag is
// the entire mutual-exclusion story between concurrent Sends: only the
// goroutine that flips idle→running spawns a processor, so at most one
// processor goroutine is ever alive per inbox.
const (
	stateIdle    int32 = 0
	stateRunning int32 = 1
	stateStopped int32 = 2
)

// Processer is what an inbox feeds batches of envelopes to. The full
// interface — including lifecycle, panic recovery, and restarts — arrives
// in issue #6; this minimal version is enough for the inbox to schedule
// against. Invoke is called with a freshly-allocated slice of envelopes
// taken from the ring buffer and may be called from any goroutine the
// inbox decides to run on, but never from more than one at a time.
type Processer interface {
	Invoke(batch []Envelope)
}

// Inboxer is the contract every inbox implementation satisfies. Engine
// code depends on this interface so alternative mailbox implementations
// (e.g. priority inboxes, bounded inboxes) can be swapped in without
// touching call sites.
type Inboxer interface {
	Send(Envelope)
	Start(Processer)
	Stop() error
}

// Inbox is a single-consumer scheduler on top of a lock-free MPSC ring
// buffer. It owns the rule that makes the whole actor model safe: at
// most one goroutine processes an inbox's messages at a time. It also
// owns the idle/park story: an idle inbox holds no goroutine, the first
// Send starts one, and the processor returns to a goroutine-less state
// after it has drained the buffer.
//
// The processor drains up to throughput messages per batch and calls
// runtime.Gosched() between batches so a single hot actor cannot starve
// other goroutines — including other actors.
type Inbox struct {
	rb         *ringbuffer.RingBuffer[Envelope]
	proc       Processer
	throughput int

	// procStatus encodes idle/running/stopped and is the only field that
	// crosses goroutine boundaries outside of the ring buffer itself. It
	// is always read and written through sync/atomic; the rest of Inbox
	// is owned by either a single processor goroutine or the constructor.
	procStatus int32
}

// NewInbox allocates an Inbox with a ring buffer sized to hold size
// messages initially. The size is rounded up to a power of two by the
// ring buffer and grows on demand, so it is a starting hint and not a
// hard limit.
func NewInbox(size int) *Inbox {
	return &Inbox{
		rb:         ringbuffer.New[Envelope](int64(size)),
		throughput: defaultThroughput,
	}
}

// Send delivers msg to the inbox. It is safe to call from any number of
// goroutines. If the inbox is idle, Send starts a processor goroutine
// via schedule(); otherwise the existing processor picks the message up
// on its next iteration.
func (in *Inbox) Send(msg Envelope) {
	in.rb.Push(msg)
	in.schedule()
}

// Start binds a Processer to the inbox. The inbox is otherwise idle —
// the first Send is what actually starts a processor goroutine.
func (in *Inbox) Start(proc Processer) {
	in.proc = proc
}

// Stop marks the inbox as stopped and returns nil. It is idempotent and
// safe to call concurrently with Send or while messages are in flight.
// The processor goroutine observes the stop state at the top of its
// loop and returns without panicking; Sends that land after Stop may
// still Push into the ring buffer, but schedule() will refuse to
// (re)start a processor and the messages will not be delivered. Issue
// #11 owns the drain-on-stop story.
func (in *Inbox) Stop() error {
	atomic.StoreInt32(&in.procStatus, stateStopped)
	return nil
}

// schedule starts a new processor goroutine iff the inbox is currently
// idle. The CAS on procStatus is the entire mutual-exclusion story
// between concurrent Sends: only the goroutine that flips idle→running
// gets to spawn the processor, so at most one processor goroutine is
// ever alive for this inbox. A stopped inbox refuses to (re)start
// because the CAS sees a state it does not match.
func (in *Inbox) schedule() {
	if atomic.CompareAndSwapInt32(&in.procStatus, stateIdle, stateRunning) {
		go in.process()
	}
}

// process is the single processing goroutine for this inbox. It loops
// draining the ring buffer in batches of up to throughput, then
// attempts to park by CAS-running→idle. The post-park re-check is the
// only thing that prevents a lost wakeup: a Send that lands in the
// window between the empty-buffer check in run() and the park CAS
// will be observed by the re-check and the processor resumes; if no
// Send lands, the processor returns and the inbox parks with no
// goroutine running at all.
func (in *Inbox) process() {
	for {
		in.run()

		// Try to park. If procStatus is not running any more we are
		// either stopped or the state was already changed by a Send;
		// either way, exit cleanly.
		if !atomic.CompareAndSwapInt32(&in.procStatus, stateRunning, stateIdle) {
			return
		}

		// Re-check the buffer. A Send that arrived between the empty
		// check inside run() and the CAS above has left a message
		// here; without this check that message would be stranded
		// until the next Send woke the inbox up — a lost wakeup.
		if in.rb.Len() == 0 {
			return
		}

		// A message is waiting. CAS idle→running to claim the
		// processor slot back; if a concurrent Send's schedule() has
		// already grabbed it, the CAS fails and we exit so the new
		// processor can take over without duplication.
		if !atomic.CompareAndSwapInt32(&in.procStatus, stateIdle, stateRunning) {
			return
		}
	}
}

// run drains the ring buffer in batches of up to throughput messages,
// handing each batch to the Processer and yielding with
// runtime.Gosched() between batches. The scheduler is observed between
// batches so a single busy inbox cannot starve other goroutines —
// including the processors of other actors that share the runtime.
// The scheduler is also re-checked on every batch, so a Stop that
// races with an in-flight batch will be honoured before the next batch
// is delivered.
//
// No lock is held across proc.Invoke: user code may block or panic,
// and the inbox must not be the bottleneck of the runtime while it
// runs.
func (in *Inbox) run() {
	for {
		if atomic.LoadInt32(&in.procStatus) == stateStopped {
			return
		}
		batch, ok := in.rb.PopN(int64(in.throughput))
		if !ok {
			return
		}
		in.proc.Invoke(batch)
		runtime.Gosched()
	}
}
