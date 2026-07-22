package actor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingActor records the kind, message and Context pointer of every
// delivery so tests can assert lifecycle ordering, batch order, and that a
// single Context is reused across messages.
type recordingActor struct {
	kinds   []string
	msgs    []any
	senders []*PID
	ctxs    []*Context
}

var _ Receiver = (*recordingActor)(nil)

func (a *recordingActor) Receive(c *Context) {
	a.ctxs = append(a.ctxs, c)
	a.msgs = append(a.msgs, c.Message())
	a.senders = append(a.senders, c.Sender())
	switch c.Message().(type) {
	case Initialized:
		a.kinds = append(a.kinds, "Initialized")
	case Started:
		a.kinds = append(a.kinds, "Started")
	case Stopped:
		a.kinds = append(a.kinds, "Stopped")
	default:
		a.kinds = append(a.kinds, "user")
	}
}

func newTestProcess(a *recordingActor) *process {
	return newProcess(&Engine{}, Opts{
		Producer:  func() Receiver { return a },
		ID:        "worker-1",
		InboxSize: 8,
	})
}

func TestProcessLifecycleOrder(t *testing.T) {
	a := &recordingActor{}
	p := newTestProcess(a)

	p.Start()
	p.Invoke([]Envelope{{Msg: "hello"}})
	p.Shutdown()

	assert.Equal(t, []string{"Initialized", "Started", "user", "Stopped"}, a.kinds)
}

func TestProcessInvokeBatch(t *testing.T) {
	a := &recordingActor{}
	p := newTestProcess(a)
	p.Start()

	batch := make([]Envelope, 5)
	for i := range batch {
		batch[i] = Envelope{Msg: i}
	}
	p.Invoke(batch)

	// Start delivered Initialized + Started; the 5 user messages follow.
	userMsgs := a.msgs[2:]
	require.Len(t, userMsgs, 5)
	for i := 0; i < 5; i++ {
		assert.Equal(t, i, userMsgs[i], "batch must arrive in order")
	}
	for _, c := range a.ctxs {
		assert.Same(t, p.context, c, "every Receive must see the same *Context")
	}
}

func TestProcessShutdownIdempotent(t *testing.T) {
	a := &recordingActor{}
	p := newTestProcess(a)
	p.Start()

	p.Shutdown()
	p.Shutdown()

	stopped := 0
	for _, k := range a.kinds {
		if k == "Stopped" {
			stopped++
		}
	}
	assert.Equal(t, 1, stopped, "Stopped must be delivered exactly once")
}

func TestProcessNoAllocPerMessage(t *testing.T) {
	p := newProcess(&Engine{}, Opts{
		Producer:  func() Receiver { return noopActor{} },
		ID:        "bench",
		InboxSize: 8,
	})
	p.receiver = noopActor{}

	// pre-boxed message so the loop measures Context reuse, not interface boxing
	var msg any = &struct{ n int }{}
	env := Envelope{Msg: msg}

	allocs := testing.AllocsPerRun(1000, func() {
		p.invokeMsg(env)
	})

	assert.Zero(t, allocs, "invokeMsg must not allocate a Context per message")
}

func TestProcessSenderVisible(t *testing.T) {
	a := &recordingActor{}
	p := newTestProcess(a)
	p.Start()

	sender := NewPID("local", "sender-1")
	p.Invoke([]Envelope{{Msg: "ping", Sender: sender}})

	last := a.senders[len(a.senders)-1]
	require.NotNil(t, last, "sender must be visible inside Receive")
	assert.True(t, sender.Equals(last))
}

type noopActor struct{}

func (noopActor) Receive(*Context) {}
