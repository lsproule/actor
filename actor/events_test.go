package actor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// eventProbe captures every non-lifecycle message it receives (i.e., broadcast
// events) into a buffered channel so tests can assert on them.
type eventProbe struct {
	ch chan any
}

func (p *eventProbe) Receive(c *Context) {
	switch c.Message().(type) {
	case Initialized, Started, Stopped:
		return
	}
	p.ch <- c.Message()
}

func spawnEventProbe(e *Engine) (*PID, chan any) {
	ch := make(chan any, 1024)
	pid := e.Spawn(func() Receiver { return &eventProbe{ch: ch} }, "probe")
	e.Subscribe(pid)
	return pid, ch
}

// customDeadLetter records DeadLetterEvent deliveries onto a channel for
// inspection by TestCustomDeadLetter.
type customDeadLetter struct {
	ch chan DeadLetterEvent
}

func (d *customDeadLetter) Receive(c *Context) {
	switch c.Message().(type) {
	case Initialized, Started, Stopped:
		return
	case DeadLetterEvent:
		d.ch <- c.Message().(DeadLetterEvent)
	}
}

func TestLifecycleEventSequence(t *testing.T) {
	e := newTestEngine(t)
	_, ch := spawnEventProbe(e)

	testPID := e.Spawn(func() Receiver { return noop{} }, "test")
	<-e.Poison(testPID).Done()

	evt1 := mustReceive(t, ch).(ActorInitializedEvent)
	assert.True(t, testPID.Equals(evt1.PID))
	assert.False(t, evt1.Timestamp.IsZero())

	evt2 := mustReceive(t, ch).(ActorStartedEvent)
	assert.True(t, testPID.Equals(evt2.PID))
	assert.False(t, evt2.Timestamp.IsZero())

	evt3 := mustReceive(t, ch).(ActorStoppedEvent)
	assert.True(t, testPID.Equals(evt3.PID))
	assert.False(t, evt3.Timestamp.IsZero())
}

func TestRestartEvent(t *testing.T) {
	e := newTestEngine(t)
	_, ch := spawnEventProbe(e)

	pid := e.Spawn(func() Receiver {
		return &poisonThenRecordActor{}
	}, "restart-test", WithRestartDelay(time.Millisecond))

	e.Send(pid, "boom-trigger")

	timeout := time.After(deliverTimeout)
	for {
		select {
		case msg := <-ch:
			if evt, ok := msg.(ActorRestartedEvent); ok {
				assert.NotNil(t, evt.Reason, "Reason should be non-nil")
				assert.NotEmpty(t, evt.Stacktrace, "Stacktrace should be non-empty")
				assert.Equal(t, int32(1), evt.Restarts, "Restarts should be 1")
				assert.True(t, pid.Equals(evt.PID))
				assert.False(t, evt.Timestamp.IsZero())
				return
			}
		case <-timeout:
			t.Fatal("timed out waiting for ActorRestartedEvent")
		}
	}
}

func TestDeadLetterOnUnknownPID(t *testing.T) {
	e := newTestEngine(t)
	_, ch := spawnEventProbe(e)

	bogus := NewPID(LocalLookupAddr, "nope")
	e.Send(bogus, "hello")

	evt := mustReceive(t, ch).(DeadLetterEvent)
	assert.True(t, bogus.Equals(evt.Target))
	assert.Equal(t, "hello", evt.Message)
	assert.Nil(t, evt.Sender)
}

func TestDeadLetterAfterPoison(t *testing.T) {
	e := newTestEngine(t)
	_, ch := spawnEventProbe(e)

	testPID := e.Spawn(func() Receiver { return noop{} }, "test")
	<-e.Poison(testPID).Done()

	// Drain lifecycle events for the test actor: Init + Started + Stopped
	for i := 0; i < 3; i++ {
		<-ch
	}

	e.Send(testPID, "after-death")

	timeout := time.After(deliverTimeout)
	for {
		select {
		case msg := <-ch:
			if evt, ok := msg.(DeadLetterEvent); ok {
				assert.True(t, testPID.Equals(evt.Target))
				assert.Equal(t, "after-death", evt.Message)
				return
			}
		case <-timeout:
			t.Fatal("timed out waiting for DeadLetterEvent")
		}
	}
}

func TestDeadLetterNoInfiniteLoop(t *testing.T) {
	e := newTestEngine(t)
	bogus := NewPID(LocalLookupAddr, "nope")

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			e.Send(bogus, i)
		}
		e.Send(e.deadletter, "ping")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("infinite loop detected: dead letters caused recursive dead letters")
	}
}

func TestCustomDeadLetter(t *testing.T) {
	deadCh := make(chan DeadLetterEvent, 1)
	cfg := NewEngineConfig().WithDeadletter(func() Receiver {
		return &customDeadLetter{ch: deadCh}
	})
	e, err := NewEngine(cfg)
	require.NoError(t, err)

	bogus := NewPID(LocalLookupAddr, "nope")
	e.Send(bogus, "test-msg")

	select {
	case evt := <-deadCh:
		assert.True(t, bogus.Equals(evt.Target))
		assert.Equal(t, "test-msg", evt.Message)
		assert.Nil(t, evt.Sender)
	case <-time.After(deliverTimeout):
		t.Fatal("timed out waiting for DeadLetterEvent on custom dead-letter channel")
	}
}
