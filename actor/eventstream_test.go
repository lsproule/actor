package actor

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// streamRecorder forwards broadcast events to ch, skipping both the actor's
// own lifecycle messages and the events the engine itself emits (actor
// lifecycle, dead letters). These tests are about the fan-out mechanics, so a
// subscriber must only see what the test broadcast.
type streamRecorder struct {
	ch chan any
}

func (r *streamRecorder) Receive(c *Context) {
	switch c.Message().(type) {
	case Initialized, Started, Stopped:
		return
	case ActorInitializedEvent, ActorStartedEvent, ActorStoppedEvent,
		ActorRestartedEvent, DeadLetterEvent:
		return
	}
	r.ch <- c.Message()
}

// spawnSubscriber spawns a recorder and subscribes it to the event stream,
// returning its PID and the channel every event it receives lands on.
func spawnSubscriber(e *Engine) (*PID, chan any) {
	ch := make(chan any, 1024)
	pid := e.Spawn(func() Receiver { return &streamRecorder{ch: ch} }, "subscriber")
	e.Subscribe(pid)
	return pid, ch
}

// TestBroadcastReachesEverySubscriber is the golden path: every subscriber
// gets every event.
func TestBroadcastReachesEverySubscriber(t *testing.T) {
	e := newTestEngine(t)
	_, a := spawnSubscriber(e)
	_, b := spawnSubscriber(e)
	_, c := spawnSubscriber(e)

	e.BroadcastEvent("first")
	e.BroadcastEvent("second")

	for _, ch := range []chan any{a, b, c} {
		assert.Equal(t, "first", mustReceive(t, ch))
		assert.Equal(t, "second", mustReceive(t, ch))
	}
}

// TestBroadcastDoesNotReachNonSubscribers pins that the stream is opt-in: a
// spawned actor that never subscribed hears nothing.
func TestBroadcastDoesNotReachNonSubscribers(t *testing.T) {
	e := newTestEngine(t)
	ch := make(chan any, 2)
	pid := e.Spawn(func() Receiver { return &recorder{ch: ch} }, "bystander")

	e.BroadcastEvent("event")
	// The inbox is FIFO, so a leaked broadcast would sit ahead of this
	// direct send: seeing the sentinel first proves nothing was delivered.
	e.Send(pid, "sentinel")

	assert.Equal(t, "sentinel", mustReceive(t, ch))
}

// TestUnsubscribeStopsDelivery covers the other half of the API: after
// Unsubscribe, later events do not arrive.
func TestUnsubscribeStopsDelivery(t *testing.T) {
	e := newTestEngine(t)
	pid, ch := spawnSubscriber(e)

	e.BroadcastEvent("before")
	require.Equal(t, "before", mustReceive(t, ch))

	e.Unsubscribe(pid)
	e.BroadcastEvent("after")
	e.Send(pid, "sentinel")

	assert.Equal(t, "sentinel", mustReceive(t, ch))
}

// TestDuplicateSubscribeDeliversOnce is acceptance criterion 4: subscribing
// twice must not double every event.
func TestDuplicateSubscribeDeliversOnce(t *testing.T) {
	e := newTestEngine(t)
	pid, ch := spawnSubscriber(e)
	e.Subscribe(pid)
	e.Subscribe(pid)

	e.BroadcastEvent("event")
	e.BroadcastEvent("sentinel")

	require.Equal(t, "event", mustReceive(t, ch))
	// A duplicate copy of "event" would be queued ahead of the sentinel, so
	// seeing the sentinel next proves the event was delivered exactly once.
	assert.Equal(t, "sentinel", mustReceive(t, ch))
}

// blocker is a subscriber that parks on release inside Receive, so its inbox
// backs up while the broadcaster keeps going.
type blocker struct{ release chan struct{} }

func (b blocker) Receive(c *Context) {
	switch c.Message().(type) {
	case Initialized, Started, Stopped:
		return
	}
	<-b.release
}

// TestBroadcastNeverBlocksOnSlowSubscriber is acceptance criterion 3: a
// subscriber stuck in Receive must not hold up the broadcaster or anybody
// else on the stream.
func TestBroadcastNeverBlocksOnSlowSubscriber(t *testing.T) {
	e := newTestEngine(t)
	release := make(chan struct{})
	defer close(release)

	slow := e.Spawn(func() Receiver { return blocker{release: release} }, "slow")
	e.Subscribe(slow)
	_, fast := spawnSubscriber(e)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			e.BroadcastEvent("event")
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(deliverTimeout):
		t.Fatal("BroadcastEvent blocked on a slow subscriber")
	}
	assert.Equal(t, "event", mustReceive(t, fast))
}

// TestStoppedSubscriberIsPrunedAndStreamSurvives is acceptance criterion 5:
// a stopped subscriber neither breaks the broadcast for the others nor stays
// on the list.
func TestStoppedSubscriberIsPrunedAndStreamSurvives(t *testing.T) {
	e := newTestEngine(t)
	dead, _ := spawnSubscriber(e)
	_, alive := spawnSubscriber(e)
	require.Equal(t, 2, userSubscribers(e))

	<-e.Poison(dead).Done()
	e.BroadcastEvent("event")

	assert.Equal(t, "event", mustReceive(t, alive))
	assert.Equal(t, 1, userSubscribers(e), "stopped subscriber must be pruned from the stream")
}

// churner subscribes and unsubscribes itself from inside its own Receive,
// which is the case acceptance criterion 6 is about.
type churner struct {
	ch chan any
}

func (a churner) Receive(c *Context) {
	switch c.Message().(type) {
	case Initialized, Started, Stopped:
		return
	}
	c.Engine().Unsubscribe(c.PID())
	c.Engine().Subscribe(c.PID())
	a.ch <- c.Message()
}

// TestSubscribeDuringBroadcastHandling is acceptance criterion 6: mutating
// the stream from inside an event handler must neither deadlock nor race,
// and the actor must still be subscribed afterwards.
func TestSubscribeDuringBroadcastHandling(t *testing.T) {
	e := newTestEngine(t)
	got := make(chan any, 16)
	pid := e.Spawn(func() Receiver { return churner{ch: got} }, "churner")
	e.Subscribe(pid)

	// Broadcast while the handler churns the subscription underneath us.
	for i := 0; i < 8; i++ {
		e.BroadcastEvent("event")
		require.Equal(t, "event", mustReceive(t, got))
	}

	assert.Equal(t, 1, userSubscribers(e))
}

// TestBroadcast100Subscribers1000Events is acceptance criterion 7: 100k
// deliveries, every one of them accounted for.
func TestBroadcast100Subscribers1000Events(t *testing.T) {
	const (
		subscribers = 100
		events      = 1000
	)
	e := newTestEngine(t)

	var wg sync.WaitGroup
	wg.Add(subscribers)
	for i := 0; i < subscribers; i++ {
		done := make(chan struct{})
		e.Subscribe(e.Spawn(func() Receiver {
			return &counter{want: events, done: done}
		}, "bulk"))
		go func() {
			defer wg.Done()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Error("subscriber did not receive all 1000 events")
			}
		}()
	}

	for i := 0; i < events; i++ {
		e.BroadcastEvent(i)
	}

	wg.Wait()
}

// TestConcurrentSubscribeUnsubscribeAndBroadcast is the race-detector test:
// all three entry points hammered from many goroutines at once.
func TestConcurrentSubscribeUnsubscribeAndBroadcast(t *testing.T) {
	e := newTestEngine(t)
	pids := make([]*PID, 20)
	for i := range pids {
		pids[i] = e.Spawn(func() Receiver { return noop{} }, "racer")
	}

	var wg sync.WaitGroup
	for _, pid := range pids {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				e.Subscribe(pid)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				e.Unsubscribe(pid)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				e.BroadcastEvent("event")
			}
		}()
	}
	wg.Wait()
}
