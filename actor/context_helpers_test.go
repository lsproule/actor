package actor

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestContextSendSetsSender verifies that Context.Send delivers with the
// sending actor as the sender, so the receiver can Respond straight back.
func TestContextSendSetsSender(t *testing.T) {
	e := newTestEngine(t)
	gotSender := make(chan *PID, 1)
	gotReply := make(chan any, 1)

	b := e.SpawnFunc(func(c *Context) {
		if _, ok := c.Message().(string); ok {
			gotSender <- c.Sender()
			c.Respond("pong")
		}
	}, "b")

	a := e.SpawnFunc(func(c *Context) {
		switch c.Message().(type) {
		case Started:
			c.Send(b, "ping")
		case string:
			gotReply <- c.Message()
		}
	}, "a")

	select {
	case sender := <-gotSender:
		require.NotNil(t, sender, "Send must not deliver a nil sender")
		assert.True(t, a.Equals(sender), "sender must be the sending actor's PID")
	case <-time.After(deliverTimeout):
		t.Fatal("receiver never saw the message")
	}

	select {
	case reply := <-gotReply:
		assert.Equal(t, "pong", reply, "Respond must reach the sender of a Context.Send")
	case <-time.After(deliverTimeout):
		t.Fatal("sender never received the Respond reply")
	}
}

// TestForwardPreservesOriginalSender is the acceptance test from the issue:
// a Request against a router that Forwards to a worker is answered by the
// worker directly, because Forward keeps the original sender intact.
func TestForwardPreservesOriginalSender(t *testing.T) {
	e := newTestEngine(t)

	worker := e.SpawnFunc(func(c *Context) {
		if _, ok := c.Message().(string); ok {
			c.Respond("done by worker")
		}
	}, "worker")

	router := e.SpawnFunc(func(c *Context) {
		if _, ok := c.Message().(string); ok {
			c.Forward(worker)
		}
	}, "router")

	result, err := e.Request(router, "job", time.Second).Result()
	require.NoError(t, err)
	assert.Equal(t, "done by worker", result,
		"the worker's Respond must reach the original requester, not the router")
}

// TestForwardVsSend documents the difference between Forward and Send: with
// c.Send the router becomes the sender, so the worker's Respond goes back to
// the router and the original requester times out.
func TestForwardVsSend(t *testing.T) {
	e := newTestEngine(t)
	routerGotReply := make(chan any, 1)

	worker := e.SpawnFunc(func(c *Context) {
		if msg, ok := c.Message().(string); ok && msg == "job" {
			c.Respond("done by worker")
		}
	}, "worker")

	router := e.SpawnFunc(func(c *Context) {
		switch msg := c.Message().(type) {
		case string:
			if msg == "job" {
				c.Send(worker, msg)
				return
			}
			routerGotReply <- msg
		}
	}, "router")

	_, err := e.Request(router, "job", 200*time.Millisecond).Result()
	require.Error(t, err, "the requester must not receive the worker's reply")
	assert.True(t, errors.Is(err, ErrRequestTimeout))

	select {
	case reply := <-routerGotReply:
		assert.Equal(t, "done by worker", reply,
			"with Send the reply goes to the router instead")
	case <-time.After(deliverTimeout):
		t.Fatal("router never received the worker's reply")
	}
}

// TestChildLookup verifies Child resolves the short name a child was spawned
// under, and returns nil — not a panic — for a name that does not exist.
func TestChildLookup(t *testing.T) {
	e := newTestEngine(t)
	lookups := make(chan *PID, 3)

	parent := e.SpawnFunc(func(c *Context) {
		switch c.Message().(type) {
		case Started:
			c.SpawnChild(func() Receiver { return noop{} }, "child", WithID("a"))
			c.SpawnChild(func() Receiver { return noop{} }, "child", WithID("b"))
		case string:
			lookups <- c.Child("a")
			lookups <- c.Child("child/b")
			lookups <- c.Child("zz")
		}
	}, "parent")

	e.Send(parent, "lookup")

	a := receivePID(t, lookups)
	b := receivePID(t, lookups)
	zz := receivePID(t, lookups)

	require.NotNil(t, a, `Child("a") must find the child spawned with WithID("a")`)
	require.NotNil(t, b, `Child("child/b") must accept the kind-qualified name`)
	assert.Nil(t, zz, "a name that was never spawned must yield nil")

	assert.NotNil(t, e.Registry().get(a), "the looked-up child must be reachable")
	waitClosed(t, e.Poison(parent).Done(), 5*time.Second, "parent never stopped")
}

// TestChildrenSnapshot spawns 5 children, mutates the slice Children returns,
// and asserts the actor's real child set is unaffected.
func TestChildrenSnapshot(t *testing.T) {
	e := newTestEngine(t)
	snapshots := make(chan []*PID, 2)

	parent := e.SpawnFunc(func(c *Context) {
		switch c.Message().(type) {
		case Started:
			for i := 0; i < 5; i++ {
				c.SpawnChild(func() Receiver { return noop{} }, "child")
			}
		case string:
			snapshots <- c.Children()
		}
	}, "parent")

	e.Send(parent, "list")
	first := receiveSnapshot(t, snapshots)
	require.Len(t, first, 5)

	// Vandalise the returned slice; the actor's child set must not notice.
	for i := range first {
		first[i] = nil
	}

	e.Send(parent, "list")
	second := receiveSnapshot(t, snapshots)
	require.Len(t, second, 5, "mutating a snapshot must not shrink the child set")
	for _, pid := range second {
		assert.NotNil(t, pid, "mutating a snapshot must not corrupt stored PIDs")
	}

	waitClosed(t, e.Poison(parent).Done(), 5*time.Second, "parent never stopped")
}

// TestHelpersDuringLifecycle calls the child helpers during Initialized and
// Stopped and asserts they are safe there: no panic, and an empty result.
func TestHelpersDuringLifecycle(t *testing.T) {
	e := newTestEngine(t)
	counts := make(chan int, 2)

	pid := e.SpawnFunc(func(c *Context) {
		switch c.Message().(type) {
		case Initialized:
			require.Nil(t, c.Child("nope"))
			counts <- len(c.Children())
		case Stopped:
			require.Nil(t, c.Child("nope"))
			counts <- len(c.Children())
		}
	}, "life")

	waitClosed(t, e.Poison(pid).Done(), 5*time.Second, "actor never stopped")

	require.Equal(t, 0, mustReceiveCount(t, counts), "no children during Initialized")
	require.Equal(t, 0, mustReceiveCount(t, counts), "no children during Stopped")
}

// TestContextSendRepeat sets up a repeater on Started, stops it on Stopped,
// and asserts deliveries happen while the actor lives and cease after it is
// poisoned.
func TestContextSendRepeat(t *testing.T) {
	e := newTestEngine(t)
	ticks := make(chan any, 256)
	peer := e.Spawn(func() Receiver { return &recorder{ch: ticks} }, "peer")

	// rep is written on Started and read on Stopped — both on the ticker
	// actor's own processor goroutine, so no lock is needed.
	var rep *SendRepeater
	ticker := e.SpawnFunc(func(c *Context) {
		switch c.Message().(type) {
		case Started:
			rep = c.SendRepeat(peer, "tick", 5*time.Millisecond)
		case Stopped:
			rep.Stop()
		}
	}, "ticker")

	for i := 0; i < 3; i++ {
		select {
		case <-ticks:
		case <-time.After(deliverTimeout):
			t.Fatalf("only %d deliveries arrived before timing out", i)
		}
	}

	// Poison waits for Stopped, and Stop() inside the Stopped handler waits
	// for the scheduler goroutine to exit — so once Done closes, no further
	// tick can be in flight.
	waitClosed(t, e.Poison(ticker).Done(), 5*time.Second, "ticker never stopped")
	for len(ticks) > 0 {
		<-ticks
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, len(ticks), "deliveries must cease once the repeater is stopped")
}

// --- helpers ---

func receivePID(t *testing.T, ch <-chan *PID) *PID {
	t.Helper()
	select {
	case pid := <-ch:
		return pid
	case <-time.After(deliverTimeout):
		t.Fatal("timed out waiting for a Child lookup result")
		return nil
	}
}

func receiveSnapshot(t *testing.T, ch <-chan []*PID) []*PID {
	t.Helper()
	select {
	case pids := <-ch:
		return pids
	case <-time.After(deliverTimeout):
		t.Fatal("timed out waiting for a Children snapshot")
		return nil
	}
}

func mustReceiveCount(t *testing.T, ch <-chan int) int {
	t.Helper()
	select {
	case n := <-ch:
		return n
	case <-time.After(deliverTimeout):
		t.Fatal("timed out waiting for a child count")
		return -1
	}
}
