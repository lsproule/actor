package actor

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSendRepeatDeliversImmediatelyAndRepeats(t *testing.T) {
	e := newTestEngine(t)
	ch := make(chan any, 3)
	pid := e.Spawn(func() Receiver { return &recorder{ch: ch} }, "repeat")

	repeater := e.SendRepeat(pid, "tick", 10*time.Millisecond)
	defer repeater.Stop()

	require.Equal(t, "tick", mustReceive(t, ch))
	require.Equal(t, "tick", mustReceive(t, ch))
	require.Equal(t, "tick", mustReceive(t, ch))
}

func TestSendRepeatStopIsIdempotentAndHaltsDelivery(t *testing.T) {
	e := newTestEngine(t)
	ch := make(chan any, 10)
	pid := e.Spawn(func() Receiver { return &recorder{ch: ch} }, "repeat")

	repeater := e.SendRepeat(pid, "tick", time.Hour)
	require.Equal(t, "tick", mustReceive(t, ch))

	repeater.Stop()
	repeater.Stop()

	select {
	case got := <-ch:
		t.Fatalf("unexpected message after Stop: %v", got)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestSendAfterDeliversOnce(t *testing.T) {
	e := newTestEngine(t)
	ch := make(chan any, 2)
	pid := e.Spawn(func() Receiver { return &recorder{ch: ch} }, "after")

	repeater := e.SendAfter(pid, "later", 10*time.Millisecond)
	defer repeater.Stop()

	require.Equal(t, "later", mustReceive(t, ch))
	select {
	case got := <-ch:
		t.Fatalf("unexpected second message: %v", got)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestSendAfterCanBeStoppedBeforeDelivery(t *testing.T) {
	e := newTestEngine(t)
	ch := make(chan any, 1)
	pid := e.Spawn(func() Receiver { return &recorder{ch: ch} }, "after")

	repeater := e.SendAfter(pid, "later", time.Hour)
	repeater.Stop()
	repeater.Stop()

	select {
	case got := <-ch:
		t.Fatalf("unexpected message after Stop: %v", got)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestSendRepeatToStoppedActorDoesNotPanicOrSpin(t *testing.T) {
	e := newTestEngine(t)
	pid := e.Spawn(func() Receiver { return noop{} }, "gone")
	<-e.Stop(pid).Done()

	repeater := e.SendRepeat(pid, "tick", time.Millisecond)
	time.Sleep(10 * time.Millisecond)

	require.NotPanics(t, repeater.Stop)
	repeater.Stop()
}

func TestSendRepeatStopDoesNotLeakGoroutines(t *testing.T) {
	baseline := runtime.NumGoroutine()
	e := newTestEngine(t)
	pid := e.Spawn(func() Receiver { return noop{} }, "leak")

	const repeaters = 1000
	handles := make([]*SendRepeater, 0, repeaters)
	for i := 0; i < repeaters; i++ {
		handles = append(handles, e.SendRepeat(pid, i, time.Hour))
	}
	for _, handle := range handles {
		handle.Stop()
	}

	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= baseline+2
	}, time.Second, 10*time.Millisecond)
}
