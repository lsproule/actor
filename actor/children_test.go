package actor

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSpawnChildFromReceive verifies that a parent spawning a child on Started
// results in the child receiving its own Started and being reachable via Send.
func TestSpawnChildFromReceive(t *testing.T) {
	e := newTestEngine(t)
	childMsg := make(chan any, 1)

	parent := e.SpawnFunc(func(ctx *Context) {
		switch ctx.Message().(type) {
		case Started:
			child := ctx.SpawnChild(func() Receiver {
				return &recorder{ch: childMsg}
			}, "child")
			e.Send(child, "hello from parent")
		}
	}, "parent")

	select {
	case msg := <-childMsg:
		require.Equal(t, "hello from parent", msg)
	case <-time.After(deliverTimeout):
		t.Fatal("child never received message")
	}

	waitClosed(t, e.Poison(parent).Done(), 5*time.Second, "parent never stopped")
}

// TestChildPIDNesting asserts the child's PID string is prefixed by the
// parent's PID string.
func TestChildPIDNesting(t *testing.T) {
	e := newTestEngine(t)
	childPID := make(chan *PID, 1)

	parent := e.SpawnFunc(func(ctx *Context) {
		switch ctx.Message().(type) {
		case Started:
			child := ctx.SpawnChild(func() Receiver { return noop{} }, "ch")
			childPID <- child
		}
	}, "parent")

	child := <-childPID
	assert.Contains(t, child.String(), parent.String(),
		"child PID must be nested under parent PID")

	waitClosed(t, e.Poison(parent).Done(), 5*time.Second, "parent never stopped")
}

// TestChildKnowsParent verifies that inside the child's Receive,
// c.Parent() returns the parent PID.
func TestChildKnowsParent(t *testing.T) {
	e := newTestEngine(t)
	gotParent := make(chan *PID, 1)

	parent := e.SpawnFunc(func(ctx *Context) {
		switch ctx.Message().(type) {
		case Started:
			ctx.SpawnChildFunc(func(c *Context) {
				switch c.Message().(type) {
				case Started:
					gotParent <- c.Parent()
				}
			}, "child")
		}
	}, "parent")

	select {
	case p := <-gotParent:
		require.NotNil(t, p)
		assert.True(t, parent.Equals(p), "child's Parent() must equal the parent PID")
	case <-time.After(deliverTimeout):
		t.Fatal("child never reported its parent")
	}

	waitClosed(t, e.Poison(parent).Done(), 5*time.Second, "parent never stopped")
}

// TestCascadeShutdown spawns a parent with 10 children, poisons the parent,
// and asserts all 11 received Stopped and the registry is empty for all of them.
func TestCascadeShutdown(t *testing.T) {
	e := newTestEngine(t)

	var mu sync.Mutex
	stoppedCount := 0
	allStopped := make(chan struct{})

	parent := e.SpawnFunc(func(ctx *Context) {
		switch ctx.Message().(type) {
		case Started:
			for i := 0; i < 10; i++ {
				ctx.SpawnChildFunc(func(c *Context) {
					switch c.Message().(type) {
					case Stopped:
						mu.Lock()
						stoppedCount++
						if stoppedCount == 10 {
							close(allStopped)
						}
						mu.Unlock()
					}
				}, "child")
			}
		case Stopped:
			mu.Lock()
			stoppedCount++
			mu.Unlock()
		}
	}, "parent")

	waitClosed(t, e.Poison(parent).Done(), 5*time.Second, "parent never stopped")

	mu.Lock()
	assert.Equal(t, 11, stoppedCount, "all actors must receive Stopped")
	mu.Unlock()

	assert.Equal(t, 0, e.Registry().len(), "registry must be empty after cascade shutdown")
}

// TestBottomUpOrdering records a global ordered log and asserts every child's
// Stopped is recorded before its parent's Stopped.
func TestBottomUpOrdering(t *testing.T) {
	e := newTestEngine(t)

	var mu sync.Mutex
	var log []string

	parent := e.SpawnFunc(func(ctx *Context) {
		switch ctx.Message().(type) {
		case Started:
			for i := 0; i < 10; i++ {
				ctx.SpawnChildFunc(func(c *Context) {
					switch c.Message().(type) {
					case Stopped:
						mu.Lock()
						log = append(log, "child")
						mu.Unlock()
					}
				}, "child")
			}
		case Stopped:
			mu.Lock()
			log = append(log, "parent")
			mu.Unlock()
		}
	}, "parent")

	waitClosed(t, e.Poison(parent).Done(), 5*time.Second, "parent never stopped")

	mu.Lock()
	require.Len(t, log, 11, "must have 11 entries (10 children + 1 parent)")
	for i := 0; i < 10; i++ {
		assert.Equal(t, "child", log[i], "child must stop before parent")
	}
	assert.Equal(t, "parent", log[10], "parent must be last")
	mu.Unlock()
}

// TestDeepTree spawns a 3-level tree with ~100 actors and asserts that after
// poisoning the root, the registry returns to its original size and Done()
// closes within 5 seconds.
func TestDeepTree(t *testing.T) {
	e := newTestEngine(t)

	root := e.SpawnFunc(func(ctx *Context) {
		switch ctx.Message().(type) {
		case Started:
			for i := 0; i < 5; i++ {
				ctx.SpawnChildFunc(func(c2 *Context) {
					switch c2.Message().(type) {
					case Started:
						for j := 0; j < 5; j++ {
							c2.SpawnChildFunc(func(c3 *Context) {
								switch c3.Message().(type) {
								case Started:
									for k := 0; k < 3; k++ {
										c3.SpawnChildFunc(func(noop *Context) {}, "leaf")
									}
								}
							}, "mid")
						}
					}
				}, "child")
			}
		}
	}, "root")

	require.Equal(t, 106, e.Registry().len(), "all actors must be registered")

	ctx := e.Poison(root)
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("root poison never completed within 5s")
	}

	require.Equal(t, 0, e.Registry().len(), "registry must be empty after root poison")
}

// TestChildSelfStopRemovesFromParent poisons a child directly, then asserts
// the parent's childPIDs() no longer contains it.
func TestChildSelfStopRemovesFromParent(t *testing.T) {
	e := newTestEngine(t)
	childPID := make(chan *PID, 1)
	parentReady := make(chan struct{})

	parent := e.SpawnFunc(func(ctx *Context) {
		switch ctx.Message().(type) {
		case Started:
			child := ctx.SpawnChildFunc(func(c *Context) {}, "child")
			childPID <- child
			close(parentReady)
		}
	}, "parent")

	<-parentReady
	child := <-childPID

	waitClosed(t, e.Poison(child).Done(), 5*time.Second, "child never stopped")

	parentProc, ok := e.Registry().get(parent).(*process)
	require.True(t, ok, "parent must still be alive")
	require.NotNil(t, parentProc)

	children := parentProc.childPIDs()
	for _, c := range children {
		if c.Equals(child) {
			t.Fatal("child should have been removed from parent's child set after self-stop")
		}
	}

	waitClosed(t, e.Poison(parent).Done(), 5*time.Second, "parent never stopped")
}

// TestDeepTreeRepeated runs the deep tree test 5 times to shake out ordering races.
func TestDeepTreeRepeated(t *testing.T) {
	for i := 0; i < 5; i++ {
		func() {
			e := newTestEngine(t)

			root := e.SpawnFunc(func(ctx *Context) {
				switch ctx.Message().(type) {
				case Started:
					for i := 0; i < 5; i++ {
						ctx.SpawnChildFunc(func(c2 *Context) {
							switch c2.Message().(type) {
							case Started:
								for j := 0; j < 5; j++ {
									c2.SpawnChildFunc(func(c3 *Context) {
										switch c3.Message().(type) {
										case Started:
											for k := 0; k < 3; k++ {
												c3.SpawnChildFunc(func(noop *Context) {}, "leaf")
											}
										}
									}, "mid")
								}
							}
						}, "child")
					}
				}
			}, "root")

			ctx := e.Poison(root)
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("root poison never completed within 5s")
			}

			require.Equal(t, 0, e.Registry().len(), "registry must return to original size")
		}()
	}
}
