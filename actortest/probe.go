package actortest

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/lsproule/actor"
)

func NewEngine(t testing.TB) *actor.Engine {
	e := actor.NewEngine()
	t.Cleanup(func() {
		// Cleanup logic if needed
	})
	return e
}

type Probe struct {
	t       testing.TB
	e       *actor.Engine
	pid     *actor.PID
	timeout time.Duration

	mu       sync.Mutex
	received []any
	msgs     chan any
}

func NewProbe(t testing.TB, e *actor.Engine) *Probe {
	p := &Probe{
		t:       t,
		e:       e,
		timeout: 2 * time.Second,
		msgs:    make(chan any, 1024),
	}

	p.pid = e.Spawn(p, "probe")
	return p
}

func (p *Probe) Receive(c *actor.Context) {
	msg := c.Message()
	
	p.mu.Lock()
	p.received = append(p.received, msg)
	p.mu.Unlock()

	select {
	case p.msgs <- msg:
	default:
	}
}

func (p *Probe) PID() *actor.PID {
	return p.pid
}

func (p *Probe) WithTimeout(d time.Duration) *Probe {
	p.timeout = d
	return p
}

func (p *Probe) Received() []any {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	snap := make([]any, len(p.received))
	copy(snap, p.received)
	return snap
}

func (p *Probe) Expect(want any) any {
	p.t.Helper()
	start := time.Now()
	timer := time.NewTimer(p.timeout)
	defer timer.Stop()

	for {
		select {
		case msg := <-p.msgs:
			if reflect.DeepEqual(msg, want) {
				return msg
			}
		case <-timer.C:
			p.failf(want, time.Since(start))
			return nil
		}
	}
}

func (p *Probe) ExpectType(sample any) any {
	p.t.Helper()
	start := time.Now()
	timer := time.NewTimer(p.timeout)
	defer timer.Stop()

	wantType := reflect.TypeOf(sample)

	for {
		select {
		case msg := <-p.msgs:
			if reflect.TypeOf(msg) == wantType {
				return msg
			}
		case <-timer.C:
			p.failf(fmt.Sprintf("type %T", sample), time.Since(start))
			return nil
		}
	}
}

func (p *Probe) ExpectNoMessage(d time.Duration) {
	p.t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case msg := <-p.msgs:
		p.t.Fatalf("probe %v expected silence for %v, but received: %v (%T)\nFull history: %v", 
			p.pid, d, msg, msg, p.Received())
	case <-timer.C:
	}
}

func (p *Probe) failf(expected any, elapsed time.Duration) {
	p.t.Helper()
	p.t.Fatalf(
		"probe %v timeout after %v waiting for %v.\nReceived messages so far: %v",
		p.pid, elapsed, expected, p.Received(),
	)
}
