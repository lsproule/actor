package actor

import (
	"sync"
	"time"
)

// SendRepeater is a stoppable scheduled send created by SendRepeat or
// SendAfter. Stop is safe to call more than once and returns only after the
// scheduler has stopped attempting deliveries.
type SendRepeater struct {
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func newSendRepeater() *SendRepeater {
	return &SendRepeater{
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

// Stop halts future scheduled deliveries. It is idempotent and waits until the
// scheduler goroutine has exited, so no later tick can race with the caller.
func (r *SendRepeater) Stop() {
	if r == nil {
		return
	}
	r.once.Do(func() { close(r.stop) })
	<-r.done
}

// SendRepeat sends msg to pid immediately, then again every interval until the
// returned SendRepeater is stopped. Each delivery goes through Send, so stopped
// or unknown actors are handled by the engine's ordinary dead-letter path.
//
// If interval is not positive, SendRepeat performs only the immediate send and
// returns an already-stopped repeater.
func (e *Engine) SendRepeat(pid *PID, msg any, interval time.Duration) *SendRepeater {
	repeater := newSendRepeater()
	e.Send(pid, msg)
	if interval <= 0 {
		close(repeater.done)
		return repeater
	}

	go func() {
		defer close(repeater.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				e.Send(pid, msg)
			case <-repeater.stop:
				return
			}
		}
	}()
	return repeater
}

// SendAfter sends msg to pid once after delay unless the returned
// SendRepeater is stopped first. The delivery goes through Send, matching
// SendRepeat and all ordinary engine routing.
func (e *Engine) SendAfter(pid *PID, msg any, delay time.Duration) *SendRepeater {
	repeater := newSendRepeater()
	go func() {
		defer close(repeater.done)
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-timer.C:
			e.Send(pid, msg)
		case <-repeater.stop:
			return
		}
	}()
	return repeater
}
