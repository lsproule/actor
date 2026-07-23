package actor

import (
	"errors"
	"time"
)

// ErrRequestTimeout is the error Response.Result returns when no reply
// arrives before the request's timeout elapses. Callers identify a timeout
// with errors.Is(err, ErrRequestTimeout) rather than comparing strings.
var ErrRequestTimeout = errors.New("actor: request timed out waiting for a reply")

// Response is the future Engine.Request hands back. It is safe to read from
// any goroutine and to call Result any number of times: whichever outcome
// (reply or timeout) resolves first is the outcome for every later call.
type Response struct {
	engine *Engine
	pid    *PID
	done   chan struct{}
	result any
	err    error
}

// Result blocks until a reply arrives or the request's timeout elapses,
// whichever comes first, and returns the reply or a non-nil error
// identifiable via errors.Is(err, ErrRequestTimeout).
//
// The temporary responder is torn down by a goroutine Engine.Request already
// started, regardless of whether or when Result is called, so a caller that
// never calls Result leaks nothing.
//
// Calling Result from inside an actor's Receive blocks that actor's own
// inbox for as long as the wait takes — no other message, including a reply
// meant for a different Request, can be processed until it returns. Prefer
// reacting to the reply asynchronously (a goroutine, or a direct message to
// the actor) when the wait might be long.
func (r *Response) Result() (any, error) {
	<-r.done
	return r.result, r.err
}

// await runs on its own goroutine, started immediately by Engine.Request. It
// resolves the Response on the first of a reply or the timeout, then always
// poisons the temporary responder before signalling done — cleanup happens
// exactly once, on exactly one of these two paths, whether or not anyone is
// waiting on Result.
func (r *Response) await(replyCh <-chan any, timeout time.Duration) {
	select {
	case msg := <-replyCh:
		r.result = msg
	case <-time.After(timeout):
		r.err = ErrRequestTimeout
	}
	<-r.engine.Poison(r.pid).Done()
	close(r.done)
}
