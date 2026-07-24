package actor

import (
	"fmt"
	"log/slog"
	"time"
)

// ReceiveFunc is the signature of a wrapped message handler in the middleware
// chain. It receives the actor's Context and processes the current message.
type ReceiveFunc func(*Context)

// Middleware wraps a ReceiveFunc to add cross-cutting behaviour such as
// logging, timing, tracing, or validation. It receives the next handler in the
// chain and must call it to continue processing. A middleware may short-circuit
// by returning without calling next — including short-circuiting lifecycle
// messages (Initialized, Started, Stopped), which is the caller's
// responsibility, not the engine's.
//
// Middleware sees every message — including the engine's lifecycle messages —
// because they all flow through the same invokeMsg path. A panic inside
// middleware is caught by the same recovery and restart path as a panic in
// Receive.
//
// The chain is composed once at actor start time, not per message, so there is
// zero allocation overhead on the hot path.
type Middleware func(next ReceiveFunc) ReceiveFunc

// WithMiddleware configures per-actor receive middleware. Middleware is composed
// outermost-first: WithMiddleware(a, b) yields a(b(receiver.Receive)) — a runs
// before b before the receiver, and unwinds in reverse.
//
// The slice is copied on the way in so caller mutations afterwards cannot
// affect the actor's Opts. An empty slice clears any previously configured
// middleware.
//
//	Middleware is per-actor in this proof of concept. Engine-wide default
//	middleware and shared metrics exporters are out of scope.
func WithMiddleware(mw ...Middleware) OptFunc {
	return func(o *Opts) {
		if len(mw) == 0 {
			o.Middleware = nil
			return
		}
		o.Middleware = make([]Middleware, len(mw))
		copy(o.Middleware, mw)
	}
}

// buildChain composes the middleware chain once, at actor start. If no
// middleware is configured it returns the bare receiver.Receive — the zero-
// overhead path. The chain is stored on the process and reused for every
// message without allocation.
//
// WithMiddleware(a, b) composes as a(b(receiver.Receive)): a runs first on the
// way in and unwinds last. The slice is iterated in reverse so the outermost
// middleware ends up as the outermost wrapper.
func buildChain(mw []Middleware, receiver Receiver) ReceiveFunc {
	if len(mw) == 0 {
		return receiver.Receive
	}
	h := receiver.Receive
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

// WithLogging returns a Middleware that logs the message type, the actor's PID,
// and the handler duration at Debug level on every message — including
// lifecycle messages.
//
// The message type is logged via fmt.Sprintf("%T", ...) so structured log
// consumers see the concrete Go type. A nil logger falls back to
// slog.Default().
func WithLogging(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next ReceiveFunc) ReceiveFunc {
		return func(c *Context) {
			start := time.Now()
			next(c)
			logger.Debug("actor receive",
				"pid", c.PID().String(),
				"type", fmt.Sprintf("%T", c.Message()),
				"duration", time.Since(start),
			)
		}
	}
}
