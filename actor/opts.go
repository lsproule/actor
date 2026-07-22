package actor

import "time"

// defaultInboxSize is the initial ring-buffer capacity hint. The inbox grows
// on demand; 1024 balances memory footprint against the common case where
// actors are briefly overloaded and the buffer accommodates the spike.
const defaultInboxSize = 1024

// defaultMaxRestarts is the number of times an actor tolerates a restart cycle
// before giving up. Setting it to 3 lets transient failures (temporary I/O
// hiccups) recover without orphaning every single actor on a glitch.
const defaultMaxRestarts = 3

// defaultRestartDelay is the minimum time between restart attempts. 500ms
// prevents tight spin loops and lets external services (databases, APIs)
// recover from transient faults before the actor floods them with retries.
const defaultRestartDelay = 500 * time.Millisecond

// Opts configures a process at construction time. It captures the Producer,
// spawn identity, inbox sizing, supervision strategy (MaxRestarts and
// RestartDelay), and user-defined metadata (Tags). All fields have documented
// defaults supplied by DefaultOpts.
type Opts struct {
	Producer     Producer
	Kind         string
	ID           string
	InboxSize    int
	MaxRestarts  int32
	RestartDelay time.Duration
	Tags         []string
}

// DefaultOpts returns Opts wired to produce actors from p with sensible
// defaults. All constants are documented; callers who accept the defaults get
// a fully functional actor.
func DefaultOpts(p Producer) Opts {
	return Opts{
		Producer:     p,
		Kind:         "actor",
		InboxSize:    defaultInboxSize,
		MaxRestarts:  defaultMaxRestarts,
		RestartDelay: defaultRestartDelay,
		Tags:         []string{},
	}
}

// OptFunc mutates Opts at spawn time. It is the variadic option type the Spawn
// family accepts. Options compose in order; if the same field is set twice,
// the last one wins. All option functions clamp invalid values to sensible
// defaults instead of panicking: 0 or negative inbox sizes fall back to
// defaultInboxSize, negative restart delays use defaultRestartDelay, and
// negative max-restart counts are clamped to 0 (meaning never restart).
type OptFunc func(*Opts)

// WithID pins an actor's identity so it is reachable under a stable
// "kind/id". Without it the engine appends a process-unique suffix so two
// spawns of the same kind never collide.
func WithID(id string) OptFunc {
	return func(o *Opts) { o.ID = id }
}

// WithInboxSize overrides the initial inbox capacity hint. The value is a
// starting size, not a hard cap — the ring buffer grows on demand. Values
// <= 0 are clamped to defaultInboxSize to prevent silent failures.
func WithInboxSize(size int) OptFunc {
	return func(o *Opts) {
		if size <= 0 {
			o.InboxSize = defaultInboxSize
		} else {
			o.InboxSize = size
		}
	}
}

// WithMaxRestarts sets the number of times an actor can restart before
// permanent failure. Negative values are clamped to 0 (never restart).
func WithMaxRestarts(n int) OptFunc {
	return func(o *Opts) {
		if n < 0 {
			o.MaxRestarts = 0
		} else {
			o.MaxRestarts = int32(n)
		}
	}
}

// WithRestartDelay sets the minimum interval between restart attempts.
// Negative values are clamped to defaultRestartDelay to prevent spin loops.
func WithRestartDelay(d time.Duration) OptFunc {
	return func(o *Opts) {
		if d < 0 {
			o.RestartDelay = defaultRestartDelay
		} else {
			o.RestartDelay = d
		}
	}
}

// WithTags attaches user-defined metadata to an actor. The slice is copied on
// the way in so caller mutations afterwards cannot affect the actor's Opts.
// Tags are inert in this POC and will feed logging and metrics in later issues.
func WithTags(tags ...string) OptFunc {
	return func(o *Opts) {
		// Copy the tags to prevent caller mutations from affecting Opts.
		o.Tags = make([]string, len(tags))
		copy(o.Tags, tags)
	}
}
