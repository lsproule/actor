package actor

// defaultInboxSize is the ring-buffer sizing hint used when Opts leaves
// InboxSize unset. It is a starting capacity, not a hard cap — the inbox
// grows on demand.
const defaultInboxSize = 128

// Opts configures a process at construction time. Issue #9 expands this with
// the full option surface (supervision strategy, middleware, mailbox kind);
// for now it carries only what newProcess needs to build an actor: the
// Producer, an identity, and the inbox sizing hint.
type Opts struct {
	Producer  Producer
	Kind      string
	ID        string
	InboxSize int
}

// DefaultOpts returns Opts wired to produce actors from p with sensible
// defaults. Issue #9 will grow the set of defaults it fills in.
func DefaultOpts(p Producer) Opts {
	return Opts{
		Producer:  p,
		Kind:      "actor",
		InboxSize: defaultInboxSize,
	}
}

// OptFunc mutates Opts at spawn time. It is the variadic option type the Spawn
// family accepts. Issue #9 owns the full option surface; issue #8 defines the
// type and only the two helpers the engine needs today.
type OptFunc func(*Opts)

// WithID pins an actor's identity so it is reachable under a stable
// "kind/id". Without it the engine appends a process-unique suffix so two
// spawns of the same kind never collide.
func WithID(id string) OptFunc {
	return func(o *Opts) { o.ID = id }
}

// WithInboxSize overrides the initial inbox capacity hint. The value is a
// starting size, not a hard cap — the ring buffer grows on demand.
func WithInboxSize(size int) OptFunc {
	return func(o *Opts) { o.InboxSize = size }
}
