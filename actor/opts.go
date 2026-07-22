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
