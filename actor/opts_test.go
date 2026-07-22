package actor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDefaultOpts verifies every field is set to its documented constant.
func TestDefaultOpts(t *testing.T) {
	p := func() Receiver { return noop{} }
	opts := DefaultOpts(p)

	require.NotNil(t, opts.Producer)
	require.Equal(t, defaultInboxSize, opts.InboxSize)
	// int32, not int: require.Equal compares dynamic types, and the untyped
	// constant would otherwise default to int and never match the field.
	require.Equal(t, int32(defaultMaxRestarts), opts.MaxRestarts)
	require.Equal(t, defaultRestartDelay, opts.RestartDelay)
	require.Equal(t, "actor", opts.Kind)
	require.Equal(t, "", opts.ID)
	require.Equal(t, []string{}, opts.Tags)
}

// TestOptionsApplyInOrder verifies that the last option wins for fields set
// more than once. WithID("a"), WithID("b") yields ID == "b".
func TestOptionsApplyInOrder(t *testing.T) {
	p := func() Receiver { return noop{} }
	opts := DefaultOpts(p)

	opts.ID = ""
	WithID("first")(&opts)
	WithID("second")(&opts)
	WithID("third")(&opts)

	require.Equal(t, "third", opts.ID)
}

// TestOptionClampingTable verifies invalid values are clamped to sensible
// defaults instead of panicking. Test cases cover each clamp scenario.
func TestOptionClamping(t *testing.T) {
	tests := []struct {
		name  string
		apply func(*Opts)
		field string
		want  interface{}
	}{
		{
			name:  "InboxSize zero clamped to default",
			apply: WithInboxSize(0),
			field: "InboxSize",
			want:  defaultInboxSize,
		},
		{
			name:  "InboxSize negative clamped to default",
			apply: WithInboxSize(-5),
			field: "InboxSize",
			want:  defaultInboxSize,
		},
		{
			name:  "InboxSize positive accepted",
			apply: WithInboxSize(512),
			field: "InboxSize",
			want:  512,
		},
		{
			name:  "MaxRestarts negative clamped to zero",
			apply: WithMaxRestarts(-2),
			field: "MaxRestarts",
			want:  int32(0),
		},
		{
			name:  "MaxRestarts zero accepted",
			apply: WithMaxRestarts(0),
			field: "MaxRestarts",
			want:  int32(0),
		},
		{
			name:  "MaxRestarts positive accepted",
			apply: WithMaxRestarts(5),
			field: "MaxRestarts",
			want:  int32(5),
		},
		{
			name:  "RestartDelay negative clamped to default",
			apply: WithRestartDelay(-1 * time.Second),
			field: "RestartDelay",
			want:  defaultRestartDelay,
		},
		{
			name:  "RestartDelay zero accepted",
			apply: WithRestartDelay(0),
			field: "RestartDelay",
			want:  time.Duration(0),
		},
		{
			name:  "RestartDelay positive accepted",
			apply: WithRestartDelay(1 * time.Second),
			field: "RestartDelay",
			want:  time.Duration(1 * time.Second),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := func() Receiver { return noop{} }
			opts := DefaultOpts(p)
			tc.apply(&opts)

			switch tc.field {
			case "InboxSize":
				require.Equal(t, tc.want, opts.InboxSize)
			case "MaxRestarts":
				require.Equal(t, tc.want, opts.MaxRestarts)
			case "RestartDelay":
				require.Equal(t, tc.want, opts.RestartDelay)
			}
		})
	}
}

// TestWithTagsCopies verifies the tags slice is copied, so mutations to the
// caller's slice do not affect Opts.Tags.
func TestWithTagsCopies(t *testing.T) {
	p := func() Receiver { return noop{} }
	opts := DefaultOpts(p)

	tags := []string{"foo", "bar"}
	WithTags(tags...)(&opts)

	// Mutate the caller's original slice.
	tags[0] = "mutated"
	tags[1] = "changed"

	// Opts.Tags should be unaffected.
	require.Equal(t, []string{"foo", "bar"}, opts.Tags)
}

// TestWithTagsEmpty verifies that WithTags with no arguments clears tags.
func TestWithTagsEmpty(t *testing.T) {
	p := func() Receiver { return noop{} }
	opts := DefaultOpts(p)

	// Start with some tags.
	WithTags("initial")(&opts)
	require.Equal(t, []string{"initial"}, opts.Tags)

	// Replace with empty.
	WithTags()(&opts)
	require.Equal(t, []string{}, opts.Tags)
}

// TestWithTagsCompose verifies tags from multiple WithTags calls compose,
// with the last one replacing all prior tags (they do not accumulate).
func TestWithTagsCompose(t *testing.T) {
	p := func() Receiver { return noop{} }
	opts := DefaultOpts(p)

	WithTags("first", "second")(&opts)
	require.Equal(t, []string{"first", "second"}, opts.Tags)

	WithTags("third")(&opts)
	require.Equal(t, []string{"third"}, opts.Tags)
}

// TestSpawnWithNoOptionsStillWorks is an end-to-end regression test: spawn
// with zero options, send a message, and assert delivery. This exercises the
// full spawn path with default-only Opts.
func TestSpawnWithNoOptionsStillWorks(t *testing.T) {
	e := newTestEngine(t)
	ch := make(chan any, 1)
	pid := e.Spawn(func() Receiver { return &recorder{ch: ch} }, "default")

	e.Send(pid, "hello")

	require.Equal(t, "hello", mustReceive(t, ch))
}

// TestSpawnWithExplicitID verifies WithID("name") produces a PID reachable
// by that ID and a PID.String that contains the ID.
//
// The registry key is the namespaced "kind/id" that buildID composes, not the
// bare string handed to WithID: that is what makes the same id reusable across
// kinds without collision.
func TestSpawnWithExplicitID(t *testing.T) {
	e := newTestEngine(t)
	ch := make(chan any, 1)
	pid := e.Spawn(
		func() Receiver { return &recorder{ch: ch} },
		"worker",
		WithID("seven"),
	)

	require.Equal(t, "local/worker/seven", pid.String())
	require.Equal(t, "worker/seven", pid.ID)

	// Verify it is reachable by Send.
	e.Send(pid, "message")
	require.Equal(t, "message", mustReceive(t, ch))
}

// TestSpawnWithAllOptions verifies Spawn with multiple options applies them
// all and the actor works correctly with custom sizing.
func TestSpawnWithAllOptions(t *testing.T) {
	e := newTestEngine(t)
	ch := make(chan any, 1)
	pid := e.Spawn(
		func() Receiver { return &recorder{ch: ch} },
		"multi",
		WithID("test"),
		WithInboxSize(2048),
		WithMaxRestarts(5),
		WithRestartDelay(1*time.Second),
		WithTags("tag1", "tag2"),
	)

	// Verify ID applied.
	require.Equal(t, "local/multi/test", pid.String())

	// Verify message delivery works.
	e.Send(pid, "content")
	require.Equal(t, "content", mustReceive(t, ch))

	// Note: MaxRestarts and RestartDelay are carried but not acted upon in
	// this issue, so we cannot assert their effect directly. They are tested
	// for clamping in TestOptionClamping.
}

// TestDefaultOptsKindNotOverwritten verifies DefaultOpts sets Kind to "actor"
// but Spawn can override it without WithKind (Kind is passed separately).
func TestDefaultOptsKindNotOverwritten(t *testing.T) {
	e := newTestEngine(t)
	pid := e.Spawn(func() Receiver { return noop{} }, "custom_kind")

	// The Kind passed to Spawn should be in the ID, not the default "actor".
	require.True(t, len(pid.String()) > 0)
	require.Contains(t, pid.String(), "custom_kind")
}
