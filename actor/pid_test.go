package actor_test

import "testing"

import "github.com/stretchr/testify/assert"
import "github.com/lsproule/actor/actor"

func TestPIDString(t *testing.T) {
	tests := []struct {
		name     string
		pid      *actor.PID
		expected string
	}{
		{
			name:     "normal",
			pid:      actor.NewPID("127.0.0.1:8080", "worker-1"),
			expected: "127.0.0.1:8080/worker-1",
		},
		{
			name:     "empty address",
			pid:      actor.NewPID("", "worker-1"),
			expected: "/worker-1",
		},
		{
			name:     "empty ID",
			pid:      actor.NewPID("127.0.0.1:8080", ""),
			expected: "127.0.0.1:8080/",
		},
		{
			name:     "nested child ID",
			pid:      actor.NewPID("local", "parent/child"),
			expected: "local/parent/child",
		},
		{
			name:     "nil receiver",
			pid:      nil,
			expected: "<nil>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.pid.String())
		})
	}
}

func TestPIDEquals(t *testing.T) {
	p1 := actor.NewPID("local", "actor-1")
	p2 := actor.NewPID("local", "actor-1")
	p3 := actor.NewPID("remote", "actor-1")
	p4 := actor.NewPID("local", "actor-2")

	tests := []struct {
		name     string
		p1       *actor.PID
		p2       *actor.PID
		expected bool
	}{
		{
			name:     "identical values constructed separately",
			p1:       p1,
			p2:       p2,
			expected: true,
		},
		{
			name:     "differing address",
			p1:       p1,
			p2:       p3,
			expected: false,
		},
		{
			name:     "differing ID",
			p1:       p1,
			p2:       p4,
			expected: false,
		},
		{
			name:     "nil receiver",
			p1:       nil,
			p2:       p1,
			expected: false,
		},
		{
			name:     "nil argument",
			p1:       p1,
			p2:       nil,
			expected: false,
		},
		{
			name:     "both nil",
			p1:       nil,
			p2:       nil,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.p1.Equals(tt.p2))
		})
	}
}

func TestPIDChild(t *testing.T) {
	t.Run("single level", func(t *testing.T) {
		parent := actor.NewPID("local", "parent")
		child := parent.Child("child-1")

		assert.Equal(t, "local", child.Address)
		assert.Equal(t, "parent/child-1", child.ID)
		assert.Equal(t, "parent", parent.ID, "parent must remain unchanged")
	})

	t.Run("two levels", func(t *testing.T) {
		root := actor.NewPID("local", "root")
		child := root.Child("level1")
		grandchild := child.Child("level2")

		assert.Equal(t, "local/root/level1/level2", grandchild.String())
	})

	t.Run("child of a PID with an empty ID", func(t *testing.T) {
		parent := actor.NewPID("local", "")
		child := parent.Child("child-1")

		assert.Equal(t, "local", child.Address)
		assert.Equal(t, "child-1", child.ID)
		assert.Equal(t, "local/child-1", child.String())
	})
}

func TestPIDStringAllocs(t *testing.T) {
	pid := actor.NewPID("127.0.0.1:8080", "system/supervisor/worker-42")

	allocs := testing.AllocsPerRun(100, func() {
		_ = pid.String()
	})

	assert.LessOrEqual(t, allocs, 1.0, "String() should perform at most 1 allocation")
}
