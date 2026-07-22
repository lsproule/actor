package actor

import "strings"

// PID is the address of an actor.
type PID struct {
	Address string
	ID      string
}

// NewPID constructs a new PID pointer for a given address and ID.
func NewPID(address, id string) *PID {
	return &PID{
		Address: address,
		ID:      id,
	}
}

// String returns the canonical "address/id" representation of the PID.
func (p *PID) String() string {
	if p == nil {
		return "<nil>"
	}

	var builder strings.Builder
	builder.Grow(len(p.Address) + 1 + len(p.ID))
	builder.WriteString(p.Address)
	builder.WriteByte('/')
	builder.WriteString(p.ID)

	return builder.String()
}

// Equals reports whether two PIDs have identical addresses and IDs.
// It handles nil receivers and arguments safely without panicking.
func (p *PID) Equals(other *PID) bool {
	if p == nil && other == nil {
		return true
	}
	if p == nil || other == nil {
		return false
	}
	return p.Address == other.Address && p.ID == other.ID
}

// Child creates a new PID under the current PID with a nested ID ("parentID/id").
func (p *PID) Child(id string) *PID {
	if p == nil {
		return NewPID("", id)
	}
	if p.ID == "" {
		return NewPID(p.Address, id)
	}

	var builder strings.Builder
	builder.Grow(len(p.ID) + 1 + len(id))
	builder.WriteString(p.ID)
	builder.WriteByte('/')
	builder.WriteString(id)

	return NewPID(p.Address, builder.String())
}
