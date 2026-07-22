package actor

import "sync"

// registry stores the live processes owned by an Engine. Processes are keyed
// by PID ID rather than PID.String because every actor in this proof of concept
// is local and therefore has the same address; avoiding the address also avoids
// building a string on the message-routing hot path.
type registry struct {
	mu     sync.RWMutex
	lookup map[string]Processer
	engine *Engine
}

func newRegistry(e *Engine) *registry {
	return &registry{
		lookup: make(map[string]Processer),
		engine: e,
	}
}

func (r *registry) get(pid *PID) Processer {
	if pid == nil {
		return nil
	}
	return r.getByID(pid.ID)
}

func (r *registry) getByID(id string) Processer {
	r.mu.RLock()
	proc := r.lookup[id]
	r.mu.RUnlock()
	return proc
}

func (r *registry) add(proc Processer) bool {
	id := proc.PID().ID

	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, exists := r.lookup[id]; exists {
		// A restart (issue #10) calls Start -> registerProcess again on the
		// very same *process, under the PID it never left the registry
		// under. That is not a collision — it is the same Processer
		// instance re-announcing itself — so tolerate it instead of
		// rejecting the restarted actor's own re-registration. A distinct
		// Processer claiming an ID still in use is a real collision and is
		// rejected as before.
		return existing == proc
	}
	r.lookup[id] = proc
	return true
}

func (r *registry) remove(pid *PID) {
	if pid == nil {
		return
	}

	r.mu.Lock()
	delete(r.lookup, pid.ID)
	r.mu.Unlock()
}

func (r *registry) len() int {
	r.mu.RLock()
	length := len(r.lookup)
	r.mu.RUnlock()
	return length
}
