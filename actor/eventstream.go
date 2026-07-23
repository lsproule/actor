package actor

import "sync"

// eventStream is the engine's pub/sub table: a set of subscriber PIDs that
// every BroadcastEvent fans out to. Subscribers are keyed by PID ID — the
// same key the registry uses — so subscribing the same actor twice is
// idempotent and each event is delivered to it once.
//
// Delivery is an ordinary Send, so an event lands in the subscriber's inbox
// like any other message and a slow subscriber slows down nobody: the inbox
// grows rather than blocking the broadcaster.
type eventStream struct {
	mu   sync.RWMutex
	subs map[string]*PID
}

func newEventStream() *eventStream {
	return &eventStream{subs: make(map[string]*PID)}
}

func (s *eventStream) subscribe(pid *PID) {
	if pid == nil {
		return
	}
	s.mu.Lock()
	s.subs[pid.ID] = pid
	s.mu.Unlock()
}

func (s *eventStream) unsubscribe(pid *PID) {
	if pid == nil {
		return
	}
	s.mu.Lock()
	delete(s.subs, pid.ID)
	s.mu.Unlock()
}

// broadcast snapshots the live subscribers, drops the ones whose actor is
// gone, and then sends msg to each — outside the lock. Sending outside the
// lock is what makes it safe for a subscriber to Subscribe or Unsubscribe
// while handling an event: the broadcaster holds nothing the handler needs.
//
// Pruning is lazy, on the broadcast that first notices the actor missing
// from the registry. A stopped subscriber therefore costs at most one
// registry miss, never a broken stream.
func (s *eventStream) broadcast(e *Engine, msg any) {
	s.mu.Lock()
	live := make([]*PID, 0, len(s.subs))
	for id, pid := range s.subs {
		if e.registry == nil || e.registry.get(pid) == nil {
			delete(s.subs, id)
			continue
		}
		live = append(live, pid)
	}
	s.mu.Unlock()

	for _, pid := range live {
		e.Send(pid, msg)
	}
}

// len reports the current subscriber count. It exists for the tests, which
// assert that stopped subscribers are eventually pruned.
func (s *eventStream) len() int {
	s.mu.RLock()
	n := len(s.subs)
	s.mu.RUnlock()
	return n
}
