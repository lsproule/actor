package actor

// Envelope pairs a message with an optional sender. It is the only thing an
// inbox ever stores and the unit of delivery to an actor's Receive method.
//
// A nil Sender is legal and represents a fire-and-forget send from outside
// the engine (tests, top-level Spawn, etc.).
type Envelope struct {
	Msg    any
	Sender *PID // nil for fire-and-forget sends from outside the engine
}
