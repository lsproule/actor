package actor

// Receiver is the interface an actor implements to handle messages.
//
// It is formally issue #4's file and is declared here with just the interface
// so that issue #3 can compile. Issue #4 will extend this file with lifecycle
// messages (Initialized, Started, Stopped) and supervision hooks.
type Receiver interface {
	Receive(*Context)
}
