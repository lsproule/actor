package actor

import "time"

type ActorInitializedEvent struct {
	PID       *PID
	Timestamp time.Time
}

type ActorStartedEvent struct {
	PID       *PID
	Timestamp time.Time
}

type ActorStoppedEvent struct {
	PID       *PID
	Timestamp time.Time
}

type ActorRestartedEvent struct {
	PID        *PID
	Timestamp  time.Time
	Stacktrace []byte
	Reason     any
	Restarts   int32
}

type DeadLetterEvent struct {
	Target  *PID
	Message any
	Sender  *PID
}
