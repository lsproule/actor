package actor

import (
	"fmt"
	"log/slog"
)

type deadLetterActor struct{}

func (deadLetterActor) Receive(c *Context) {
	switch msg := c.Message().(type) {
	case DeadLetterEvent:
		slog.Warn("dead letter",
			"target", msg.Target,
			"sender", msg.Sender,
			"type", fmt.Sprintf("%T", msg.Message),
		)
	}
}

func newDeadLetter() Receiver {
	return deadLetterActor{}
}
