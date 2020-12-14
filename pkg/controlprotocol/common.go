package controlprotocol

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

const AckMessageType = "ack.control.knative.dev"

type ControlInterface interface {
	SendAndWaitForAck(event cloudevents.Event) error
	InboundMessages() <-chan ControlMessage
	Close(ctx context.Context)
}

type ControlMessage struct {
	event   cloudevents.Event
	ackFunc func()
}

func (c ControlMessage) Event() cloudevents.Event {
	return c.event
}

func (c ControlMessage) Ack() {
	c.ackFunc()
}
