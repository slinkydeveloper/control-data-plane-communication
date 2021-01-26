package service

import (
	ctrl "knative.dev/control-data-plane-communication/pkg/control"
)

type ControlMessage struct {
	inboundMessage *ctrl.InboundMessage
	ackFunc        func()
}

func (c ControlMessage) Headers() ctrl.MessageHeader {
	return c.inboundMessage.MessageHeader
}

func (c ControlMessage) Payload() []byte {
	return c.inboundMessage.Payload
}

func (c ControlMessage) Ack() {
	c.ackFunc()
}
