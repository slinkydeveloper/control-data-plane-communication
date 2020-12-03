package controlprotocol

import cloudevents "github.com/cloudevents/sdk-go/v2"

const AckMessageType = "ack.control.knative.dev"

type ControlInterface interface {
	SendAndWaitForAck(event cloudevents.Event) error
	Receive() (event cloudevents.Event, ackFunc func(), err error)
}
