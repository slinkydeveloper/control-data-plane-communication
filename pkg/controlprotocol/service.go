package controlprotocol

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"knative.dev/pkg/logging"
)

const (
	controlServiceSendTimeout = 10 * time.Second
	controlServiceSendRetries = 5
)

type ControlMessage struct {
	inboundMessage *InboundMessage
	ackFunc        func()
}

func (c ControlMessage) Headers() MessageHeader {
	return c.inboundMessage.MessageHeader
}

func (c ControlMessage) Payload() []byte {
	return c.inboundMessage.Payload
}

func (c ControlMessage) Ack() {
	c.ackFunc()
}

type ControlMessageHandler interface {
	HandleControlMessage(ctx context.Context, message ControlMessage)
}

type ControlMessageHandlerFunc func(ctx context.Context, message ControlMessage)

func (c ControlMessageHandlerFunc) HandleControlMessage(ctx context.Context, message ControlMessage) {
	c(ctx, message)
}

var NoopControlMessageHandler ControlMessageHandlerFunc = func(ctx context.Context, message ControlMessage) {
	logging.FromContext(ctx).Warnf("Discarding control message '%s'", message.Headers().UUID())
	message.Ack()
}

type ErrorHandler interface {
	HandleServiceError(ctx context.Context, err error)
}

type ErrorHandlerFunc func(ctx context.Context, err error)

func (c ErrorHandlerFunc) HandleServiceError(ctx context.Context, err error) {
	c(ctx, err)
}

var LoggerErrorHandler ErrorHandlerFunc = func(ctx context.Context, err error) {
	logging.FromContext(ctx).Warnf("Error from the connection: %s", err)
}

// Service is the high level interface that handles send with retries and acks
type Service interface {
	SendAndWaitForAck(opcode uint8, payload []byte) error
	// This is non blocking, because a polling loop is already running inside.
	InboundMessageHandler(handler ControlMessageHandler)
	// This is non blocking, because a polling loop is already running inside.
	ErrorHandler(handler ErrorHandler)
}

type service struct {
	ctx context.Context

	connection Connection

	waitingAcksMutex sync.RWMutex
	waitingAcks      map[uuid.UUID]chan interface{}

	handlerMutex sync.RWMutex
	handler      ControlMessageHandler

	errorHandlerMutex sync.RWMutex
	errorHandler      ErrorHandler
}

func newService(ctx context.Context, connection Connection) *service {
	cs := &service{
		ctx:          ctx,
		connection:   connection,
		waitingAcks:  make(map[uuid.UUID]chan interface{}),
		handler:      NoopControlMessageHandler,
		errorHandler: LoggerErrorHandler,
	}
	cs.startPolling()
	return cs
}

func (c *service) SendAndWaitForAck(opcode uint8, payload []byte) error {
	msg, err := NewOutboundMessage(opcode, payload)
	if err != nil {
		return err
	}

	logging.FromContext(c.ctx).Debugf("Going to send message with opcode %d and uuid %s", msg.OpCode(), msg.UUID().String())

	// Register the ack between the waiting acks
	ackCh := make(chan interface{}, 1)
	c.waitingAcksMutex.Lock()
	c.waitingAcks[msg.uuid] = ackCh
	c.waitingAcksMutex.Unlock()

	defer func() {
		c.waitingAcksMutex.Lock()
		delete(c.waitingAcks, msg.uuid)
		c.waitingAcksMutex.Unlock()
	}()

	for i := 0; i < controlServiceSendRetries; i++ {
		c.connection.OutboundMessages() <- &msg
		select {
		case <-ackCh:
			return nil
		case <-c.ctx.Done():
			logging.FromContext(c.ctx).Warnf("Dropping message because context cancelled: %s", msg.UUID().String())
			return nil
		case <-time.After(controlServiceSendTimeout):
			logging.FromContext(c.ctx).Debugf("Timeout waiting for the ack, retrying to send: %s", msg.UUID().String())
		}
	}

	return fmt.Errorf("retry exceeded for outgoing message: %s", msg.UUID().String())

}

func (c *service) InboundMessageHandler(handler ControlMessageHandler) {
	c.handlerMutex.Lock()
	c.handler = handler
	c.handlerMutex.Unlock()
}

func (c *service) ErrorHandler(handler ErrorHandler) {
	c.errorHandlerMutex.Lock()
	c.errorHandler = handler
	c.errorHandlerMutex.Unlock()
}

func (c *service) startPolling() {
	go func() {
		for {
			select {
			case msg, ok := <-c.connection.InboundMessages():
				if !ok {
					logging.FromContext(c.ctx).Debugf("InboundMessages channel closed, closing the polling")
					return
				}
				c.accept(msg)
			case err, ok := <-c.connection.Errors():
				if !ok {
					logging.FromContext(c.ctx).Debugf("Errors channel closed")
				}
				c.errorHandlerMutex.RLock()
				c.errorHandler.HandleServiceError(c.ctx, err)
				c.errorHandlerMutex.RUnlock()
			case <-c.ctx.Done():
				logging.FromContext(c.ctx).Debugf("Context closed, closing polling loop of control service")
				return
			}
		}
	}()
}

func (c *service) accept(msg *InboundMessage) {
	if msg.opcode == AckOpCode {
		// Propagate the ack
		c.waitingAcksMutex.RLock()
		ackCh := c.waitingAcks[msg.uuid]
		c.waitingAcksMutex.RUnlock()
		if ackCh != nil {
			close(ackCh)
		} else {
			logging.FromContext(c.ctx).Debugf("Ack received but no channel available: %s", msg.UUID().String())
		}
	} else {
		ackFunc := func() {
			ackMsg := newAckMessage(msg.uuid)
			// TODO this can panic because outbound message might be closed!
			c.connection.OutboundMessages() <- &ackMsg
		}
		c.handlerMutex.RLock()
		c.handler.HandleControlMessage(c.ctx, ControlMessage{inboundMessage: msg, ackFunc: ackFunc})
		c.handlerMutex.RUnlock()
	}
}
