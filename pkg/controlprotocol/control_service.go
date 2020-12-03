package controlprotocol

import (
	"context"
	"io"
	"sync"

	cews "github.com/cloudevents/sdk-go/protocol/ws/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"knative.dev/pkg/logging"
)

type controlService struct {
	ctx    context.Context
	source string

	outbound chan cloudevents.Event
	inbound  chan ControlMessage

	waitingAcksMutex sync.RWMutex
	waitingAcks      map[string]chan interface{}
}

func newControlService(ctx context.Context, source string) *controlService {
	return &controlService{
		ctx:         ctx,
		outbound:    make(chan cloudevents.Event, 10),
		inbound:     make(chan ControlMessage, 10),
		source:      source,
		waitingAcks: make(map[string]chan interface{}),
	}
}

func (c *controlService) SendAndWaitForAck(event cloudevents.Event) error {
	event.SetSource(c.source)
	c.outbound <- event

	ackCh := make(chan interface{}, 1)

	// Register the ack between the waiting acks
	c.waitingAcksMutex.Lock()
	c.waitingAcks[event.ID()] = ackCh
	c.waitingAcksMutex.Unlock()

	// TODO This needs a timeout and also a retry mechanism
	<-ackCh

	return nil
}

func (c *controlService) InboundMessages() <-chan ControlMessage {
	return c.inbound
}

func (c *controlService) runPollingLoops(ctx context.Context, p *cews.Protocol) {
	// Start read goroutine
	go func() {
		for {
			m, err := p.UnsafeReceive(ctx)
			if err == io.EOF {
				return
			}
			if err != nil {
				logging.FromContext(ctx).Warnf("Error while reading a new control message: %v", err)
			}
			ev, err := binding.ToEvent(ctx, m)
			if err != nil {
				logging.FromContext(ctx).Warnf("Error while translating the new control message to event: %v", err)
			}
			logging.FromContext(ctx).Infof("Read goroutine received event: %v", ev)
			c.accept(*ev)
		}
	}()

	// Start write goroutine
	go func() {
		for {
			select {
			case ev := <-c.outbound:
				err := p.Send(ctx, binding.ToMessage(&ev))
				if err != nil {
					logging.FromContext(ctx).Warnf("Error while sending a control message: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	<-ctx.Done()
}

func (c *controlService) accept(event cloudevents.Event) {
	if event.Type() == AckMessageType {
		// Propagate the ack
		c.waitingAcksMutex.RLock()
		ackCh := c.waitingAcks[event.ID()]
		c.waitingAcksMutex.RUnlock()
		ackCh <- nil
	} else {
		ackFunc := func() {
			ackEv := cloudevents.NewEvent()
			ackEv.SetID(event.ID())
			ackEv.SetType(AckMessageType)
			ackEv.SetSource(c.source)
			c.outbound <- ackEv
		}
		c.inbound <- ControlMessage{event: event, ackFunc: ackFunc}
	}
}

func (c *controlService) close() {
	close(c.outbound)
	close(c.inbound)
}
