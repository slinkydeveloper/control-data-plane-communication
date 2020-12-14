package controlprotocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	cews "github.com/cloudevents/sdk-go/protocol/ws/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"knative.dev/pkg/logging"
)

const sendRetries = 5
const sendTimeout = 10 * time.Second

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

func (c *controlService) send(event cloudevents.Event) <-chan interface{} {
	ackCh := make(chan interface{}, 1)

	event.SetSource(c.source)
	c.outbound <- event

	// Register the ack between the waiting acks
	c.waitingAcksMutex.Lock()
	c.waitingAcks[event.ID()] = ackCh
	c.waitingAcksMutex.Unlock()

	return ackCh
}

func (c *controlService) SendAndWaitForAck(event cloudevents.Event) error {
	for i := 0; i < sendRetries; i++ {
		select {
		case <-c.send(event):
			c.waitingAcksMutex.Lock()
			delete(c.waitingAcks, event.ID())
			c.waitingAcksMutex.Unlock()
			return nil
		case <-time.After(sendTimeout):
			logging.FromContext(c.ctx).Debugf("Timeout waiting for the ack, retrying to send: %s", event.Type())
			c.waitingAcksMutex.Lock()
			delete(c.waitingAcks, event.ID())
			c.waitingAcksMutex.Unlock()
		}
	}

	return fmt.Errorf("retry exceeded for event %v", event)
}

func (c *controlService) InboundMessages() <-chan ControlMessage {
	return c.inbound
}

func (c *controlService) blockOnPolling(ctx context.Context, p *cews.Protocol) {
	receiveCtx, receiveCancelFn := context.WithCancel(ctx)
	sendCtx, sendCancelFn := context.WithCancel(ctx)

	// Start read goroutine
	go func() {
		defer receiveCancelFn()
		for {
			m, err := p.UnsafeReceive(ctx)
			if errors.Is(err, io.EOF) || (err != nil && err == ctx.Err()) {
				logging.FromContext(ctx).Debugf("EOF or context cancelled: %v", err)
				return
			}
			if err != nil {
				logging.FromContext(ctx).Warnf("Error while reading a new control message: %v", err)
				return
			}
			ev, err := binding.ToEvent(ctx, m)
			_ = m.Finish(nil)
			if err != nil {
				logging.FromContext(ctx).Warnf("Error while translating the new control message to event: %v", err)
			}
			c.accept(*ev)
		}
	}()

	// Start write goroutine
	go func() {
		defer sendCancelFn()
		for {
			select {
			case ev := <-c.outbound:
				err := p.Send(ctx, binding.ToMessage(&ev))
				if errors.Is(err, io.EOF) || (err != nil && err == ctx.Err()) {
					logging.FromContext(ctx).Debugf("EOF or context cancelled: %v", err)
					c.outbound <- ev
					return
				}
				if err != nil {
					logging.FromContext(ctx).Warnf("Error while sending a control message: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
	case <-receiveCtx.Done():
	case <-sendCtx.Done():
	}
}

func (c *controlService) accept(event cloudevents.Event) {
	if event.Type() == AckMessageType {
		// Propagate the ack
		c.waitingAcksMutex.RLock()
		ackCh := c.waitingAcks[event.ID()]
		c.waitingAcksMutex.RUnlock()
		if ackCh != nil {
			close(ackCh)
		} else {
			logging.FromContext(c.ctx).Debugf("Ack received but no channel available: %s", event.ID())
		}
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

func (c *controlService) Close(ctx context.Context) {
	close(c.outbound)
	close(c.inbound)
}
