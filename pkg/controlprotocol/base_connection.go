package controlprotocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"go.uber.org/zap"
)

// Connection handles the low level stuff, reading and writing to the wire
type Connection interface {
	OutboundMessages() chan<- *OutboundMessage
	InboundMessages() <-chan *InboundMessage
	// On this channel we get only very bad, usually fatal, errors (like cannot re-establish the connection after several attempts)
	Errors() <-chan error
}

type baseTcpConnection struct {
	ctx    context.Context
	logger *zap.SugaredLogger

	conn      net.Conn
	connMutex sync.RWMutex

	outboundMessageChannel chan *OutboundMessage
	inboundMessageChannel  chan *InboundMessage
	errors                 chan error
}

func (t *baseTcpConnection) OutboundMessages() chan<- *OutboundMessage {
	return t.outboundMessageChannel
}

func (t *baseTcpConnection) InboundMessages() <-chan *InboundMessage {
	return t.inboundMessageChannel
}

func (t *baseTcpConnection) Errors() <-chan error {
	return t.errors
}

func (t *baseTcpConnection) read() error {
	msg := &InboundMessage{}
	t.connMutex.RLock()
	n, err := msg.ReadFrom(t.conn)
	t.connMutex.RUnlock()
	if err != nil {
		return err
	}
	if n != int64(msg.Length())+24 {
		return fmt.Errorf("the number of read bytes doesn't match the expected length: %d != %d", n, int64(msg.Length())+24)
	}

	t.inboundMessageChannel <- msg
	return nil
}

func (t *baseTcpConnection) write(msg *OutboundMessage) error {
	t.connMutex.RLock()
	n, err := msg.WriteTo(t.conn)
	t.connMutex.RUnlock()
	if err != nil {
		return err
	}
	if n != int64(msg.Length())+24 {
		return fmt.Errorf("the number of read bytes doesn't match the expected length: %d != %d", n, int64(msg.Length())+24)
	}
	return nil
}

func (t *baseTcpConnection) consumeConnection(conn net.Conn) {
	t.logger.Infof("Setting new conn: %s", conn.RemoteAddr())
	t.connMutex.Lock()
	t.conn = conn
	t.connMutex.Unlock()

	closedConnCtx, closedConnCancel := context.WithCancel(t.ctx)

	var wg sync.WaitGroup
	wg.Add(2)

	// We have 2 polling loops:
	// * One polls outbound messages and writes to the conn
	// * One reads from the conn and push to inbound messages
	go func() {
		defer wg.Done()
		for {
			select {
			case msg, ok := <-t.outboundMessageChannel:
				if !ok {
					t.logger.Debugf("Outbound channel closed, closing the polling")
					return
				}
				err := t.write(msg)
				if err != nil {
					if isEOF(err) {
						return // Closed conn
					}
					t.tryPropagateError(closedConnCtx, err)
					if !isTransientError(err) {
						return // Broken conn
					}

					// Try to send to outboundMessageChannel if context not closed
					t.tryPushOutboundChannel(closedConnCtx, msg)
				}
			case <-closedConnCtx.Done():
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		defer closedConnCancel()
		for {
			// Blocking read
			err := t.read()
			if err != nil {
				if isEOF(err) {
					return // Closed conn
				}
				t.tryPropagateError(closedConnCtx, err)
				if !isTransientError(err) {
					return // Broken conn
				}
			}

			select {
			case <-closedConnCtx.Done():
				return
			default:
				continue
			}
		}
	}()

	wg.Wait()

	t.logger.Debugf("Stopped consuming connection with local %s and remote %s", conn.LocalAddr().String(), conn.RemoteAddr().String())
	t.connMutex.RLock()
	err := t.conn.Close()
	t.connMutex.RUnlock()
	if err != nil && !isEOF(err) {
		t.logger.Warnf("Error while closing the previous connection: %s", err)
	}
}

func (t *baseTcpConnection) tryPropagateError(ctx context.Context, err error) {
	select {
	case <-ctx.Done():
		return
	default:
		t.errors <- err
	}
}

func (t *baseTcpConnection) tryPushOutboundChannel(ctx context.Context, msg *OutboundMessage) {
	select {
	case <-ctx.Done():
		return
	default:
		t.outboundMessageChannel <- msg
	}
}

func (t *baseTcpConnection) close() (err error) {
	t.connMutex.RLock()
	if t.conn != nil {
		err = t.conn.Close()
	}
	t.connMutex.RUnlock()
	close(t.inboundMessageChannel)
	close(t.outboundMessageChannel)
	close(t.errors)
	return err
}

func isTransientError(err error) bool {
	// Transient errors are fine
	if neterr, ok := err.(net.Error); ok {
		if neterr.Temporary() || neterr.Timeout() {
			return true
		}
	}
	return false
}

func isEOF(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
