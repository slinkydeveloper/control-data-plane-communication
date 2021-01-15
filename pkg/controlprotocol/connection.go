package controlprotocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"go.uber.org/atomic"
	"knative.dev/pkg/logging"
)

// Connection handles the low level stuff, reading and writing to the wire
type Connection interface {
	OutboundMessages() chan<- *OutboundMessage
	InboundMessages() <-chan *InboundMessage
	// On this channel we get only very bad, usually fatal, errors (like cannot re-establish the connection after several attempts)
	Errors() <-chan error
}

type baseTcpConnection struct {
	ctx context.Context

	connMutex sync.RWMutex
	conn      net.Conn

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

type clientTcpConnection struct {
	baseTcpConnection

	closing *atomic.Bool
}

func newClientTcpConnection(ctx context.Context, conn net.Conn) *clientTcpConnection {
	c := &clientTcpConnection{
		baseTcpConnection: baseTcpConnection{
			ctx:                    ctx,
			conn:                   conn,
			outboundMessageChannel: make(chan *OutboundMessage, 10),
			inboundMessageChannel:  make(chan *InboundMessage, 10),
			errors:                 make(chan error, 10),
		},
		closing: atomic.NewBool(false),
	}
	c.startPolling()
	return c
}

func (t *clientTcpConnection) startPolling() {
	// We have 3 polling loops:
	// * One polls the context and closes the connection when the context is closed
	// * One polls outbound messages and writes to the conn
	// * One reads from the conn and push to inbound messages
	go func() {
		for {
			select {
			case <-t.ctx.Done():
				t.closing.Store(true)
				err := t.conn.Close()
				if err != nil {
					logging.FromContext(t.ctx).Warnf("Error while closing the connection: %s", err)
				}
				close(t.errors)
				return
			}
		}
	}()
	go func() {
		defer close(t.outboundMessageChannel)
		for {
			select {
			case msg, ok := <-t.outboundMessageChannel:
				if !ok {
					logging.FromContext(t.ctx).Debugf("Outbound channel closed, closing the polling")
					return
				}
				var err error
				for i := 0; i < writeRetries; i++ {
					err = t.write(msg)
					if err == nil || !t.handleError(err) {
						err = nil
						break
					}
				}
				if err != nil {
					t.errors <- fmt.Errorf("write retry exceeded. msg %s not delivered. last error: %v", msg.UUID(), err)
				}
			case <-t.ctx.Done():
				logging.FromContext(t.ctx).Debugf("Context closed, closing outbound polling loop of tcp connection")
				return
			}
		}
	}()
	go func() {
		defer close(t.inboundMessageChannel)
		for {
			// Blocking read
			err := t.read()
			if err != nil {
				t.handleError(err)
			}

			select {
			case <-t.ctx.Done():
				logging.FromContext(t.ctx).Debugf("Context closed, closing inbound polling loop of tcp connection")
				return
			default:
				continue
			}
		}
	}()
}

// Returns true if we should retry
func (t *clientTcpConnection) handleError(err error) bool {
	if t.closing.Load() {
		// Everything fine, we can discard it
		return false
	}
	// Transient errors are fine
	if isTransientError(err) {
		return true
	}

	// Check connection closed https://stackoverflow.com/questions/12741386/how-to-know-tcp-connection-is-closed-in-net-package
	if errors.Is(err, io.EOF) {
		// We need a new conn
		// TODO there could be a race condition here because the two goroutines might try at the same time to connect here!
		t.connMutex.Lock()
		t.conn, err = tryDial(t.ctx, t.conn.RemoteAddr().String(), clientReconnectionRetry, clientDialRetryInterval)
		t.connMutex.Unlock()
		if err == nil {
			// All good, we're reconnected!
			return true
		}
		err = fmt.Errorf("connection closed, retried to reconnect but failed: %s", err)
	}

	// Something bad happened
	t.errors <- err
	return false
}

type serverTcpConnection struct {
	baseTcpConnection
}

func newServerTcpConnection(ctx context.Context) *serverTcpConnection {
	c := &serverTcpConnection{
		baseTcpConnection: baseTcpConnection{
			ctx:                    ctx,
			outboundMessageChannel: make(chan *OutboundMessage, 10),
			inboundMessageChannel:  make(chan *InboundMessage, 10),
			errors:                 make(chan error, 10),
		},
	}
	return c
}

func (t *serverTcpConnection) setConn(conn net.Conn) {
	logging.FromContext(t.ctx).Infof("Setting new conn: %s", conn.RemoteAddr())
	t.connMutex.Lock()
	if t.conn != nil {
		err := t.conn.Close()
		if err != nil {
			logging.FromContext(t.ctx).Warnf("Error while closing the previous connection: %s", err)
		}
	}
	t.conn = conn
	t.connMutex.Unlock()
}

func (t *serverTcpConnection) startPolling() {
	// We have 3 polling loops:
	// * One checks if the context is closed and closes the internal channels
	// * One polls outbound messages and writes to the conn
	// * One reads from the conn and push to inbound messages
	go func() {
		for {
			select {
			case <-t.ctx.Done():
				close(t.inboundMessageChannel)
				close(t.outboundMessageChannel)
				close(t.errors)
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case msg, ok := <-t.outboundMessageChannel:
				if !ok {
					logging.FromContext(t.ctx).Debugf("Outbound channel closed, closing the polling")
					return
				}
				var err error
				for i := 0; i < writeRetries; i++ {
					err = t.write(msg)
					if err == nil {
						break
					}
					// Transient errors are fine
					if neterr, ok := err.(net.Error); ok {
						if neterr.Temporary() || neterr.Timeout() {
							continue
						}
					}
					// Check connection closed https://stackoverflow.com/questions/12741386/how-to-know-tcp-connection-is-closed-in-net-package
					if errors.Is(err, io.EOF) {
						t.handleDisconnection()
						continue
					}
					// Something bad happened, but we can retry
					t.errors <- err
				}
				if err != nil {
					t.errors <- fmt.Errorf("write retry exceeded. msg %s not delivered. last error: %v", msg.UUID(), err)
				}
			case <-t.ctx.Done():
				logging.FromContext(t.ctx).Debugf("Context closed, closing outbound polling loop of tcp connection")
				return
			}
		}
	}()
	go func() {
		for {
			// Blocking read
			err := t.read()
			if err != nil {
				// Check connection closed https://stackoverflow.com/questions/12741386/how-to-know-tcp-connection-is-closed-in-net-package
				if errors.Is(err, io.EOF) {
					t.handleDisconnection()
					continue
				}

				// Everything is bad except transient errors and io.EOF
				if !isTransientError(err) {
					// Something bad happened, but we can retry
					t.errors <- err
				}
			}

			select {
			case <-t.ctx.Done():
				logging.FromContext(t.ctx).Debugf("Context closed, closing inbound polling loop of tcp connection")
				return
			default:
				continue
			}
		}
	}()
}

func (t *serverTcpConnection) handleDisconnection() {
	time.Sleep(serverWaitForReconn)
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
