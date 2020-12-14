package controlprotocol

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type controlConnections struct {
	connsLock sync.Mutex
	conns     map[string]atomic.Value
}

type controlConnectionsKey struct{}

func ControlInterfaceFromContext(ctx context.Context, source string, host string) (ControlInterface, error) {
	controlConns := ctx.Value(controlConnectionsKey{}).(*controlConnections)
	if controlConns == nil {
		return nil, fmt.Errorf("the context is not configured with control connections")
	}

	var connLock atomic.Value
	controlConns.connsLock.Lock()
	var ok bool
	if connLock, ok = controlConns.conns[host]; !ok {
		// Need to start new conn
		newConn, err := StartControlClient(ctx, source, host)
		if err != nil {
			controlConns.connsLock.Unlock()
			return nil, err
		}
		connLock = atomic.Value{}
		connLock.Store(newConn)
		controlConns.conns[host] = connLock
	}
	controlConns.connsLock.Unlock()
	return connLock.Load().(ControlInterface), nil
}

func WithControlConnections(ctx context.Context) context.Context {
	return context.WithValue(ctx, controlConnectionsKey{}, &controlConnections{
		conns: make(map[string]atomic.Value),
	})
}
