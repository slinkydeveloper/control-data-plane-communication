package controlprotocol

import (
	"context"
	"sync"
)

type ControlConnections struct {
	source string

	connsLock   sync.Mutex
	conns       map[string]ControlInterface
	cancelFuncs map[string]context.CancelFunc
}

func New(source string) *ControlConnections {
	return &ControlConnections{
		conns:       make(map[string]ControlInterface),
		cancelFuncs: make(map[string]context.CancelFunc),
		source:      source,
	}
}

func (cc *ControlConnections) ResolveControlInterface(key string) ControlInterface {
	cc.connsLock.Lock()
	defer cc.connsLock.Unlock()
	return cc.conns[key]
}

func (cc *ControlConnections) RemoveConnection(ctx context.Context, key string) {
	cc.connsLock.Lock()
	defer cc.connsLock.Unlock()
	if controlInterface, ok := cc.conns[key]; ok {
		controlInterface.Close(ctx)
	}
	delete(cc.conns, key)
}

func (cc *ControlConnections) CreateNewControlInterface(ctx context.Context, key string, host string) (ControlInterface, error) {
	// Need to start new conn
	ctx, cancelFn := context.WithCancel(ctx)
	newConn, err := StartControlClient(ctx, cc.source, host)
	if err != nil {
		cancelFn()
		return nil, err
	}

	cc.connsLock.Lock()
	cc.conns[key] = newConn
	cc.cancelFuncs[key] = cancelFn
	cc.connsLock.Unlock()

	return newConn, nil
}
