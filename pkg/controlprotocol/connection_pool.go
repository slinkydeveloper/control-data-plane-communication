package controlprotocol

import (
	"context"
	"fmt"
	"sync"

	"knative.dev/pkg/logging"
)

type ControlPlaneConnectionPool struct {
	source string

	connsLock   sync.Mutex
	conns       map[string]Service
	cancelFuncs map[string]context.CancelFunc
}

func NewControlPlaneConnectionPool(source string) *ControlPlaneConnectionPool {
	return &ControlPlaneConnectionPool{
		conns:       make(map[string]Service),
		cancelFuncs: make(map[string]context.CancelFunc),
		source:      source,
	}
}

func (cc *ControlPlaneConnectionPool) ResolveControlInterface(key string) Service {
	cc.connsLock.Lock()
	defer cc.connsLock.Unlock()
	return cc.conns[key]
}

func (cc *ControlPlaneConnectionPool) RemoveConnection(ctx context.Context, key string) {
	cc.connsLock.Lock()
	cc.cancelFuncs[key]()
	delete(cc.cancelFuncs, key)
	delete(cc.conns, key)
	cc.connsLock.Unlock()
}

func (cc *ControlPlaneConnectionPool) DialControlService(ctx context.Context, key string, host string) (Service, error) {
	// Need to start new conn
	ctx, cancelFn := context.WithCancel(ctx)
	newConn, err := startClientService(ctx, host)
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

func startClientService(ctx context.Context, target string) (Service, error) {
	target = target + ":9000"
	logging.FromContext(ctx).Infof("Starting control client to %s", target)

	// Let's try the dial
	conn, err := tryDial(ctx, target, clientInitialDialRetry, clientDialRetryInterval)
	if err != nil {
		return nil, fmt.Errorf("cannot perform the initial dial to target %s: %w", target, err)
	}

	tcpConn := newClientTcpConnection(ctx, conn)
	svc := newService(ctx, tcpConn)

	return svc, nil
}
