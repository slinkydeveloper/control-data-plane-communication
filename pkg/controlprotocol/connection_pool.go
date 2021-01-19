package controlprotocol

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"
)

type ControlPlaneConnectionPool struct {
	controllerKeyPair *KeyHolder
	dataPlaneKeyPair  *KeyHolder
	dialOptions       *net.Dialer

	connsLock sync.Mutex
	conns     map[string]clientServiceHolder
}

type clientServiceHolder struct {
	service  Service
	host     string
	cancelFn context.CancelFunc
}

func NewControlPlaneConnectionPool(controllerKeyPair *KeyHolder, dataPlaneKeyPair *KeyHolder) *ControlPlaneConnectionPool {
	tlsConfig := tls.Config{}

	dialOptions := &net.Dialer{
		KeepAlive: keepAlive,
		Deadline:  time.Time{},
	}

	return &ControlPlaneConnectionPool{
		controllerKeyPair: controllerKeyPair,
		dataPlaneKeyPair:  dataPlaneKeyPair,
		conns:             make(map[string]clientServiceHolder),
		dialOptions:       dialOptions,
	}
}

func (cc *ControlPlaneConnectionPool) ResolveControlInterface(key string) (string, Service) {
	cc.connsLock.Lock()
	defer cc.connsLock.Unlock()
	if holder, ok := cc.conns[key]; ok {
		return holder.host, holder.service
	}
	return "", nil
}

func (cc *ControlPlaneConnectionPool) RemoveConnection(ctx context.Context, key string) {
	cc.connsLock.Lock()
	defer cc.connsLock.Unlock()
	holder, ok := cc.conns[key]
	if !ok {
		return
	}
	holder.cancelFn()
	delete(cc.conns, key)
}

func (cc *ControlPlaneConnectionPool) DialControlService(ctx context.Context, key string, host string) (string, Service, error) {
	// Need to start new conn
	ctx, cancelFn := context.WithCancel(ctx)
	newSvc, err := StartControlClient(ctx, cc.dialOptions, host)
	if err != nil {
		cancelFn()
		return "", nil, err
	}

	cc.connsLock.Lock()
	cc.conns[key] = clientServiceHolder{
		service:  newSvc,
		host:     host,
		cancelFn: cancelFn,
	}
	cc.connsLock.Unlock()

	return host, newSvc, nil
}

func (cc *ControlPlaneConnectionPool) DataPlaneKeyPair() *KeyHolder {
	return cc.dataPlaneKeyPair
}

func (cc *ControlPlaneConnectionPool) ControllerKeyPair() *KeyHolder {
	return cc.controllerKeyPair
}
