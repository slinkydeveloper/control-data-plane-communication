package controlprotocol

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"knative.dev/pkg/logging"
)

type ControlPlaneConnectionPool struct {
	certificateManager *CertificateManager
	baseDialOptions    *net.Dialer

	connsLock sync.Mutex
	conns     map[string]map[string]clientServiceHolder
}

type clientServiceHolder struct {
	service  Service
	cancelFn context.CancelFunc
}

func NewInsecureControlPlaneConnectionPool() *ControlPlaneConnectionPool {
	return NewControlPlaneConnectionPool(nil)
}

func NewControlPlaneConnectionPool(certificateManager *CertificateManager) *ControlPlaneConnectionPool {
	return &ControlPlaneConnectionPool{
		certificateManager: certificateManager,
		baseDialOptions: &net.Dialer{
			KeepAlive: keepAlive,
			Deadline:  time.Time{},
		},
		conns: make(map[string]map[string]clientServiceHolder),
	}
}

func (cc *ControlPlaneConnectionPool) GetConnectedHosts(key string) []string {
	cc.connsLock.Lock()
	defer cc.connsLock.Unlock()
	var m map[string]clientServiceHolder
	var ok bool
	if m, ok = cc.conns[key]; !ok {
		return nil
	}
	hosts := make([]string, 0, len(m))
	for k, _ := range m {
		hosts = append(hosts, k)
	}
	return hosts
}

func (cc *ControlPlaneConnectionPool) GetServices(key string) map[string]Service {
	cc.connsLock.Lock()
	defer cc.connsLock.Unlock()
	var m map[string]clientServiceHolder
	var ok bool
	if m, ok = cc.conns[key]; !ok {
		return nil
	}
	svcs := make(map[string]Service, len(m))
	for k, h := range m {
		svcs[k] = h.service
	}
	return svcs
}

func (cc *ControlPlaneConnectionPool) ResolveControlInterface(key string, host string) (string, Service) {
	cc.connsLock.Lock()
	defer cc.connsLock.Unlock()
	if m, ok := cc.conns[key]; !ok {
		return "", nil
	} else if holder, ok := m[host]; !ok {
		return host, holder.service
	}

	return "", nil
}

func (cc *ControlPlaneConnectionPool) RemoveConnection(ctx context.Context, key string, host string) {
	cc.connsLock.Lock()
	defer cc.connsLock.Unlock()
	m, ok := cc.conns[key]
	if !ok {
		return
	}
	holder, ok := m[host]
	if !ok {
		return
	}
	holder.cancelFn()
	delete(m, host)
	if len(m) == 0 {
		delete(cc.conns, key)
	}
}

func (cc *ControlPlaneConnectionPool) RemoveAllConnections(ctx context.Context, key string) {
	cc.connsLock.Lock()
	defer cc.connsLock.Unlock()
	m, ok := cc.conns[key]
	if !ok {
		return
	}
	for _, holder := range m {
		holder.cancelFn()
	}
	delete(cc.conns, key)
}

func (cc *ControlPlaneConnectionPool) ReconcileConnections(ctx context.Context, key string, wantConnections []string, newServiceCb func(string, Service), oldServiceCb func(string)) (map[string]Service, error) {
	existingConnections := cc.GetConnectedHosts(key)

	newConnections := setDifference(wantConnections, existingConnections)
	oldConnections := setDifference(existingConnections, wantConnections)

	logging.FromContext(ctx).Debugf("New connections: %v", newConnections)
	logging.FromContext(ctx).Debugf("Old connections: %v", oldConnections)

	for _, newConn := range newConnections {
		logging.FromContext(ctx).Debugf("Creating a new control connection: %s", newConn)

		// Dial the service
		_, ctrl, err := cc.DialControlService(ctx, key, newConn)
		if err != nil {
			return nil, fmt.Errorf("cannot connect to the pod: %w", err)
		}

		if newServiceCb != nil {
			newServiceCb(newConn, ctrl)
		}
	}

	for _, oldConn := range oldConnections {
		logging.FromContext(ctx).Debugf("Cleaning up old connection: %s", oldConn)
		if oldServiceCb != nil {
			oldServiceCb(oldConn)
		}
		cc.RemoveConnection(ctx, key, oldConn)
	}

	logging.FromContext(ctx).Debugf("Now connected to: %v", cc.GetConnectedHosts(key))

	return cc.GetServices(key), nil
}

func (cc *ControlPlaneConnectionPool) DialControlService(ctx context.Context, key string, host string) (string, Service, error) {
	var dialer Dialer
	dialer = cc.baseDialOptions
	// Check if certificateManager is set up, otherwise connect without tls
	if cc.certificateManager != nil {
		// Create TLS dialer
		var err error
		dialer, err = createTLSDialer(cc.certificateManager, cc.baseDialOptions)
		if err != nil {
			return "", nil, err
		}
	}

	// Need to start new conn
	ctx, cancelFn := context.WithCancel(ctx)
	newSvc, err := StartControlClient(ctx, dialer, host)
	if err != nil {
		cancelFn()
		return "", nil, err
	}

	cc.connsLock.Lock()
	var m map[string]clientServiceHolder
	var ok bool
	if m, ok = cc.conns[key]; !ok {
		m = make(map[string]clientServiceHolder)
		cc.conns[key] = m
	}
	m[host] = clientServiceHolder{
		service:  newSvc,
		cancelFn: cancelFn,
	}
	cc.connsLock.Unlock()

	return host, newSvc, nil
}

func setDifference(a, b []string) (diff []string) {
	m := make(map[string]bool)

	for _, item := range b {
		m[item] = true
	}

	for _, item := range a {
		if _, ok := m[item]; !ok {
			diff = append(diff, item)
		}
	}
	return
}
