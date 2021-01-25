package controlprotocol

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"sync"
	"time"
)

type ControlPlaneConnectionPool struct {
	certificateManager *CertificateManager
	baseDialOptions    *net.Dialer

	connsLock sync.Mutex
	conns     map[string]clientServiceHolder
}

type clientServiceHolder struct {
	service  Service
	host     string
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
		conns: make(map[string]clientServiceHolder),
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
	cc.conns[key] = clientServiceHolder{
		service:  newSvc,
		host:     host,
		cancelFn: cancelFn,
	}
	cc.connsLock.Unlock()

	return host, newSvc, nil
}

func createTLSDialer(certificateManager *CertificateManager, baseDialOptions *net.Dialer) (*tls.Dialer, error) {
	caCert := certificateManager.caCert
	controlPlaneKeyPair := certificateManager.controllerKeyPair

	controlPlaneCert, err := tls.X509KeyPair(controlPlaneKeyPair.CertBytes(), controlPlaneKeyPair.PrivateKeyBytes())
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	certPool.AddCert(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{controlPlaneCert},
		RootCAs:      certPool,
		ServerName:   fakeDnsName,
	}

	// Copy from base dial options
	dialOptions := *baseDialOptions

	return &tls.Dialer{
		NetDialer: &dialOptions,
		Config:    tlsConfig,
	}, nil
}
