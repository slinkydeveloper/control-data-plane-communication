package controlprotocol

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"
	"time"

	"knative.dev/pkg/logging"
)

const (
	baseCertsPath = "/etc/secret"
)

var listenConfig = net.ListenConfig{
	KeepAlive: 30 * time.Second,
}

func LoadTLSConfig() (*tls.Config, error) {
	dataPlaneCert, err := tls.LoadX509KeyPair(baseCertsPath+"/data_plane_cert.pem", baseCertsPath+"/data_plane_secret.pem")
	if err != nil {
		return nil, err
	}

	caCert, err := ioutil.ReadFile(baseCertsPath + "/ca_cert.pem")
	if err != nil {
		return nil, err
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caCert)
	conf := &tls.Config{
		Certificates: []tls.Certificate{dataPlaneCert},
		ClientCAs:    certPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ServerName:   fakeDnsName,
	}

	return conf, nil
}

func StartControlServer(ctx context.Context, tlsConf *tls.Config) (Service, error) {
	ln, err := listenConfig.Listen(ctx, "tcp", ":9000")
	if err != nil {
		return nil, err
	}
	ln = tls.NewListener(ln, tlsConf)

	tcpConn := newServerTcpConnection(ctx, ln)
	ctrlService := newService(ctx, tcpConn)

	logging.FromContext(ctx).Infof("Started listener: %s", ln.Addr().String())

	tcpConn.startAcceptPolling()

	return ctrlService, nil
}

type serverTcpConnection struct {
	baseTcpConnection

	listener net.Listener
}

func newServerTcpConnection(ctx context.Context, listener net.Listener) *serverTcpConnection {
	c := &serverTcpConnection{
		baseTcpConnection: baseTcpConnection{
			ctx:                    ctx,
			logger:                 logging.FromContext(ctx),
			outboundMessageChannel: make(chan *OutboundMessage, 10),
			inboundMessageChannel:  make(chan *InboundMessage, 10),
			errors:                 make(chan error, 10),
		},
		listener: listener,
	}
	return c
}

func (t *serverTcpConnection) startAcceptPolling() {
	// We have 2 goroutines:
	// * One polls the listener to accept new conns
	// * One blocks on context done and closes the listener and the connection
	go func() {
		for {
			conn, err := t.listener.Accept()
			if err != nil {
				t.logger.Warnf("Error while accepting the connection, closing the accept loop: %s", err)
				return
			}
			t.logger.Debugf("Accepting new control connection from %s", conn.RemoteAddr())
			t.consumeConnection(conn)
			select {
			case <-t.ctx.Done():
				return
			default:
				continue
			}
		}
	}()
	go func() {
		<-t.ctx.Done()
		t.logger.Infof("Closing control server")
		err := t.listener.Close()
		t.logger.Infof("Listener closed")
		if err != nil {
			t.logger.Warnf("Error while closing the server: %s", err)
		}
		err = t.close()
	}()
}
