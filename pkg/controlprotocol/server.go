package controlprotocol

import (
	"context"
	"net"
	"sync"
	"time"

	"knative.dev/pkg/logging"
)

var listenConfig = net.ListenConfig{
	KeepAlive: 30 * time.Second,
}

func StartControlServer(ctx context.Context) (Service, error) {
	ln, err := listenConfig.Listen(ctx, "tcp", ":9000")
	if err != nil {
		return nil, err
	}

	tcpConn := newServerTcpConnection(ctx)
	ctrlService := newService(ctx, tcpConn)

	once := sync.Once{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				logging.FromContext(ctx).Warnf("Error while accepting the connection, closing the accept loop: %s", err)
				return
			}
			logging.FromContext(ctx).Debugf("Accepting new control connection from %s", conn.RemoteAddr())
			tcpConn.setConn(conn)
			once.Do(tcpConn.startPolling)
		}
	}()
	go func() {
		<-ctx.Done()
		logging.FromContext(ctx).Infof("Closing control server")
		tcpConn.connMutex.RLock()
		err := tcpConn.conn.Close()
		tcpConn.connMutex.RUnlock()
		logging.FromContext(ctx).Infof("Connection closed")
		if err != nil {
			logging.FromContext(ctx).Warnf("Error while closing the connection: %s", err)
		}
		err = ln.Close()
		logging.FromContext(ctx).Infof("Listener closed")
		if err != nil {
			logging.FromContext(ctx).Warnf("Error while closing the server: %s", err)
		}
	}()
	return ctrlService, nil
}
