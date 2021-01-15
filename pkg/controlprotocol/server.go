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
			logging.FromContext(ctx).Debugf("Accepting new control connection from %s", conn.RemoteAddr())
			if err != nil {
				logging.FromContext(ctx).Warnf("Error while accepting the connection: %s", err)
			} else {
				tcpConn.setConn(conn)
			}

			once.Do(tcpConn.startPolling)

			select {
			case <-ctx.Done():
				logging.FromContext(ctx).Infof("Closing control server")
				err := ln.Close()
				if err != nil {
					logging.FromContext(ctx).Warnf("Error while closing the server: %s", err)
				}
			default:
			}
		}
	}()
	return ctrlService, nil
}
