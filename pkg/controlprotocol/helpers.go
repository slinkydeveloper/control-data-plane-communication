package controlprotocol

import (
	"context"
	"net"
	"time"

	"knative.dev/pkg/logging"
)

var dialOptions = net.Dialer{
	KeepAlive: keepAlive,
	Deadline:  time.Time{},
}

func tryDial(ctx context.Context, target string, retries int, interval time.Duration) (net.Conn, error) {
	var conn net.Conn
	var err error
	for i := 0; i < retries; i++ {
		conn, err = dialOptions.DialContext(ctx, "tcp", target)
		if err == nil {
			// Set some stuff
			return conn, nil
		}
		logging.FromContext(ctx).Warnf("Error while trying to connect %v", err)
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(interval):
		}
	}
	return nil, err
}
