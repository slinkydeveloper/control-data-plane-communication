package controlprotocol

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"knative.dev/pkg/logging"
)

func TestInsecureConnectionPool_ReconcileConnections(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := logging.WithLogger(context.TODO(), logger.Sugar())

	serverCtx, serverCancelFn := context.WithCancel(ctx)

	server, closedServerSignal, err := StartInsecureControlServer(serverCtx)
	require.NoError(t, err)
	t.Cleanup(func() {
		serverCancelFn()
		<-closedServerSignal
	})

	connectionPool := NewInsecureControlPlaneConnectionPool()

	connectionPoolReconcileTest(t, server, connectionPool)
}

func TestTLSConnectionPool_ReconcileConnections(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := logging.WithLogger(context.TODO(), logger.Sugar())

	serverCtx, serverCancelFn := context.WithCancel(ctx)

	certManager, err := NewCertificateManager(ctx)
	require.NoError(t, err)

	server, closedServerSignal, err := StartControlServer(serverCtx, mustGenerateTLSServerConf(t, certManager))
	require.NoError(t, err)
	t.Cleanup(func() {
		serverCancelFn()
		<-closedServerSignal
	})

	connectionPool := NewControlPlaneConnectionPool(certManager)

	connectionPoolReconcileTest(t, server, connectionPool)
}

func connectionPoolReconcileTest(t *testing.T, server Service, connectionPool *ControlPlaneConnectionPool) {
	newServiceInvokedCounter := atomic.NewInt32(0)
	oldServiceInvokedCounter := atomic.NewInt32(0)

	conns, err := connectionPool.ReconcileConnections(context.TODO(), "hello", []string{"127.0.0.1"}, func(string, Service) {
		newServiceInvokedCounter.Inc()
	}, func(string) {
		oldServiceInvokedCounter.Inc()
	})
	require.NoError(t, err)
	require.Contains(t, conns, "127.0.0.1")
	require.Equal(t, int32(1), newServiceInvokedCounter.Load())
	require.Equal(t, int32(0), oldServiceInvokedCounter.Load())

	sendReceiveTest(t, server, conns["127.0.0.1"])

	newServiceInvokedCounter.Store(0)
	oldServiceInvokedCounter.Store(0)

	conns, err = connectionPool.ReconcileConnections(context.TODO(), "hello", []string{}, func(string, Service) {
		newServiceInvokedCounter.Inc()
	}, func(string) {
		oldServiceInvokedCounter.Inc()
	})
	require.NoError(t, err)
	require.NotContains(t, conns, "127.0.0.1")
	require.Equal(t, int32(0), newServiceInvokedCounter.Load())
	require.Equal(t, int32(1), oldServiceInvokedCounter.Load())
}
