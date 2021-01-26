package reconciler

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"knative.dev/pkg/logging"

	"knative.dev/control-data-plane-communication/pkg/control/protocol"
)

func TestInsecureConnectionPool_ReconcileConnections(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ctx := logging.WithLogger(context.TODO(), logger.Sugar())

	serverCtx, serverCancelFn := context.WithCancel(ctx)

	server, closedServerSignal, err := protocol.StartInsecureControlServer(serverCtx)
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

	certManager, err := protocol.NewCertificateManager(ctx)
	require.NoError(t, err)

	server, closedServerSignal, err := protocol.StartControlServer(serverCtx, mustGenerateTLSServerConf(t, certManager))
	require.NoError(t, err)
	t.Cleanup(func() {
		serverCancelFn()
		<-closedServerSignal
	})

	connectionPool := NewControlPlaneConnectionPool(certManager)

	connectionPoolReconcileTest(t, server, connectionPool)
}

func connectionPoolReconcileTest(t *testing.T, server protocol.Service, connectionPool *ControlPlaneConnectionPool) {
	newServiceInvokedCounter := atomic.NewInt32(0)
	oldServiceInvokedCounter := atomic.NewInt32(0)

	conns, err := connectionPool.ReconcileConnections(context.TODO(), "hello", []string{"127.0.0.1"}, func(string, protocol.Service) {
		newServiceInvokedCounter.Inc()
	}, func(string) {
		oldServiceInvokedCounter.Inc()
	})
	require.NoError(t, err)
	require.Contains(t, conns, "127.0.0.1")
	require.Equal(t, int32(1), newServiceInvokedCounter.Load())
	require.Equal(t, int32(0), oldServiceInvokedCounter.Load())

	runSendReceiveTest(t, server, conns["127.0.0.1"])

	newServiceInvokedCounter.Store(0)
	oldServiceInvokedCounter.Store(0)

	conns, err = connectionPool.ReconcileConnections(context.TODO(), "hello", []string{}, func(string, protocol.Service) {
		newServiceInvokedCounter.Inc()
	}, func(string) {
		oldServiceInvokedCounter.Inc()
	})
	require.NoError(t, err)
	require.NotContains(t, conns, "127.0.0.1")
	require.Equal(t, int32(0), newServiceInvokedCounter.Load())
	require.Equal(t, int32(1), oldServiceInvokedCounter.Load())
}

func mustGenerateTLSServerConf(t *testing.T, certManager *protocol.CertificateManager) *tls.Config {
	dataPlaneKeyPair, err := certManager.EmitNewDataPlaneCertificate(context.TODO())
	require.NoError(t, err)

	dataPlaneCert, err := tls.X509KeyPair(dataPlaneKeyPair.CertBytes(), dataPlaneKeyPair.PrivateKeyBytes())
	require.NoError(t, err)

	certPool := x509.NewCertPool()
	certPool.AddCert(certManager.CaCert())
	return &tls.Config{
		Certificates: []tls.Certificate{dataPlaneCert},
		ClientCAs:    certPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ServerName:   certManager.CaCert().DNSNames[0],
	}
}

func runSendReceiveTest(t *testing.T, sender protocol.Service, receiver protocol.Service) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	receiver.ErrorHandler(protocol.ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))
	sender.ErrorHandler(protocol.ErrorHandlerFunc(func(ctx context.Context, err error) {
		require.NoError(t, err)
	}))

	receiver.InboundMessageHandler(protocol.ControlMessageHandlerFunc(func(ctx context.Context, message protocol.ControlMessage) {
		require.Equal(t, uint8(1), message.Headers().OpCode())
		require.Equal(t, "Funky!", string(message.Payload()))
		message.Ack()
		wg.Done()
	}))

	require.NoError(t, sender.SendBinaryAndWaitForAck(1, []byte("Funky!")))

	wg.Wait()
}
