package controlprotocol

import (
	"context"
	"sync"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestStartClientAndServer(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.TODO())
	t.Cleanup(cancelFn)

	_, err := StartControlServer(ctx, "test-server")
	require.NoError(t, err)
	_, err = StartControlClient(ctx, "test-client", "localhost")
	require.NoError(t, err)
}

func TestE2EServerToClient(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.TODO())
	t.Cleanup(cancelFn)

	server, err := StartControlServer(ctx, "test-server")
	require.NoError(t, err)
	client, err := StartControlClient(ctx, "test-client", "localhost")
	require.NoError(t, err)

	ev := cloudevents.NewEvent()
	ev.SetID(uuid.New().String())
	ev.SetType("aaa")
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		require.NoError(t, server.SendAndWaitForAck(ev))
		wg.Done()
	}()
	go func() {
		ctrlMessage := <-client.InboundMessages()
		ctrlMessage.Ack()
		require.Equal(t, ev.ID(), ctrlMessage.event.ID())
		wg.Done()
	}()

	wg.Wait()
}

func TestE2EClientToServer(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.TODO())
	t.Cleanup(cancelFn)

	server, err := StartControlServer(ctx, "test-server")
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	client, err := StartControlClient(ctx, "test-client", "localhost")
	require.NoError(t, err)

	ev := cloudevents.NewEvent()
	ev.SetID(uuid.New().String())
	ev.SetType("aaa")
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		require.NoError(t, client.SendAndWaitForAck(ev))
		wg.Done()
	}()
	go func() {
		ctrlMessage := <-server.InboundMessages()
		ctrlMessage.Ack()
		require.Equal(t, ev.ID(), ctrlMessage.event.ID())
		wg.Done()
	}()

	wg.Wait()
}
