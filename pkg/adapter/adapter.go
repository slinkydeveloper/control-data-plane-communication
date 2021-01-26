/*
Copyright 2019 The Knative Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
		http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package adapter implements a sample receive adapter that generates events
// at a regular interval.
package adapter

import (
	"context"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"

	"knative.dev/eventing/pkg/adapter/v2"
	"knative.dev/pkg/logging"

	"knative.dev/control-data-plane-communication/pkg/control/network"
	"knative.dev/control-data-plane-communication/pkg/control/samplesource"
	"knative.dev/control-data-plane-communication/pkg/control/service"
)

type envConfig struct {
	// Include the standard adapter.EnvConfig used by all adapters.
	adapter.EnvConfig
}

func NewEnv() adapter.EnvConfigAccessor { return &envConfig{} }

type State uint8

const (
	Stopped State = 0
	Running       = 1
)

// Adapter generates events at a regular interval.
type Adapter struct {
	client cloudevents.Client
	logger *zap.SugaredLogger

	intervalMutex sync.RWMutex
	interval      time.Duration

	nextID int

	controlServer service.Service

	stateMutex       sync.Mutex
	state            State
	closeRunningPing context.CancelFunc
}

type dataExample struct {
	Sequence  int    `json:"sequence"`
	Heartbeat string `json:"heartbeat"`
}

func (a *Adapter) newEvent() cloudevents.Event {
	event := cloudevents.NewEvent()
	event.SetType("dev.knative.sample")
	event.SetSource("sample.knative.dev/heartbeat-source")

	if err := event.SetData(cloudevents.ApplicationJSON, &dataExample{
		Sequence:  a.nextID,
		Heartbeat: a.interval.String(),
	}); err != nil {
		a.logger.Errorw("failed to set data")
	}
	a.nextID++
	return event
}

func (a *Adapter) HandleControlMessage(ctx context.Context, msg service.ControlMessage) {
	a.logger.Debugf("Received control message")

	msg.Ack()

	switch msg.Headers().OpCode() {
	case samplesource.UpdateIntervalOpCode:
		var interval samplesource.Duration
		err := interval.UnmarshalBinary(msg.Payload())
		if err != nil {
			a.logger.Errorf("Cannot parse the new interval. This should not happen, some controller bug?: %v", err)
		}

		a.intervalMutex.Lock()
		a.interval = time.Duration(interval)
		a.logger.Infof("Interval set %v", a.interval)
		a.intervalMutex.Unlock()

		err = a.controlServer.SendAndWaitForAck(samplesource.NotifyIntervalOpCode, interval)
		if err != nil {
			a.logger.Errorf("Something is broken in the update event: %v", err)
		}
	case samplesource.UpdateActiveStatusOpCode:
		var active samplesource.ActiveStatus
		err := active.UnmarshalBinary(msg.Payload())
		if err != nil {
			a.logger.Errorf("Cannot unmarshal the active status. This should not happen, some controller bug?: %v", err)
		}
		if active.IsRunning() {
			a.logger.Debugf("Received resume signal")
			a.stateMutex.Lock()
			if a.state == Stopped {
				a.closeRunningPing = a.startPingGoroutine(ctx)
				a.state = Running
			}
			a.stateMutex.Unlock()
		} else {
			a.logger.Debugf("Received stop signal")
			a.stateMutex.Lock()
			if a.state == Running {
				a.closeRunningPing()
			}
			a.stateMutex.Unlock()
		}
	default:
		a.logger.Warnw(
			"Received an unknown message, I don't know what to do with it",
			zap.Uint8("opcode", msg.Headers().OpCode()),
			zap.ByteString("payload", msg.Payload()),
		)
	}
}

// Start runs the adapter.
// Returns if ctx is cancelled or Send() returns an error.
func (a *Adapter) Start(ctx context.Context) error {
	// Start control server
	tlsConf, err := network.LoadServerTLSConfig()
	if err != nil {
		logging.FromContext(ctx).Warnf("Cannot load the TLS config: %v", err)
		return err
	}
	a.controlServer, _, err = network.StartControlServer(ctx, tlsConf)
	if err != nil {
		return err
	}
	a.logger.Info("Control server started")
	a.logger.Infof("Waiting for the first interval to set")

	a.controlServer.InboundMessageHandler(a)

	<-ctx.Done()
	return nil
}

func (a *Adapter) startPingGoroutine(adapterCtx context.Context) context.CancelFunc {
	loopCtx, cancelFn := context.WithCancel(context.TODO())

	a.logger.Info("Starting the ping goroutine")
	err := a.controlServer.SendAndWaitForAck(samplesource.NotifyActiveStatusOpCode, samplesource.ActiveStatus(true))
	if err != nil {
		a.logger.Warnf("Cannot send the resumed signal! %v", err)
	}

	go func() {
		a.intervalMutex.RLock()
		a.logger.Infow("Starting heartbeat", zap.String("interval", a.interval.String()))
		a.intervalMutex.RUnlock()
		for {
			a.intervalMutex.RLock()
			interval := a.interval
			a.intervalMutex.RUnlock()
			select {
			case <-time.After(interval):
				event := a.newEvent()
				//a.logger.Infow("Sending new event", zap.String("event", event.String()))
				if result := a.client.Send(context.Background(), event); !cloudevents.IsACK(result) {
					a.logger.Infow("failed to send event", zap.String("event", event.String()), zap.Error(result))
					// We got an error but it could be transient, try again next interval.
					continue
				}
			case <-adapterCtx.Done():
				a.logger.Info("Shutting down adapter")
				return
			case <-loopCtx.Done():
				a.logger.Info("Requested a shut down of the ping goroutine")
				err = a.controlServer.SendAndWaitForAck(samplesource.NotifyActiveStatusOpCode, samplesource.ActiveStatus(false))
				if err != nil {
					a.logger.Warnf("Cannot send the stopped signal! %v", err)
				}
				a.stateMutex.Lock()
				a.state = Stopped
				a.stateMutex.Unlock()
				return
			}
		}
	}()

	return cancelFn
}

func NewAdapter(ctx context.Context, aEnv adapter.EnvConfigAccessor, ceClient cloudevents.Client) adapter.Adapter {
	logger := logging.FromContext(ctx)
	return &Adapter{
		interval: 0,
		client:   ceClient,
		logger:   logger,
		state:    Stopped,
	}
}
