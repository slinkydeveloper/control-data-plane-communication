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

	"knative.dev/control-data-plane-communication/pkg/control"
	"knative.dev/control-data-plane-communication/pkg/controlprotocol"
)

type envConfig struct {
	// Include the standard adapter.EnvConfig used by all adapters.
	adapter.EnvConfig
}

func NewEnv() adapter.EnvConfigAccessor { return &envConfig{} }

// Adapter generates events at a regular interval.
type Adapter struct {
	client cloudevents.Client
	logger *zap.SugaredLogger

	intervalMutex sync.Mutex
	interval      time.Duration

	nextID int

	controlServer controlprotocol.Service

	startPingGoroutineOnce sync.Once
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

func (a *Adapter) HandleControlMessage(ctx context.Context, msg controlprotocol.ControlMessage) {
	a.startPingGoroutineOnce.Do(func() {
		a.logger.Debugf("Starting ping goroutine")
		a.startPingGoroutine(ctx)
	})

	a.logger.Debugf("Received control message")

	msg.Ack()

	switch msg.Headers().OpCode() {
	case control.UpdateIntervalOpCode:
		var interval control.Duration
		err := interval.UnmarshalBinary(msg.Payload())
		if err != nil {
			a.logger.Errorf("Cannot parse the new interval. This should not happen, some controller bug?: %v", err)
		}

		a.intervalMutex.Lock()
		a.interval = time.Duration(interval)
		a.logger.Infof("Interval set %v", a.interval)
		a.intervalMutex.Unlock()

		err = a.controlServer.SendAndWaitForAck(control.StatusUpdateOpCode, interval)
		if err != nil {
			a.logger.Errorf("Something is broken in the update event: %v", err)
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
func (a *Adapter) Start(ctx context.Context) (err error) {
	// Start control server
	a.controlServer, err = controlprotocol.StartControlServer(ctx)
	if err != nil {
		return
	}
	a.logger.Info("Control server started")
	a.logger.Infof("Waiting for the first interval to set")

	a.controlServer.InboundMessageHandler(a)

	<-ctx.Done()
	return
}

func (a *Adapter) startPingGoroutine(ctx context.Context) {
	go func() {
		a.intervalMutex.Lock()
		a.logger.Infow("Starting heartbeat", zap.String("interval", a.interval.String()))
		a.intervalMutex.Unlock()
		for {
			a.intervalMutex.Lock()
			interval := a.interval
			a.intervalMutex.Unlock()
			select {
			case <-time.After(interval):
				event := a.newEvent()
				//a.logger.Infow("Sending new event", zap.String("event", event.String()))
				if result := a.client.Send(context.Background(), event); !cloudevents.IsACK(result) {
					//a.logger.Infow("failed to send event", zap.String("event", event.String()), zap.Error(result))
					// We got an error but it could be transient, try again next interval.
					continue
				}
			case <-ctx.Done():
				a.logger.Info("Shutting down...")
				return
			}
		}
	}()
}

func NewAdapter(ctx context.Context, aEnv adapter.EnvConfigAccessor, ceClient cloudevents.Client) adapter.Adapter {
	logger := logging.FromContext(ctx)
	return &Adapter{
		interval: 0,
		client:   ceClient,
		logger:   logger,
	}
}
