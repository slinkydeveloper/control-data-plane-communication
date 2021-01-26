package sample

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/logging"

	"knative.dev/control-data-plane-communication/pkg/control/samplesource"
)

// TODO might be good to generalize this
type StatusUpdateStore struct {
	enqueueKey func(name types.NamespacedName)

	lastReceivedIntervalAcked     map[string]time.Duration
	lastReceivedIntervalAckedLock sync.Mutex

	isActive      map[string]bool
	isActiveMutex sync.Mutex
}

func (sus *StatusUpdateStore) ControlMessageHandler(ctx context.Context, opcode uint8, payload []byte, podIp string, srcName types.NamespacedName) {
	logger := logging.FromContext(ctx)

	switch opcode {
	case samplesource.NotifyIntervalOpCode:
		// We're good to go now, let's signal that and re-enqueue
		var interval samplesource.Duration
		err := interval.UnmarshalBinary(payload)
		if err != nil {
			logger.Errorf("Cannot parse the set interval (sounds like a programming error of the adapter): %w", err)
		}

		// Register the update
		sus.lastReceivedIntervalAckedLock.Lock()
		sus.lastReceivedIntervalAcked[podIp] = time.Duration(interval)
		sus.lastReceivedIntervalAckedLock.Unlock()

		logger.Infof("Registered new interval for '%v': %s", srcName, interval)

		// Trigger the reconciler again
		sus.enqueueKey(srcName)
	case samplesource.NotifyActiveStatusOpCode:
		var status samplesource.ActiveStatus
		err := status.UnmarshalBinary(payload)
		if err != nil {
			logger.Errorf("Cannot parse the status (sounds like a programming error of the adapter): %w", err)
		}

		sus.isActiveMutex.Lock()
		sus.isActive[podIp] = bool(status)
		sus.isActiveMutex.Unlock()

		// Trigger the reconciler again
		sus.enqueueKey(srcName)
	default:
		logger.Warnw(
			"Received an unknown message, I don't know what to do with it",
			zap.Uint8("opcode", opcode),
			zap.ByteString("payload", payload),
		)
	}
}

func (sus *StatusUpdateStore) GetLastUpdate(podIp string) (time.Duration, bool) {
	sus.lastReceivedIntervalAckedLock.Lock()
	defer sus.lastReceivedIntervalAckedLock.Unlock()
	t, ok := sus.lastReceivedIntervalAcked[podIp]
	return t, ok
}

func (sus *StatusUpdateStore) GetLastActiveStatus(podIp string) (bool, bool) {
	sus.isActiveMutex.Lock()
	defer sus.isActiveMutex.Unlock()
	b, ok := sus.isActive[podIp]
	return b, ok
}
