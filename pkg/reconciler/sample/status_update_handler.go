package sample

import (
	"context"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/logging"
)

// TODO might be good to generalize this
type StatusUpdateStore struct {
	enqueueKey func(name types.NamespacedName)

	lastReceivedStatusUpdate     map[types.UID]time.Duration
	lastReceivedStatusUpdateLock sync.Mutex
}

func (sus *StatusUpdateStore) ControlMessageHandler(ctx context.Context, event cloudevents.Event, uuid types.UID, srcName types.NamespacedName) {
	logger := logging.FromContext(ctx)

	if event.Type() == controlStatusUpdateType {
		// We're good to go now, let's signal that and re-enqueue
		var intervalStr string
		err := event.ExtensionAs("interval", &intervalStr)
		if err != nil {
			logger.Errorf("Cannot read the set interval: %v", err)
		}

		interval, err := time.ParseDuration(intervalStr)
		if err != nil {
			logger.Errorf("Cannot parse the set interval (sounds like a programming error of the adapter): %w", err)
		}

		// Register the update
		sus.lastReceivedStatusUpdateLock.Lock()
		sus.lastReceivedStatusUpdate[uuid] = interval
		sus.lastReceivedStatusUpdateLock.Unlock()

		logger.Infof("Registered new interval for '%v': %s", srcName, interval)

		// Trigger the reconciler again
		sus.enqueueKey(srcName)

		return
	}

	logger.Warnw("Received an unknown message, I don't know what to do with it", zap.Stringer("event", event))
}

func (sus *StatusUpdateStore) GetLastUpdate(srcUid types.UID) (time.Duration, bool) {
	sus.lastReceivedStatusUpdateLock.Lock()
	defer sus.lastReceivedStatusUpdateLock.Unlock()
	t, ok := sus.lastReceivedStatusUpdate[srcUid]
	return t, ok
}
