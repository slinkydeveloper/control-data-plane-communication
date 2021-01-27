package service

import (
	"context"

	"knative.dev/pkg/logging"

	ctrl "knative.dev/control-data-plane-communication/pkg/control"
)

var NoopControlMessageHandler ctrl.MessageHandlerFunc = func(ctx context.Context, message ctrl.ServiceMessage) {
	logging.FromContext(ctx).Warnf("Discarding control message '%s'", message.Headers().UUID())
	message.Ack()
}

var LoggerErrorHandler ctrl.ErrorHandlerFunc = func(ctx context.Context, err error) {
	logging.FromContext(ctx).Debugf("Error from the connection: %s", err)
}
