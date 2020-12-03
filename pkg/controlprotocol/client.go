package controlprotocol

import (
	"context"

	cews "github.com/cloudevents/sdk-go/protocol/ws/v2"
	"knative.dev/pkg/logging"
)

func StartControlClient(ctx context.Context, source string, target string) (ControlInterface, error) {
	ctrlService := newControlService(ctx, source)

	go func() {
		p, err := cews.Dial(ctx, target, nil)
		if err != nil {
			logging.FromContext(ctx).Errorf("Error while starting a new control connection")
			return
		}

		ctrlService.runPollingLoops(ctx, p)
		_ = p.Close(ctx)
	}()

	return &controlService{}, nil
}
