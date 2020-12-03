package controlprotocol

import (
	"context"

	cews "github.com/cloudevents/sdk-go/protocol/ws/v2"
	"knative.dev/pkg/logging"
)

func StartControlClient(ctx context.Context, source string, target string) (ControlInterface, error) {
	ctrlService := newControlService(ctx, source)
	p, err := cews.Dial(ctx, "http://"+target+":9090/", nil)
	if err != nil {
		logging.FromContext(ctx).Errorf("Error while starting a new control connection: %v", err)
		return nil, err
	}

	go func() {
		ctrlService.runPollingLoops(ctx, p)
		_ = p.Close(ctx)
		ctrlService.close()
	}()

	return ctrlService, nil
}
