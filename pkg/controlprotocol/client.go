package controlprotocol

import (
	"context"
	"net/http"
	"time"

	cews "github.com/cloudevents/sdk-go/protocol/ws/v2"
	"knative.dev/pkg/logging"
	"nhooyr.io/websocket"
)

const startRetry = 5
const startTimeout = 5 * time.Second

func StartControlClient(ctx context.Context, source string, target string) (ControlInterface, error) {
	target = "ws://" + target + ":9090/"
	logging.FromContext(ctx).Infof("Starting control client to %s", target)
	ctrlService := newControlService(ctx, source)

	var p *cews.Protocol
	var err error
	for i := 0; i < startRetry; i++ {
		p, err = cews.Dial(ctx, target, &websocket.DialOptions{HTTPClient: &http.Client{}})
		if err == nil {
			break
		}
		logging.FromContext(ctx).Warnf("Error while trying to connect %v, retrying", err)
		time.Sleep(startTimeout)
	}
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
