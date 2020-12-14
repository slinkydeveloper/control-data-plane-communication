package controlprotocol

import (
	"context"
	"errors"
	"net/http"
	"time"

	cews "github.com/cloudevents/sdk-go/protocol/ws/v2"
	"knative.dev/pkg/logging"
	"nhooyr.io/websocket"
)

const retryConn = 100 * time.Millisecond

func StartControlClient(ctx context.Context, source string, target string) (ControlInterface, error) {
	target = "ws://" + target + ":9000/"
	logging.FromContext(ctx).Infof("Starting control client to %s", target)
	ctrlService := newControlService(ctx, source)

	go func() {
		for {
			p, err := cews.Dial(ctx, target, &websocket.DialOptions{HTTPClient: &http.Client{}})
			if err != nil {
				if errors.Is(err, ctx.Err()) {
					return
				}
				time.Sleep(retryConn)
				logging.FromContext(ctx).Warnf("Error while trying to connect %v, retrying", err)
			} else {
				ctrlService.blockOnPolling(ctx, p)
				_ = p.Close(ctx)
			}
			select {
			case <-ctx.Done():
				return
			default:
				logging.FromContext(ctx).Warnf("Connection lost, reconnecting")
			}
		}
	}()

	return ctrlService, nil
}
