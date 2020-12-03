package controlprotocol

import (
	"context"
	"net/http"

	cews "github.com/cloudevents/sdk-go/protocol/ws/v2"
	"knative.dev/pkg/logging"
)

func StartControlServer(ctx context.Context, source string) (ControlInterface, error) {
	ctrlService := newControlService(ctx, source)

	go func() {
		err := http.ListenAndServe(":8080", http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			p, err := cews.Accept(ctx, writer, request, nil)
			if err != nil {
				logging.FromContext(ctx).Errorf("Error while accepting a new control connection")
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}

			ctrlService.runPollingLoops(ctx, p)
			_ = p.Close(ctx)
		}))
		if err != nil {
			logging.FromContext(ctx).Errorf("Error while closing the control server: %v", err)
		}
	}()

	return &controlService{}, nil
}
