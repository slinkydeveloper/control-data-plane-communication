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
		server := http.Server{
			Addr: ":9090",
			Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				logging.FromContext(ctx).Infof("Received new incoming request")
				p, err := cews.Accept(ctx, writer, request, nil)
				if err != nil {
					logging.FromContext(ctx).Errorf("Error while accepting a new control connection")
					writer.WriteHeader(http.StatusInternalServerError)
					return
				}

				ctrlService.runPollingLoops(ctx, p)
				_ = p.Close(ctx)
				ctrlService.close()
			}),
		}
		err := server.ListenAndServe()
		if err != nil {
			logging.FromContext(ctx).Errorf("Error while closing the control server: %v", err)
		}
	}()

	return ctrlService, nil
}
