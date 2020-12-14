package controlprotocol

import (
	"context"
	"net/http"
	"time"

	cews "github.com/cloudevents/sdk-go/protocol/ws/v2"
	"knative.dev/pkg/logging"
)

type controlHandler struct {
	ctx         context.Context
	ctrlService *controlService
}

func (cs *controlHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	p, err := cews.Accept(cs.ctx, writer, request, nil)
	if err != nil {
		logging.FromContext(cs.ctx).Errorf("Error while accepting a new control connection")
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	logging.FromContext(cs.ctx).Debugf("New link established")

	cs.ctrlService.blockOnPolling(cs.ctx, p)

	_ = p.Close(cs.ctx)
}

func StartControlServer(ctx context.Context, source string) (ControlInterface, error) {
	ctrlService := newControlService(ctx, source)

	server := &http.Server{
		Addr:         ":9000",
		Handler:      &controlHandler{ctx: ctx, ctrlService: ctrlService},
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
	}

	go func() {
		err := server.ListenAndServe()
		if err == http.ErrServerClosed {
			return
		}
		if err != nil {
			logging.FromContext(ctx).Errorf("Error while closing the control server: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		_ = server.Shutdown(ctx)
	}()

	return ctrlService, nil
}
