package reconciler

import (
	"knative.dev/control-data-plane-communication/pkg/control"
)

type ControlPlaneConnectionPoolOption func(*ControlPlaneConnectionPool)

func WithServiceWrapper(wrapper control.ServiceWrapper) ControlPlaneConnectionPoolOption {
	return func(pool *ControlPlaneConnectionPool) {
		pool.serviceWrapperFactories = append(pool.serviceWrapperFactories, wrapper)
	}
}
