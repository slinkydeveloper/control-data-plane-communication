package reconciler

import ctrlservice "knative.dev/control-data-plane-communication/pkg/control/service"

type ControlPlaneConnectionPoolOption func(*ControlPlaneConnectionPool)

func WithServiceWrapper(wrapper ctrlservice.ServiceWrapper) ControlPlaneConnectionPoolOption {
	return func(pool *ControlPlaneConnectionPool) {
		pool.serviceWrapperFactories = append(pool.serviceWrapperFactories, wrapper)
	}
}
