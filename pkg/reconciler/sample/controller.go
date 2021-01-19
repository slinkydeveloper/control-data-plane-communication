/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sample

import (
	"context"
	"time"

	reconcilersource "knative.dev/eventing/pkg/reconciler/source"

	"github.com/kelseyhightower/envconfig"
	"k8s.io/client-go/tools/cache"

	"knative.dev/control-data-plane-communication/pkg/apis/samples/v1alpha1"
	"knative.dev/control-data-plane-communication/pkg/controlprotocol"

	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"

	"knative.dev/control-data-plane-communication/pkg/reconciler"

	kubeclient "knative.dev/pkg/client/injection/kube/client"
	deploymentinformer "knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment"

	samplesourceinformer "knative.dev/control-data-plane-communication/pkg/client/injection/informers/samples/v1alpha1/samplesource"
	"knative.dev/control-data-plane-communication/pkg/client/injection/reconciler/samples/v1alpha1/samplesource"
)

// NewController initializes the controller and is called by the generated code
// Registers event handlers to enqueue events
func NewController(
	ctx context.Context,
	cmw configmap.Watcher,
) *controller.Impl {
	deploymentInformer := deploymentinformer.Get(ctx)
	sampleSourceInformer := samplesourceinformer.Get(ctx)

	r := &Reconciler{
		dr: &reconciler.DeploymentReconciler{KubeClientSet: kubeclient.Get(ctx)},
		// Config accessor takes care of tracing/config/logging config propagation to the receive adapter
		configAccessor:     reconcilersource.WatchConfigurations(ctx, "control-data-plane-communication", cmw),
		controlConnections: controlprotocol.NewControlPlaneConnectionPool("samplesource-controller"),

		lastIntervalUpdateSent: make(map[string]time.Duration),
	}
	if err := envconfig.Process("", r); err != nil {
		logging.FromContext(ctx).Panicf("required environment variable is not defined: %v", err)
	}

	impl := samplesource.NewImpl(ctx, r)

	r.sinkResolver = resolver.NewURIResolver(ctx, impl.EnqueueKey)
	r.statusUpdateStore = &StatusUpdateStore{enqueueKey: impl.EnqueueKey, lastReceivedStatusUpdate: make(map[string]time.Duration)}

	logging.FromContext(ctx).Info("Setting up event handlers")

	sampleSourceInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	deploymentInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterControllerGK(v1alpha1.Kind("SampleSource")),
		Handler:    controller.HandleAll(impl.EnqueueControllerOf),
	})

	return impl
}
