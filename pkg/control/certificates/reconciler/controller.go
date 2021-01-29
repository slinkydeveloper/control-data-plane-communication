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

	"github.com/kelseyhightower/envconfig"
	"k8s.io/client-go/tools/cache"
	"knative.dev/pkg/client/injection/kube/reconciler/core/v1/secret"

	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"
)

// NewController initializes the controller and is called by the generated code
// Registers event handlers to enqueue events
func NewController(
	ctx context.Context,
	cmw configmap.Watcher,
) *controller.Impl {
	sampleSourceInformer := secretinformer.Get(ctx)

	r := &reconciler{
		sampleSourceInformer: sampleSourceInformer,
	}
	if err := envconfig.Process("", r); err != nil {
		logging.FromContext(ctx).Panicf("required environment variable is not defined: %v", err)
	}

	impl := secret.NewImpl(ctx, r)

	logging.FromContext(ctx).Info("Setting up event handlers")

	// TODO some magic should happen here I guess

	sampleSourceInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: nil,
		Handler:    nil, //TODO
	})

	return impl
}
