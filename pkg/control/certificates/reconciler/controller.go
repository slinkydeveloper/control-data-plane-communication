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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/client/injection/kube/reconciler/core/v1/secret"
	"knative.dev/pkg/system"

	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"
)

const (
	caSecretNamePostfix    = "-ctrl-ca"
	secretLabelNamePostfix = "-ctrl"
)

func NewControllerFactory(componentName string) func(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
	return func(
		ctx context.Context,
		cmw configmap.Watcher,
	) *controller.Impl {
		ctx = logging.WithLogger(ctx, logging.FromContext(ctx).Named("ctrl-secrets-controller"))

		secretInformer := secretinformer.Get(ctx)

		caSecretName := componentName + caSecretNamePostfix
		labelName := componentName + secretLabelNamePostfix

		r := &reconciler{
			client: kubeclient.Get(ctx),

			secretLister:        secretInformer.Lister(),
			caSecretName:        caSecretName,
			secretTypeLabelName: labelName,

			logger: logging.FromContext(ctx),
		}
		if err := envconfig.Process("", r); err != nil {
			logging.FromContext(ctx).Panicf("required environment variable is not defined: %v", err)
		}

		impl := secret.NewImpl(ctx, r)

		logging.FromContext(ctx).Info("Setting up event handlers")

		filterWithLabel := func(obj interface{}) bool {
			sec := obj.(*corev1.Secret)
			_, ok := sec.Labels[labelName]
			return ok
		}

		// If the ca secret changes, global resync
		secretInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
			FilterFunc: controller.FilterWithNameAndNamespace(system.Namespace(), caSecretName),
			Handler: controller.HandleAll(func(i interface{}) {
				impl.FilteredGlobalResync(filterWithLabel, secretInformer.Informer())
			}),
		})

		// Enqueue only secrets with expected label
		secretInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
			FilterFunc: filterWithLabel,
			Handler:    controller.HandleAll(impl.Enqueue),
		})

		return impl
	}
}
