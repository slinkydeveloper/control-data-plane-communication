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
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	// k8s.io imports
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	// knative.dev/pkg imports
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	// knative.dev/eventing imports
	sourcesv1 "knative.dev/eventing/pkg/apis/sources/v1"
	reconcilersource "knative.dev/eventing/pkg/reconciler/source"

	"knative.dev/control-data-plane-communication/pkg/apis/samples/v1alpha1"
	reconcilersamplesource "knative.dev/control-data-plane-communication/pkg/client/injection/reconciler/samples/v1alpha1/samplesource"
	"knative.dev/control-data-plane-communication/pkg/controlprotocol"
	"knative.dev/control-data-plane-communication/pkg/reconciler"
	"knative.dev/control-data-plane-communication/pkg/reconciler/sample/resources"
)

// Reconciler reconciles a SampleSource object
type Reconciler struct {
	ReceiveAdapterImage string `envconfig:"SAMPLE_SOURCE_RA_IMAGE" required:"true"`

	dr *reconciler.DeploymentReconciler

	sinkResolver *resolver.URIResolver

	configAccessor reconcilersource.ConfigAccessor
}

// Check that our Reconciler implements Interface
var _ reconcilersamplesource.Interface = (*Reconciler)(nil)

func (r *Reconciler) UpdateInterval(ctx context.Context, src *v1alpha1.SampleSource) pkgreconciler.Event {
	namespace := src.GetObjectMeta().GetNamespace()
	deploymentName := resources.MakeReceiveAdapterDeploymentName(src)

	_, err := r.dr.KubeClientSet.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		logging.FromContext(ctx).Infof("Deployment '%s' in namespace '%s' not found", deploymentName, namespace)

		// We need to create from scratch the ra and go through the usual reconcile
		return r.ReconcileKind(ctx, src)
	} else if err != nil {
		return fmt.Errorf("error getting receive adapter %q: %v", deploymentName, err)
	}

	// We need to get all the pods for that ra deployment
	pods, err := r.dr.KubeClientSet.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: resources.LabelSelector(src.Name),
	})
	if err != nil {
		return fmt.Errorf("error getting receive adapter pods %q: %v", deploymentName, err)
	}

	for _, pod := range pods.Items {
		podIp := pod.Status.PodIP
		logging.FromContext(ctx).Infof("Updating interval for pod '%s' with ip '%s'", pod.Name, podIp)

		ctrl, err := controlprotocol.ControlInterfaceFromContext(ctx, "samplesource-controller", podIp)
		if err != nil {
			return err
		}

		event := cloudevents.NewEvent()
		event.SetID(uuid.New().String())
		event.SetType("updateinterval.samplesource.control.knative.dev")
		event.SetSource("samplesource-controller")
		event.SetExtension("newinterval", src.Spec.Interval)

		err = ctrl.SendAndWaitForAck(event)
		if err != nil {
			return err
		}
	}

	return nil
}

// ReconcileKind implements Interface.ReconcileKind.
func (r *Reconciler) ReconcileKind(ctx context.Context, src *v1alpha1.SampleSource) pkgreconciler.Event {

	ctx = sourcesv1.WithURIResolver(ctx, r.sinkResolver)

	ra, sb, event := r.dr.ReconcileDeployment(ctx, src, makeSinkBinding(src),
		resources.MakeReceiveAdapter(&resources.ReceiveAdapterArgs{
			EventSource:    src.Namespace + "/" + src.Name,
			Image:          r.ReceiveAdapterImage,
			Source:         src,
			Labels:         resources.Labels(src.Name),
			AdditionalEnvs: r.configAccessor.ToEnvVars(), // Grab config envs for tracing/logging/metrics
		}),
	)
	if ra != nil {
		src.Status.PropagateDeploymentAvailability(ra)
	}
	if sb != nil {
		if c := sb.Status.GetCondition(sourcesv1.SinkBindingConditionSinkProvided); c.IsTrue() {
			src.Status.MarkSink(sb.Status.SinkURI)
		} else if c.IsFalse() {
			src.Status.MarkNoSink(c.GetReason(), "%s", c.GetMessage())
		}
	}
	if event != nil {
		logging.FromContext(ctx).Infof("returning because event from ReconcileDeployment")
		return event
	}

	return nil
}

func makeSinkBinding(src *v1alpha1.SampleSource) *sourcesv1.SinkBinding {
	return &sourcesv1.SinkBinding{
		ObjectMeta: metav1.ObjectMeta{
			// this is necessary to track the change of sink reference.
			Name:      src.GetName(),
			Namespace: src.GetNamespace(),
		},
		Spec: sourcesv1.SinkBindingSpec{
			SourceSpec: src.Spec.SourceSpec,
		},
	}
}
