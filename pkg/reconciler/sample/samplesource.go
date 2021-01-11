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
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"

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
	v1alpha1informers "knative.dev/control-data-plane-communication/pkg/client/informers/externalversions/samples/v1alpha1"
	reconcilersamplesource "knative.dev/control-data-plane-communication/pkg/client/injection/reconciler/samples/v1alpha1/samplesource"
	"knative.dev/control-data-plane-communication/pkg/controlprotocol"
	"knative.dev/control-data-plane-communication/pkg/reconciler"
	"knative.dev/control-data-plane-communication/pkg/reconciler/sample/resources"
)

const (
	controlStatusUpdateType = "statusupdate"
)

// Reconciler reconciles a SampleSource object
type Reconciler struct {
	ReceiveAdapterImage string `envconfig:"SAMPLE_SOURCE_RA_IMAGE" required:"true"`

	dr                   *reconciler.DeploymentReconciler
	sinkResolver         *resolver.URIResolver
	sampleSourceInformer v1alpha1informers.SampleSourceInformer
	configAccessor       reconcilersource.ConfigAccessor
	enqueueKey           func(name types.NamespacedName)

	// TODO we should move this to control.go, together with its logic
	srcPodsIPs     map[string]string
	srcPodsIPsLock sync.Mutex

	controlConnections *controlprotocol.ControlConnections

	lastIntervalUpdateSent     map[string]time.Duration
	lastIntervalUpdateSentLock sync.Mutex

	lastReceivedStatusUpdate     map[string]time.Duration
	lastReceivedStatusUpdateLock sync.Mutex
}

// Check that our Reconciler implements Interface
var _ reconcilersamplesource.Interface = (*Reconciler)(nil)

// ReconcileKind implements Interface.ReconcileKind.
func (r *Reconciler) ReconcileKind(ctx context.Context, src *v1alpha1.SampleSource) pkgreconciler.Event {
	logger := logging.FromContext(ctx)

	ctx = sourcesv1.WithURIResolver(ctx, r.sinkResolver)

	// TODO generate mTLS stuff here

	// -- Create deployment (that's the same as usual, except we don't provide the interval)
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
		logger.Infof("returning because event from ReconcileDeployment")
		return event
	}

	logger.Infof("we have a RA deployment")

	// --- If connection is not established, establish one
	var ctrl controlprotocol.ControlInterface
	if ctrl = r.controlConnections.ResolveControlInterface(ra.Name); ctrl == nil {
		// TODO should we change this with endpoint tracking, creating a kube svc for the control endpoint?
		//  How do we handle connections to specific pods for partitioning then?

		// We need to get all the pods for that ra deployment
		pods, err := r.dr.KubeClientSet.CoreV1().Pods(src.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: resources.LabelSelector(src.Name),
		})
		if err != nil {
			return fmt.Errorf("error getting receive adapter pods %q: %v", ra.Name, err)
		}

		// TODO configure the connection with the mTLS stuff, if not set

		if len(pods.Items) == 0 {
			logger.Infof("returning because there is still no pod up for the deployment '%s'", ra.Name)
			return nil
		}
		if len(pods.Items) > 1 {
			// No need to fix this here, out of the scope of the prototype
			return fmt.Errorf("wrong pods number: %d", len(pods.Items))
		}

		podIp := pods.Items[0].Status.PodIP
		if podIp == "" {
			logger.Infof("returning because there is still no pod ip for the deployment '%s'", ra.Name)
			return nil
		}

		// Check if there is an old ip, so we can cleanup that conn
		r.srcPodsIPsLock.Lock()
		if oldIP, ok := r.srcPodsIPs[string(src.UID)]; ok && oldIP != podIp {
			r.controlConnections.RemoveConnection(ctx, string(src.UID))
		}
		// Update with new one
		r.srcPodsIPs[string(src.UID)] = podIp
		r.srcPodsIPsLock.Unlock()

		ctrl, err = r.controlConnections.CreateNewControlInterface(ctx, string(src.UID), podIp)
		if err != nil {
			return fmt.Errorf("cannot connect to the pod: %w", err)
		}

		// We need to start the message listener for this control interface
		deploymentName := ra.Name
		srcName := src.Name
		srcNamespace := src.Namespace
		go func() {
			for ev := range ctrl.InboundMessages() {
				r.controlMessageHandler(ctx, ev.Event(), deploymentName, srcName, srcNamespace)
				ev.Ack()
			}
		}()
	}

	logger.Infof("we have a control connection")

	// --- If last configuration update to that deployment doesn't contain the interval, set it
	actualInterval, _ := time.ParseDuration(src.Spec.Interval) // No need to check the error here, the webhook already did it
	if old, ok := r.getLastSentInterval(ra); !ok || old != actualInterval {
		event := cloudevents.NewEvent()
		event.SetID(uuid.New().String())
		event.SetType("updateinterval.samplesource.control.knative.dev")
		event.SetSource("samplesource-controller")
		event.SetExtension("newinterval", src.Spec.Interval)

		err := ctrl.SendAndWaitForAck(event)
		if err != nil {
			return fmt.Errorf("cannot send the event to the pod: %w", err)
		}

		r.lastIntervalUpdateSentLock.Lock()
		r.lastIntervalUpdateSent[ra.Name] = actualInterval
		r.lastIntervalUpdateSentLock.Unlock()
	}

	logger.Infof("we have sent the interval update")

	src.Status.MarkConfigurationNotPropagated()

	// --- If last status update to that deployment doesn't contain the correct interval,
	// it means we still didn't received that status update message
	// TODO here we're using interval for simplicity,
	//  but we may need to find another way to uniquely identify a configuration (hash of the in memory object?)
	if ackedInterval, ok := r.getLastUpdate(ra); !ok || ackedInterval != actualInterval {
		return nil
	}

	logger.Infof("we have received the status update")

	src.Status.MarkConfigurationPropagated()

	return nil
}

func (r *Reconciler) FinalizeKind(ctx context.Context, src *v1alpha1.SampleSource) pkgreconciler.Event {
	// Check if there is an old ip, so we can cleanup that conn
	r.srcPodsIPsLock.Lock()
	if _, ok := r.srcPodsIPs[string(src.UID)]; ok {
		r.controlConnections.RemoveConnection(ctx, string(src.UID))
	}
	// Update with new one
	delete(r.srcPodsIPs, string(src.UID))
	r.srcPodsIPsLock.Unlock()

	return nil
}

func (r *Reconciler) controlMessageHandler(ctx context.Context, event cloudevents.Event, deploymentName string, srcName string, srcNamespace string) {
	logger := logging.FromContext(ctx)

	if event.Type() == controlStatusUpdateType {
		// We're good to go now, let's signal that and re-enqueue
		var intervalStr string
		err := event.ExtensionAs("interval", &intervalStr)
		if err != nil {
			logger.Errorf("Cannot read the set interval: %v", err)
		}

		interval, err := time.ParseDuration(intervalStr)
		if err != nil {
			logger.Errorf("Cannot parse the set interval (sounds like a programming error of the adapter): %w", err)
		}

		// Register the update
		r.lastReceivedStatusUpdateLock.Lock()
		r.lastReceivedStatusUpdate[deploymentName] = interval
		r.lastReceivedStatusUpdateLock.Unlock()

		logger.Infof("Registered new interval for '%s' in namespace '%s': %s", srcName, srcNamespace, interval)

		// Trigger the reconciler again
		r.enqueueKey(types.NamespacedName{Name: srcName, Namespace: srcNamespace})

		return
	}

	logger.Warnw("Received an unknown message, I don't know what to do with it", zap.Stringer("event", event))
}

func (r *Reconciler) getLastSentInterval(dep *appsv1.Deployment) (time.Duration, bool) {
	r.lastIntervalUpdateSentLock.Lock()
	defer r.lastIntervalUpdateSentLock.Unlock()
	t, ok := r.lastIntervalUpdateSent[dep.Name]
	return t, ok
}

func (r *Reconciler) getLastUpdate(dep *appsv1.Deployment) (time.Duration, bool) {
	r.lastReceivedStatusUpdateLock.Lock()
	defer r.lastReceivedStatusUpdateLock.Unlock()
	t, ok := r.lastReceivedStatusUpdate[dep.Name]
	return t, ok
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
