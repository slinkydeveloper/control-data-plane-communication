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
	reconcilersamplesource "knative.dev/control-data-plane-communication/pkg/client/injection/reconciler/samples/v1alpha1/samplesource"
	"knative.dev/control-data-plane-communication/pkg/control"
	ctrlnetwork "knative.dev/control-data-plane-communication/pkg/control/network"
	ctrlreconciler "knative.dev/control-data-plane-communication/pkg/control/reconciler"
	ctrlsamplesource "knative.dev/control-data-plane-communication/pkg/control/samplesource"
	ctrlservice "knative.dev/control-data-plane-communication/pkg/control/service"
	"knative.dev/control-data-plane-communication/pkg/reconciler"
	"knative.dev/control-data-plane-communication/pkg/reconciler/sample/resources"
)

// Reconciler reconciles a SampleSource object
type Reconciler struct {
	ReceiveAdapterImage string `envconfig:"SAMPLE_SOURCE_RA_IMAGE" required:"true"`

	dr             *reconciler.DeploymentReconciler
	sinkResolver   *resolver.URIResolver
	configAccessor reconcilersource.ConfigAccessor

	certificateManager *ctrlnetwork.CertificateManager
	controlConnections *ctrlreconciler.ControlPlaneConnectionPool

	keyPairs      map[types.NamespacedName]*ctrlnetwork.KeyPair
	keyPairsMutex sync.Mutex

	activeStatusNotificationsStore *ctrlreconciler.NotificationStore
	intervalNotificationsStore     *ctrlreconciler.NotificationStore
}

// Check that our Reconciler implements Interface
var _ reconcilersamplesource.Interface = (*Reconciler)(nil)

// ReconcileKind implements Interface.ReconcileKind.
func (r *Reconciler) ReconcileKind(ctx context.Context, src *v1alpha1.SampleSource) pkgreconciler.Event {
	logger := logging.FromContext(ctx)

	ctx = sourcesv1.WithURIResolver(ctx, r.sinkResolver)

	// Generate the key pair, if not existing
	raNamespacedName := types.NamespacedName{
		Name:      resources.MakeReceiveAdapterDeploymentName(src),
		Namespace: src.Namespace,
	}
	var dataPlaneKeyPair *ctrlnetwork.KeyPair
	if dataPlaneKeyPair, _ = r.getKeyPair(raNamespacedName); dataPlaneKeyPair == nil {
		var err error
		dataPlaneKeyPair, err = r.certificateManager.EmitNewDataPlaneCertificate(ctx)
		if err != nil {
			return err
		}

		r.keyPairsMutex.Lock()
		r.keyPairs[raNamespacedName] = dataPlaneKeyPair
		r.keyPairsMutex.Unlock()
	}

	logger.Infof("we have a data plane key pair")

	// -- Reconcile secret
	secret, event := r.dr.ReconcileSecret(ctx, src, resources.MakeSecret(
		src,
		r.certificateManager.CaCertBytes(),
		dataPlaneKeyPair.PrivateKeyBytes(),
		dataPlaneKeyPair.CertBytes(),
	))
	if event != nil {
		logger.Infof("returning because event from ReconcileSecret")
		return event
	}

	logger.Infof("we have a secret")

	// -- Create deployment (that's the same as usual, except we don't provide the interval)
	raArgs := &resources.ReceiveAdapterArgs{
		EventSource:    src.Namespace + "/" + src.Name,
		Image:          r.ReceiveAdapterImage,
		Source:         src,
		Labels:         resources.Labels(src.Name),
		AdditionalEnvs: r.configAccessor.ToEnvVars(), // Grab config envs for tracing/logging/metrics
	}
	ra, sb, event := r.dr.ReconcileDeployment(ctx, src, makeSinkBinding(src), resources.MakeReceiveAdapter(raArgs, secret))
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

	// We need to get all the pods for that ra deployment
	pods, err := r.dr.KubeClientSet.CoreV1().Pods(src.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: resources.LabelSelector(src.Name),
	})
	if err != nil {
		return fmt.Errorf("error getting receive adapter pods %q: %v", ra.Name, err)
	}

	if len(pods.Items) == 0 {
		logger.Infof("returning because there is still no pod up for the deployment '%s'", ra.Name)
		return nil
	}
	if len(pods.Items) > 1 {
		// No need to fix this here, out of the scope of the prototype
		return fmt.Errorf("wrong pods number: %d", len(pods.Items))
	}

	// TODO should we change this with endpoint tracking, creating a kube svc for the control endpoint?
	//  How do we handle connections to specific pods for partitioning then?
	raPodIp := pods.Items[0].Status.PodIP
	if raPodIp == "" {
		logger.Infof("returning because there is still no pod ip for the deployment '%s'", ra.Name)
		return nil
	}

	// --- Reconcile the connections (in our case, only one connection is there)
	srcNamespacedName := types.NamespacedName{Name: src.Name, Namespace: src.Namespace}
	conns, err := r.controlConnections.ReconcileConnections(
		ctx,
		string(src.UID),
		[]string{raPodIp},
		func(podIp string, service control.Service) {
			service.MessageHandler(ctrlservice.MessageRouter{
				ctrlsamplesource.NotifyIntervalOpCode:     r.intervalNotificationsStore.ControlMessageHandler(srcNamespacedName, podIp, ctrlreconciler.PassNewValue),
				ctrlsamplesource.NotifyActiveStatusOpCode: r.activeStatusNotificationsStore.ControlMessageHandler(srcNamespacedName, podIp, ctrlreconciler.PassNewValue),
			})
		},
		func(podIp string) {
			r.intervalNotificationsStore.CleanPodNotification(srcNamespacedName, podIp)
			r.activeStatusNotificationsStore.CleanPodNotification(srcNamespacedName, podIp)
		},
	)
	if err != nil {
		return fmt.Errorf("cannot reconcile connections: %w", err)
	}
	connectedIp := raPodIp
	ctrl := conns[connectedIp]

	logger.Infof("we have a control connection to %s", connectedIp)

	// TODO lionel add your fancy MT scheduler here!

	actualInterval, _ := time.ParseDuration(src.Spec.Interval) // No need to check the error here, the webhook already did it
	err = ctrl.SendAndWaitForAck(ctrlsamplesource.UpdateIntervalOpCode, ctrlsamplesource.Duration(actualInterval))
	if err != nil {
		return fmt.Errorf("cannot send the event to the pod: %w", err)
	}

	logger.Infof("we have sent the interval update")

	src.Status.MarkConfigurationNotPropagated()

	// --- If last status update to that deployment doesn't contain the correct interval,
	// it means we still didn't received that status update message
	// TODO here we're using interval for simplicity,
	//  but we may need to find another way to uniquely identify a configuration (hash of the in memory object?)
	if lastAckedInterval, ok := r.intervalNotificationsStore.GetPodNotification(srcNamespacedName, connectedIp); !ok || time.Duration(*(lastAckedInterval.(*ctrlsamplesource.Duration))) != actualInterval {
		return nil
	}

	logger.Infof("we have received the status update")

	src.Status.MarkConfigurationPropagated()

	// --- Reconcile active state
	if src.Spec.Active == nil || *src.Spec.Active == true {
		logger.Infof("Reconciling active status")

		// Reconcile active
		err := ctrl.SendAndWaitForAck(ctrlsamplesource.UpdateActiveStatusOpCode, ctrlsamplesource.ActiveStatus(true))
		if err != nil {
			return fmt.Errorf("cannot send the event to the pod: %w", err)
		}
		src.Status.MarkResuming()
		logger.Infof("Sent resume command")

		activeStatus, ok := r.activeStatusNotificationsStore.GetPodNotification(srcNamespacedName, connectedIp)
		if !ok || activeStatus.(*ctrlsamplesource.ActiveStatus).IsPaused() {
			// Short-circuit because we still didn't received the status update
			return nil
		}
		src.Status.MarkActive()
		logger.Infof("Received active adapter signal")

	} else {
		logger.Infof("Reconciling stop status")

		// Reconcile stop
		err := ctrl.SendAndWaitForAck(ctrlsamplesource.UpdateActiveStatusOpCode, ctrlsamplesource.ActiveStatus(false))
		if err != nil {
			return fmt.Errorf("cannot send the event to the pod: %w", err)
		}
		src.Status.MarkPausing()
		logger.Infof("Sent stop command")

		activeStatus, ok := r.activeStatusNotificationsStore.GetPodNotification(srcNamespacedName, connectedIp)
		if !ok || activeStatus.(*ctrlsamplesource.ActiveStatus).IsRunning() {
			// Short-circuit because we still didn't received the status update
			return nil
		}
		src.Status.MarkPaused()
		logger.Infof("Received paused adapter signal")

	}

	return nil
}

func (r *Reconciler) FinalizeKind(ctx context.Context, src *v1alpha1.SampleSource) pkgreconciler.Event {
	// Check if there is an old ip, so we can cleanup that conn
	r.controlConnections.RemoveAllConnections(ctx, string(src.UID))

	srcNamespacedName := types.NamespacedName{Name: src.Name, Namespace: src.Namespace}
	r.intervalNotificationsStore.CleanPodsNotifications(srcNamespacedName)
	r.activeStatusNotificationsStore.CleanPodsNotifications(srcNamespacedName)

	return nil
}

func (r *Reconciler) getKeyPair(name types.NamespacedName) (*ctrlnetwork.KeyPair, bool) {
	r.keyPairsMutex.Lock()
	defer r.keyPairsMutex.Unlock()
	kp, ok := r.keyPairs[name]
	return kp, ok
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
