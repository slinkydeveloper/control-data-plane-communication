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
	"knative.dev/control-data-plane-communication/pkg/controlprotocol"
	"knative.dev/control-data-plane-communication/pkg/reconciler"
	"knative.dev/control-data-plane-communication/pkg/reconciler/sample/resources"
)

// Reconciler reconciles a SampleSource object
type Reconciler struct {
	ReceiveAdapterImage string `envconfig:"SAMPLE_SOURCE_RA_IMAGE" required:"true"`

	dr             *reconciler.DeploymentReconciler
	sinkResolver   *resolver.URIResolver
	configAccessor reconcilersource.ConfigAccessor

	certificateManager *controlprotocol.CertificateManager
	controlConnections *controlprotocol.ControlPlaneConnectionPool

	keyPairs      map[types.NamespacedName]*controlprotocol.KeyPair
	keyPairsMutex sync.Mutex

	// TODO abstract this
	lastSentStateIsActive map[string]bool
	lastSentStateMutex    sync.Mutex
	// TODO abstract this
	lastIntervalUpdateSent     map[string]time.Duration
	lastIntervalUpdateSentLock sync.Mutex

	statusUpdateStore *StatusUpdateStore
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
	var dataPlaneKeyPair *controlprotocol.KeyPair
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
	conns, err := r.controlConnections.ReconcileConnections(
		ctx,
		string(src.UID),
		[]string{raPodIp},
		func(s string, service controlprotocol.Service) {
			srcNamespacedName := types.NamespacedName{Name: src.Name, Namespace: src.Namespace}
			service.InboundMessageHandler(controlprotocol.ControlMessageHandlerFunc(func(ctx context.Context, message controlprotocol.ControlMessage) {
				message.Ack()
				r.statusUpdateStore.ControlMessageHandler(ctx, message.Headers().OpCode(), message.Payload(), s, srcNamespacedName)
			}))
		},
		nil, // TODO Should clean up
	)
	if err != nil {
		return fmt.Errorf("cannot reconcile connections: %w", err)
	}
	connectedIp := raPodIp
	ctrl := conns[connectedIp]

	logger.Infof("we have a control connection to %s", connectedIp)

	// TODO lionel add your fancy MT scheduler here!

	// --- If last configuration update to that deployment doesn't contain the interval, set it
	actualInterval, _ := time.ParseDuration(src.Spec.Interval) // No need to check the error here, the webhook already did it
	if old, ok := r.getLastSentInterval(connectedIp); !ok || old != actualInterval {
		err := ctrl.SendAndWaitForAck(control.UpdateIntervalOpCode, control.Duration(actualInterval))
		if err != nil {
			return fmt.Errorf("cannot send the event to the pod: %w", err)
		}

		r.lastIntervalUpdateSentLock.Lock()
		r.lastIntervalUpdateSent[connectedIp] = actualInterval
		r.lastIntervalUpdateSentLock.Unlock()
	}

	logger.Infof("we have sent the interval update")

	src.Status.MarkConfigurationNotPropagated()

	// --- If last status update to that deployment doesn't contain the correct interval,
	// it means we still didn't received that status update message
	// TODO here we're using interval for simplicity,
	//  but we may need to find another way to uniquely identify a configuration (hash of the in memory object?)
	if ackedInterval, ok := r.statusUpdateStore.GetLastUpdate(connectedIp); !ok || ackedInterval != actualInterval {
		return nil
	}

	logger.Infof("we have received the status update")

	src.Status.MarkConfigurationPropagated()

	// --- Reconcile active state
	if src.Spec.Active == nil || *src.Spec.Active == true {
		logger.Infof("Reconciling active status")

		// Reconcile active
		if sentResumeSignal, ok := r.getLastSentSignal(connectedIp); !ok || !sentResumeSignal {
			err := ctrl.SendSignalAndWaitForAck(control.ResumeOpCode)
			if err != nil {
				return fmt.Errorf("cannot send the event to the pod: %w", err)
			}

			r.setLastSentSignal(connectedIp, true)
		}
		src.Status.MarkResuming()
		logger.Infof("Sent resume command")

		if adapterIsActive, ok := r.statusUpdateStore.GetLastActiveStatus(connectedIp); !ok || !adapterIsActive {
			// Short-circuit because we still didn't received the status update
			return nil
		}
		src.Status.MarkActive()
		logger.Infof("Received active adapter signal")

	} else {
		logger.Infof("Reconciling stop status")

		// Reconcile stop
		if sentResumeSignal, ok := r.getLastSentSignal(connectedIp); !ok || sentResumeSignal {
			err := ctrl.SendSignalAndWaitForAck(control.StopOpCode)
			if err != nil {
				return fmt.Errorf("cannot send the event to the pod: %w", err)
			}

			r.setLastSentSignal(connectedIp, false)
		}
		src.Status.MarkPausing()
		logger.Infof("Sent stop command")

		if adapterIsActive, ok := r.statusUpdateStore.GetLastActiveStatus(connectedIp); !ok || adapterIsActive {
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

	return nil
}

func (r *Reconciler) getKeyPair(name types.NamespacedName) (*controlprotocol.KeyPair, bool) {
	r.keyPairsMutex.Lock()
	defer r.keyPairsMutex.Unlock()
	kp, ok := r.keyPairs[name]
	return kp, ok
}

func (r *Reconciler) getLastSentInterval(podIp string) (time.Duration, bool) {
	r.lastIntervalUpdateSentLock.Lock()
	defer r.lastIntervalUpdateSentLock.Unlock()
	t, ok := r.lastIntervalUpdateSent[podIp]
	return t, ok
}

func (r *Reconciler) setLastSentSignal(podIp string, active bool) {
	r.lastSentStateMutex.Lock()
	r.lastSentStateIsActive[podIp] = active
	defer r.lastSentStateMutex.Unlock()
}

func (r *Reconciler) getLastSentSignal(podIp string) (bool, bool) {
	r.lastSentStateMutex.Lock()
	defer r.lastSentStateMutex.Unlock()
	b, ok := r.lastSentStateIsActive[podIp]
	return b, ok
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
