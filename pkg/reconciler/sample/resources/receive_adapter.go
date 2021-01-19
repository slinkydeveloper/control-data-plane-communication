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

package resources

import (
	"fmt"

	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/system"

	"knative.dev/control-data-plane-communication/pkg/apis/samples/v1alpha1"
	"knative.dev/control-data-plane-communication/pkg/controlprotocol"
)

// ReceiveAdapterArgs are the arguments needed to create a Sample Source Receive Adapter.
// Every field is required.
type ReceiveAdapterArgs struct {
	Image          string
	Labels         map[string]string
	Source         *v1alpha1.SampleSource
	EventSource    string
	AdditionalEnvs []corev1.EnvVar
}

func MakeReceiveAdapterDeploymentName(src *v1alpha1.SampleSource) string {
	return kmeta.ChildName(fmt.Sprintf("samplesource-%s-", src.Name), string(src.GetUID()))
}

// MakeReceiveAdapter generates (but does not insert into K8s) the Receive Adapter Deployment for
// Sample sources.
func MakeReceiveAdapter(args *ReceiveAdapterArgs, secret *corev1.Secret) *v1.Deployment {
	replicas := int32(1)
	return &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: args.Source.Namespace,
			Name:      MakeReceiveAdapterDeploymentName(args.Source),
			Labels:    args.Labels,
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(args.Source),
			},
		},
		Spec: v1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: args.Labels,
			},
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: args.Labels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: args.Source.Spec.ServiceAccountName,
					Containers: []corev1.Container{
						{
							Name:  "receive-adapter",
							Image: args.Image,
							Env: append(
								makeEnv(args.EventSource),
								args.AdditionalEnvs...,
							),
							Ports: []corev1.ContainerPort{{
								Name:          "ws",
								ContainerPort: 9090,
							}},
							VolumeMounts: []corev1.VolumeMount{{
								Name:      "control-secret",
								MountPath: "/etc/secret",
								ReadOnly:  true,
							}},
						},
					},
					Volumes: []corev1.Volume{{
						Name: "control-secret",
						VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
							SecretName: secret.Name,
						}},
					}},
				},
			},
		},
	}
}

func MakeSecret(controllerKeyPair *controlprotocol.KeyHolder, dataPlaneKeyPair *controlprotocol.KeyHolder) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: system.Namespace(),
			Name:      "sample-source-control",
		},
		Immutable: pointer.BoolPtr(true),
		Data: map[string][]byte{
			"controller_public.pem": controllerKeyPair.PublicKeyBytes(),
			"data_plane_secret.pem": dataPlaneKeyPair.PrivateKeyBytes(),
			"data_plane_public.pem": dataPlaneKeyPair.PublicKeyBytes(),
		},
	}
}

func makeEnv(eventSource string) []corev1.EnvVar {
	return []corev1.EnvVar{{
		Name:  "EVENT_SOURCE",
		Value: eventSource,
	}, {
		Name: "NAMESPACE",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "metadata.namespace",
			},
		},
	}, {
		Name: "NAME",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "metadata.name",
			},
		},
	}, {
		Name:  "METRICS_DOMAIN",
		Value: "knative.dev/eventing",
	}}
}
