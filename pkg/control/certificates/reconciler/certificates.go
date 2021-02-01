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
	"crypto/rsa"
	"crypto/x509"
	"fmt"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	listerv1 "k8s.io/client-go/listers/core/v1"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/system"

	reconcilersecret "knative.dev/pkg/client/injection/kube/reconciler/core/v1/secret"

	"knative.dev/control-data-plane-communication/pkg/control/certificates/reconciler/resources"
)

const (
	caCertKey          = "ca.cert"
	certKey            = "public.cert"
	privateKeyKey      = "private.key"
	expirationInterval = time.Hour * 24 * 30 // 30 days

	controlPlaneSecretType = "control-plane"
	dataPlaneSecretType    = "data-plane"
)

// Reconciler reconciles a SampleSource object
type reconciler struct {
	client              kubernetes.Interface
	secretLister        listerv1.SecretLister
	caSecretName        string
	secretTypeLabelName string

	logger *zap.SugaredLogger
}

// Check that our Reconciler implements Interface
var _ reconcilersecret.Interface = (*reconciler)(nil)

// ReconcileKind implements Interface.ReconcileKind.
func (r *reconciler) ReconcileKind(ctx context.Context, secret *corev1.Secret) pkgreconciler.Event {

	// Reconcile CA secret first
	caSecret, err := r.secretLister.Secrets(system.Namespace()).Get(r.caSecretName)
	if apierrors.IsNotFound(err) {
		// The secret should be created explicitly by a higher-level system
		// that's responsible for install/updates.  We simply populate the
		// secret information.
		return nil
	} else if err != nil {
		r.logger.Errorf("Error accessing CA certificate secret %q %q: %v", system.Namespace(), r.caSecretName, err)
		return err
	}
	caCert, caPk, err := parseAndValidateSecret(caSecret, false)
	if err != nil {
		// We need to generate a new CA cert, then shortcircuit the reconciler
		keyPair, err := resources.CreateCACerts(ctx, expirationInterval)
		if err != nil {
			return fmt.Errorf("cannot generate the CA cert: %v", err)
		}
		return r.commitUpdatedSecret(ctx, caSecret, keyPair)
	}

	_, _, err = parseAndValidateSecret(secret, true)
	if err != nil {
		// Check the secret to reconcile type
		var keyPair *resources.KeyPair
		if secret.Labels[r.secretTypeLabelName] == dataPlaneSecretType {
			keyPair, err = resources.CreateDataPlaneCert(ctx, caPk, caCert, expirationInterval)
		} else if secret.Labels[r.secretTypeLabelName] == controlPlaneSecretType {
			keyPair, err = resources.CreateControlPlaneCert(ctx, caPk, caCert, expirationInterval)
		} else {
			return fmt.Errorf("unknown cert type: %v", r.secretTypeLabelName)
		}
		if err != nil {
			return fmt.Errorf("cannot generate the cert: %v", err)
		}
		secret.Data[caCertKey] = caSecret.Data[certKey]
		return r.commitUpdatedSecret(ctx, secret, keyPair)
	}

	return nil
}

func parseAndValidateSecret(secret *corev1.Secret, shouldContainCaCert bool) (*x509.Certificate, *rsa.PrivateKey, error) {
	certBytes, ok := secret.Data[certKey]
	if !ok {
		return nil, nil, fmt.Errorf("missing cert bytes")
	}
	pkBytes, ok := secret.Data[privateKeyKey]
	if !ok {
		return nil, nil, fmt.Errorf("missing pk bytes")
	}
	if shouldContainCaCert {
		if _, ok := secret.Data[caCertKey]; !ok {
			return nil, nil, fmt.Errorf("missing ca cert bytes")
		}
	}

	caCert, caPk, err := resources.ParseCert(certBytes, pkBytes)
	if err != nil {
		return nil, nil, err
	}
	if err := resources.ValidateCert(caCert); err != nil {
		return nil, nil, err
	}
	return caCert, caPk, nil
}

func (r *reconciler) commitUpdatedSecret(ctx context.Context, secret *corev1.Secret, keyPair *resources.KeyPair) error {
	secret.Data[certKey] = keyPair.CertBytes()
	secret.Data[privateKeyKey] = keyPair.PrivateKeyBytes()

	_, err := r.client.CoreV1().Secrets(secret.Namespace).Update(ctx, secret, metav1.UpdateOptions{})
	return err
}
