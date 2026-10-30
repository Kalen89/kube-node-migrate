package nodemigrate

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

const healthRequestTimeout = 15 * time.Second

func (e *Engine) pullSecrets(ctx context.Context, namespace string, spec *corev1.PodSpec) ([]corev1.LocalObjectReference, error) {
	secrets := append([]corev1.LocalObjectReference(nil), spec.ImagePullSecrets...)
	if len(secrets) == 0 {
		name := spec.ServiceAccountName
		if name == "" {
			name = "default"
		}
		var account corev1.ServiceAccount
		if err := e.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &account); err != nil {
			return nil, err
		}
		secrets = append(secrets, account.ImagePullSecrets...)
	}
	return secrets, nil
}
