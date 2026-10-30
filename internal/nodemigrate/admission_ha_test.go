package nodemigrate

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestApplyWaitsForEveryCurrentAPIServerBeforeEviction(t *testing.T) {
	e, w := instanceFixture(t)
	e.State.Options.Precopy = false
	primary := e.AdmissionProbe
	stale, observed := true, 0
	secondary := interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, options ...client.CreateOption) error {
		if review, ok := obj.(*authorizationv1.SelfSubjectAccessReview); ok {
			review.Status.Allowed = true
			return nil
		}
		create := &client.CreateOptions{}
		for _, option := range options {
			option.ApplyToCreate(create)
		}
		require.NotEmpty(t, create.DryRun, "HA validation must never persist a probe")
		observed++
		if stale {
			return nil // Another API server has not observed the hold yet.
		}
		return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, obj.GetName(), errors.New(holdMessage(e.State, w)))
	}})
	refreshes := 0
	e.AdmissionProbes = func(context.Context) ([]client.Client, error) {
		refreshes++
		if refreshes == 1 {
			return []client.Client{primary}, nil // Initial preflight membership.
		}
		return []client.Client{primary, secondary}, nil
	}
	require.Error(t, e.Apply(t.Context(), true))
	require.Greater(t, observed, 0)
	var original corev1.Pod
	require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(&w.Instance.Pod), &original))
	require.Equal(t, w.Instance.Pod.UID, original.UID)
	require.True(t, podReady(&original))
	require.True(t, w.Instance.EvictionRequestedAt.IsZero())

	stale = false
	require.NoError(t, e.Apply(t.Context(), true))
	require.GreaterOrEqual(t, refreshes, 4)
	require.NoError(t, e.Verify(t.Context()))
}

func TestAdmissionEndpointDiscoveryCannotSilentlySkipProtection(t *testing.T) {
	for _, failure := range []string{"read-error", "empty", "nil-client"} {
		t.Run(failure, func(t *testing.T) {
			e, w := instanceFixture(t)
			e.AdmissionProbes = func(context.Context) ([]client.Client, error) {
				switch failure {
				case "read-error":
					return nil, errors.New("endpoint discovery unavailable")
				case "empty":
					return nil, nil
				default:
					return []client.Client{nil}, nil
				}
			}
			require.Error(t, e.Apply(t.Context(), true))
			var original corev1.Pod
			require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(&w.Instance.Pod), &original))
			require.True(t, w.Instance.EvictionRequestedAt.IsZero())
		})
	}
}
