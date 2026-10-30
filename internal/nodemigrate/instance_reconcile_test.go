package nodemigrate

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestReconcilePermissionIsCheckedBeforeStoppingSource(t *testing.T) {
	e, w := instanceFixture(t)
	e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*appsv1.StatefulSet); ok {
				return apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, obj.GetName(), errors.New("patch denied"))
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	require.ErrorContains(t, e.Apply(t.Context(), true), "patch dry-run")
	var original corev1.Pod
	require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(&w.Instance.Pod), &original))
	require.Equal(t, w.Instance.Pod.UID, original.UID)
	require.True(t, podReady(&original))
	for _, v := range w.Volumes {
		var claim corev1.PersistentVolumeClaim
		require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(&v.PVC), &claim))
		require.Equal(t, v.PVC.UID, claim.UID)
	}
}

func TestReconcileAnnotationRetriesConflictsAndRecoversOwnMarker(t *testing.T) {
	e, w := instanceFixture(t)
	ctx := t.Context()
	key := reconcileAnnotation(e.State, w)
	attempts := 0
	e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			attempts++
			data, err := patch.Data(obj)
			require.NoError(t, err)
			require.NotContains(t, string(data), `"spec"`)
			require.NotContains(t, string(data), `"status"`)
			require.Contains(t, string(data), `"resourceVersion"`)
			if attempts == 1 {
				var changed appsv1.StatefulSet
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), &changed))
				changed.Annotations = map[string]string{"operator-note": "concurrent-update"}
				require.NoError(t, c.Update(ctx, &changed))
				return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, obj.GetName(), errors.New("concurrent update"))
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	require.NoError(t, e.patchReconcileAnnotation(ctx, w, false))
	require.Equal(t, 2, attempts)
	// An interruption leaves an owned marker; the same checkpoint can notify
	// again, then clear only that marker after observing the target Pod.
	require.NoError(t, e.patchReconcileAnnotation(ctx, w, false))
	var sts appsv1.StatefulSet
	require.NoError(t, e.Client.Get(ctx, client.ObjectKey{Namespace: w.Namespace, Name: w.Name}, &sts))
	require.Contains(t, sts.Annotations, key)
	require.Equal(t, *w.Instance.StatefulSetSpec, sts.Spec)
	require.NoError(t, e.patchReconcileAnnotation(ctx, w, true))
	require.NoError(t, e.patchReconcileAnnotation(ctx, w, true))
	require.NoError(t, e.Client.Get(ctx, client.ObjectKeyFromObject(&sts), &sts))
	require.Equal(t, map[string]string{"operator-note": "concurrent-update"}, sts.Annotations)
}

func TestReconcileRejectsForeignAnnotationAndControllerDrift(t *testing.T) {
	for _, tc := range []struct {
		name  string
		alter func(*appsv1.StatefulSet, string)
	}{
		{"foreign-marker", func(s *appsv1.StatefulSet, key string) { s.Annotations = map[string]string{key: "other"} }},
		{"empty-foreign-marker", func(s *appsv1.StatefulSet, key string) { s.Annotations = map[string]string{key: ""} }},
		{"uid-changed", func(s *appsv1.StatefulSet, _ string) { s.UID = "another-controller" }},
		{"replicas-changed", func(s *appsv1.StatefulSet, _ string) { s.Spec.Replicas = pointer(int32(4)) }},
		{"spec-changed", func(s *appsv1.StatefulSet, _ string) { s.Spec.MinReadySeconds++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, w := instanceFixture(t)
			var sts appsv1.StatefulSet
			require.NoError(t, e.Client.Get(t.Context(), client.ObjectKey{Namespace: w.Namespace, Name: w.Name}, &sts))
			tc.alter(&sts, reconcileAnnotation(e.State, w))
			require.NoError(t, e.Client.Update(t.Context(), &sts))
			before := sts.DeepCopy()
			require.Error(t, e.patchReconcileAnnotation(t.Context(), w, false))
			require.Error(t, e.patchReconcileAnnotation(t.Context(), w, true))
			require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(&sts), &sts))
			require.Equal(t, before.Spec, sts.Spec)
			require.Equal(t, before.Annotations, sts.Annotations)
		})
	}
}

func TestReconcileRequiresAllVolumesSwitchedAndHoldAbsent(t *testing.T) {
	e, w := instanceFixture(t)
	require.ErrorContains(t, e.requestStatefulSetReconcile(t.Context(), w), "全部 PVC")
	for _, v := range w.Volumes {
		v.Phase = phaseSwitched
	}
	policy, binding := e.holdObjects(w)
	require.NoError(t, e.Client.Create(t.Context(), policy))
	require.NoError(t, e.Client.Create(t.Context(), binding))
	require.ErrorContains(t, e.requestStatefulSetReconcile(t.Context(), w), "保护仍存在")
}
