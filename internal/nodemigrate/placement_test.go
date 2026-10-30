package nodemigrate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func appPlacementFixture(t *testing.T) (*Engine, *Workload, Options) {
	t.Helper()
	e, w, o := fixture(t)
	w.Kind = kindDeployment
	w.Volume = nil
	w.Owner = &metav1.OwnerReference{APIVersion: "dev.qingteng.cn/v2", Kind: kindApp, Name: "app", UID: "app-uid", Controller: pointer(true)}
	w.Template.Spec.Volumes = nil
	w.Template.Spec.NodeSelector = map[string]string{hostnameLabel: "old", "kubernetes.io/os": "linux"}
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: w.Name, Namespace: w.Namespace, UID: w.UID, OwnerReferences: []metav1.OwnerReference{*w.Owner}}, Spec: appsv1.DeploymentSpec{Replicas: pointer(int32(1)), Template: w.Template}}
	require.NoError(t, e.Client.Create(t.Context(), d))
	var pod corev1.Pod
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: "db-0"}, &pod))
	pod.OwnerReferences[0].Kind = kindDeployment
	pod.Spec.Volumes = nil
	require.NoError(t, e.Client.Update(t.Context(), &pod))
	var target corev1.Node
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: "new"}, &target))
	target.Labels["kubernetes.io/os"] = "linux"
	require.NoError(t, e.Client.Update(t.Context(), &target))
	return e, w, o
}

func TestLegacyPlanMovesOnlySourceHostnamePin(t *testing.T) {
	e, w, o := appPlacementFixture(t)
	s, err := e.legacyPlan(t.Context(), o)
	require.NoError(t, err)
	require.Len(t, s.Workloads, 1)
	require.Equal(t, "old", s.Workloads[0].Template.Spec.NodeSelector[hostnameLabel], "plan must preserve the original snapshot")
	var d appsv1.Deployment
	require.NoError(t, e.Client.Get(t.Context(), client.ObjectKey{Namespace: w.Namespace, Name: w.Name}, &d))
	d.Spec.Template.Spec.NodeSelector[hostnameLabel] = "third-node"
	require.NoError(t, e.Client.Update(t.Context(), &d))
	_, err = e.legacyPlan(t.Context(), o)
	require.ErrorContains(t, err, "nodeSelector")
}

func TestLegacyPinnedStatelessAppMovesAndPreservesOwnerAndOtherSelectors(t *testing.T) {
	e, w, _ := appPlacementFixture(t)
	e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if err := c.Update(ctx, obj, opts...); err != nil {
			return err
		}
		if d, ok := obj.(*appsv1.Deployment); ok && d.Spec.Template.Spec.NodeSelector[hostnameLabel] == "new" {
			var pod corev1.Pod
			if err := c.Get(ctx, client.ObjectKey{Namespace: w.Namespace, Name: "db-0"}, &pod); err != nil {
				return err
			}
			pod.Spec.NodeName = "new"
			return c.Update(ctx, &pod)
		}
		return nil
	}})
	require.NoError(t, e.moveWorkload(t.Context(), w))
	var d appsv1.Deployment
	require.NoError(t, e.Client.Get(t.Context(), client.ObjectKey{Namespace: w.Namespace, Name: w.Name}, &d))
	require.Equal(t, "new", d.Spec.Template.Spec.NodeSelector[hostnameLabel])
	require.Equal(t, "linux", d.Spec.Template.Spec.NodeSelector["kubernetes.io/os"])
	require.Equal(t, w.Owner.UID, metav1.GetControllerOf(&d).UID)
	require.Equal(t, int32(1), *d.Spec.Replicas)
}

func TestLegacyRolloutWaitsForOriginalPodAndRejectsWrongReplacement(t *testing.T) {
	e, w, _ := appPlacementFixture(t)
	err := e.waitWorkload(t.Context(), w, true)
	require.ErrorContains(t, err, "context deadline exceeded", "the original source Pod must be allowed to drain")
	var pod corev1.Pod
	key := client.ObjectKey{Namespace: w.Namespace, Name: "db-0"}
	require.NoError(t, e.Client.Get(t.Context(), key, &pod))
	pod.Spec.NodeName = "third-node"
	require.NoError(t, e.Client.Update(t.Context(), &pod))
	require.ErrorContains(t, e.waitWorkload(t.Context(), w, true), "调度到 third-node")
	pod.Spec.NodeName = "new"
	require.NoError(t, e.Client.Update(t.Context(), &pod))
	require.NoError(t, e.waitWorkload(t.Context(), w, true))
}
