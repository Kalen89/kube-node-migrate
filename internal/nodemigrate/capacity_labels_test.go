package nodemigrate

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestPodRequestsAccountsForInitSidecarsAndOverhead(t *testing.T) {
	spec := corev1.PodSpec{
		Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("256Mi")}}}},
		InitContainers: []corev1.Container{
			{RestartPolicy: pointer(corev1.ContainerRestartPolicyAlways), Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")}}},
			{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("900m"), corev1.ResourceMemory: resource.MustParse("128Mi")}}},
		}, Overhead: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("20m"), corev1.ResourceMemory: resource.MustParse("16Mi")},
	}
	q := podRequests(&spec)
	cpu, memory := q[corev1.ResourceCPU], q[corev1.ResourceMemory]
	require.Equal(t, "1020m", cpu.String())
	require.Equal(t, "336Mi", memory.String())
}

func TestCapacityBlocksBeforeAnyServiceIsStopped(t *testing.T) {
	e, w, _ := fixture(t)
	var pod corev1.Pod
	key := types.NamespacedName{Namespace: w.Namespace, Name: "db-0"}
	require.NoError(t, e.Client.Get(t.Context(), key, &pod))
	pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("33Gi")}
	require.NoError(t, e.Client.Update(t.Context(), &pod))
	require.ErrorContains(t, applyLegacyComponents(e, t.Context(), true), "目标容量不足")
	require.Equal(t, phasePlanned, w.Phase)
	require.NoError(t, e.Client.Get(t.Context(), key, &pod))
	require.Equal(t, "old", pod.Spec.NodeName)
}

func TestLegacyPlanLabelsAreReviewableAndAppliedOnlyDuringExecution(t *testing.T) {
	e, w, o := appPlacementFixture(t)
	// Use the same selector as the current demo, without changing the Node.
	var app appsv1.Deployment
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &app))
	app.Spec.Template.Spec.NodeSelector[MigrationLabel] = "true"
	require.NoError(t, e.Client.Update(t.Context(), &app))
	s, err := e.legacyPlan(t.Context(), o)
	require.NoError(t, err)
	require.Equal(t, "true", s.Options.TargetLabels[MigrationLabel])
	var target corev1.Node
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: "new"}, &target))
	require.NotContains(t, target.Labels, MigrationLabel)
	require.NoError(t, e.ensureTargetLabels(t.Context()))
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: "new"}, &target))
	require.Equal(t, "true", target.Labels[MigrationLabel])
	require.NotContains(t, target.Labels, "migration-demo-source")
	require.NoError(t, e.ensureTargetLabels(t.Context()))
	target.Labels[MigrationLabel] = "false"
	require.NoError(t, e.Client.Update(t.Context(), &target))
	require.ErrorContains(t, e.ensureTargetLabels(t.Context()), "拒绝覆盖")
}

func TestTargetLabelsNeverCopyIdentityAndConflicts(t *testing.T) {
	for _, key := range []string{hostnameLabel, "node-role.kubernetes.io/storage", "node.kubernetes.io/instance-type", labelKey} {
		require.Error(t, validateTargetLabels(map[string]string{key: "x"}))
	}
	require.Error(t, validateTargetLabels(map[string]string{MigrationLabel: "false"}))
	require.NoError(t, validateTargetLabels(map[string]string{MigrationLabel: "true", "example.com/storage": "yes"}))
}

func TestQuotaAndLimitRangeAreCheckedBeforePreparingPVCs(t *testing.T) {
	e, w, _ := fixture(t)
	w.Volume.TemporaryUID = ""
	quota := corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "storage", Namespace: w.Namespace}, Status: corev1.ResourceQuotaStatus{
		Hard: corev1.ResourceList{corev1.ResourcePersistentVolumeClaims: resource.MustParse("1")}, Used: corev1.ResourceList{corev1.ResourcePersistentVolumeClaims: resource.MustParse("1")},
	}}
	require.ErrorContains(t, e.namespaceCapacity(w.Namespace, []corev1.ResourceQuota{quota}, nil, nil, true), "ResourceQuota")
	w.Volume.TemporaryUID = "already-created"
	require.NoError(t, e.namespaceCapacity(w.Namespace, []corev1.ResourceQuota{quota}, nil, nil, true))
	limit := corev1.LimitRange{ObjectMeta: metav1.ObjectMeta{Name: "tiny"}, Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{Type: corev1.LimitTypeContainer, Max: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")}}}}}
	require.ErrorContains(t, e.namespaceCapacity(w.Namespace, nil, []corev1.LimitRange{limit}, nil, true), "LimitRange")
}

func TestCapacityReservesHelperPodSlots(t *testing.T) {
	e, _, _ := fixture(t)
	var node corev1.Node
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: "new"}, &node))
	node.Status.Allocatable[corev1.ResourcePods] = resource.MustParse("2")
	require.NoError(t, e.Client.Status().Update(t.Context(), &node))
	err := e.checkCapacity(context.Background())
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "pods"))
}

func TestCapacityIncludesExistingTargetPods(t *testing.T) {
	e, w, _ := fixture(t)
	existing := businessPod(w, "new")
	existing.Name = "other-app"
	existing.UID = "other-pod"
	existing.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16")}
	require.NoError(t, e.Client.Create(t.Context(), existing))
	require.ErrorContains(t, e.checkCapacity(t.Context()), "目标容量不足 resource=cpu")
}
