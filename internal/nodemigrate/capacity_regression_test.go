package nodemigrate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestCapacityIncludesStoppedInstanceAndDeduplicatesReplacement(t *testing.T) {
	e, w := instanceFixture(t)
	w.Instance.Pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Gi")}
	w.Phase = phaseStopped
	require.NoError(t, e.Client.Delete(t.Context(), &w.Instance.Pod))
	var target corev1.Node
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: e.State.Target.Name}, &target))
	target.Status.Allocatable[corev1.ResourceMemory] = resource.MustParse("10Gi")
	require.NoError(t, e.Client.Status().Update(t.Context(), &target))
	existing := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "other"}, Spec: corev1.PodSpec{NodeName: target.Name, Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")}}}}}}
	require.NoError(t, e.Client.Create(t.Context(), existing))
	require.ErrorContains(t, e.checkCapacity(t.Context()), "目标容量不足")
	require.Equal(t, "8Gi", e.State.Capacity.Incoming.Memory().String())
	require.NoError(t, e.Client.Delete(t.Context(), existing))
	replacement := w.Instance.Pod.DeepCopy()
	replacement.ResourceVersion, replacement.UID = "", "replacement"
	replacement.Spec.NodeName = e.State.Target.Name
	require.NoError(t, e.Client.Create(t.Context(), replacement))
	require.NoError(t, e.checkCapacity(t.Context()))
	require.True(t, e.State.Capacity.Incoming.Memory().IsZero())
	require.Equal(t, "8Gi", e.State.Capacity.Existing.Memory().String())
	// An admitted replacement waiting for the scheduler is still incoming.
	replacement.Spec.NodeName = ""
	replacement.Spec.NodeSelector = map[string]string{hostnameLabel: target.Name}
	require.NoError(t, e.Client.Update(t.Context(), replacement))
	require.NoError(t, e.checkCapacity(t.Context()))
	require.Equal(t, "8Gi", e.State.Capacity.Incoming.Memory().String())
}

func TestSourceCopyCapacityWithoutPrecopyUsesReleasedInstance(t *testing.T) {
	for _, tc := range []struct {
		name, original, other string
		precopy               bool
		wantError             bool
	}{
		{"offline-insufficient", "100m", "1500m", false, true},
		{"offline-reuses-released-resources", "1", "1", false, false},
		{"online-needs-overlap", "1", "1", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, w := instanceFixture(t)
			e.State.Options.Precopy = tc.precopy
			e.State.Options.SSHDResources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}
			var source corev1.Node
			require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: e.State.Source.Name}, &source))
			source.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("2")
			require.NoError(t, e.Client.Status().Update(t.Context(), &source))
			var original corev1.Pod
			require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(&w.Instance.Pod), &original))
			original.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(tc.original)}
			require.NoError(t, e.Client.Update(t.Context(), &original))
			require.NoError(t, e.Client.Create(t.Context(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "other"}, Spec: corev1.PodSpec{NodeName: source.Name, Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(tc.other)}}}}}}))
			err := e.checkCapacity(t.Context())
			if tc.wantError {
				require.ErrorContains(t, err, "源节点缺少复制助手容量")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNamespaceQuotaUsesSerialStagesInsteadOfNodeReserve(t *testing.T) {
	e, w := instanceFixture(t)
	e.State.Options.Precopy = false
	e.State.Options.ReserveCPU, e.State.Options.ReserveMemory, e.State.Options.ReservePods = "8", "16Gi", 20
	original := w.Instance.Pod.DeepCopy()
	original.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
	}
	w.Instance.Pod = *original.DeepCopy()
	w.Template.Spec.Containers = original.Spec.Containers
	quota := corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "exact-stage"}, Status: corev1.ResourceQuotaStatus{
		Hard: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("4"), "count/pods": resource.MustParse("4"), corev1.ResourceRequestsCPU: resource.MustParse("400m"), corev1.ResourceLimitsCPU: resource.MustParse("4")},
		Used: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("3"), "count/pods": resource.MustParse("3"), corev1.ResourceRequestsCPU: resource.MustParse("300m"), corev1.ResourceLimitsCPU: resource.MustParse("3")},
	}}
	require.NoError(t, e.namespaceCapacity(w.Namespace, []corev1.ResourceQuota{quota}, nil, []corev1.Pod{*original}, false))
	e.State.Options.Precopy = true
	require.ErrorContains(t, e.namespaceCapacity(w.Namespace, []corev1.ResourceQuota{quota}, nil, []corev1.Pod{*original}, false), "ResourceQuota")
	// Without PVCs and with cached images, eviction/replacement needs no extra
	// namespace quota, even though the target retains node-level headroom.
	w.Kind, w.Volumes = kindDeployment, nil
	quota.Status.Hard = quota.Status.Used.DeepCopy()
	require.NoError(t, e.namespaceCapacity(w.Namespace, []corev1.ResourceQuota{quota}, nil, []corev1.Pod{*original}, false))
	require.ErrorContains(t, e.namespaceCapacity(w.Namespace, []corev1.ResourceQuota{quota}, nil, []corev1.Pod{*original}, true), "ResourceQuota")
}

func TestNamespaceLimitsApplyPodLimitsAndResourceDefaults(t *testing.T) {
	e, w := instanceFixture(t)
	w.Template.Spec.Containers[0].Resources = DefaultServiceResources()
	limits := []corev1.LimitRange{{ObjectMeta: metav1.ObjectMeta{Name: "resources"}, Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{
		{Type: corev1.LimitTypeContainer, DefaultRequest: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("10Mi")}, Default: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("20Mi")}, Min: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("1Mi")}},
		{Type: corev1.LimitTypePod, Max: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("768Mi")}},
	}}}}
	spec := corev1.PodSpec{Containers: []corev1.Container{{Name: "one", Resources: DefaultServiceResources()}, {Name: "two", Resources: DefaultServiceResources()}}}
	require.ErrorContains(t, namespacePodLimits(w.Namespace, "business", &spec, limits), "Pod 约束")
	quota := corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "ephemeral"}, Status: corev1.ResourceQuotaStatus{Hard: corev1.ResourceList{corev1.ResourceRequestsEphemeralStorage: resource.MustParse("19Mi")}, Used: corev1.ResourceList{corev1.ResourceRequestsEphemeralStorage: resource.MustParse("0")}}}
	// Both copy helpers inherit 10 MiB; the pair exceeds the quota.
	require.ErrorContains(t, e.namespaceCapacity(w.Namespace, []corev1.ResourceQuota{quota}, limits, nil, false), "requests.ephemeral-storage")
}

func TestCapacityCacheReadOnceForMultipleNamespaces(t *testing.T) {
	e, w := instanceFixture(t)
	other := *w
	other.Namespace = "other"
	e.State.Workloads = append(e.State.Workloads, &other)
	require.NoError(t, e.Client.Create(t.Context(), &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: other.Namespace}}))
	for _, ns := range []string{w.Namespace, other.Namespace} {
		quota := &corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "quota", Namespace: ns}, Status: corev1.ResourceQuotaStatus{Hard: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("100")}, Used: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("0")}}}
		require.NoError(t, e.Client.Create(t.Context(), quota))
	}
	calls := 0
	previous := e.Hosts
	e.Hosts = hostFunc(func(ctx context.Context, h Host, command string) (string, error) {
		if command == "k3s ctr -n k8s.io images check" {
			calls++
		}
		return previous.Run(ctx, h, command)
	})
	require.NoError(t, e.checkCapacity(t.Context()))
	require.LessOrEqual(t, calls, 2, "at most one cache read per source/target host")
}

func TestSourcePrewarmChecksHelperBeforeStoppingEvenWithoutPrecopy(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "cached"}[cached], func(t *testing.T) {
			e, w := instanceFixture(t)
			e.State.Options.Precopy = false
			var source corev1.Node
			require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: e.State.Source.Name}, &source))
			source.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("2")
			require.NoError(t, e.Client.Status().Update(t.Context(), &source))
			var original corev1.Pod
			require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(&w.Instance.Pod), &original))
			original.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}
			require.NoError(t, e.Client.Update(t.Context(), &original))
			previous := e.Hosts
			e.Hosts = hostFunc(func(ctx context.Context, h Host, command string) (string, error) {
				if h.Name == e.State.Source.Name && command == "k3s ctr -n k8s.io images check" && !cached {
					return "", nil
				}
				return previous.Run(ctx, h, command)
			})
			err := e.checkCapacity(t.Context())
			if cached {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "源节点缺少复制助手容量")
			}
		})
	}
}

func TestCapacityAndQuotaDoNotDoubleCountResumedCopyHelpers(t *testing.T) {
	e, w := instanceFixture(t)
	w.Phase = phaseStopped
	e.State.Options.Precopy = false
	require.NoError(t, e.Client.Delete(t.Context(), &w.Instance.Pod))
	w.Instance.Pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}
	var target corev1.Node
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: e.State.Target.Name}, &target))
	target.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("300m")
	require.NoError(t, e.Client.Status().Update(t.Context(), &target))
	var pods []corev1.Pod
	for i, node := range []string{e.State.Source.Name, e.State.Target.Name} {
		pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: []string{"sshd", "rsync"}[i], Namespace: w.Namespace, Labels: map[string]string{"app.kubernetes.io/instance": "pv-migrate-" + w.Volumes[0].CopyID + "-clusterip"}}, Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "copy", Resources: DefaultServiceResources()}}}}
		require.NoError(t, e.Client.Create(t.Context(), &pod))
		pods = append(pods, pod)
	}
	require.NoError(t, e.checkCapacity(t.Context()))
	quota := corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "exact"}, Status: corev1.ResourceQuotaStatus{Hard: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("2"), "count/pods": resource.MustParse("2")}, Used: corev1.ResourceList{corev1.ResourcePods: resource.MustParse("2"), "count/pods": resource.MustParse("2")}}}
	require.NoError(t, e.namespaceCapacity(w.Namespace, []corev1.ResourceQuota{quota}, nil, pods, false))
}

func TestLimitRequestRatioUsesExactQuantities(t *testing.T) {
	requests := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("300m")}
	limits := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("900m")}
	rule := corev1.LimitRangeItem{MaxLimitRequestRatio: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")}}
	require.NoError(t, checkLimitResources(requests, limits, rule))
	require.Equal(t, "300m", requests.Cpu().String(), "the comparison must not mutate configured requests")
	limits[corev1.ResourceCPU] = resource.MustParse("901m")
	require.ErrorContains(t, checkLimitResources(requests, limits, rule), "比例")
}
