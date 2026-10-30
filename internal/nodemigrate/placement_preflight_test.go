package nodemigrate

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestPendingPlacementRefreshesTargetConstraints(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Workload, *corev1.Node)
		want   string
	}{
		{"taint", func(_ *Workload, target *corev1.Node) {
			target.Spec.Taints = append(target.Spec.Taints, corev1.Taint{Key: "blocked", Effect: corev1.TaintEffectNoSchedule})
		}, "不容忍"},
		{"selector", func(w *Workload, _ *corev1.Node) {
			w.Template.Spec.NodeSelector = map[string]string{"storage": "fast"}
		}, "nodeSelector"},
		{"affinity", func(w *Workload, _ *corev1.Node) {
			w.Template.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "storage", Operator: corev1.NodeSelectorOpIn, Values: []string{"fast"}}}}}}}}
		}, "NodeAffinity"},
		{"topology", func(w *Workload, _ *corev1.Node) {
			w.Template.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{TopologyKey: hostnameLabel, WhenUnsatisfiable: corev1.DoNotSchedule, MaxSkew: 1}}
		}, "topology spread"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, w := instanceFixture(t)
			var target corev1.Node
			require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: e.State.Target.Name}, &target))
			tc.change(w, &target)
			require.NoError(t, e.Client.Update(t.Context(), &target))
			require.ErrorContains(t, e.checkPendingPlacement(t.Context()), tc.want)
			require.ErrorContains(t, e.checkInstancePlacement(t.Context(), w), tc.want)
			// Checks are read-only and leave the original running.
			var original corev1.Pod
			require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Instance.Pod.Name}, &original))
			require.True(t, podReady(&original))
		})
	}
}

func TestPlacementPreservesDeploymentHostnameRewrite(t *testing.T) {
	e, w := instanceFixture(t)
	w.Kind = kindDeployment
	w.Template.Spec.NodeSelector = map[string]string{hostnameLabel: e.State.Source.Name}
	require.NoError(t, e.checkInstancePlacement(t.Context(), w))
	require.Equal(t, e.State.Source.Name, w.Template.Spec.NodeSelector[hostnameLabel])
	w.Kind = kindStatefulSet
	require.ErrorContains(t, e.checkInstancePlacement(t.Context(), w), "nodeSelector")
}

func TestPlacementRejectsExistingAndPlannedHostPortConflicts(t *testing.T) {
	e, w := instanceFixture(t)
	w.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{HostPort: 8080}}
	existing := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "existing", Namespace: "other"}, Spec: corev1.PodSpec{NodeName: e.State.Target.Name, Containers: []corev1.Container{{Name: "app", Ports: []corev1.ContainerPort{{HostPort: 8080, Protocol: corev1.ProtocolTCP, HostIP: "10.0.0.2"}}}}}}
	require.NoError(t, e.Client.Create(t.Context(), existing))
	require.ErrorContains(t, e.checkInstancePlacement(t.Context(), w), "hostPort TCP/8080")
	require.NoError(t, e.Client.Delete(t.Context(), existing))
	other := *w
	other.Namespace, other.Name = "another", "another"
	e.State.Workloads = append(e.State.Workloads, &other)
	require.ErrorContains(t, e.checkPendingPlacement(t.Context()), "hostPort TCP/8080")
}

func TestHostPortMatchingHonorsProtocolAndHostIP(t *testing.T) {
	spec := func(ip string, protocol corev1.Protocol) *corev1.PodSpec {
		return &corev1.PodSpec{Containers: []corev1.Container{{Ports: []corev1.ContainerPort{{HostIP: ip, Protocol: protocol, HostPort: 8080}}}}}
	}
	for _, tc := range []struct {
		ip       string
		protocol corev1.Protocol
		conflict bool
	}{
		{"10.0.0.2", corev1.ProtocolTCP, true},
		{"0.0.0.0", corev1.ProtocolTCP, true},
		{"10.0.0.3", corev1.ProtocolTCP, false},
		{"::", corev1.ProtocolTCP, false},
		{"10.0.0.2", corev1.ProtocolUDP, false},
	} {
		_, conflict := conflictingHostPort(spec("10.0.0.2", corev1.ProtocolTCP), spec(tc.ip, tc.protocol))
		require.Equal(t, tc.conflict, conflict)
	}
}

func TestPlacementDoesNotConflictWithItsOwnReplacement(t *testing.T) {
	e, w := instanceFixture(t)
	w.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{HostPort: 8080}}
	require.NoError(t, e.Client.Delete(t.Context(), &w.Instance.Pod))
	replacement := w.Instance.Pod.DeepCopy()
	replacement.ResourceVersion, replacement.UID = "", "replacement"
	replacement.Spec.NodeName = e.State.Target.Name
	replacement.Spec.Containers = w.Template.Spec.Containers
	require.NoError(t, e.Client.Create(t.Context(), replacement))
	require.NoError(t, e.checkPendingPlacement(t.Context()))
}

func TestSuccessfulPlacementRefreshesHelperTaintsWithoutChangingBusinessTolerations(t *testing.T) {
	e, w := instanceFixture(t)
	for _, node := range []string{e.State.Source.Name, e.State.Target.Name} {
		var current corev1.Node
		require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: node}, &current))
		current.Spec.Taints = []corev1.Taint{{Key: "storage-" + node, Effect: corev1.TaintEffectNoSchedule}}
		require.NoError(t, e.Client.Update(t.Context(), &current))
	}
	w.Template.Spec.Tolerations = []corev1.Toleration{{Key: "storage-" + e.State.Target.Name, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}
	original := append([]corev1.Toleration(nil), w.Template.Spec.Tolerations...)
	require.NoError(t, e.checkPendingPlacement(t.Context()))
	require.Equal(t, "storage-"+e.State.Source.Name, e.State.SourceTaints[0].Key)
	require.Equal(t, "storage-"+e.State.Target.Name, e.State.TargetTaints[0].Key)
	require.Equal(t, original, w.Template.Spec.Tolerations)
	var target corev1.Node
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: e.State.Target.Name}, &target))
	target.Spec.Taints = append(target.Spec.Taints, corev1.Taint{Key: "new-blocker", Effect: corev1.TaintEffectNoSchedule})
	require.NoError(t, e.Client.Update(t.Context(), &target))
	require.ErrorContains(t, e.checkInstancePlacement(t.Context(), w), "不容忍")
	require.Len(t, e.State.TargetTaints, 1, "a failed business check must not adopt broader helper tolerations")
	w.Template.Spec.Tolerations = append(w.Template.Spec.Tolerations, corev1.Toleration{Key: "new-blocker", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule})
	require.NoError(t, e.checkInstancePlacement(t.Context(), w))
	require.Len(t, e.State.TargetTaints, 2)
}

func TestHostPortIPv6WildcardConflictsWithSpecificAddress(t *testing.T) {
	pod := func(ip string) *corev1.PodSpec {
		return &corev1.PodSpec{Containers: []corev1.Container{{Ports: []corev1.ContainerPort{{HostIP: ip, HostPort: 8080}}}}}
	}
	_, conflict := conflictingHostPort(pod("::"), pod("2001:db8::1"))
	require.True(t, conflict)
	_, conflict = conflictingHostPort(pod("2001:db8::1"), pod("::"))
	require.True(t, conflict)
	_, conflict = conflictingHostPort(pod("2001:db8::1"), pod("2001:db8::2"))
	require.False(t, conflict)
	_, conflict = conflictingHostPort(pod("2001:db8::1"), pod("2001:0db8:0:0:0:0:0:1"))
	require.True(t, conflict)
}
