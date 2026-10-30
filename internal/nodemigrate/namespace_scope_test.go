package nodemigrate

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestNamespaceAllowlistTakesPrecedenceOverBlocklist(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options Options
		ns      string
		want    bool
	}{
		{"default-business", Options{}, "prod", true},
		{"default-system", Options{}, "kube-system", false},
		{"custom-exclusion", Options{ExcludeNamespaces: []string{"monitoring"}}, "monitoring", false},
		{"default-system-with-custom-exclusion", Options{ExcludeNamespaces: []string{"monitoring"}}, "kube-system", false},
		{"allowlist-match", Options{Namespaces: []string{"prod", "pre"}}, "pre", true},
		{"allowlist-other", Options{Namespaces: []string{"prod"}}, "pre", false},
		{"allowlist-overrides-exclusion", Options{Namespaces: []string{"monitoring"}, ExcludeNamespaces: []string{"monitoring"}}, "monitoring", true},
		{"explicit-system-allowlist", Options{Namespaces: []string{"kube-system"}, ExcludeNamespaces: []string{"kube-system"}}, "kube-system", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, selectedNamespace(tc.options, tc.ns))
		})
	}
}

func addScopeDeployment(t *testing.T, e *Engine, ns, name string) {
	t.Helper()
	template := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "db:v1"}}}}
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(name + "-deployment")}, Spec: appsv1.DeploymentSpec{Replicas: pointer(int32(1)), Template: template}}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: name + "-rs", Namespace: ns, UID: types.UID(name + "-rs"), OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: kindDeployment, Name: d.Name, UID: d.UID, Controller: pointer(true)}}}, Spec: appsv1.ReplicaSetSpec{Replicas: pointer(int32(1)), Template: template}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name + "-pod", Namespace: ns, UID: types.UID(name + "-pod"), Labels: template.Labels, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: pointer(true)}}}, Spec: *template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	pod.Spec.NodeName = e.State.Source.Name
	for _, obj := range []client.Object{&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: ns}}, d, rs, pod} {
		require.NoError(t, e.Client.Create(t.Context(), obj))
	}
}

func TestPlanResolvesDefaultNamespaceScopeAndPreservesSnapshot(t *testing.T) {
	e, _ := instanceFixture(t)
	addScopeDeployment(t, e, "pre", "infra")
	addScopeDeployment(t, e, "monitoring", "monitor")
	addScopeDeployment(t, e, "kube-system", "system")
	for _, pod := range []*corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "static-pod", Namespace: "prod", Annotations: map[string]string{corev1.MirrorPodAnnotationKey: "mirror"}}, Spec: corev1.PodSpec{NodeName: e.State.Source.Name}},
		{ObjectMeta: metav1.ObjectMeta{Name: "daemon-pod", Namespace: "pre", OwnerReferences: []metav1.OwnerReference{{Kind: kindDaemonSet, UID: "daemon", Controller: pointer(true)}}}, Spec: corev1.PodSpec{NodeName: e.State.Source.Name}},
		{ObjectMeta: metav1.ObjectMeta{Name: "excluded-ownerless", Namespace: "monitoring"}, Spec: corev1.PodSpec{NodeName: e.State.Source.Name}},
	} {
		require.NoError(t, e.Client.Create(t.Context(), pod))
	}
	o := e.State.Options
	o.Namespaces = nil
	o.ExcludeNamespaces = []string{"monitoring"}
	s, err := e.Plan(t.Context(), o)
	require.NoError(t, err)
	require.Equal(t, []string{"pre", "prod"}, s.Options.Namespaces)
	require.Equal(t, []string{"kube-system", "monitoring"}, s.Options.ExcludeNamespaces)
	require.Len(t, s.Workloads, 2)
	count := s.Capacity.Incoming[corev1.ResourcePods]
	require.Equal(t, int64(2), count.Value())
	for _, w := range s.Workloads {
		require.NotEqual(t, "monitoring", w.Namespace)
		require.NotEqual(t, "kube-system", w.Namespace)
	}
	// Resuming uses this namespace snapshot, not all future namespaces.
	require.False(t, selectedNamespace(s.Options, "new-business"))
	require.NoError(t, validateState(s))
}

func TestExplicitNamespaceScopeOverridesCustomExclusion(t *testing.T) {
	e, _ := instanceFixture(t)
	addScopeDeployment(t, e, "monitoring", "monitor")
	o := e.State.Options
	o.Namespaces = []string{"monitoring"}
	o.ExcludeNamespaces = []string{"monitoring"}
	s, err := e.Plan(t.Context(), o)
	require.NoError(t, err)
	require.Len(t, s.Workloads, 1)
	require.Equal(t, "monitoring", s.Workloads[0].Namespace)
}
