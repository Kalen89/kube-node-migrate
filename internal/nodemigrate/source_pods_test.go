package nodemigrate

import (
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"testing"
)

func TestMigrationIgnoresStaticAndDaemonSetSystemPods(t *testing.T) {
	e, _ := instanceFixture(t)
	for _, name := range []string{"static-control", "system-daemon"} {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", UID: types.UID(name)}, Spec: corev1.PodSpec{NodeName: e.State.Source.Name}}
		if name == "static-control" {
			p.Annotations = map[string]string{corev1.MirrorPodAnnotationKey: "mirror-hash"}
		} else {
			p.OwnerReferences = []metav1.OwnerReference{{Kind: kindDaemonSet, Name: "system-daemon", UID: "daemon-uid", Controller: pointer(true)}}
		}
		require.NoError(t, e.Client.Create(t.Context(), p))
	}
	s, err := e.Plan(t.Context(), e.State.Options)
	require.NoError(t, err)
	require.Len(t, s.Workloads, 1)
}
