package nodemigrate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestCopyRuntimeUsesOnlySuccessfulOwnedContainer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(*corev1.Pod)
		known  bool
	}{
		{"success", func(*corev1.Pod) {}, true},
		{"foreign-job", func(p *corev1.Pod) { p.OwnerReferences[0].UID = "foreign" }, false},
		{"failed", func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.ExitCode = 1 }, false},
		{"missing-time", func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.StartedAt = metav1.Time{} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, w, _ := fixture(t)
			job := copyJob(w, w.Volume.CopyID)
			start := time.Now().UTC().Truncate(time.Second)
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "rsync", Namespace: w.Namespace,
					Labels:          map[string]string{"job-name": job.Name},
					OwnerReferences: []metav1.OwnerReference{{Name: job.Name, Kind: "Job", UID: job.UID, Controller: ptr.To(true)}}},
				Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "rsync"}}},
				Status: corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{Name: "rsync", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, StartedAt: metav1.NewTime(start), FinishedAt: metav1.NewTime(start.Add(2 * time.Second))}}}}},
			}
			tc.modify(pod)
			require.NoError(t, e.Client.Create(t.Context(), pod))
			millis, err := (&processCopier{engine: e}).copyRuntime(t.Context(), job)
			require.NoError(t, err)
			if tc.known {
				require.Equal(t, int64(2000), *millis)
			} else {
				require.Nil(t, millis)
			}
		})
	}
}
