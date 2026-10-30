package nodemigrate

import (
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func copyJob(w *Workload, id string) *batchv1.Job {
	release := "pv-migrate-" + id + "-clusterip"
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: release + "-rsync", Namespace: w.Namespace, UID: types.UID(id + "-uid"), Labels: map[string]string{"app.kubernetes.io/instance": release, "app.kubernetes.io/managed-by": "Helm"}, Annotations: map[string]string{"meta.helm.sh/release-name": release, "meta.helm.sh/release-namespace": w.Namespace}}}
}

func TestCopyCleanupUsesExactReleaseAndKeepsOtherOperation(t *testing.T) {
	e, w, _ := fixture(t)
	own, other := copyJob(w, w.Volume.CopyID), copyJob(w, w.Volume.CopyID+"-other")
	require.NoError(t, e.Client.Create(t.Context(), own))
	require.NoError(t, e.Client.Create(t.Context(), other))
	w.Volume.CopyJobName, w.Volume.CopyJobUID = own.Name, own.UID
	c := &processCopier{engine: e}
	require.NoError(t, c.cleanup(t.Context(), e.State, w))
	var got batchv1.Job
	require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(other), &got))
	require.Equal(t, other.UID, got.UID)
}

func TestCopyCleanupRejectsReplacedJob(t *testing.T) {
	e, w, _ := fixture(t)
	job := copyJob(w, w.Volume.CopyID)
	require.NoError(t, e.Client.Create(t.Context(), job))
	w.Volume.CopyJobName, w.Volume.CopyJobUID = job.Name, "original-uid"
	c := &processCopier{engine: e}
	require.ErrorContains(t, c.cleanup(t.Context(), e.State, w), "UID")
	var got batchv1.Job
	require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(job), &got))
}

func TestOccupiedCopyReleaseIsNeverOverwritten(t *testing.T) {
	e, w, _ := fixture(t)
	release := copyRelease(w)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "sh.helm.release.v1." + release + ".v1", Namespace: w.Namespace, UID: "existing", Labels: map[string]string{"owner": "helm", "name": release}}, Type: "helm.sh/release.v1"}
	require.NoError(t, e.Client.Create(t.Context(), secret))
	c := &processCopier{engine: e}
	require.ErrorContains(t, c.requireAbsentRelease(t.Context(), w), "拒绝覆盖")
	var got corev1.Secret
	require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(secret), &got))
}
