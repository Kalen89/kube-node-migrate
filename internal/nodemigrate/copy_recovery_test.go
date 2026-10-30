package nodemigrate

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestApplyResumesFinalSyncAfterRepeatedDigestMismatch(t *testing.T) {
	e, w := instanceFixture(t)
	baseHosts := e.Hosts
	copies, sourceScans := 0, 0
	e.Hosts = hostFunc(func(ctx context.Context, host Host, command string) (string, error) {
		if strings.Contains(command, "--mode 'digest'") {
			if host.Name == e.State.Source.Name {
				sourceScans++
			}
			if host.Name == e.State.Target.Name && copies < 3 {
				return `{"version":2,"sha256":"` + strings.Repeat("b", 64) + `","entries":2,"bytes":8}`, nil
			}
		}
		return baseHosts.Run(ctx, host, command)
	})
	e.Copier = copierFunc(func(_ context.Context, _ *State, view *Workload) error {
		if view.Volume == w.Volumes[0] && view.Volume.Phase == phaseCopying {
			copies++
		}
		return nil
	})
	var checkpoint []byte
	e.Save = func(s *State) error {
		var err error
		checkpoint, err = json.Marshal(s)
		return err
	}
	for attempt := 1; attempt <= 2; attempt++ {
		require.ErrorContains(t, e.Apply(t.Context(), true), "摘要不一致")
		require.Equal(t, attempt, copies)
		require.Equal(t, attempt, sourceScans)
		require.True(t, w.Volumes[0].FinalSyncRetry)
		require.Equal(t, phaseStopped, w.Phase)
		require.Empty(t, w.Volumes[0].NewPVCUID)
		require.NoError(t, validateCopyRecoveryState(volumeView(w, w.Volumes[0])))
		// Resume the serialized checkpoint, including the failed validation intent.
		var resumed State
		require.NoError(t, json.Unmarshal(checkpoint, &resumed))
		e.State, w = &resumed, resumed.Workloads[0]
	}
	require.NoError(t, e.Apply(t.Context(), true))
	require.Equal(t, 3, copies)
	require.Equal(t, 4, sourceScans) // First PVC rescanned each retry, then the second PVC.
	require.False(t, w.Volumes[0].FinalSyncRetry)
	require.NoError(t, e.Verify(t.Context()))
}

func TestFinalSyncRetryRefusesTargetWritesAndConsumers(t *testing.T) {
	for _, change := range []string{"replacement", "sibling-rebound", "consumer", "source-running"} {
		t.Run(change, func(t *testing.T) {
			e, w := instanceFixture(t)
			e.State.Options.Precopy = false
			require.NoError(t, e.ensureHold(t.Context(), w))
			require.NoError(t, e.Client.Delete(t.Context(), &w.Instance.Pod))
			w.Phase = phaseStopped
			v := w.Volumes[0]
			v.Phase, v.CopySucceeded, v.FinalSyncRetry = phaseCopied, true, true
			v.SourceDigest = &Digest{SHA256: strings.Repeat("a", 64), Entries: 2, Bytes: 8}
			v.TargetDigest = &Digest{SHA256: strings.Repeat("b", 64), Entries: 2, Bytes: 8}
			switch change {
			case "replacement":
				w.Instance.ReplacementUID = "already-started"
			case "sibling-rebound":
				w.Volumes[1].NewPVCUID = "already-rebound"
			case "consumer":
				require.NoError(t, e.Client.Create(t.Context(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "unexpected", Namespace: w.Namespace}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "target", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: v.TemporaryPVC}}}}}}))
			case "source-running":
				baseHosts := e.Hosts
				e.Hosts = hostFunc(func(ctx context.Context, host Host, command string) (string, error) {
					if strings.HasPrefix(command, runtimeCLI) {
						return `{"containers":[{"labels":{"io.kubernetes.pod.uid":"` + string(w.Instance.Pod.UID) + `"}}]}`, nil
					}
					return baseHosts.Run(ctx, host, command)
				})
			}
			calls := 0
			e.Copier = copierFunc(func(context.Context, *State, *Workload) error { calls++; return nil })
			require.Error(t, e.copyVolume(t.Context(), volumeView(w, v)))
			require.Zero(t, calls)
			require.True(t, v.FinalSyncRetry)
		})
	}
}

func TestCopyResumesPersistedCleanupAfterJobDeletion(t *testing.T) {
	for _, failure := range []string{"api-error", "resource-version-conflict"} {
		t.Run(failure, func(t *testing.T) {
			e, w, _ := fixture(t)
			enableTestPrecopy(e)
			w.Volume.Phase = phasePrecopying
			job := copyJob(w, w.Volume.CopyID)
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
			require.NoError(t, e.Client.Create(t.Context(), job))
			release := copyRelease(w)
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "sh.helm.release.v1." + release + ".v1", Namespace: w.Namespace, UID: "own-release", Labels: map[string]string{"owner": "helm", "name": release}}, Type: "helm.sh/release.v1"}
			require.NoError(t, e.Client.Create(t.Context(), secret))
			binary := filepath.Join(t.TempDir(), "copy-tool")
			require.NoError(t, os.WriteFile(binary, []byte("#!/bin/sh\nexit 17\n"), 0o700))
			e.State.Options.CopyBinary = binary
			e.State.Options.StatePath = filepath.Join(t.TempDir(), "state.json")
			var checkpoint []byte
			e.Save = func(s *State) error {
				var err error
				checkpoint, err = json.Marshal(s)
				return err
			}
			blocked := true
			e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*corev1.Secret); ok && blocked {
					if failure == "resource-version-conflict" {
						var updated corev1.Secret
						require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), &updated))
						updated.Annotations = map[string]string{"controller-update": "changed"}
						require.NoError(t, c.Update(ctx, &updated))
						return apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, obj.GetName(), errors.New("controller changed RV"))
					}
					return errors.New("cleanup temporarily unavailable")
				}
				return c.Delete(ctx, obj, opts...)
			}})
			c := &processCopier{engine: e}
			require.Error(t, c.Copy(t.Context(), e.State, w))
			require.True(t, apierrors.IsNotFound(e.Client.Get(t.Context(), client.ObjectKeyFromObject(job), &batchv1.Job{})))
			require.NotNil(t, w.Volume.CopyCleanup)
			require.NoError(t, validateCopyRecoveryState(w))
			var resumed State
			require.NoError(t, json.Unmarshal(checkpoint, &resumed))
			e.State, w = &resumed, resumed.Workloads[0]
			blocked = false
			// The actual Copy function must finish cleanup and reach a fresh tool invocation.
			require.ErrorContains(t, c.Copy(t.Context(), e.State, w), "exit status 17")
			require.Nil(t, w.Volume.CopyCleanup)
			require.True(t, apierrors.IsNotFound(e.Client.Get(t.Context(), client.ObjectKeyFromObject(secret), &corev1.Secret{})))
		})
	}
}

func TestCopyCleanupRefusesReplacementAndOwnershipDrift(t *testing.T) {
	for _, change := range []string{"uid", "ownership"} {
		t.Run(change, func(t *testing.T) {
			e, w, _ := fixture(t)
			w.Volume.Phase = phaseCopying
			job := copyJob(w, w.Volume.CopyID)
			require.NoError(t, e.Client.Create(t.Context(), job))
			w.Volume.CopyJobName, w.Volume.CopyJobUID = job.Name, job.UID
			// Persist the cleanup intent, then interrupt before the first deletion.
			e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
				return errors.New("interrupted")
			}})
			c := &processCopier{engine: e}
			require.ErrorContains(t, c.cleanup(t.Context(), e.State, w), "interrupted")
			var changed batchv1.Job
			require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(job), &changed))
			if change == "uid" {
				changed.UID = "replacement"
			} else {
				changed.Annotations["meta.helm.sh/release-name"] = "foreign"
			}
			require.NoError(t, e.Client.Update(t.Context(), &changed))
			require.Error(t, c.Copy(t.Context(), e.State, w))
			require.NotNil(t, w.Volume.CopyCleanup)
			require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(job), &batchv1.Job{}))
		})
	}
}

func TestCopyNeverCleansUnknownReleaseWithoutIntent(t *testing.T) {
	e, w, _ := fixture(t)
	enableTestPrecopy(e)
	w.Volume.Phase = phasePrecopying
	release := copyRelease(w)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "sh.helm.release.v1." + release + ".v1", Namespace: w.Namespace, UID: "unknown-release", Labels: map[string]string{"owner": "helm", "name": release}}, Type: "helm.sh/release.v1"}
	require.NoError(t, e.Client.Create(t.Context(), secret))
	c := &processCopier{engine: e}
	require.ErrorContains(t, c.Copy(t.Context(), e.State, w), "没有可接续的 Job")
	require.Nil(t, w.Volume.CopyCleanup)
	require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(secret), &corev1.Secret{}))
}
