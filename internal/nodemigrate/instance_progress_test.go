package nodemigrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestInstanceRequeuesStatefulSetAfterHoldRelease(t *testing.T) {
	exerciseInstanceReconcile(t, false)
}

func TestInstanceResumesReconcileFailureWithoutRecopying(t *testing.T) {
	exerciseInstanceReconcile(t, true)
}

func exerciseInstanceReconcile(t *testing.T, failNotification bool) {
	t.Helper()
	e, w := instanceFixture(t)
	ctx := t.Context()
	copies := 0
	originalCopier := e.Copier
	e.Copier = copierFunc(func(ctx context.Context, s *State, w *Workload) error {
		copies++
		return originalCopier.Copy(ctx, s, w)
	})
	var before appsv1.StatefulSet
	require.NoError(t, e.Client.Get(ctx, client.ObjectKey{Namespace: w.Namespace, Name: w.Name}, &before))
	before.Annotations = map[string]string{"operator-note": "keep"}
	require.NoError(t, e.Client.Update(ctx, &before))
	patches := 0
	e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if err := c.Delete(ctx, obj, opts...); err != nil {
				return err
			}
			// A real controller may be in backoff when the hold is removed.
			// Suppress the fixture's immediate recreation until it sees an STS update.
			if _, ok := obj.(*admissionv1.ValidatingAdmissionPolicyBinding); ok {
				var p corev1.Pod
				if err := c.Get(ctx, client.ObjectKeyFromObject(&w.Instance.Pod), &p); err == nil {
					return c.Delete(ctx, &p)
				}
			}
			return nil
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			options := &client.PatchOptions{}
			for _, opt := range opts {
				opt.ApplyToPatch(options)
			}
			if _, ok := obj.(*appsv1.StatefulSet); ok && len(options.DryRun) == 0 && failNotification {
				return errors.New("notification unavailable")
			}
			if err := c.Patch(ctx, obj, patch, opts...); err != nil {
				return err
			}
			if sts, ok := obj.(*appsv1.StatefulSet); ok && len(options.DryRun) == 0 {
				patches++
				require.Equal(t, before.Spec, sts.Spec)
				require.Equal(t, "keep", sts.Annotations["operator-note"])
				var holds admissionv1.ValidatingAdmissionPolicyBindingList
				require.NoError(t, c.List(ctx, &holds, client.MatchingLabels{labelKey: e.State.ID}))
				require.Empty(t, holds.Items)
				for _, v := range w.Volumes {
					require.Equal(t, phaseSwitched, v.Phase)
				}
				var p corev1.Pod
				if apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(&w.Instance.Pod), &p)) {
					p = *w.Instance.Pod.DeepCopy()
					p.ResourceVersion, p.UID, p.Spec.NodeName = "", "requeued-pod", e.State.Target.Name
					return c.Create(ctx, &p)
				}
			}
			return nil
		},
	})
	if failNotification {
		require.ErrorContains(t, e.Apply(ctx, true), "notification unavailable")
		require.Equal(t, phaseStarting, w.Phase)
		beforeResume := copies
		path := filepath.Join(t.TempDir(), "state.json")
		require.NoError(t, SaveState(path, e.State))
		loaded, err := LoadState(path)
		require.NoError(t, err)
		e.State, w = loaded, loaded.Workloads[0]
		failNotification = false
		require.NoError(t, e.Apply(ctx, true))
		require.Equal(t, beforeResume, copies, "switched volumes must not be copied again")
	} else {
		require.NoError(t, e.Apply(ctx, true))
	}
	require.GreaterOrEqual(t, patches, 2)
	var after appsv1.StatefulSet
	require.NoError(t, e.Client.Get(ctx, client.ObjectKeyFromObject(&before), &after))
	require.Equal(t, before.Spec, after.Spec)
	require.Equal(t, before.Annotations, after.Annotations)
	require.NoError(t, e.Verify(ctx))
}

func TestInstanceReportsSeparateRecoveryAndVolumeStages(t *testing.T) {
	e, _ := instanceFixture(t)
	var output bytes.Buffer
	e.Output = &output
	require.NoError(t, e.Apply(t.Context(), true))
	for _, stage := range []string{"source-digest", "final-sync", "target-digest", "wait-pod-created", "wait-pod-scheduled", "wait-container-started", "wait-pod-ready"} {
		require.Contains(t, output.String(), "stage="+stage)
		found := false
		for _, event := range e.State.Events {
			if event.Stage == stage {
				found = true
				require.Equal(t, "succeeded", event.Status)
			}
		}
		require.True(t, found, "missing event %s", stage)
	}
}

func TestCheckpointRefreshesRedactedReportDuringStage(t *testing.T) {
	e, w := instanceFixture(t)
	w.Instance.Pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "PASSWORD", Value: "must-not-leak"}}
	path := filepath.Join(t.TempDir(), "state.json")
	e.Save = func(s *State) error { return SaveState(path, s) }
	read := func() map[string]any {
		t.Helper()
		data, err := os.ReadFile(path + ".report.json")
		require.NoError(t, err)
		require.NotContains(t, string(data), "must-not-leak")
		require.NotContains(t, string(data), "PASSWORD")
		var report map[string]any
		require.NoError(t, json.Unmarshal(data, &report))
		return report
	}
	require.NoError(t, e.checkpoint())
	w.Phase = phasePreparing
	require.NoError(t, e.checkpoint())
	require.Equal(t, phasePreparing, read()["Workloads"].([]any)[0].(map[string]any)["Phase"])
	require.NoError(t, e.stage(t.Context(), "source-digest", "prod/db", func(context.Context) error {
		active := read()["ActiveStage"].(map[string]any)
		require.Equal(t, "source-digest", active["stage"])
		require.Equal(t, "running", active["status"])
		return nil
	}))
	require.Nil(t, read()["ActiveStage"])
	info, err := os.Stat(path + ".report.json")
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, strings.Contains(string(data), "must-not-leak"))
}

func TestReportFailurePreservesDurableCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	require.NoError(t, os.Mkdir(path+".report.json", 0o700))
	s := testState()
	require.ErrorContains(t, SaveState(path, s), "检查点已保存")
	loaded, err := LoadState(path)
	require.NoError(t, err)
	require.Equal(t, s.ID, loaded.ID)
}

func TestFailedCheckpointNeverPublishesNewerReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := testState()
	require.NoError(t, SaveState(path, s))
	before, err := os.ReadFile(path + ".report.json")
	require.NoError(t, err)
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Mkdir(path, 0o700))
	s.Phase = phaseValidated
	require.Error(t, SaveState(path, s))
	after, err := os.ReadFile(path + ".report.json")
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestRecoveryWaitsShareOneDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, w := instanceFixture(t)
		require.NoError(t, e.Apply(t.Context(), true))
		e.State.Options.WaitTimeout = 10 * time.Second
		started := time.Now()
		e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if pod, ok := obj.(*corev1.Pod); ok && pod.Name == w.Instance.Pod.Name {
					pod.Spec.NodeName = ""
					pod.Status.Conditions = nil
					pod.Status.ContainerStatuses = nil
					if time.Since(started) >= 6*time.Second {
						pod.Spec.NodeName = e.State.Target.Name
					}
					if time.Since(started) >= 12*time.Second {
						pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
					}
				}
				return nil
			},
		})
		err := e.waitTargetInstance(t.Context(), w, w.Instance.Pod.Name, w.UID)
		require.ErrorContains(t, err, "目标容器启动")
		require.Equal(t, 10*time.Second, time.Since(started))
	})
}

func TestStageDoesNotRunWhenStartCheckpointFails(t *testing.T) {
	e, _ := instanceFixture(t)
	e.Save = func(*State) error { return errors.New("checkpoint unavailable") }
	ran := false
	err := e.stage(t.Context(), "statefulset-reconcile", "prod/db", func(context.Context) error {
		ran = true
		return nil
	})
	require.ErrorContains(t, err, "checkpoint unavailable")
	require.False(t, ran)
	require.Nil(t, e.State.ActiveStage)
	require.Equal(t, "failed", e.State.Events[0].Status)
}

func TestResumeDoesNotRestoreStaleActiveStage(t *testing.T) {
	e, _ := instanceFixture(t)
	e.State.ActiveStage = &StageEvent{Stage: "old-attempt", Status: "running"}
	require.NoError(t, e.Apply(t.Context(), true))
	require.Nil(t, e.State.ActiveStage)
}
