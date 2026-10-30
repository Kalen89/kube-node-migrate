package nodemigrate

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func enableTestPrecopy(e *Engine) {
	e.State.Options.Precopy = true
	e.State.Options.PrecopyRounds = 1
	e.State.Options.PrecopyThreshold = 0
}

func TestPrecopyRunsBeforeStopAndAlwaysRepeatsCopyAfterStop(t *testing.T) {
	e, w, _ := fixture(t)
	simulateControllers(e, w)
	enableTestPrecopy(e)
	var phases []string
	e.Copier = copierFunc(func(ctx context.Context, _ *State, current *Workload) error {
		phases = append(phases, current.Volume.Phase)
		var sts appsv1.StatefulSet
		require.NoError(t, e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &sts))
		if current.Volume.Phase == phasePrecopying {
			require.Equal(t, int32(1), *sts.Spec.Replicas)
			require.Nil(t, current.Volume.SourceDigest)
			require.Nil(t, current.Volume.TargetDigest)
			current.Volume.CopySucceeded = true // An online success must not skip the offline copy.
			current.Volume.CopyJobName, current.Volume.CopyJobUID = "online-job", "online-job-uid"
		} else {
			require.Zero(t, *sts.Spec.Replicas)
			require.NotNil(t, current.Volume.SourceDigest)
			require.False(t, current.Volume.CopySucceeded)
			require.Empty(t, current.Volume.CopyJobUID)
		}
		return nil
	})
	require.NoError(t, applyLegacyComponents(e, t.Context(), true))
	require.Equal(t, []string{phasePrecopying, phaseCopying}, phases)
	require.Equal(t, phaseStarted, w.Volume.Phase)
	require.True(t, w.Volume.Precopy.Ready)
	require.Len(t, w.Volume.Precopy.Rounds, 1)
	require.NoError(t, validateState(e.State))
}

func TestPrecopyFailureKeepsOriginalPodRunningAndCanResume(t *testing.T) {
	e, w, _ := fixture(t)
	simulateControllers(e, w)
	enableTestPrecopy(e)
	e.Copier = copierFunc(func(context.Context, *State, *Workload) error { return errors.New("online network failure") })
	require.ErrorContains(t, applyLegacyComponents(e, t.Context(), true), "online network failure")
	var pod corev1.Pod
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Name + "-0"}, &pod))
	require.Equal(t, "old", pod.Spec.NodeName)
	require.Equal(t, w.OriginalPods[0], pod.UID)
	require.Nil(t, w.Volume.SourceDigest)
	require.NoError(t, validateState(e.State))
	var phases []string
	e.Copier = copierFunc(func(_ context.Context, _ *State, current *Workload) error {
		phases = append(phases, current.Volume.Phase)
		return nil
	})
	require.NoError(t, applyLegacyComponents(e, t.Context(), true))
	require.Equal(t, []string{phasePrecopying, phaseCopying}, phases)
}

func TestNonConvergentPrecopyDoesNotStopServiceAndResumeGetsAnotherBudget(t *testing.T) {
	e, w, _ := fixture(t)
	enableTestPrecopy(e)
	e.State.Options.PrecopyThreshold = time.Second
	e.State.Options.PrecopyRounds = 2
	e.Copier = copierFunc(func(_ context.Context, _ *State, current *Workload) error {
		current.Volume.Precopy.RoundStartedAt = time.Now().UTC().Add(-time.Minute)
		millis := int64(60000)
		current.Volume.CopyRuntimeMillis = &millis
		return nil
	})
	require.ErrorContains(t, e.precopyVolume(t.Context(), w), "仍未达到")
	require.Equal(t, phasePrecopying, w.Volume.Phase)
	require.Len(t, w.Volume.Precopy.Rounds, 2)
	var sts appsv1.StatefulSet
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &sts))
	require.Equal(t, int32(1), *sts.Spec.Replicas)
	require.NoError(t, validateState(e.State))
	e.Copier = copierFunc(func(_ context.Context, _ *State, current *Workload) error {
		millis := int64(0)
		current.Volume.CopyRuntimeMillis = &millis
		return nil
	})
	require.NoError(t, e.precopyVolume(t.Context(), w))
	require.Equal(t, phasePrecopyReady, w.Volume.Phase)
	require.Len(t, w.Volume.Precopy.Rounds, 3)
	require.NoError(t, validateState(e.State))
	// A saved ready estimate becomes stale while the source keeps writing.
	require.NoError(t, e.precopyVolume(t.Context(), w))
	require.Len(t, w.Volume.Precopy.Rounds, 4)
	require.NoError(t, validateState(e.State))
}

func TestPrecopyRejectsReplacedSourcePodAndForeignDestination(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, *Engine, *Workload)
		want   string
	}{
		{"source-replaced", func(t *testing.T, e *Engine, w *Workload) {
			var p corev1.Pod
			require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: "db-0"}, &p))
			p.UID = "replacement-uid"
			require.NoError(t, e.Client.Update(t.Context(), &p))
		}, "原始源 Pod"},
		{"foreign-destination", func(t *testing.T, e *Engine, w *Workload) {
			var pvc corev1.PersistentVolumeClaim
			require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Volume.TemporaryPVC}, &pvc))
			pvc.Labels[labelKey] = "another-migration"
			require.NoError(t, e.Client.Update(t.Context(), &pvc))
		}, "不属于本次迁移"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, w, _ := fixture(t)
			enableTestPrecopy(e)
			tc.change(t, e, w)
			called := false
			e.Copier = copierFunc(func(context.Context, *State, *Workload) error { called = true; return nil })
			require.ErrorContains(t, e.precopyVolume(t.Context(), w), tc.want)
			require.False(t, called)
		})
	}
}

func TestPrecopyFinishedJobAllowsOnlyOriginalSourceConsumer(t *testing.T) {
	e, w, _ := fixture(t)
	enableTestPrecopy(e)
	w.Volume.Phase = phasePrecopying
	w.Volume.Precopy = &PrecopyProgress{RoundStartedAt: time.Now().UTC()}
	job := copyJob(w, w.Volume.CopyID)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	require.NoError(t, e.Client.Create(t.Context(), job))
	c := &processCopier{engine: e}
	require.NoError(t, c.Copy(t.Context(), e.State, w))
	require.True(t, w.Volume.CopySucceeded)
	var original corev1.Pod
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Name + "-0"}, &original))
	ready, err := e.copyMountsReleased(t.Context(), w)
	require.NoError(t, err)
	require.True(t, ready)
	extra := businessPod(w, "new")
	extra.Name, extra.UID = "unexpected-target", "unexpected-target-uid"
	extra.OwnerReferences = nil
	extra.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: w.Volume.TemporaryPVC}}}}
	require.NoError(t, e.Client.Create(t.Context(), extra))
	ready, err = e.copyMountsReleased(t.Context(), w)
	require.NoError(t, err)
	require.False(t, ready)
}

func TestPrecopyCannotSkipConvergenceOrFinalDigest(t *testing.T) {
	e, w, _ := fixture(t)
	enableTestPrecopy(e)
	require.ErrorContains(t, e.stopVolume(t.Context(), w), "禁止停止")
	simulateControllers(e, w)
	wanted := e.Hosts
	e.Hosts = hostFunc(func(ctx context.Context, host Host, command string) (string, error) {
		result, err := wanted.Run(ctx, host, command)
		if host.Name == "new" && strings.Contains(command, "'digest'") {
			return strings.ReplaceAll(result, strings.Repeat("a", 64), strings.Repeat("b", 64)), err
		}
		return result, err
	})
	require.ErrorContains(t, applyLegacyComponents(e, t.Context(), true), "摘要不一致")
	var original corev1.PersistentVolumeClaim
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Volume.PVC.Name}, &original))
	require.Equal(t, w.Volume.PVC.UID, original.UID)
	var sts appsv1.StatefulSet
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &sts))
	require.Zero(t, *sts.Spec.Replicas)
}

func TestCopyArgumentsSeparateOnlineAndFinalCopy(t *testing.T) {
	for _, tc := range []struct {
		name, phase     string
		online, precopy bool
	}{
		{"ordinary", phaseCopying, false, false}, {"online", phasePrecopying, true, true}, {"final", phaseCopying, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, w, _ := fixture(t)
			e.State.Options.Precopy = tc.precopy
			e.State.Options.RsyncBandwidthLimit = 51200
			w.Volume.Phase = tc.phase
			args := copyArgs(e.State, w, "private-values.json")
			require.Equal(t, tc.online, slices.Contains(args, "--ignore-mounted"))
			require.Equal(t, tc.precopy, slices.Contains(args, "--dest-delete-extraneous-files"))
			require.NotContains(t, args, "--source-mount-read-write")
			index := -1
			for i, value := range args {
				if value == "--rsync-extra-args" {
					index = i
					break
				}
			}
			require.GreaterOrEqual(t, index, 0)
			require.Contains(t, args[index+1], "--numeric-ids --bwlimit=51200")
			require.Equal(t, tc.precopy && !tc.online, strings.Contains(args[index+1], "--checksum"))
		})
	}
}

func TestPrecopyStateRejectsUnprovenConvergence(t *testing.T) {
	e, w, _ := fixture(t)
	enableTestPrecopy(e)
	e.State.Options.PrecopyThreshold = time.Minute
	w.Volume.Phase = phasePrecopyReady
	w.Volume.Precopy = &PrecopyProgress{Converged: true}
	require.Error(t, validateState(e.State))
	start := time.Now().UTC().Add(-2 * time.Minute)
	w.Volume.Precopy.Rounds = []PrecopyRound{{StartedAt: start, FinishedAt: start.Add(2 * time.Minute), ElapsedMillis: 120000}}
	require.ErrorContains(t, validateState(e.State), "超出切换条件")
	w.Volume.Precopy.Rounds[0].FinishedAt = start.Add(time.Second)
	w.Volume.Precopy.Rounds[0].ElapsedMillis = 1000
	require.NoError(t, validateState(e.State))
	e.State.Options.Precopy = false
	require.Error(t, validateState(e.State))
}

func TestPrecopyOptionsRejectInvalidRoundBudgetAndThreshold(t *testing.T) {
	for _, tc := range []struct {
		name      string
		rounds    int
		threshold time.Duration
		valid     bool
	}{
		{"valid", 3, 30 * time.Second, true}, {"zero-rounds", 0, time.Second, false},
		{"excess-rounds", 21, time.Second, false}, {"no-threshold", 3, 0, true}, {"negative-threshold", 3, -time.Second, false},
		{"excess-threshold", 3, 2 * time.Hour, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := testState().Options
			o.Precopy, o.PrecopyRounds, o.PrecopyThreshold = true, tc.rounds, tc.threshold
			err := ValidateOptions(o)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "precopy")
			}
		})
	}
}

func TestPrecopyDefaultCompletesRoundsDespiteLongDuration(t *testing.T) {
	e, w, _ := fixture(t)
	simulateControllers(e, w)
	enableTestPrecopy(e)
	e.State.Options.PrecopyRounds = 3
	var phases []string
	e.Copier = copierFunc(func(_ context.Context, _ *State, current *Workload) error {
		phases = append(phases, current.Volume.Phase)
		if current.Volume.Phase == phasePrecopying {
			// 模拟持续写入的卷：每轮成功，但耗时一直超过旧默认阈值。
			current.Volume.Precopy.RoundStartedAt = time.Now().UTC().Add(-time.Minute)
		}
		return nil
	})
	require.NoError(t, applyLegacyComponents(e, t.Context(), true))
	require.Equal(t, []string{phasePrecopying, phasePrecopying, phasePrecopying, phaseCopying}, phases)
	require.True(t, w.Volume.Precopy.Ready)
	require.False(t, w.Volume.Precopy.Converged)
	require.Equal(t, w.Volume.SourceDigest, w.Volume.TargetDigest)
	require.NoError(t, validateState(e.State))
}

func TestPrecopyThresholdUsesSyncRuntimeAndRejectsUnknownRuntime(t *testing.T) {
	for _, tc := range []struct {
		name    string
		runtime *int64
		ready   bool
	}{
		{"short-sync-long-overhead", int64Pointer(1000), true},
		{"long-sync", int64Pointer(60000), false},
		{"unknown-sync", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, w, _ := fixture(t)
			enableTestPrecopy(e)
			e.State.Options.PrecopyThreshold = 5 * time.Second
			e.Copier = copierFunc(func(_ context.Context, _ *State, current *Workload) error {
				current.Volume.Precopy.RoundStartedAt = time.Now().UTC().Add(-time.Minute)
				current.Volume.CopyRuntimeMillis = tc.runtime
				return nil
			})
			err := e.precopyVolume(t.Context(), w)
			if tc.ready {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "显式阈值")
			}
			require.Equal(t, tc.ready, w.Volume.Precopy.Ready)
			require.NoError(t, validateState(e.State))
			var sts appsv1.StatefulSet
			require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &sts))
			require.Equal(t, int32(1), *sts.Spec.Replicas)
		})
	}
}

func int64Pointer(n int64) *int64 { return &n }

func TestPrecopyReadyStillChecksSourceBeforeStop(t *testing.T) {
	e, w, _ := fixture(t)
	enableTestPrecopy(e)
	e.Copier = copierFunc(func(context.Context, *State, *Workload) error { return nil })
	require.NoError(t, e.precopyVolume(t.Context(), w))
	var pod corev1.Pod
	key := types.NamespacedName{Namespace: w.Namespace, Name: w.Name + "-0"}
	require.NoError(t, e.Client.Get(t.Context(), key, &pod))
	pod.UID = "replacement-after-precopy"
	require.NoError(t, e.Client.Update(t.Context(), &pod))
	require.ErrorContains(t, e.stopVolume(t.Context(), w), "原始源 Pod")
	var sts appsv1.StatefulSet
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &sts))
	require.Equal(t, int32(1), *sts.Spec.Replicas)
}
