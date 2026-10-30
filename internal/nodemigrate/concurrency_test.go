package nodemigrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func addTestVolume(t *testing.T, e *Engine, first *Workload, index int) *Workload {
	t.Helper()
	w, err := cloneWorkload(first)
	require.NoError(t, err)
	w.Name = fmt.Sprintf("db%d", index)
	w.UID = types.UID(w.Name + "-uid")
	w.OriginalPods = []types.UID{types.UID(w.Name + "-0-uid")}
	v := w.Volume
	v.PVC.Name, v.PVC.UID = "data-"+w.Name+"-0", types.UID("source-"+w.Name)
	v.PVC.Spec.VolumeName = "old-pv-" + w.Name
	v.CopyID = fmt.Sprintf("km-%s-%d", e.State.ID, index)
	v.TemporaryPVC, v.TemporaryUID = strings.TrimPrefix(v.CopyID, "km-"), types.UID("temporary-"+w.Name)
	v.TemporaryPVC = "km-migrate-" + v.TemporaryPVC
	v.PV = *testPV(v.PVC.Spec.VolumeName, "old", v.PVC.Name, v.PVC.UID)
	v.TargetPV = testPV("new-pv-"+w.Name, "new", v.TemporaryPVC, v.TemporaryUID)
	w.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = v.PVC.Name
	temporary := v.PVC.DeepCopy()
	temporary.Name, temporary.UID, temporary.Spec.VolumeName = v.TemporaryPVC, v.TemporaryUID, v.TargetPV.Name
	temporary.Labels = map[string]string{labelKey: e.State.ID}
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: w.Name, Namespace: w.Namespace, UID: w.UID, Generation: 1}, Spec: appsv1.StatefulSetSpec{Replicas: pointer(int32(1)), Template: w.Template}}
	for _, obj := range []client.Object{&v.PVC, &v.PV, v.TargetPV, temporary, sts, businessPod(w, "old")} {
		require.NoError(t, e.Client.Create(t.Context(), obj))
	}
	e.State.Workloads = append(e.State.Workloads, w)
	return w
}

func TestParallelMigrationPersistsPeersAndResumesFailedVolume(t *testing.T) {
	e, first, _ := fixture(t)
	second := addTestVolume(t, e, first, 1)
	third := addTestVolume(t, e, first, 2)
	simulateControllers(e, first, second, third)
	e.State.Options.PVConcurrency = 2
	path := filepath.Join(t.TempDir(), "state.json")
	e.State.Options.StatePath = path
	e.Save = func(s *State) error { return SaveState(path, s) }
	var output bytes.Buffer
	e.Output = &output // -race also checks concurrent progress and subprocess writers.
	entered := make(chan string, 3)
	release := make(chan struct{})
	e.Copier = copierFunc(func(ctx context.Context, _ *State, w *Workload) error {
		entered <- w.Name
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
		}
		if w.Name == first.Name {
			return errors.New("simulated copy failure")
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- applyLegacyComponents(e, ctx, true) }()
	names := []string{}
	for range 2 {
		select {
		case name := <-entered:
			names = append(names, name)
		case err := <-result:
			t.Fatalf("copy stopped before both workers entered: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	require.ElementsMatch(t, []string{first.Name, second.Name}, names)
	close(release)
	require.ErrorContains(t, <-result, "simulated copy failure")
	select {
	case extra := <-entered:
		t.Fatalf("later batch started after a failure: %s", extra)
	default:
	}
	loaded, err := LoadState(path)
	require.NoError(t, err)
	require.Equal(t, phaseCopying, loaded.Workloads[0].Volume.Phase)
	require.Equal(t, phaseStarted, loaded.Workloads[1].Volume.Phase)
	require.Equal(t, phasePrepared, loaded.Workloads[2].Volume.Phase)
	require.NotEmpty(t, loaded.Events)
	var original corev1.PersistentVolumeClaim
	require.NoError(t, e.Client.Get(ctx, types.NamespacedName{Namespace: first.Namespace, Name: first.Volume.PVC.Name}, &original))
	require.Equal(t, first.Volume.PVC.UID, original.UID)
	// Use the on-disk state, as a fresh resume process would. The successful peer
	// must not be copied or stopped a second time.
	e.State = loaded
	var copied atomic.Int32
	e.Copier = copierFunc(func(_ context.Context, _ *State, w *Workload) error {
		if w.Name == second.Name {
			return errors.New("completed volume was copied again")
		}
		copied.Add(1)
		return nil
	})
	require.NoError(t, applyLegacyComponents(e, ctx, true))
	require.Equal(t, int32(2), copied.Load())
	require.Equal(t, phaseValidated, e.State.Phase)
	for _, w := range e.State.Workloads {
		require.Equal(t, phaseStarted, w.Volume.Phase)
		require.Equal(t, w.Volume.SourceDigest, w.Volume.TargetDigest)
	}
	for _, stage := range []string{"stop-writes", "copy-and-verify", "switch-pvc", "start-workload"} {
		for _, w := range e.State.Workloads {
			found := false
			for _, event := range e.State.Events {
				found = found || (event.Stage == stage && event.Resource == workloadKey(w) && event.Status == "succeeded")
			}
			require.True(t, found, "missing %s event for %s", stage, w.Name)
		}
	}
}

func TestParallelCheckpointFailureStopsAllWorkersBeforeNextAction(t *testing.T) {
	e, first, _ := fixture(t)
	second := addTestVolume(t, e, first, 1)
	var saves, actions atomic.Int32
	e.Save = func(*State) error { saves.Add(1); return errors.New("checkpoint disk full") }
	err := e.runVolumeBatch(t.Context(), []*Workload{first, second}, func(_ context.Context, worker *Engine, w *Workload) error {
		w.Volume.Phase = phaseStopping
		if err := worker.checkpoint(); err != nil {
			return err
		}
		actions.Add(1)
		return nil
	})
	require.ErrorContains(t, err, "checkpoint disk full")
	require.Equal(t, int32(1), saves.Load())
	require.Zero(t, actions.Load())
}

func TestParallelPrecopyKeepsPerVolumeRoundsAndFinalCopies(t *testing.T) {
	e, first, _ := fixture(t)
	second := addTestVolume(t, e, first, 1)
	simulateControllers(e, first, second)
	enableTestPrecopy(e)
	e.State.Options.PVConcurrency = 2
	path := filepath.Join(t.TempDir(), "state.json")
	e.State.Options.StatePath = path
	e.Save = func(s *State) error { return SaveState(path, s) }
	entered := make(chan string, 2)
	release := make(chan struct{})
	var final atomic.Int32
	e.Copier = copierFunc(func(ctx context.Context, _ *State, w *Workload) error {
		if w.Volume.Phase == phasePrecopying {
			entered <- w.Name
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		}
		var sts appsv1.StatefulSet
		if err := e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &sts); err != nil {
			return err
		}
		if *sts.Spec.Replicas != 0 || w.Volume.CopySucceeded || !w.Volume.Precopy.Ready {
			return errors.New("final copy did not follow a fresh, converged online round and stop")
		}
		final.Add(1)
		return nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- applyLegacyComponents(e, ctx, true) }()
	for range 2 {
		select {
		case <-entered:
		case err := <-result:
			t.Fatalf("precopy failed before both workers entered: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	for _, w := range []*Workload{first, second} {
		var sts appsv1.StatefulSet
		require.NoError(t, e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &sts))
		require.Equal(t, int32(1), *sts.Spec.Replicas)
	}
	close(release)
	require.NoError(t, <-result)
	require.Equal(t, int32(2), final.Load())
	loaded, err := LoadState(path)
	require.NoError(t, err)
	for _, w := range loaded.Workloads {
		require.Equal(t, phaseStarted, w.Volume.Phase)
		require.Len(t, w.Volume.Precopy.Rounds, 1)
		require.True(t, w.Volume.Precopy.Ready)
	}
}

func TestConcurrencyAndBandwidthValidation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		parallel  int
		bandwidth int64
		valid     bool
	}{
		{"serial", 1, 0, true}, {"parallel-limited", 4, 51200, true}, {"max", 16, 1 << 30, true},
		{"zero-workers", 0, 0, false}, {"negative-workers", -1, 0, false}, {"unbounded-workers", 17, 0, false},
		{"negative-bandwidth", 1, -1, false}, {"excess-bandwidth", 1, 1<<30 + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := testState().Options
			o.PVConcurrency, o.RsyncBandwidthLimit = tc.parallel, tc.bandwidth
			err := ValidateOptions(o)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	require.Equal(t, "--numeric-ids", rsyncExtraArgs(Options{}))
	require.Equal(t, "--numeric-ids --bwlimit=51200", rsyncExtraArgs(Options{RsyncBandwidthLimit: 51200}))
}

func TestConcurrentCapacityRaisesReserveAndChecksSource(t *testing.T) {
	e, _, _ := fixture(t)
	e.State.Options.PVConcurrency = 3
	reserve, err := capacityReserve(e.State.Options)
	require.NoError(t, err)
	for key, value := range map[corev1.ResourceName]string{corev1.ResourceCPU: "300m", corev1.ResourceMemory: "384Mi", corev1.ResourcePods: "3"} {
		q := reserve[key]
		require.Equal(t, value, q.String())
	}
	quota := corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "helpers"}, Status: corev1.ResourceQuotaStatus{
		Hard: corev1.ResourceList{corev1.ResourceLimitsCPU: resource.MustParse("5")}, Used: corev1.ResourceList{corev1.ResourceLimitsCPU: resource.MustParse("0")},
	}}
	require.ErrorContains(t, e.namespaceCapacity("prod", []corev1.ResourceQuota{quota}, nil, nil, true), "limits.cpu")
	var source corev1.Node
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: "old"}, &source))
	source.Status.Allocatable[corev1.ResourcePods] = resource.MustParse("3")
	require.NoError(t, e.Client.Status().Update(t.Context(), &source))
	require.ErrorContains(t, e.checkCapacity(t.Context()), "源节点缺少复制助手容量")
}

func TestVolumeBatchesRespectLimitAndManagedServiceBarriers(t *testing.T) {
	makeVolume := func(name string) *Workload {
		return &Workload{Kind: kindStatefulSet, Namespace: "test", Name: name, Volume: &Volume{Phase: phasePrepared}}
	}
	a, b, managed, c, d, done := makeVolume("a"), makeVolume("b"), makeVolume("managed"), makeVolume("c"), makeVolume("d"), makeVolume("done")
	managed.Owner = &metav1.OwnerReference{Kind: "Prometheus", Name: "monitor"}
	done.Volume.Phase = phaseStarted
	workloads := []*Workload{a, b, managed, c, &Workload{Name: "stateless"}, done, d}
	for _, tc := range []struct {
		name  string
		limit int
		want  [][]*Workload
	}{
		{"serial", 1, [][]*Workload{{a}, {b}, {managed}, {c}, {d}}},
		{"two", 2, [][]*Workload{{a, b}, {managed}, {c, d}}},
		{"managed-barrier", 4, [][]*Workload{{a, b}, {managed}, {c, d}}},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, volumeBatches(workloads, tc.limit)) })
	}
	// Even if inventory is later extended to represent several volumes/Pods of
	// one StatefulSet, they must never occupy two slots in the same batch.
	aSecond := makeVolume("a")
	aSecond.Volume.PVC.Name = "data-a-1"
	require.Equal(t, [][]*Workload{{a, b}, {aSecond, c}}, volumeBatches([]*Workload{a, b, aSecond, c}, 4))
}
