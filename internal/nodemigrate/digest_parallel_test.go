package nodemigrate

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestApplyParallelDigestsWaitForBothScansAfterFinalSync(t *testing.T) {
	e, w := instanceFixture(t)
	e.State.Options.ParallelDigest = true
	base := e.Hosts
	var active, peak, calls atomic.Int32
	e.State.Options.CopyTimeout = time.Second
	gates := []chan struct{}{make(chan struct{}), make(chan struct{})}
	e.Hosts = hostFunc(func(ctx context.Context, host Host, command string) (string, error) {
		if strings.Contains(command, "--mode 'digest'") {
			current := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
			}
			n := calls.Add(1)
			gate := gates[(n-1)/2]
			if n%2 == 0 {
				close(gate)
			}
			select {
			case <-gate:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		return base.Run(ctx, host, command)
	})
	require.NoError(t, e.Apply(t.Context(), true))
	require.Equal(t, int32(4), calls.Load())
	require.Equal(t, int32(2), peak.Load())
	require.Zero(t, active.Load())
	require.Equal(t, phaseStarted, w.Phase)
	require.NoError(t, e.Verify(t.Context()))
}

func TestParallelDigestFailureCancelsAndJoinsPeer(t *testing.T) {
	e, w := instanceFixture(t)
	e.State.Options.ParallelDigest = true
	base := e.Hosts
	both := make(chan struct{})
	var targetExited atomic.Bool
	e.Hosts = hostFunc(func(ctx context.Context, host Host, command string) (string, error) {
		if strings.Contains(command, "--mode 'digest'") {
			if host.Name == e.State.Source.Name {
				<-both
				return "", errors.New("source read failed")
			}
			close(both)
			<-ctx.Done()
			targetExited.Store(true)
			return "", ctx.Err()
		}
		return base.Run(ctx, host, command)
	})
	require.ErrorContains(t, e.Apply(t.Context(), true), "source read failed")
	require.True(t, targetExited.Load())
	require.Equal(t, phaseCopied, w.Volumes[0].Phase)
	require.Nil(t, w.Volumes[0].SourceDigest)
	require.Nil(t, w.Volumes[0].TargetDigest)
	require.Empty(t, w.Volumes[0].NewPVCUID)
}

func TestParallelDigestMismatchResumesByRecopy(t *testing.T) {
	e, w := instanceFixture(t)
	e.State.Options.ParallelDigest = true
	base := e.Hosts
	mismatch := true // Changed only after Apply joins all digest workers.
	e.Hosts = hostFunc(func(ctx context.Context, host Host, command string) (string, error) {
		if mismatch && host.Name == e.State.Target.Name && strings.Contains(command, "--mode 'digest'") {
			return `{"version":2,"sha256":"` + strings.Repeat("b", 64) + `","entries":2,"bytes":8}`, nil
		}
		return base.Run(ctx, host, command)
	})
	copies := 0
	e.Copier = copierFunc(func(_ context.Context, _ *State, view *Workload) error {
		if view.Volume.Phase == phaseCopying {
			copies++
		}
		return nil
	})
	require.ErrorContains(t, e.Apply(t.Context(), true), "摘要不一致")
	require.Equal(t, 1, copies)
	require.True(t, w.Volumes[0].FinalSyncRetry)
	mismatch = false
	require.NoError(t, e.Apply(t.Context(), true))
	require.Equal(t, 3, copies)
	require.NoError(t, e.Verify(t.Context()))
}

func TestParallelDigestResumesCopiedVolumeWithoutSavedSourceHash(t *testing.T) {
	e, w := instanceFixture(t)
	e.State.Options.Precopy = false
	e.State.Options.ParallelDigest = true
	require.NoError(t, e.ensureHold(t.Context(), w))
	require.NoError(t, e.Client.Delete(t.Context(), &w.Instance.Pod))
	w.Phase = phaseStopped
	v := w.Volumes[0]
	v.Phase, v.CopySucceeded = phaseCopied, true
	e.Copier = copierFunc(func(context.Context, *State, *Workload) error { return errors.New("must reuse successful final sync") })
	require.NoError(t, e.copyVolume(t.Context(), volumeView(w, v)))
	require.Equal(t, phaseVerified, v.Phase)
	require.NotNil(t, v.SourceDigest)
	require.Equal(t, v.SourceDigest, v.TargetDigest)
	require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(&v.PVC), v.PVC.DeepCopy()))
}
