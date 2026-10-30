package nodemigrate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestApplyWithoutPrecopyAllowsOriginalConsumer(t *testing.T) {
	e, w := instanceFixture(t)
	e.State.Options.Precopy = false
	var phases []string
	e.Copier = copierFunc(func(_ context.Context, _ *State, view *Workload) error {
		phases = append(phases, view.Volume.Phase)
		return nil
	})
	require.NoError(t, e.Apply(t.Context(), true))
	require.Equal(t, []string{phaseCopying, phaseCopying}, phases)
	require.Equal(t, phaseStarted, w.Phase)
	require.NoError(t, e.Verify(t.Context()))
}

func TestResumeRefreshesPrecopyAfterEvictionRefused(t *testing.T) {
	e, w := instanceFixture(t)
	blocked := true
	e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{
		SubResourceCreate: func(ctx context.Context, c client.Client, sub string, obj, body client.Object, opts ...client.SubResourceCreateOption) error {
			if blocked && sub == "eviction" {
				return errors.New("PDB refuses eviction")
			}
			return c.SubResource(sub).Create(ctx, obj, body, opts...)
		},
	})
	rounds := 0
	e.Copier = copierFunc(func(_ context.Context, _ *State, view *Workload) error {
		if view.Volume.Phase == phasePrecopying {
			rounds++
		}
		return nil
	})
	require.ErrorContains(t, e.Apply(t.Context(), true), "PDB refuses")
	require.Equal(t, 2, rounds)
	require.Equal(t, phaseStopping, w.Phase)
	blocked = false
	require.NoError(t, e.Apply(t.Context(), true))
	require.Equal(t, 4, rounds, "both stale online volume estimates must be refreshed")
	require.NoError(t, e.Verify(t.Context()))
}

func TestRefreshPrecopyNeverResetsTerminatingInstance(t *testing.T) {
	e, w := instanceFixture(t)
	w.Phase = phaseStopping
	pod := w.Instance.Pod.DeepCopy()
	pod.Finalizers = []string{"test/keep-terminating"}
	require.NoError(t, e.Client.Update(t.Context(), pod))
	require.NoError(t, e.Client.Delete(t.Context(), pod))
	require.NoError(t, e.refreshOnlinePrecopy(t.Context(), w))
	require.Equal(t, phaseStopping, w.Phase)
}

func TestSourceImagePullFailureStopsBeforeEvictionWithoutPrecopy(t *testing.T) {
	e, w := instanceFixture(t)
	e.State.Options.Precopy = false
	base := e.Hosts
	e.Hosts = hostFunc(func(ctx context.Context, host Host, command string) (string, error) {
		if host.Name == e.State.Source.Name && strings.HasPrefix(command, "k3s ctr") {
			return "", nil
		}
		return base.Run(ctx, host, command)
	})
	var warmedSource bool
	e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if pod, ok := obj.(*corev1.Pod); ok && strings.HasPrefix(pod.Name, "km-migration-warm-") {
			require.Equal(t, e.State.Source.Name, pod.Spec.NodeSelector[hostnameLabel])
			warmedSource = true
		}
		return c.Create(ctx, obj, opts...)
	}})
	require.Error(t, e.Apply(t.Context(), true))
	require.True(t, warmedSource)
	var original corev1.Pod
	require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(&w.Instance.Pod), &original))
	require.Equal(t, w.Instance.Pod.UID, original.UID)
	require.Nil(t, original.DeletionTimestamp)
	require.True(t, w.Instance.EvictionRequestedAt.IsZero())
	var holds corev1.PodList
	require.NoError(t, e.Client.List(t.Context(), &holds, client.InNamespace(w.Namespace)))
	require.NotEmpty(t, holds.Items)
}
