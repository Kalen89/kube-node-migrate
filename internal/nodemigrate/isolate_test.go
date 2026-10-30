package nodemigrate

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestMigrationNeverManagesNodeServicesRolesOrMembership(t *testing.T) {
	e, _ := instanceFixture(t)
	ctx := context.Background()
	var before corev1.Node
	require.NoError(t, e.Client.Get(ctx, types.NamespacedName{Name: e.State.Source.Name}, &before))
	previous := e.Hosts
	e.Hosts = hostFunc(func(ctx context.Context, h Host, command string) (string, error) {
		require.NotContains(t, command, "systemctl")
		require.NotContains(t, command, "etcd")
		require.NotContains(t, command, " stopp")
		require.False(t, strings.Contains(command, "crictl") && strings.Contains(command, " stop "))
		return previous.Run(ctx, h, command)
	})
	var cordoned bool
	e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if n, ok := obj.(*corev1.Node); ok {
				var old corev1.Node
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(n), &old))
				require.Equal(t, old.Labels, n.Labels)
				require.Equal(t, old.Spec.Taints, n.Spec.Taints)
				if n.Name == e.State.Source.Name && n.Spec.Unschedulable {
					cordoned = true
				}
			}
			return c.Update(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			_, node := obj.(*corev1.Node)
			require.False(t, node, "migration must never delete a Node")
			return c.Delete(ctx, obj, opts...)
		},
	})
	require.NoError(t, e.Apply(ctx, true))
	require.True(t, cordoned)
	var after corev1.Node
	require.NoError(t, e.Client.Get(ctx, client.ObjectKeyFromObject(&before), &after))
	require.Equal(t, before.Labels, after.Labels)
	require.Equal(t, before.Spec, after.Spec)
}

func TestCompletedMigrationCanBeVerifiedAfterUserRetiresOldMachine(t *testing.T) {
	e, _ := instanceFixture(t)
	ctx := context.Background()
	require.NoError(t, e.Apply(ctx, true))
	var old corev1.Node
	require.NoError(t, e.Client.Get(ctx, types.NamespacedName{Name: e.State.Source.Name}, &old))
	require.NoError(t, e.Client.Delete(ctx, &old))
	for _, w := range e.State.Workloads {
		for _, v := range w.Volumes {
			require.NoError(t, e.Client.Delete(ctx, &v.PV))
		}
	}
	require.NoError(t, e.Verify(ctx))
	require.NoError(t, e.Apply(ctx, true))
}

func TestLegacyNodeDispositionCheckpointIsNeverExecuted(t *testing.T) {
	e, _, _ := fixture(t)
	require.ErrorContains(t, e.Apply(context.Background(), true), "仅支持 status")
	var pod corev1.Pod
	require.NoError(t, e.Client.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "db-0"}, &pod))
	require.Nil(t, pod.DeletionTimestamp)
	require.Equal(t, "old", pod.Spec.NodeName)
}

func TestTargetNodeRoleDoesNotChangePodMigrationSemantics(t *testing.T) {
	e, _ := instanceFixture(t)
	ctx := context.Background()
	var target corev1.Node
	require.NoError(t, e.Client.Get(ctx, types.NamespacedName{Name: e.State.Target.Name}, &target))
	target.Labels["node-role.kubernetes.io/control-plane"] = "true"
	target.Labels["node-role.kubernetes.io/etcd"] = "true"
	require.NoError(t, e.Client.Update(ctx, &target))
	_, err := e.Plan(ctx, e.State.Options)
	require.NoError(t, err)
	// Rebuild the prepared fixture so Plan's new transaction does not adopt
	// target volumes belonging to an earlier operation.
	e, _ = instanceFixture(t)
	require.NoError(t, e.Client.Get(ctx, types.NamespacedName{Name: e.State.Target.Name}, &target))
	target.Labels["node-role.kubernetes.io/control-plane"] = "true"
	require.NoError(t, e.Client.Update(ctx, &target))
	require.NoError(t, e.Apply(ctx, true))
}
