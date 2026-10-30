package cmd

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/Kalen89/kube-node-migrate/internal/nodemigrate"
)

func TestPlanDefaultsScopeAndStateWithoutConnecting(t *testing.T) {
	plan, _, err := New("test").Find([]string{"plan"})
	require.NoError(t, err)
	namespaces, err := plan.Flags().GetStringSlice("namespace")
	require.NoError(t, err)
	require.Empty(t, namespaces)
	require.Equal(t, ".kube-node-migrate/state.json", plan.Flags().Lookup("state").DefValue)
	require.NoError(t, plan.Flags().Parse([]string{"-n", "prod,pre", "--exclude-namespace", "monitoring"}))
	namespaces, err = plan.Flags().GetStringSlice("namespace")
	require.NoError(t, err)
	require.Equal(t, []string{"prod", "pre"}, namespaces)
}

func TestCLIOnlyExposesPodMigrationAndNoNodeRetirement(t *testing.T) {
	root := New("test")
	var commands []string
	for _, cmd := range root.Commands() {
		commands = append(commands, cmd.Name())
	}
	require.ElementsMatch(t, []string{"plan", "apply", "resume", "status", "verify", "config"}, commands)
	plan, _, err := root.Find([]string{"plan"})
	require.NoError(t, err)
	for _, name := range []string{"mode", "source-policy", "target-label"} {
		require.Nil(t, plan.Flags().Lookup(name))
	}
	path := filepath.Join(t.TempDir(), "private", "state.json")
	root.SetArgs([]string{"plan", "--namespace", "prod", "--source", "old", "--target", "new", "--kubeconfig", "/no-such-kubeconfig", "--state", path})
	err = root.Execute()
	require.Error(t, err)
	require.NotContains(t, err.Error(), "source-policy")
	require.NotContains(t, err.Error(), "--mode")
}

func TestStatusIsLocalAndRedactsSnapshots(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	options := nodemigrate.Options{Source: "old", Target: "new", StatePath: path, Kubeconfig: "/not-an-existing-kubeconfig", SSHUser: "root", SSHPort: 22, SourcePolicy: "stop-agent", WaitTimeout: time.Minute, CopyTimeout: time.Hour}
	s := &nodemigrate.State{Version: 2, ID: "0123456789abcdef", ClusterUID: "cluster", Phase: "planned", Options: options,
		Source: nodemigrate.NodeIdentity{Name: "old", UID: "old-uid"}, Target: nodemigrate.NodeIdentity{Name: "new", UID: "new-uid"},
		SourceConfig: nodemigrate.Host{Name: "old", IP: "10.0.0.1", Architecture: "amd64"}, TargetConfig: nodemigrate.Host{Name: "new", IP: "10.0.0.2", Architecture: "amd64"},
		Workloads: []*nodemigrate.Workload{{Kind: "Deployment", Namespace: "app", Name: "api", UID: "api-uid", Replicas: 1, Phase: "planned", Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "api:1", Env: []corev1.EnvVar{{Name: "PASSWORD", Value: "private-value"}}}}}}}}}
	require.NoError(t, nodemigrate.SaveState(path, s))
	var out bytes.Buffer
	cmd := New("test")
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"status", "--state", path, "--json"})
	require.NoError(t, cmd.Execute())
	require.Contains(t, out.String(), "old")
	require.NotContains(t, out.String(), "private-value")
	require.NotContains(t, out.String(), "PASSWORD")
}
