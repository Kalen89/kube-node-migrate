package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Kalen89/kube-node-migrate/internal/nodemigrate"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestPrintDefaultsIsLocalAndRoundTripsToPlanDefaults(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("KUBECONFIG", "missing-a"+string(os.PathListSeparator)+"missing-b")
	var out, stderr bytes.Buffer
	root := New("test")
	root.SetOut(&out)
	root.SetErr(&stderr)
	root.SetArgs([]string{"config", "print", "defaults"})
	require.NoError(t, root.Execute())
	require.Empty(t, stderr.String())
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
	var document map[string]any
	require.NoError(t, yaml.UnmarshalStrict(out.Bytes(), &document))
	for _, key := range []string{"helper", "sshd", "rsync"} {
		service, ok := document[key].(map[string]any)
		require.True(t, ok)
		require.Contains(t, service, "image")
		require.Contains(t, service, "resources")
	}
	reserve, ok := document["reserve"].(map[string]any)
	require.True(t, ok)
	require.Len(t, reserve, 3)
	for _, key := range []string{"cpu", "memory", "pods"} {
		require.Contains(t, reserve, key)
	}
	for _, key := range []string{"reserveCPU", "reserveMemory", "reservePods", "images", "helperImage", "sshdImage", "rsyncImage", "sshKnownHosts", "pvMigrate", "pvMigrateArchive", "filesystemHelper"} {
		require.NotContains(t, document, key)
	}
	path := filepath.Join(dir, "migration.yaml")
	require.NoError(t, os.WriteFile(path, out.Bytes(), 0o600))
	plan, _, err := New("test").Find([]string{"plan"})
	require.NoError(t, err)
	var o nodemigrate.Options
	require.NoError(t, applyPlanConfig(plan, &o, path))
	require.Empty(t, o.Namespaces)
	require.Equal(t, []string{"kube-system"}, o.ExcludeNamespaces)
	defaults := nodemigrate.DefaultServiceResources()
	require.Equal(t, &defaults, o.HelperResources)
	require.Equal(t, &defaults, o.SSHDResources)
	require.Equal(t, &defaults, o.RsyncResources)
	for flag, value := range map[string]any{
		"helper-image": o.HelperImage, "sshd-image": o.SSHDImage, "rsync-image": o.RsyncImage,
		"reserve-cpu": o.ReserveCPU, "reserve-memory": o.ReserveMemory, "reserve-pods": o.ReservePods,
		"pv-concurrency": o.PVConcurrency, "rsync-bandwidth-limit": o.RsyncBandwidthLimit,
		"parallel-digest": o.ParallelDigest, "precopy": o.Precopy, "precopy-rounds": o.PrecopyRounds, "precopy-threshold": o.PrecopyThreshold,
	} {
		require.Equal(t, plan.Flags().Lookup(flag).DefValue, fmt.Sprint(value), flag)
	}
}

func TestPrintDefaultsRejectsArgumentsAndPropagatesOutputErrors(t *testing.T) {
	root := New("test")
	root.SetArgs([]string{"config", "print", "defaults", "extra"})
	require.Error(t, root.Execute())
	root = New("test")
	root.SetOut(failingConfigWriter{})
	root.SetArgs([]string{"config", "print", "defaults"})
	require.ErrorContains(t, root.Execute(), "output failed")
}

type failingConfigWriter struct{}

func (failingConfigWriter) Write([]byte) (int, error) {
	return 0, errors.New("output failed")
}
