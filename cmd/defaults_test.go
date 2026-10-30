package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Kalen89/kube-node-migrate/internal/nodemigrate"
	"github.com/stretchr/testify/require"
)

func TestDetectKubeconfigPreservesCredentialPriority(t *testing.T) {
	dir := t.TempDir()
	userPath, k3sPath := filepath.Join(dir, "user-config"), filepath.Join(dir, "k3s-config")
	require.NoError(t, os.WriteFile(k3sPath, []byte("test"), 0o600))
	got, err := detectKubeconfig("", userPath, k3sPath)
	require.NoError(t, err)
	require.Equal(t, k3sPath, got)
	require.NoError(t, os.WriteFile(userPath, []byte("test"), 0o600))
	got, err = detectKubeconfig("", userPath, k3sPath)
	require.NoError(t, err)
	require.Equal(t, userPath, got)
	got, err = detectKubeconfig("explicit-env", userPath, k3sPath)
	require.NoError(t, err)
	require.Equal(t, "explicit-env", got)
	_, err = detectKubeconfig(userPath+string(os.PathListSeparator)+k3sPath, userPath, k3sPath)
	require.ErrorContains(t, err, "多个文件")
	_, err = detectKubeconfig("", filepath.Join(dir, "missing"), filepath.Join(dir, "missing-k3s"))
	require.ErrorContains(t, err, "未找到 kubeconfig")
}

func TestConfigExclusionsAndExplicitFlags(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scope.yaml")
	require.NoError(t, os.WriteFile(path, []byte("namespaces: [prod, pre]\nexcludeNamespaces: [kube-system, monitoring]\n"), 0o600))
	cmd, _, err := New("test").Find([]string{"plan"})
	require.NoError(t, err)
	var options nodemigrate.Options
	require.NoError(t, applyPlanConfig(cmd, &options, path))
	require.Equal(t, []string{"prod", "pre"}, options.Namespaces)
	require.Equal(t, []string{"kube-system", "monitoring"}, options.ExcludeNamespaces)
	require.NoError(t, cmd.Flags().Parse([]string{"--exclude-namespace", "logging"}))
	options.ExcludeNamespaces = []string{"logging"}
	require.NoError(t, applyPlanConfig(cmd, &options, path))
	require.Equal(t, []string{"logging"}, options.ExcludeNamespaces)
}
