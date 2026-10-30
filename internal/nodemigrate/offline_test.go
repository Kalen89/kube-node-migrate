package nodemigrate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOfflineDiscoveryPrefersExecutableAndRejectsSymlinks(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "pv-migrate_v3.7.0_linux_x86_64.tar.gz")
	require.NoError(t, os.WriteFile(archive, []byte("placeholder"), 0o600))
	binary, found := discoverCopyTool(dir, "amd64")
	require.Empty(t, binary)
	require.Equal(t, archive, found)
	require.NoError(t, os.Symlink(archive, filepath.Join(dir, "pv-migrate")))
	binary, found = discoverCopyTool(dir, "amd64")
	require.Empty(t, binary)
	require.Equal(t, archive, found)
	_, found = discoverCopyTool(dir, "arm64")
	require.Empty(t, found)
	require.NoError(t, os.Remove(filepath.Join(dir, "pv-migrate")))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pv-migrate"), []byte("binary"), 0o700))
	binary, found = discoverCopyTool(dir, "amd64")
	require.Equal(t, filepath.Join(dir, "pv-migrate"), binary)
	require.Empty(t, found)
}
