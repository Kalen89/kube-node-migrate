package nodemigrate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilesystemHelperCacheUploadAndReuse(t *testing.T) {
	if runtime.GOOS != linuxOS {
		t.Skip("remote cache uses Linux stat")
	}
	base := filepath.Join(t.TempDir(), "helper-cache")
	data := []byte("immutable helper contents")
	hash := sha256.Sum256(data)
	digest := hex.EncodeToString(hash[:])
	run := func(script string, input []byte) (string, error) {
		cmd := exec.CommandContext(t.Context(), "sh", "-c", strings.ReplaceAll(script, helperCache, base))
		cmd.Stdin = bytes.NewReader(input)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	out, err := run(helperCacheCheck(digest), nil)
	require.NoError(t, err, out)
	require.Equal(t, "missing", out)
	out, err = run(helperUpload(digest), data)
	require.NoError(t, err, out)
	out, err = run(helperCacheCheck(digest), nil)
	require.NoError(t, err, out)
	require.Equal(t, "ready", out)
	path := filepath.Join(base, "fs-"+digest)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o500), info.Mode().Perm())
	// A bad upload must not replace a valid published helper or leave temporary files.
	out, err = run(helperUpload(digest), []byte("bad data"))
	require.Error(t, err, out)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, got)
	entries, err := os.ReadDir(base)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	// A tampered cache is rejected, rather than silently repaired or executed.
	require.NoError(t, os.Chmod(path, 0o700))
	require.NoError(t, os.WriteFile(path, []byte("tampered"), 0o500))
	require.NoError(t, os.Chmod(path, 0o500))
	out, err = run(helperCacheCheck(digest), nil)
	require.Error(t, err, out)
}

func TestFilesystemHelperCacheRejectsUnsafeDirectory(t *testing.T) {
	if runtime.GOOS != linuxOS {
		t.Skip("remote cache uses Linux stat")
	}
	dir := t.TempDir()
	base := filepath.Join(dir, "cache")
	other := filepath.Join(dir, "other")
	require.NoError(t, os.Mkdir(other, 0o700))
	require.NoError(t, os.Symlink(other, base))
	cmd := exec.CommandContext(t.Context(), "sh", "-c", strings.ReplaceAll(helperCacheCheck(strings.Repeat("a", 64)), helperCache, base))
	require.Error(t, cmd.Run())
}

func TestFilesystemRejectsOldHelperProtocol(t *testing.T) {
	e, _, _ := fixture(t)
	e.Hosts = hostFunc(func(_ context.Context, _ Host, _ string) (string, error) {
		return `{"version":1,"entries":1,"sha256":"` + strings.Repeat("a", 64) + `"}`, nil
	})
	_, err := e.filesystem(t.Context(), e.State.SourceConfig, "/data", "digest")
	require.ErrorContains(t, err, "协议版本")
}

func TestPackagedFilesystemHelperAndArchitectureCheck(t *testing.T) {
	path := os.Getenv("K3S_TEST_FS_HELPER")
	if path == "" {
		t.Skip("set K3S_TEST_FS_HELPER to exercise a packaged Linux helper")
	}
	file, hash, err := openFilesystemHelper(path, runtime.GOARCH)
	require.NoError(t, err)
	require.Len(t, hash, 64)
	magic := make([]byte, 4)
	_, err = file.Read(magic)
	require.NoError(t, err)
	require.Equal(t, []byte{0x7f, 'E', 'L', 'F'}, magic)
	require.NoError(t, file.Close())
	other := "amd64"
	if runtime.GOARCH == "amd64" {
		other = "arm64"
	}
	_, _, err = openFilesystemHelper(path, other)
	require.ErrorContains(t, err, "架构")
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data"), []byte("verified"), 0o600))
	cmd := exec.CommandContext(t.Context(), path, "--path", dir, "--mode", "digest", "--timeout", "10s")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	var summary diskSummary
	require.NoError(t, json.Unmarshal(output, &summary))
	require.Equal(t, 2, summary.Version)
	require.Equal(t, uint64(2), summary.Entries)
	require.Equal(t, uint64(8), summary.Bytes)
	require.True(t, validDigest(&summary.Digest))
}
