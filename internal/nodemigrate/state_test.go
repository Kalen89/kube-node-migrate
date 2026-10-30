package nodemigrate

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatePrivateAtomicCheckpointAndLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migration", "state.json")
	lock, err := OpenState(path)
	require.NoError(t, err)
	_, err = OpenState(path)
	require.ErrorContains(t, err, "另一个进程")
	s := testState()
	require.NoError(t, SaveState(path, s))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	got, err := LoadState(path)
	require.NoError(t, err)
	require.Equal(t, s.ID, got.ID)
	require.NoError(t, lock.Close())
	lock, err = OpenState(path)
	require.NoError(t, err)
	require.NoError(t, lock.Close())
	require.NoError(t, os.Chmod(path, 0o644))
	_, err = LoadState(path)
	require.ErrorContains(t, err, "0600")
}

func TestVersionTwoStateCanResumeWithoutInventingNewOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := testState()
	s.Version = 2
	require.NoError(t, SaveState(path, s))
	loaded, err := LoadState(path)
	require.NoError(t, err)
	require.Equal(t, 5, loaded.Version)
	require.Nil(t, loaded.Options.TargetLabels)
	require.Empty(t, loaded.Options.HealthChecks)
	require.NoError(t, SaveState(path, loaded))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), `"version": 5`)
}

func TestLegacyStatesKeepSerialExecutionAndVersionFourPreservesOptions(t *testing.T) {
	for _, version := range []int{2, 3, 4} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.json")
			s := testState()
			s.Version = version
			if version == 4 {
				s.Options.PVConcurrency, s.Options.RsyncBandwidthLimit = 3, 51200
			} else {
				s.Options.PVConcurrency = 0
			}
			require.NoError(t, SaveState(path, s))
			loaded, err := LoadState(path)
			require.NoError(t, err)
			require.Equal(t, 5, loaded.Version)
			if version == 4 {
				require.Equal(t, 3, loaded.Options.PVConcurrency)
				require.Equal(t, int64(51200), loaded.Options.RsyncBandwidthLimit)
			} else {
				require.Equal(t, 1, loaded.Options.PVConcurrency)
				require.Zero(t, loaded.Options.RsyncBandwidthLimit)
			}
		})
	}
}

func TestStateRejectsMalformedOrIncompleteCheckpoints(t *testing.T) {
	for _, tc := range []struct {
		name  string
		alter func(*State)
	}{
		{"schema", func(s *State) { s.Version++ }},
		{"old-schema", func(s *State) { s.Version = 1 }},
		{"node-uid", func(s *State) { s.Target.UID = "" }},
		{"same-host", func(s *State) { s.TargetConfig.IP = s.SourceConfig.IP }},
		{"unknown-phase", func(s *State) { s.Phase = "complete" }},
		{"nil-workload", func(s *State) { s.Workloads = []*Workload{nil} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.json")
			s := testState()
			tc.alter(s)
			require.NoError(t, SaveState(path, s))
			_, err := LoadState(path)
			require.Error(t, err)
		})
	}
}

func TestStateRefusesSymlinksAndPublicDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	require.NoError(t, SaveState(path, testState()))
	link := filepath.Join(dir, "link.json")
	require.NoError(t, os.Symlink(path, link))
	_, err := LoadState(link)
	require.Error(t, err)
	require.NoError(t, os.Symlink(path, path+".lock"))
	_, err = OpenState(path)
	require.Error(t, err)
	require.NoError(t, os.Chmod(dir, 0o755))
	_, err = OpenState(filepath.Join(dir, "other.json"))
	require.ErrorContains(t, err, "0700")
}

func TestExtractToolVerifiesArchiveBeforeWriting(t *testing.T) {
	var data bytes.Buffer
	gz := gzip.NewWriter(&data)
	writer := tar.NewWriter(gz)
	body := []byte("fixed-binary")
	require.NoError(t, writer.WriteHeader(&tar.Header{Name: "pv-migrate", Typeflag: tar.TypeReg, Mode: 0o755, Size: int64(len(body))}))
	_, err := writer.Write(body)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, gz.Close())
	dir := t.TempDir()
	archive := filepath.Join(dir, "tool.tar.gz")
	require.NoError(t, os.WriteFile(archive, data.Bytes(), 0o600))
	_, err = extractTool(archive, dir, strings.Repeat("0", 64))
	require.ErrorContains(t, err, "SHA-256")
	hash := sha256.Sum256(data.Bytes())
	binary, err := extractTool(archive, dir, hex.EncodeToString(hash[:]))
	require.NoError(t, err)
	got, err := os.ReadFile(binary)
	require.NoError(t, err)
	require.Equal(t, body, got)
	info, err := os.Stat(binary)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
}
