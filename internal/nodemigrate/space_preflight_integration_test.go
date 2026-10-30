//go:build integration

package nodemigrate

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Kalen89/kube-node-migrate/internal/filesystem"
	"github.com/stretchr/testify/require"
)

type limitedInventoryHosts struct {
	Hosts
	source, target string
	freeBytes      uint64
}

func (h limitedInventoryHosts) Filesystem(ctx context.Context, node Host, _, _, mode string) (string, error) {
	path := h.source
	if node.Name == "new" {
		path = h.target
	}
	summary, err := filesystem.Scan(ctx, path, mode)
	if err != nil {
		return "", err
	}
	if node.Name == "new" {
		summary.FreeBytes, summary.FreeInodes = h.freeBytes, 2
	}
	data, err := json.Marshal(summary)
	return string(data), err
}

func TestSpacePreflightAfterRealRsyncUsesLargestReplacement(t *testing.T) {
	rsync, err := exec.LookPath("rsync")
	if err != nil {
		t.Skip("integration test requires rsync")
	}
	for _, tc := range []struct {
		name        string
		files, size int
		wantError   bool
	}{
		{"many-precopied-files", 8, 64 << 10, false},
		{"one-large-precopied-file", 1, 512 << 10, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, target := t.TempDir(), t.TempDir()
			for i := range tc.files {
				require.NoError(t, os.WriteFile(filepath.Join(source, string(rune('a'+i))), []byte(strings.Repeat("x", tc.size)), 0o600))
			}
			// The real metadata scan remains read-only. macOS provenance xattrs
			// are outside the supported Linux volume contract.
			if _, err := filesystem.Scan(t.Context(), source, "inventory"); err != nil {
				if runtime.GOOS == "darwin" && strings.Contains(err.Error(), "extended attributes") {
					t.Skip(err)
				}
				t.Fatal(err)
			}
			output, err := exec.CommandContext(t.Context(), rsync, "-a", source+"/", target+"/").CombinedOutput()
			require.NoError(t, err, string(output))
			e, w, _ := fixture(t)
			w.Volume.Phase = phasePrecopyReady
			e.Hosts = limitedInventoryHosts{Hosts: e.Hosts, source: source, target: target, freeBytes: 128 << 10}
			err = e.checkSpace(t.Context())
			if tc.wantError {
				require.ErrorContains(t, err, "空间/inode 不足")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
