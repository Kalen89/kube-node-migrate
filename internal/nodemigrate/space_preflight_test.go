package nodemigrate

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Kalen89/kube-node-migrate/internal/filesystem"
	"github.com/stretchr/testify/require"
)

func spaceInventory(files int, size, allocated, freeBytes, freeInodes uint64) *filesystem.Summary {
	summary := &filesystem.Summary{Version: filesystem.Version, Device: 7, BlockSize: 4096, FreeBytes: freeBytes, FreeInodes: freeInodes,
		Digest:    filesystem.Digest{Entries: uint64(files + 1), Bytes: uint64(files) * size},
		Inventory: []filesystem.FileMetadata{{Path: []byte("."), Type: filesystem.TypeDirectory, Size: 4096, AllocatedBytes: 4096}}}
	for i := range files {
		summary.Inventory = append(summary.Inventory, filesystem.FileMetadata{Path: []byte{byte('a' + i)}, Type: filesystem.TypeRegular, Size: size, AllocatedBytes: allocated})
	}
	return summary
}

func inventoryHosts(t *testing.T, e *Engine, source, target *filesystem.Summary) {
	t.Helper()
	e.Hosts = hostFunc(func(_ context.Context, h Host, command string) (string, error) {
		if !strings.HasSuffix(command, "'inventory'") {
			return "", errors.New("space preflight must inspect inventories without content hashing")
		}
		summary := source
		if h.Name == e.State.Target.Name {
			summary = target
		}
		data, err := json.Marshal(summary)
		return string(data), err
	})
}

func TestSpacePreflightUsesIncrementalMetadataAndSerialTemporaryPeak(t *testing.T) {
	const gib uint64 = 1 << 30
	for _, tc := range []struct {
		name           string
		source, target *filesystem.Summary
		wantError      bool
	}{
		{"many-precopied-files", spaceInventory(9, 10*gib, 10*gib, 0, 100), spaceInventory(9, 10*gib, 10*gib, 60*gib, 1), false},
		{"single-large-replacement", spaceInventory(1, 90*gib, 90*gib, 0, 100), spaceInventory(1, 90*gib, 90*gib, 60*gib, 1), true},
		{"sparse-destination-is-not-free-space", spaceInventory(1, 90*gib, 90*gib, 0, 100), spaceInventory(1, 90*gib, 4096, 60*gib, 1), true},
		{"new-files-need-summed-growth", spaceInventory(9, 10*gib, 10*gib, 0, 100), spaceInventory(0, 0, 0, 60*gib, 100), true},
		{"replacement-needs-temporary-inode", spaceInventory(1, 4096, 4096, 0, 100), spaceInventory(1, 4096, 4096, 4096, 0), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, w, _ := fixture(t)
			w.Volume.Phase = phasePrecopyReady
			inventoryHosts(t, e, tc.source, tc.target)
			err := e.checkSpace(t.Context())
			if tc.wantError {
				require.ErrorContains(t, err, "空间/inode 不足")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSpacePreflightSharedDeviceSumsGrowthButNotTemporaryFiles(t *testing.T) {
	e, first := instanceFixture(t)
	for _, v := range first.Volumes {
		v.Phase = phasePrecopyReady
	}
	// Both volumes use one filesystem. Each replaces a 4 KiB file; the global
	// serial queue needs one temporary file and inode, rather than two.
	inventoryHosts(t, e, spaceInventory(1, 4096, 4096, 0, 100), spaceInventory(1, 4096, 4096, 4096, 1))
	require.NoError(t, e.checkSpace(t.Context()))
	// Two new files persist after copying both volumes and cannot share space.
	inventoryHosts(t, e, spaceInventory(1, 4096, 4096, 0, 100), spaceInventory(0, 0, 0, 4096, 100))
	require.ErrorContains(t, e.checkSpace(t.Context()), "空间/inode 不足")
}

func TestFilesystemRejectsMissingInventoryFromOldHelper(t *testing.T) {
	e, _, _ := fixture(t)
	e.Hosts = hostFunc(func(context.Context, Host, string) (string, error) {
		return `{"version":2,"entries":1,"bytes":0,"blockSize":4096}`, nil
	})
	_, err := e.filesystem(t.Context(), e.State.SourceConfig, "/data", "inventory")
	require.ErrorContains(t, err, "inventory 无效")
}
