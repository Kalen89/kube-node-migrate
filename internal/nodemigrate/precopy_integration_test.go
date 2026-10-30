//go:build integration

package nodemigrate

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Kalen89/kube-node-migrate/internal/filesystem"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
)

type localFilesystemHosts struct {
	Hosts
	source, target string
}

func (h localFilesystemHosts) Filesystem(ctx context.Context, node Host, _, _, mode string) (string, error) {
	path := h.source
	if node.Name == "new" {
		path = h.target
	}
	summary, err := filesystem.Scan(ctx, path, mode)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(summary)
	return string(data), err
}

// This uses real rsync with Kubernetes controllers simulated. Linux additionally
// uses real directory digests; macOS keeps the host mock because its provenance
// attributes are outside the supported Linux volume contract.
func TestPrecopyFinalSyncRepairsSameMetadataAndDeletesStaleFiles(t *testing.T) {
	rsync, err := exec.LookPath("rsync")
	if err != nil {
		t.Skip("integration test requires rsync")
	}
	e, w, _ := fixture(t)
	enableTestPrecopy(e)
	simulateControllers(e, w)
	source, target := t.TempDir(), t.TempDir()
	write := func(name, contents string) {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(source, name), []byte(contents), 0o600))
	}
	write("data", "AAAA")
	write("deleted", "obsolete")
	write("old-name", "renamed")
	// rsync can preserve whole-second timestamps even on older implementations.
	stamp := time.Unix(1700000000, 0)
	require.NoError(t, os.Chtimes(filepath.Join(source, "data"), stamp, stamp))
	if runtime.GOOS == "linux" {
		e.Hosts = localFilesystemHosts{e.Hosts, source, target}
	}
	var phases []string
	e.Copier = copierFunc(func(ctx context.Context, state *State, current *Workload) error {
		phases = append(phases, current.Volume.Phase)
		flags := copyArgs(state, current, "unused-for-local-rsync")
		args := []string{"-a"}
		for i, flag := range flags {
			if flag == "--rsync-extra-args" {
				args = append(args, strings.Fields(flags[i+1])...)
			}
			if flag == "--dest-delete-extraneous-files" {
				args = append(args, "--delete")
			}
		}
		args = append(args, source+"/", target+"/")
		out, err := exec.CommandContext(ctx, rsync, args...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("rsync: %w: %s", err, out)
		}
		if current.Volume.Phase == phasePrecopying {
			// The online copy now contains stale bytes with identical size/mtime.
			write("data", "BBBB")
			require.NoError(t, os.Chtimes(filepath.Join(source, "data"), stamp, stamp))
			require.NoError(t, os.Remove(filepath.Join(source, "deleted")))
			require.NoError(t, os.Rename(filepath.Join(source, "old-name"), filepath.Join(source, "new-name")))
			write("created", "new data")
		} else {
			var sts appsv1.StatefulSet
			require.NoError(t, e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &sts))
			require.Zero(t, *sts.Spec.Replicas)
		}
		return nil
	})
	require.NoError(t, applyLegacyComponents(e, t.Context(), true))
	require.Equal(t, []string{phasePrecopying, phaseCopying}, phases)
	contents, err := os.ReadFile(filepath.Join(target, "data"))
	require.NoError(t, err)
	require.Equal(t, "BBBB", string(contents))
	for _, name := range []string{"deleted", "old-name"} {
		_, err := os.Stat(filepath.Join(target, name))
		require.True(t, os.IsNotExist(err))
	}
	for _, name := range []string{"created", "new-name"} {
		_, err := os.Stat(filepath.Join(target, name))
		require.NoError(t, err)
	}
	require.Equal(t, w.Volume.SourceDigest, w.Volume.TargetDigest)
	require.Equal(t, phaseStarted, w.Volume.Phase)
}
