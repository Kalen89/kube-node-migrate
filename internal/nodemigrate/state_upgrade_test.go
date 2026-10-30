package nodemigrate

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionSixAndSevenWorkloadCheckpointsUpgradeWithoutChangingPolicy(t *testing.T) {
	for _, version := range []int{6, 7} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			e, _ := instanceFixture(t)
			e.State.Version = version
			e.State.Options.HelperResources = nil
			e.State.Options.SSHDResources = nil
			e.State.Options.RsyncResources = nil
			path := filepath.Join(t.TempDir(), "state.json")
			require.NoError(t, SaveState(path, e.State))
			loaded, err := LoadState(path)
			require.NoError(t, err)
			require.Equal(t, StateVersion(), loaded.Version)
			require.False(t, loaded.Options.ParallelDigest)
			require.Equal(t, e.State.Options.Precopy, loaded.Options.Precopy)
			require.Equal(t, e.State.Options.PrecopyRounds, loaded.Options.PrecopyRounds)
			require.Equal(t, e.State.Options.PrecopyThreshold, loaded.Options.PrecopyThreshold)
			defaults := DefaultServiceResources()
			require.Equal(t, &defaults, loaded.Options.HelperResources)
			require.Equal(t, &defaults, loaded.Options.SSHDResources)
			require.Equal(t, &defaults, loaded.Options.RsyncResources)
		})
	}
}
