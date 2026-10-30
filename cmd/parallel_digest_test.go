package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Kalen89/kube-node-migrate/internal/nodemigrate"
	"github.com/stretchr/testify/require"
)

func TestParallelDigestConfigurationRespectsExplicitFlag(t *testing.T) {
	for _, tc := range []struct {
		name, config, flag string
		want               bool
	}{
		{"default", "", "", false},
		{"config enables", "parallelDigest: true\n", "", true},
		{"flag disables", "parallelDigest: true\n", "false", false},
		{"flag enables", "parallelDigest: false\n", "true", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, _, err := New("test").Find([]string{"plan"})
			require.NoError(t, err)
			if tc.flag != "" {
				require.NoError(t, plan.Flags().Set("parallel-digest", tc.flag))
			}
			value, err := plan.Flags().GetBool("parallel-digest")
			require.NoError(t, err)
			o := nodemigrate.Options{ParallelDigest: value}
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.config), 0o600))
			require.NoError(t, applyPlanConfig(plan, &o, path))
			require.Equal(t, tc.want, o.ParallelDigest)
		})
	}
}
