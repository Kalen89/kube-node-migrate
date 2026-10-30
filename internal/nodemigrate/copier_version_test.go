package nodemigrate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCopierAcceptsActualReleaseVersionOutput(t *testing.T) {
	for _, tc := range []struct {
		name, output string
		ok           bool
	}{
		{"release", "pv-migrate version 3.7.0 (commit: c29add55f5ce56174fa280f7cdb26c59492af3df)", true},
		{"prefixed", "pv-migrate version v3.7.0", true},
		{"other-version", "pv-migrate version 3.6.0", false},
		{"unrelated-program", "other-program version 3.7.0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pv-migrate")
			require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\nprintf '%s\\n' "+shellQuote(tc.output)+"\n"), 0o700))
			err := CheckCopyTool(t.Context(), Options{CopyBinary: path, SSHDImage: "example.com/sshd:v3.7.0", RsyncImage: "example.com/rsync:v3.7.0"})
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
