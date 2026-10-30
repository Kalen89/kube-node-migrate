package cmd

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Kalen89/kube-node-migrate/internal/nodemigrate"
	"github.com/stretchr/testify/require"
)

func TestConfigUsesRelativeDependencyPathsAndExplicitFlagsWin(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "migration.yaml")
	require.NoError(t, os.WriteFile(path, []byte("helperImage: configured:v1\npvMigrateArchive: pv.tar.gz\nreserveMemory: 1Gi\nhealthChecks:\n  - name: login\n    url: https://example.com/\n"), 0o600))
	root := New("test")
	plan, _, err := root.Find([]string{"plan"})
	require.NoError(t, err)
	require.NoError(t, plan.Flags().Parse([]string{"--helper-image", "explicit:v2"}))
	opts := nodemigrate.Options{HelperImage: "explicit:v2"}
	require.NoError(t, applyPlanConfig(plan, &opts, path))
	require.Equal(t, "explicit:v2", opts.HelperImage)
	require.Equal(t, filepath.Join(dir, "pv.tar.gz"), opts.Archive)
	require.Equal(t, "1Gi", opts.ReserveMemory)
	require.Empty(t, opts.TargetLabels)
	require.Len(t, opts.HealthChecks, 1)
}

func TestImageConfigPrecedenceAndCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config string
		flags  []string
		want   imageConfig
	}{
		{
			name:   "omitted-keeps-defaults",
			config: "images: {}\n",
			want:   imageConfig{"helper:default", "sshd:default", "rsync:default"},
		},
		{
			name:   "nested-images",
			config: "images:\n  helper: helper:new\n  sshd: sshd:new\n  rsync: rsync:new\n",
			want:   imageConfig{"helper:new", "sshd:new", "rsync:new"},
		},
		{
			name:   "legacy-top-level",
			config: "helperImage: helper:old\nsshdImage: sshd:old\nrsyncImage: rsync:old\n",
			want:   imageConfig{"helper:old", "sshd:old", "rsync:old"},
		},
		{
			name:   "nested-overrides-legacy-per-image",
			config: "helperImage: helper:old\nsshdImage: sshd:old\nimages:\n  helper: helper:new\n  rsync: rsync:new\n",
			want:   imageConfig{"helper:new", "sshd:old", "rsync:new"},
		},
		{
			name:   "partial-and-empty-preserve-defaults",
			config: "images:\n  helper: ''\n  sshd: sshd:new\n",
			want:   imageConfig{"helper:default", "sshd:new", "rsync:default"},
		},
		{
			name:   "service-overrides-older-forms",
			config: "helperImage: helper:old\nimages:\n  helper: helper:grouped\n  sshd: sshd:grouped\nhelper:\n  image: helper:service\nrsync:\n  image: rsync:service\n",
			want:   imageConfig{"helper:service", "sshd:grouped", "rsync:service"},
		},
		{
			name:   "explicit-flags-win",
			config: "helperImage: helper:old\nimages:\n  helper: helper:new\n  sshd: sshd:new\n  rsync: rsync:new\nhelper:\n  image: helper:service\nsshd:\n  image: sshd:service\nrsync:\n  image: rsync:service\n",
			flags:  []string{"--helper-image", "helper:flag", "--sshd-image", "sshd:flag", "--rsync-image", "rsync:flag"},
			want:   imageConfig{"helper:flag", "sshd:flag", "rsync:flag"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "images.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.config), 0o600))
			plan, _, err := New("test").Find([]string{"plan"})
			require.NoError(t, err)
			require.NoError(t, plan.Flags().Parse(tc.flags))
			o := nodemigrate.Options{HelperImage: "helper:default", SSHDImage: "sshd:default", RsyncImage: "rsync:default"}
			for _, field := range []struct {
				flag string
				dst  *string
			}{{"helper-image", &o.HelperImage}, {"sshd-image", &o.SSHDImage}, {"rsync-image", &o.RsyncImage}} {
				if plan.Flags().Changed(field.flag) {
					*field.dst, err = plan.Flags().GetString(field.flag)
					require.NoError(t, err)
				}
			}
			require.NoError(t, applyPlanConfig(plan, &o, path))
			require.Equal(t, tc.want, imageConfig{o.HelperImage, o.SSHDImage, o.RsyncImage})
		})
	}
}

func TestImageConfigRejectsUnknownNestedKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "images.yaml")
	require.NoError(t, os.WriteFile(path, []byte("images:\n  rysnc: typo:v1\n"), 0o600))
	plan, _, err := New("test").Find([]string{"plan"})
	require.NoError(t, err)
	require.ErrorContains(t, applyPlanConfig(plan, &nodemigrate.Options{}, path), "配置无效")
}

func TestServiceConfigPassesResourcesAndRejectsTypos(t *testing.T) {
	path := filepath.Join(t.TempDir(), "services.yaml")
	require.NoError(t, os.WriteFile(path, []byte("helper:\n  resources:\n    requests: {cpu: 200m}\nsshd:\n  resources:\n    limits: {memory: 1Gi}\nrsync:\n  resources:\n    requests: {memory: 256Mi}\n"), 0o600))
	plan, _, err := New("test").Find([]string{"plan"})
	require.NoError(t, err)
	var o nodemigrate.Options
	require.NoError(t, applyPlanConfig(plan, &o, path))
	require.Equal(t, "200m", o.HelperResources.Requests.Cpu().String())
	require.Equal(t, "1Gi", o.SSHDResources.Limits.Memory().String())
	require.Equal(t, "256Mi", o.RsyncResources.Requests.Memory().String())
	require.NoError(t, os.WriteFile(path, []byte("rsync:\n  resource: {}\n"), 0o600))
	require.ErrorContains(t, applyPlanConfig(plan, &o, path), "配置无效")
}

func TestReserveConfigPrecedenceAndCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, config string
		flags        []string
		cpu, memory  string
		pods         int64
	}{
		{"defaults", "{}", nil, "200m", "256Mi", 2},
		{"legacy", "reserveCPU: 300m\nreserveMemory: 1Gi\nreservePods: 3\n", nil, "300m", "1Gi", 3},
		{"grouped", "reserve:\n  cpu: 400m\n  memory: 2Gi\n  pods: 4\n", nil, "400m", "2Gi", 4},
		{"partial", "reserve:\n  cpu: 300m\n", nil, "300m", "256Mi", 2},
		{"grouped-overrides-legacy", "reserveCPU: 300m\nreserveMemory: 1Gi\nreservePods: 3\nreserve:\n  cpu: 400m\n  pods: 4\n", nil, "400m", "1Gi", 4},
		{"flags-win", "reserve:\n  cpu: 400m\n  memory: 2Gi\n  pods: 4\n", []string{"--reserve-cpu", "500m", "--reserve-memory", "3Gi", "--reserve-pods", "5"}, "500m", "3Gi", 5},
		{"explicit-zero-does-not-use-legacy", "reservePods: 4\nreserve:\n  pods: 0\n", nil, "200m", "256Mi", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "reserve.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.config), 0o600))
			plan, _, err := New("test").Find([]string{"plan"})
			require.NoError(t, err)
			require.NoError(t, plan.Flags().Parse(tc.flags))
			var o nodemigrate.Options
			o.ReserveCPU, err = plan.Flags().GetString("reserve-cpu")
			require.NoError(t, err)
			o.ReserveMemory, err = plan.Flags().GetString("reserve-memory")
			require.NoError(t, err)
			o.ReservePods, err = plan.Flags().GetInt64("reserve-pods")
			require.NoError(t, err)
			require.NoError(t, applyPlanConfig(plan, &o, path))
			require.Equal(t, tc.cpu, o.ReserveCPU)
			require.Equal(t, tc.memory, o.ReserveMemory)
			require.Equal(t, tc.pods, o.ReservePods)
		})
	}
}

func TestInvalidReserveConfigFailsBeforeOpeningState(t *testing.T) {
	for _, tc := range []struct{ name, config string }{
		{"unknown-key", "reserve: {memroy: 1Gi}\n"},
		{"invalid-pod-type", "reserve: {pods: many}\n"},
		{"invalid-cpu", "reserve: {cpu: invalid}\n"},
		{"too-few-pod-slots", "reserve: {pods: 1}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "reserve.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.config), 0o600))
			root := New("test")
			stateDir := filepath.Join(dir, "private")
			root.SetArgs([]string{"plan", "--source", "old", "--target", "new", "--config", path, "--state", filepath.Join(stateDir, "state.json")})
			require.Error(t, root.Execute())
			_, err := os.Stat(stateDir)
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestExampleConfigsUseSupportedImageSettings(t *testing.T) {
	paths, err := filepath.Glob("../examples/*.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	for _, path := range paths {
		if filepath.Base(path) == "demo.yaml" {
			continue // This is a Kubernetes manifest, not a migration configuration.
		}
		t.Run(filepath.Base(path), func(t *testing.T) {
			plan, _, err := New("test").Find([]string{"plan"})
			require.NoError(t, err)
			require.NoError(t, applyPlanConfig(plan, &nodemigrate.Options{}, path))
		})
	}
}

func TestWorkloadsConfigUsesDedicatedKnownHostsAndExplicitNamespace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "workloads.yaml")
	require.NoError(t, os.WriteFile(path, []byte("namespaces: [prod]\nsshKnownHosts: ./trusted_hosts\n"), 0o600))
	plan, _, err := New("test").Find([]string{"plan"})
	require.NoError(t, err)
	o := nodemigrate.Options{}
	require.NoError(t, applyPlanConfig(plan, &o, path))
	require.Nil(t, plan.Flags().Lookup("mode"))
	require.Equal(t, []string{"prod"}, o.Namespaces)
	require.Nil(t, plan.Flags().Lookup("source-policy"))
	require.Equal(t, filepath.Join(dir, "trusted_hosts"), o.SSHKnownHosts)
	require.NoError(t, plan.Flags().Parse([]string{"--namespace", "other"}))
	o.Namespaces = []string{"other"}
	require.NoError(t, applyPlanConfig(plan, &o, path))
	require.Equal(t, []string{"other"}, o.Namespaces)
}

func TestWorkloadsModeRejectsInvalidScopeBeforeOpeningState(t *testing.T) {
	for _, tc := range []struct {
		name  string
		flags []string
	}{{"invalid-allowlist", []string{"--namespace", "bad/name"}}, {"invalid-blocklist", []string{"--exclude-namespace", "bad/name"}}, {"parallel", []string{"--namespace", "prod", "--pv-concurrency", "2"}}} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "private", "state.json")
			cmd := New("test")
			args := []string{"plan", "--source", "old", "--target", "new", "--state", path}
			cmd.SetArgs(append(args, tc.flags...))
			require.Error(t, cmd.Execute())
			_, err := os.Stat(filepath.Dir(path))
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestConfigRejectsUnknownKeysAndUnsafeLabelsBeforeConnecting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	require.NoError(t, os.WriteFile(path, []byte("helprImage: typo:v1\n"), 0o600))
	root := New("test")
	root.SetArgs([]string{"plan", "--source", "old", "--target", "new", "--namespace", "prod", "--state", filepath.Join(t.TempDir(), "state.json"), "--config", path})
	require.ErrorContains(t, root.Execute(), "配置无效")
	root = New("test")
	root.SetArgs([]string{"plan", "--source", "old", "--target", "new", "--namespace", "prod", "--state", filepath.Join(t.TempDir(), "state.json"), "--target-label", "kubernetes.io/hostname=spoofed"})
	require.ErrorContains(t, root.Execute(), "unknown flag")
}

func TestConfigRejectsRemovedNodeLifecycleSettings(t *testing.T) {
	for _, input := range []string{"mode: agent\n", "sourcePolicy: stop-agent\n", "targetLabels: {example.com/role: worker}\n"} {
		t.Run(input, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "migration.yaml")
			require.NoError(t, os.WriteFile(path, []byte(input), 0o600))
			plan, _, err := New("test").Find([]string{"plan"})
			require.NoError(t, err)
			require.ErrorContains(t, applyPlanConfig(plan, &nodemigrate.Options{}, path), "配置无效")
		})
	}
}

func TestConcurrencyConfigAndExplicitFlags(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migration.yaml")
	require.NoError(t, os.WriteFile(path, []byte("pvConcurrency: 3\nrsyncBandwidthLimit: 51200\n"), 0o600))
	for _, tc := range []struct {
		name      string
		flags     []string
		parallel  int
		bandwidth int64
	}{
		{"config", nil, 3, 51200},
		{"flags", []string{"--pv-concurrency", "2", "--rsync-bandwidth-limit", "0"}, 2, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, _, err := New("test").Find([]string{"plan"})
			require.NoError(t, err)
			require.NoError(t, plan.Flags().Parse(tc.flags))
			o := nodemigrate.Options{}
			o.PVConcurrency, err = plan.Flags().GetInt("pv-concurrency")
			require.NoError(t, err)
			o.RsyncBandwidthLimit, err = plan.Flags().GetInt64("rsync-bandwidth-limit")
			require.NoError(t, err)
			require.NoError(t, applyPlanConfig(plan, &o, path))
			require.Equal(t, tc.parallel, o.PVConcurrency)
			require.Equal(t, tc.bandwidth, o.RsyncBandwidthLimit)
		})
	}
}

func TestInvalidConcurrencyFailsBeforeOpeningStateOrConnecting(t *testing.T) {
	for _, tc := range []struct{ name, flag, value string }{
		{"zero", "--pv-concurrency", "0"}, {"unbounded", "--pv-concurrency", "17"}, {"bandwidth", "--rsync-bandwidth-limit", "-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "new-directory", "state.json")
			cmd := New("test")
			cmd.SetArgs([]string{"plan", "--source", "old", "--target", "new", "--namespace", "prod", "--state", path, tc.flag, tc.value})
			require.ErrorContains(t, cmd.Execute(), tc.flag)
			_, err := os.Stat(filepath.Dir(path))
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestPrecopyDefaultsEnabledAndConfigCanDisableIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migration.yaml")
	require.NoError(t, os.WriteFile(path, []byte("precopy: false\nprecopyRounds: 5\nprecopyThreshold: 45s\n"), 0o600))
	for _, tc := range []struct {
		name            string
		flags           []string
		useConfig, want bool
	}{
		{"default", nil, false, true},
		{"disabled-by-config", nil, true, false},
		{"flag-enables", []string{"--precopy=true"}, true, true},
		{"flag-disables", []string{"--precopy=false"}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, _, err := New("test").Find([]string{"plan"})
			require.NoError(t, err)
			require.NoError(t, plan.Flags().Parse(tc.flags))
			o := nodemigrate.Options{}
			o.Precopy, err = plan.Flags().GetBool("precopy")
			require.NoError(t, err)
			o.PrecopyRounds, err = plan.Flags().GetInt("precopy-rounds")
			require.NoError(t, err)
			o.PrecopyThreshold, err = plan.Flags().GetDuration("precopy-threshold")
			require.NoError(t, err)
			if tc.useConfig {
				require.NoError(t, applyPlanConfig(plan, &o, path))
			}
			require.Equal(t, tc.want, o.Precopy)
			if tc.useConfig {
				require.Equal(t, 5, o.PrecopyRounds)
				require.Equal(t, 45*time.Second, o.PrecopyThreshold)
			} else {
				require.Equal(t, 3, o.PrecopyRounds)
				require.Zero(t, o.PrecopyThreshold)
			}
		})
	}
}
