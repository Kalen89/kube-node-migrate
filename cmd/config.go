package cmd

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Kalen89/kube-node-migrate/internal/nodemigrate"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

type imageConfig struct {
	Helper string `json:"helper"`
	SSHD   string `json:"sshd"`
	Rsync  string `json:"rsync"`
}

type serviceConfig struct {
	Image     string                       `json:"image"`
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

type reserveConfig struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
	Pods   *int64 `json:"pods,omitempty"`
}

type planConfig struct {
	Namespaces          []string                  `json:"namespaces"`
	ExcludeNamespaces   []string                  `json:"excludeNamespaces"`
	SSHKnownHosts       string                    `json:"sshKnownHosts,omitempty"`
	HealthChecks        []nodemigrate.HealthCheck `json:"healthChecks"`
	Helper              serviceConfig             `json:"helper"`
	SSHD                serviceConfig             `json:"sshd"`
	Rsync               serviceConfig             `json:"rsync"`
	CopyBinary          string                    `json:"pvMigrate,omitempty"`
	Archive             string                    `json:"pvMigrateArchive,omitempty"`
	FilesystemHelper    string                    `json:"filesystemHelper,omitempty"`
	Reserve             reserveConfig             `json:"reserve"`
	PVConcurrency       *int                      `json:"pvConcurrency"`
	RsyncBandwidthLimit *int64                    `json:"rsyncBandwidthLimit"`
	ParallelDigest      *bool                     `json:"parallelDigest"`
	Precopy             *bool                     `json:"precopy"`
	PrecopyRounds       *int                      `json:"precopyRounds"`
	PrecopyThreshold    string                    `json:"precopyThreshold"`

	// Retain the original keys for existing configuration files.
	Images        *imageConfig `json:"images,omitempty"`
	HelperImage   string       `json:"helperImage,omitempty"`
	SSHDImage     string       `json:"sshdImage,omitempty"`
	RsyncImage    string       `json:"rsyncImage,omitempty"`
	ReserveCPU    string       `json:"reserveCPU,omitempty"`
	ReserveMemory string       `json:"reserveMemory,omitempty"`
	ReservePods   *int64       `json:"reservePods,omitempty"`
}

// Use the same static defaults for CLI flags and generated configuration.
// Host-specific credentials and paths are resolved only when executing a plan.
func defaultPlanConfig() planConfig {
	reservePods, concurrency, bandwidth := int64(2), 1, int64(0)
	precopy, rounds, parallelDigest := true, 3, false
	resources := nodemigrate.DefaultServiceResources()
	return planConfig{
		Namespaces: []string{}, ExcludeNamespaces: []string{"kube-system"},
		Helper:        serviceConfig{Image: "docker.io/rancher/mirrored-library-busybox:1.36.1", Resources: resources.DeepCopy()},
		SSHD:          serviceConfig{Image: "docker.io/utkuozdemir/pv-migrate-sshd:v3.7.0", Resources: resources.DeepCopy()},
		Rsync:         serviceConfig{Image: "docker.io/utkuozdemir/pv-migrate-rsync:v3.7.0", Resources: resources.DeepCopy()},
		Reserve:       reserveConfig{CPU: "200m", Memory: "256Mi", Pods: &reservePods},
		PVConcurrency: &concurrency, RsyncBandwidthLimit: &bandwidth,
		ParallelDigest: &parallelDigest, Precopy: &precopy, PrecopyRounds: &rounds, PrecopyThreshold: "0s",
		HealthChecks: []nodemigrate.HealthCheck{},
	}
}

func applyPlanConfig(cmd *cobra.Command, o *nodemigrate.Options, path string) error {
	if path == "" {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return fmt.Errorf("配置必须是不超过 1 MiB 的普通 YAML/JSON 文件")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var cfg planConfig
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return fmt.Errorf("迁移配置无效: %w", err)
	}
	var oldImages imageConfig
	if cfg.Images != nil {
		oldImages = *cfg.Images
	}
	for _, field := range []struct {
		flag  string
		dst   *string
		value string
		file  bool
	}{
		{"ssh-known-hosts", &o.SSHKnownHosts, cfg.SSHKnownHosts, true},
		{"helper-image", &o.HelperImage, cmp.Or(cfg.Helper.Image, oldImages.Helper, cfg.HelperImage), false},
		{"sshd-image", &o.SSHDImage, cmp.Or(cfg.SSHD.Image, oldImages.SSHD, cfg.SSHDImage), false},
		{"rsync-image", &o.RsyncImage, cmp.Or(cfg.Rsync.Image, oldImages.Rsync, cfg.RsyncImage), false},
		{"pv-migrate", &o.CopyBinary, cfg.CopyBinary, true}, {"pv-migrate-archive", &o.Archive, cfg.Archive, true}, {"filesystem-helper", &o.FilesystemHelper, cfg.FilesystemHelper, true},
		{"reserve-cpu", &o.ReserveCPU, cmp.Or(cfg.Reserve.CPU, cfg.ReserveCPU), false},
		{"reserve-memory", &o.ReserveMemory, cmp.Or(cfg.Reserve.Memory, cfg.ReserveMemory), false},
	} {
		if cmd.Flags().Changed(field.flag) || field.value == "" {
			continue
		}
		value := field.value
		if field.file && !filepath.IsAbs(value) {
			value = filepath.Join(filepath.Dir(path), value)
		}
		*field.dst = value
	}
	o.HelperResources = cfg.Helper.Resources.DeepCopy()
	o.SSHDResources = cfg.SSHD.Resources.DeepCopy()
	o.RsyncResources = cfg.Rsync.Resources.DeepCopy()
	if cfg.Namespaces != nil && !cmd.Flags().Changed("namespace") {
		o.Namespaces = cfg.Namespaces
	}
	if cfg.ExcludeNamespaces != nil && !cmd.Flags().Changed("exclude-namespace") {
		o.ExcludeNamespaces = cfg.ExcludeNamespaces
	}
	if pods := cmp.Or(cfg.Reserve.Pods, cfg.ReservePods); pods != nil && !cmd.Flags().Changed("reserve-pods") {
		o.ReservePods = *pods
	}
	if cfg.PVConcurrency != nil && !cmd.Flags().Changed("pv-concurrency") {
		o.PVConcurrency = *cfg.PVConcurrency
	}
	if cfg.RsyncBandwidthLimit != nil && !cmd.Flags().Changed("rsync-bandwidth-limit") {
		o.RsyncBandwidthLimit = *cfg.RsyncBandwidthLimit
	}
	if cfg.ParallelDigest != nil && !cmd.Flags().Changed("parallel-digest") {
		o.ParallelDigest = *cfg.ParallelDigest
	}
	if cfg.Precopy != nil && !cmd.Flags().Changed("precopy") {
		o.Precopy = *cfg.Precopy
	}
	if cfg.PrecopyRounds != nil && !cmd.Flags().Changed("precopy-rounds") {
		o.PrecopyRounds = *cfg.PrecopyRounds
	}
	if cfg.PrecopyThreshold != "" && !cmd.Flags().Changed("precopy-threshold") {
		o.PrecopyThreshold, err = time.ParseDuration(cfg.PrecopyThreshold)
		if err != nil {
			return fmt.Errorf("precopyThreshold 无效: %w", err)
		}
	}
	o.HealthChecks = cfg.HealthChecks
	return nil
}
