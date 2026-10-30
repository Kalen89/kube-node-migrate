// Package cmd defines the migration subcommands and their shared execution flow.
package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/client-go/rest"

	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/Kalen89/kube-node-migrate/internal/nodemigrate"
)

// New builds a fresh command tree; it never connects until an action is executed.
func New(version string) *cobra.Command {
	root := &cobra.Command{
		Use: "kube-node-migrate", Version: version,
		Short: "Migrate selected workload Pods and PVCs; node retirement is managed by the user",
		Args:  cobra.NoArgs, SilenceUsage: true, SilenceErrors: true,
	}
	for _, action := range []string{"plan", "apply", "resume", "status", "verify"} {
		root.AddCommand(actionCommand(action))
	}
	root.AddCommand(newConfigCommand())
	return root
}

func actionCommand(action string) *cobra.Command {
	var o nodemigrate.Options
	var downtime, jsonOutput bool
	var configPath string
	cmd := &cobra.Command{Use: action, Short: "Workload migration " + action, Args: cobra.NoArgs}
	f := cmd.Flags()
	f.StringVar(&o.StatePath, "state", ".kube-node-migrate/state.json", "Private checkpoint file; its directory is created automatically (0700)")
	f.BoolVar(&jsonOutput, "json", false, "Print a redacted JSON summary")
	if action == "plan" {
		defaults := defaultPlanConfig()
		o.Mode = nodemigrate.ModeWorkloads
		f.StringSliceVarP(&o.Namespaces, "namespace", "n", nil, "Optional namespace allowlist; when set it takes precedence over exclusions")
		f.StringSliceVar(&o.ExcludeNamespaces, "exclude-namespace", nil, "Additional namespaces to exclude when no allowlist is set; kube-system is excluded by default")
		f.StringVar(&o.SSHKnownHosts, "ssh-known-hosts", "", "Optional trusted SSH known_hosts file; host key verification is always required")
		f.StringVar(&configPath, "config", "", "YAML/JSON configuration for namespace scope, images, capacity reserve and health checks")
		f.StringVar(&o.ReserveCPU, "reserve-cpu", defaults.Reserve.CPU, "CPU request reserve for temporary migration helpers")
		f.StringVar(&o.ReserveMemory, "reserve-memory", defaults.Reserve.Memory, "Memory request reserve for temporary migration helpers")
		f.Int64Var(&o.ReservePods, "reserve-pods", *defaults.Reserve.Pods, "Spare target Pod slots (minimum 2)")
		f.StringVar(&o.Source, "source", "", "Source Node name, from kubectl get nodes")
		f.StringVar(&o.Target, "target", "", "Ready schedulable target Node in the same cluster")
		f.StringVar(&o.Kubeconfig, "kubeconfig", clientcmd.RecommendedHomeFile, "Kubeconfig file; otherwise detect KUBECONFIG, user config or K3s config")
		f.StringVar(&o.Context, "context", "", "Kubernetes context (default: current context)")
		f.StringVar(&o.SSHUser, "ssh-user", "root", "SSH user on both nodes; non-root requires sudo -n")
		f.IntVar(&o.SSHPort, "ssh-port", 22, "SSH port on both nodes")
		f.StringVar(&o.SSHKey, "ssh-key", "", "SSH identity file; default uses ssh-agent/config")
		f.StringVar(&o.CopyBinary, "pv-migrate", "", "Absolute path to pv-migrate v3.7.0")
		f.StringVar(&o.Archive, "pv-migrate-archive", "", "Offline v3.7.0 archive, verified against embedded SHA-256")
		f.StringVar(&o.FilesystemHelper, "filesystem-helper", "", "Local Linux Go filesystem helper (default: kube-node-fs-linux-ARCH next to this executable)")
		f.StringVar(&o.HelperImage, "helper-image", defaults.Helper.Image, "Volume binding helper image")
		f.StringVar(&o.SSHDImage, "sshd-image", defaults.SSHD.Image, "SSH helper image")
		f.StringVar(&o.RsyncImage, "rsync-image", defaults.Rsync.Image, "Rsync helper image")
		f.DurationVar(&o.WaitTimeout, "wait-timeout", 10*time.Minute, "Timeout per Kubernetes transition")
		f.DurationVar(&o.CopyTimeout, "copy-timeout", 12*time.Hour, "Timeout per volume copy/digest")
		f.IntVar(&o.PVConcurrency, "pv-concurrency", *defaults.PVConcurrency, "Serial instance migration; must remain 1")
		f.Int64Var(&o.RsyncBandwidthLimit, "rsync-bandwidth-limit", *defaults.RsyncBandwidthLimit, "Per-rsync bandwidth limit in KiB/s (0: unlimited)")
		f.BoolVar(&o.ParallelDigest, "parallel-digest", *defaults.ParallelDigest, "Read source and target digests concurrently after final sync; default serial for shared storage")
		f.BoolVar(&o.Precopy, "precopy", *defaults.Precopy, "Copy while the source runs; final stopped-source sync and digest verification remain mandatory")
		f.IntVar(&o.PrecopyRounds, "precopy-rounds", *defaults.PrecopyRounds, "Maximum online rounds per pending volume per apply/resume attempt (1-20)")
		f.DurationVar(&o.PrecopyThreshold, "precopy-threshold", 0, "0: complete the configured successful rounds; otherwise require a successful rsync container runtime within this threshold (up to 1h); not a downtime guarantee")
	}
	if action == "apply" || action == "resume" {
		f.BoolVar(&downtime, "allow-downtime", false, "Accept temporary downtime of selected workload instances")
	}
	cmd.RunE = func(cmd *cobra.Command, _ []string) (resultErr error) {
		if o.StatePath == "" {
			return errors.New("必须指定 --state")
		}
		path, err := filepath.Abs(o.StatePath)
		if err != nil {
			return err
		}
		o.StatePath = path
		if action == "plan" {
			if err := applyPlanConfig(cmd, &o, configPath); err != nil {
				return err
			}
			if err := absolutePaths(&o); err != nil {
				return err
			}
			if err := nodemigrate.ValidateOptions(o); err != nil {
				return err
			}
			if !f.Changed("kubeconfig") {
				o.Kubeconfig, err = detectKubeconfig(os.Getenv("KUBECONFIG"), clientcmd.RecommendedHomeFile, "/etc/rancher/k3s/k3s.yaml")
				if err != nil {
					return err
				}
				o.Kubeconfig, err = filepath.Abs(o.Kubeconfig)
				if err != nil {
					return err
				}
			}
		}
		if action != "plan" {
			// status remains purely local and does not need Kubernetes/SSH credentials.
			state, err := nodemigrate.LoadState(path)
			if err != nil {
				return err
			}
			if action == "status" {
				return printState(cmd, state, jsonOutput)
			}
		}
		lock, err := nodemigrate.OpenState(path)
		if err != nil {
			return err
		}
		defer func() { _ = lock.Close() }()
		engine := &nodemigrate.Engine{Hosts: nodemigrate.SSHHosts{}, Output: cmd.ErrOrStderr(), Save: nodemigrate.NewCheckpointWriter(path)}
		if action == "plan" {
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				return errors.New("状态文件已存在或无法检查，请使用新文件或 resume")
			}
		} else {
			engine.State, err = nodemigrate.LoadState(path)
			if err != nil {
				return err
			}
			o = engine.State.Options
			o.StatePath = path
			engine.State.Options = o
			if err = nodemigrate.CheckExecutionState(engine.State); err != nil {
				return err
			}
		}
		logFile, err := nodemigrate.OpenLog(path)
		if err != nil {
			return err
		}
		engine.Output = io.MultiWriter(logFile, cmd.ErrOrStderr())
		defer func() {
			// The checkpoint writer publishes reports after persisting transaction changes.
			// Do not publish newer in-memory state if checkpoint persistence failed.
			if resultErr != nil {
				_, _ = fmt.Fprintf(engine.Output, "[%s] 操作失败 action=%s error=%v；检查日志 %s.log 与报告 %s.report.json\n", time.Now().UTC().Format(time.RFC3339), action, resultErr, path, path)
			}
			resultErr = errors.Join(resultErr, logFile.Close())
		}()
		if _, err := fmt.Fprintf(engine.Output, "[%s] 操作开始 action=%s state=%s\n", time.Now().UTC().Format(time.RFC3339), action, path); err != nil {
			return err
		}
		loading := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(&clientcmd.ClientConfigLoadingRules{ExplicitPath: o.Kubeconfig}, &clientcmd.ConfigOverrides{CurrentContext: o.Context})
		restConfig, err := loading.ClientConfig()
		if err != nil {
			return err
		}
		restConfig.Timeout = 30 * time.Second
		engine.Client, err = client.New(restConfig, client.Options{Scheme: scheme.Scheme})
		if err != nil {
			return err
		}
		if o.Mode == nodemigrate.ModeWorkloads {
			probeConfig := rest.CopyConfig(restConfig)
			probeConfig.Impersonate = rest.ImpersonationConfig{UserName: "system:serviceaccount:kube-system:statefulset-controller", Groups: []string{"system:masters"}}
			engine.AdmissionProbe, err = client.New(probeConfig, client.Options{Scheme: scheme.Scheme})
			if err != nil {
				return err
			}
			engine.AdmissionProbes = newAdmissionProbeProvider(probeConfig, engine.Client, engine.AdmissionProbe)
		}
		switch action {
		case "plan":
			if _, err = engine.Plan(cmd.Context(), o); err == nil {
				err = engine.Preflight(cmd.Context())
			}
			if err == nil {
				err = engine.Save(engine.State)
			}
		case "verify":
			err = engine.Verify(cmd.Context())
		default:
			if !downtime {
				return errors.New("请检查迁移计划并指定 --allow-downtime")
			}
			if nodemigrate.HasPendingVolumes(engine.State) {
				if err = nodemigrate.PrepareCopyTool(cmd.Context(), &engine.State.Options); err != nil {
					return err
				}
				if err = engine.Save(engine.State); err != nil {
					return err
				}
			}
			engine.UseDefaultCopier()
			err = engine.Apply(cmd.Context(), true)
		}
		if err != nil {
			if engine.State == nil {
				return err
			}
			if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
				return fmt.Errorf("计划未完成，尚未保存可执行检查点: %w", err)
			}
			return fmt.Errorf("阶段 %s，状态保留在 %s: %w", engine.State.Phase, path, err)
		}
		if _, err := fmt.Fprintf(engine.Output, "[%s] 操作完成 action=%s phase=%s log=%s.log report=%s.report.json\n", time.Now().UTC().Format(time.RFC3339), action, engine.State.Phase, path, path); err != nil {
			return err
		}
		return printState(cmd, engine.State, jsonOutput)
	}
	return cmd
}

func absolutePaths(o *nodemigrate.Options) error {
	for _, p := range []*string{&o.Kubeconfig, &o.CopyBinary, &o.Archive, &o.SSHKey, &o.SSHKnownHosts, &o.FilesystemHelper} {
		if *p == "" {
			continue
		}
		path, err := filepath.Abs(*p)
		if err != nil {
			return err
		}
		*p = path
	}
	return nil
}

func printState(cmd *cobra.Command, s *nodemigrate.State, asJSON bool) error {
	type item struct{ Kind, Namespace, Name, Phase, PVC, VolumePhase string }
	result := struct {
		ID, Source, Target, Phase, SourcePolicy string
		Workloads                               []item
	}{ID: s.ID, Source: s.Source.Name, Target: s.Target.Name, Phase: s.Phase, SourcePolicy: s.Options.SourcePolicy}
	for _, w := range s.Workloads {
		i := item{Kind: w.Kind, Namespace: w.Namespace, Name: w.Name, Phase: w.Phase}
		if w.Instance != nil {
			i.Name += "/" + w.Instance.Pod.Name
		}
		if w.Volume != nil {
			i.PVC, i.VolumePhase = w.Volume.PVC.Name, w.Volume.Phase
		}
		result.Workloads = append(result.Workloads, i)
		for _, v := range w.Volumes {
			result.Workloads = append(result.Workloads, item{Kind: w.Kind, Namespace: w.Namespace, Name: i.Name, Phase: w.Phase, PVC: v.PVC.Name, VolumePhase: v.Phase})
		}
	}
	if asJSON {
		return json.NewEncoder(cmd.OutOrStdout()).Encode(nodemigrate.Summary(s))
	}
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Operation: %s\nSource: %s\nTarget: %s\nPhase: %s\n", result.ID, result.Source, result.Target, result.Phase); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Namespaces: %s\nState: %s\n", strings.Join(s.Options.Namespaces, ", "), s.Options.StatePath); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Extra HTTP health checks: %d\n", len(s.Options.HealthChecks)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "PV concurrency: %d\nPer-rsync bandwidth limit: %d KiB/s (0: unlimited)\n", s.Options.PVConcurrency, s.Options.RsyncBandwidthLimit); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Online precopy: %t (rounds per attempt: %d, threshold: %s)\n", s.Options.Precopy, s.Options.PrecopyRounds, s.Options.PrecopyThreshold); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Parallel final digests: %t\n", s.Options.ParallelDigest); err != nil {
		return err
	}
	if s.Capacity != nil {
		data, err := json.Marshal(s.Capacity.Required)
		if err != nil {
			return err
		}
		if _, err = fmt.Fprintf(cmd.OutOrStdout(), "Required target resources (including reserve): %s\n", data); err != nil {
			return err
		}
	}
	for _, w := range result.Workloads {
		if _, err := fmt.Fprintf(cmd.OutOrStdout(), "%s/%s/%s\t%s\tPVC=%s %s\n", w.Namespace, w.Kind, w.Name, w.Phase, w.PVC, w.VolumePhase); err != nil {
			return err
		}
	}
	return nil
}
