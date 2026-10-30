package nodemigrate

import (
	"errors"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
)

// ValidateOptions checks the persisted execution contract without touching a cluster.
func ValidateOptions(o Options) error {
	if strings.ContainsAny(o.SSHKnownHosts, "\r\n\x00") {
		return errors.New("SSH known_hosts 文件路径无效")
	}
	if o.Source == "" || o.Target == "" || o.Source == o.Target {
		return errors.New("必须指定两个不同的 --source / --target 节点")
	}
	if o.StatePath == "" || o.Kubeconfig == "" || o.WaitTimeout <= 0 || o.CopyTimeout <= 0 {
		return errors.New("状态文件、kubeconfig 和正数超时为必填项")
	}
	if o.SSHUser == "" || strings.ContainsAny(o.SSHUser, "@ \t\r\n") || strings.HasPrefix(o.SSHUser, "-") || o.SSHPort < 1 || o.SSHPort > 65535 {
		return errors.New("SSH 用户或端口无效")
	}
	if !oneOf(o.Mode, "", "agent", ModeWorkloads) {
		return errors.New("--mode 必须为 agent 或 workloads")
	}
	if o.Mode == ModeWorkloads {
		if !oneOf(o.SourcePolicy, "", "keep-running") || o.PVConcurrency != 1 {
			return errors.New("Pod 迁移要求 --pv-concurrency 1，不支持节点处置策略")
		}
		seen := map[string]bool{}
		for _, ns := range o.Namespaces {
			if len(validation.IsDNS1123Label(ns)) != 0 || seen[ns] {
				return errors.New("namespace 必须唯一且有效")
			}
			seen[ns] = true
		}
		for _, ns := range o.ExcludeNamespaces {
			if len(validation.IsDNS1123Label(ns)) != 0 {
				return errors.New("exclude namespace 必须有效")
			}
		}
	} else if len(o.Namespaces) != 0 || !oneOf(o.SourcePolicy, "cordon", "stop-agent") {
		return errors.New("--source-policy 必须是 cordon 或 stop-agent")
	}
	if o.PVConcurrency < 1 || o.PVConcurrency > 16 {
		return errors.New("--pv-concurrency 必须在 1 到 16 之间")
	}
	if o.RsyncBandwidthLimit < 0 || o.RsyncBandwidthLimit > 1<<30 {
		return errors.New("--rsync-bandwidth-limit 必须在 0 到 1073741824 KiB/s 之间，0 表示不限速")
	}
	if o.Precopy && (o.PrecopyRounds < 1 || o.PrecopyRounds > 20 || o.PrecopyThreshold < 0 || o.PrecopyThreshold > time.Hour) {
		return errors.New("开启 --precopy 时，--precopy-rounds 必须为 1～20，--precopy-threshold 必须为 0～1h；0 表示按成功轮数完成")
	}
	if err := validateTargetLabels(o.TargetLabels); err != nil {
		return err
	}
	if err := validateServiceResources(o); err != nil {
		return err
	}
	if _, err := capacityReserve(o); err != nil {
		return err
	}
	if err := validateHealthChecks(o.HealthChecks); err != nil {
		return err
	}
	return nil
}

// HasPendingVolumes reports whether the external copier is still required.
func HasPendingVolumes(s *State) bool {
	for _, w := range s.Workloads {
		for _, v := range workloadVolumes(w) {
			if v.Phase != phaseStarted {
				return true
			}
		}
	}
	return false
}
