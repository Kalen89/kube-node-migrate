package nodemigrate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"slices"
	"sort"
	"strings"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const operationRandomBytes = 8

// Plan inventories every active source Pod. Unsupported objects block the plan.
func (e *Engine) Plan(ctx context.Context, options Options) (*State, error) {
	if options.Mode != "" && options.Mode != ModeWorkloads {
		return nil, errors.New("工具只迁移业务 Pod/PVC，节点替换及下线由用户处理")
	}
	if options.SourcePolicy != "" || len(options.TargetLabels) != 0 {
		return nil, errors.New("节点处置策略和节点标签修改已移除，请由用户单独配置")
	}
	options.Mode = ModeWorkloads
	options.ExcludeNamespaces = append([]string{systemNamespace}, options.ExcludeNamespaces...)
	sort.Strings(options.ExcludeNamespaces)
	options.ExcludeNamespaces = slices.Compact(options.ExcludeNamespaces)
	return e.plan(ctx, options)
}

func (e *Engine) plan(ctx context.Context, options Options) (*State, error) {
	if options.Mode != ModeWorkloads || options.SourcePolicy != "" || len(options.TargetLabels) != 0 {
		return nil, errors.New("只支持业务 Pod/PVC 迁移计划")
	}
	if err := ValidateOptions(options); err != nil {
		return nil, err
	}
	normalizeServiceResources(&options)
	if options.Source == "" || options.Target == "" || options.Source == options.Target {
		return nil, errors.New("必须指定两个不同的 --source / --target 节点")
	}
	var source, target corev1.Node
	if err := e.Client.Get(ctx, types.NamespacedName{Name: options.Source}, &source); err != nil {
		return nil, err
	}
	if err := e.Client.Get(ctx, types.NamespacedName{Name: options.Target}, &target); err != nil {
		return nil, fmt.Errorf("目标节点必须先通过 join 加入集群: %w", err)
	}
	if err := validatePlanNodes(&source, &target, options.Mode); err != nil {
		return nil, err
	}
	oldConfig, err := nodeHost(&source, options)
	if err != nil {
		return nil, err
	}
	newConfig, err := nodeHost(&target, options)
	if err != nil {
		return nil, err
	}
	if oldConfig.Addr() == newConfig.Addr() {
		return nil, errors.New("源和目标指向同一主机")
	}
	var system corev1.Namespace
	if err := e.Client.Get(ctx, types.NamespacedName{Name: systemNamespace}, &system); err != nil {
		return nil, err
	}
	var random [operationRandomBytes]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, err
	}
	state := &State{Version: stateVersion, ID: hex.EncodeToString(random[:]), ClusterUID: system.UID,
		CreatedAt: time.Now().UTC(), Source: NodeIdentity{source.Name, source.UID},
		Target: NodeIdentity{target.Name, target.UID}, SourceConfig: oldConfig, TargetConfig: newConfig,
		SourceTaints: source.Spec.Taints, TargetTaints: target.Spec.Taints, Options: options, Phase: phasePlanned}
	state.SourceUnschedulable = source.Spec.Unschedulable
	if err := e.inventoryInstances(ctx, state, &source, &target); err != nil {
		return nil, err
	}
	if HasPendingVolumes(state) && state.Options.FilesystemHelper == "" {
		state.Options.FilesystemHelper, err = defaultFilesystemHelper(source.Status.NodeInfo.Architecture)
		if err != nil {
			return nil, err
		}
	}
	e.State = state
	if err := e.checkCapacity(ctx); err != nil {
		return nil, err
	}
	e.logf("迁移计划 source=%s target=%s workloads=%d volumes=%d daemonSets=%d targetLabels=%v", source.Name, target.Name, len(state.Workloads), volumeCount(state), len(state.DaemonSets), options.TargetLabels)
	return state, nil
}

func volumeCount(s *State) int {
	n := 0
	for _, w := range s.Workloads {
		n += len(workloadVolumes(w))
	}
	return n
}

func nodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func configAddressMatches(entry Host, n *corev1.Node) bool {
	for _, a := range n.Status.Addresses {
		if a.Type != corev1.NodeInternalIP {
			continue
		}
		for _, address := range []string{entry.IP, entry.IPV6} {
			actual, wanted := net.ParseIP(a.Address), net.ParseIP(address)
			if actual != nil && wanted != nil && actual.Equal(wanted) {
				return true
			}
		}
	}
	return false
}

func terminalPod(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
}

func workloadKey(w *Workload) string {
	key := w.Namespace + "/" + w.Kind + "/" + w.Name
	if w.Instance != nil {
		key += "/" + w.Instance.Pod.Name
	}
	return key
}

func replicas(value *int32) int32 {
	if value == nil {
		return 1
	}
	return *value
}

func supportedOwner(owner *metav1.OwnerReference) bool {
	return (owner.Kind == kindApp && strings.HasPrefix(owner.APIVersion, "dev.qingteng.cn/")) ||
		((owner.Kind == "Prometheus" || owner.Kind == kindAlertmanager) && owner.APIVersion == "monitoring.coreos.com/v1")
}

func (e *Engine) rejectHPA(ctx context.Context, workloads []*Workload) error {
	var hpas autoscalingv2.HorizontalPodAutoscalerList
	if err := e.Client.List(ctx, &hpas); err != nil {
		return err
	}
	for _, hpa := range hpas.Items {
		for _, w := range workloads {
			ref := hpa.Spec.ScaleTargetRef
			if hpa.Namespace == w.Namespace && ref.Kind == w.Kind && ref.Name == w.Name {
				return fmt.Errorf("%s 由 HPA 管理，须先暂停协调", workloadKey(w))
			}
		}
	}
	return nil
}

func (e *Engine) inspectClaim(ctx context.Context, pvc *corev1.PersistentVolumeClaim, source *corev1.Node) (*corev1.PersistentVolume, error) {
	if pvc.Status.Phase != corev1.ClaimBound || len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != corev1.ReadWriteOnce ||
		(pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode != corev1.PersistentVolumeFilesystem) {
		return nil, errors.New("要求已绑定的 RWO Filesystem PVC")
	}
	if pvc.Spec.Selector != nil || pvc.Spec.DataSource != nil || pvc.Spec.DataSourceRef != nil || len(pvc.OwnerReferences) != 0 {
		return nil, errors.New("带 selector、克隆来源或 owner 的 PVC 需要单独迁移策略")
	}
	var pv corev1.PersistentVolume
	if err := e.Client.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, &pv); err != nil {
		return nil, err
	}
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID || !pvOnNode(&pv, source.Name) {
		return nil, errors.New("PV 节点或绑定 UID 不匹配")
	}
	if len(pv.OwnerReferences) != 0 {
		return nil, errors.New("带 owner 的 PV 需要单独迁移策略")
	}
	if _, err := volumePath(&pv); err != nil {
		return nil, err
	}
	if pvc.Spec.StorageClassName == nil {
		return nil, errors.New("PVC 未指定 StorageClass")
	}
	if err := e.localPathClass(ctx, *pvc.Spec.StorageClassName); err != nil {
		return nil, err
	}
	return &pv, nil
}
