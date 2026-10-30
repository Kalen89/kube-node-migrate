package nodemigrate

// This harness preserves historical component scenarios for shared primitives.
// It is compiled only by go test; no legacy inventory or execution entry point
// is included in the shipped binary. New behavior is tested through Plan/Apply.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (e *Engine) apply(ctx context.Context) error {
	if err := e.guard(ctx, true); err != nil {
		return err
	}
	if err := e.claimNodes(ctx); err != nil {
		return err
	}
	if err := e.stage(ctx, "target-labels", e.State.Target.Name, e.ensureTargetLabels); err != nil {
		return err
	}
	if err := e.stage(ctx, "target-daemonsets", e.State.Target.Name, e.waitTargetDaemonSets); err != nil {
		return err
	}
	if e.State.Phase == phaseValidated {
		return e.legacyValidate(ctx)
	}
	if err := e.stage(ctx, "capacity-preflight", e.State.Target.Name, e.checkCapacity); err != nil {
		return err
	}
	if !e.State.HealthBefore {
		if err := e.stage(ctx, "health-before", "", e.checkHealth); err != nil {
			return err
		}
		e.State.HealthBefore = true
		if err := e.checkpoint(); err != nil {
			return err
		}
	}
	pendingVolumes := false
	for _, w := range e.State.Workloads {
		if w.Volume != nil && w.Volume.Phase != phaseStarted {
			pendingVolumes = true
		}
	}
	if pendingVolumes {
		if e.Copier == nil {
			return errors.New("复制执行器未初始化")
		}
		if _, external := e.Copier.(*processCopier); external {
			if err := CheckCopyTool(ctx, e.State.Options); err != nil {
				return err
			}
		}
	}
	e.logf("准备目标镜像，尚未停止业务")
	if err := e.stage(ctx, "image-prewarm", e.State.Target.Name, e.warmImages); err != nil {
		return err
	}
	volumeIndex := 0
	for _, w := range e.State.Workloads {
		if w.Volume == nil {
			continue
		}
		volumeIndex++
		e.logf("数据卷进度 %d/%d pvc=%s/%s phase=%s", volumeIndex, volumeCount(e.State), w.Namespace, w.Volume.PVC.Name, w.Volume.Phase)
		if w.Volume.Phase == phaseStarted {
			e.logf("卷已完成，续跑跳过准备和复制 pvc=%s/%s", w.Namespace, w.Volume.PVC.Name)
			continue
		}
		if err := e.stage(ctx, "prepare-volume", workloadKey(w), func(ctx context.Context) error { return e.prepareVolume(ctx, w) }); err != nil {
			return fmt.Errorf("%s: %w", workloadKey(w), err)
		}
	}
	if err := e.stage(ctx, "disk-preflight", e.State.Target.Name, e.checkSpace); err != nil {
		return err
	}
	if err := e.migrateVolumes(ctx); err != nil {
		return err
	}
	e.State.Phase = phaseVolumesMoved
	if err := e.checkpoint(); err != nil {
		return err
	}
	if err := e.cordonSource(ctx); err != nil {
		return err
	}
	workloads := append([]*Workload(nil), e.State.Workloads...)
	sort.SliceStable(workloads, func(i, j int) bool {
		// Keep the operator available until its managed resources have recovered.
		a, b := strings.Contains(workloads[i].Name, "operator"), strings.Contains(workloads[j].Name, "operator")
		return !a && b
	})
	for i, w := range workloads {
		e.logf("工作负载进度 %d/%d resource=%s phase=%s", i+1, len(workloads), workloadKey(w), w.Phase)
		if err := e.stage(ctx, "move-workload", workloadKey(w), func(ctx context.Context) error { return e.moveWorkload(ctx, w) }); err != nil {
			return fmt.Errorf("%s: %w", workloadKey(w), err)
		}
	}
	e.State.Phase = phaseWorkloadsMoved
	if err := e.checkpoint(); err != nil {
		return err
	}
	if err := e.stage(ctx, "validate-workloads", "", e.legacyValidate); err != nil {
		return err
	}
	if err := e.stage(ctx, "health-after", "", e.checkHealth); err != nil {
		return err
	}
	e.State.Phase = phaseValidated
	e.logf("所有工作负载完成，旧卷保留；节点后续处置由用户决定")
	return e.checkpoint()
}

func (e *Engine) legacyValidate(ctx context.Context) error {
	if e.State != nil && e.State.Options.Mode == ModeWorkloads {
		return e.validateInstances(ctx)
	}
	if err := e.guard(ctx, false); err != nil {
		return err
	}
	for _, w := range e.State.Workloads {
		if w.Phase != phaseStarted {
			return fmt.Errorf("%s 尚未完成迁移", workloadKey(w))
		}
		if err := e.waitWorkload(ctx, w, w.Replicas == 1); err != nil {
			return err
		}
		if w.Volume == nil {
			continue
		}
		v := w.Volume
		if v.Phase != phaseStarted || v.SourceDigest == nil || v.TargetDigest == nil || *v.SourceDigest != *v.TargetDigest {
			return errors.New("业务卷缺少成功的数据校验记录")
		}
		var claim corev1.PersistentVolumeClaim
		if err := e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: v.PVC.Name}, &claim); err != nil {
			return err
		}
		if claim.Status.Phase != corev1.ClaimBound || claim.Spec.VolumeName != v.TargetPV.Name || claim.Labels[labelKey] != e.State.ID {
			return errors.New("迁移后的 PVC 绑定改变")
		}
		for _, expected := range []*corev1.PersistentVolume{&v.PV, v.TargetPV} {
			var pv corev1.PersistentVolume
			if err := e.Client.Get(ctx, types.NamespacedName{Name: expected.Name}, &pv); err != nil {
				return err
			}
			if pv.UID != expected.UID || pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
				return errors.New("新/旧 PV 身份或 Retain 策略改变")
			}
		}
		if obj, err := e.operator(ctx, w); err != nil {
			return err
		} else if obj != nil {
			if w.OperatorPaused == nil {
				return errors.New("检查 Operator 原 paused 状态缺失")
			}
			if err := e.verifyOperatorRestored(obj, w); err != nil {
				return err
			}
		}
	}
	return e.verifyEvacuated(ctx)
}

func (e *Engine) verifyEvacuated(ctx context.Context) error {
	var pods corev1.PodList
	if err := e.Client.List(ctx, &pods); err != nil {
		return err
	}
	for _, p := range pods.Items {
		if p.Spec.NodeName != e.State.Source.Name || terminalPod(&p) {
			continue
		}
		owner := metav1.GetControllerOf(&p)
		if owner == nil || owner.Kind != kindDaemonSet {
			return fmt.Errorf("源节点仍有活动 Pod %s/%s，禁止完成迁移或隔离源节点", p.Namespace, p.Name)
		}
		found := false
		for _, target := range pods.Items {
			if target.Namespace == p.Namespace && target.Spec.NodeName == e.State.Target.Name && ownedBy(&target, owner.UID) && podReady(&target) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("DaemonSet %s/%s 在目标节点未 Ready", p.Namespace, owner.Name)
		}
	}
	var pvs corev1.PersistentVolumeList
	if err := e.Client.List(ctx, &pvs); err != nil {
		return err
	}
	for _, pv := range pvs.Items {
		if pv.Status.Phase != corev1.VolumeBound {
			continue
		}
		if affinityMentionsNode(&pv, e.State.Source.Name) {
			return fmt.Errorf("检查 Bound PV %s 仍依赖源节点", pv.Name)
		}
	}
	return nil
}

func affinityMentionsNode(pv *corev1.PersistentVolume, node string) bool {
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return false
	}
	for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, r := range term.MatchExpressions {
			if r.Key != hostnameLabel {
				continue
			}
			for _, value := range r.Values {
				if value == node && r.Operator == corev1.NodeSelectorOpIn {
					return true
				}
			}
		}
	}
	return false
}

func (e *Engine) legacyPlan(ctx context.Context, options Options) (*State, error) {
	options.TargetLabels = maps.Clone(options.TargetLabels)
	if options.TargetLabels == nil {
		options.TargetLabels = map[string]string{}
	}
	if _, exists := options.TargetLabels[MigrationLabel]; !exists && options.Mode != ModeWorkloads {
		options.TargetLabels[MigrationLabel] = "true"
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
	if err := validateNodes(&source, &target); err != nil {
		return nil, err
	}
	// Validate scheduling against planned labels without mutating the Node during plan.
	if err := desiredTargetLabels(&target, options.TargetLabels); err != nil {
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
	var inventoryErr error
	if options.Mode == ModeWorkloads {
		inventoryErr = e.inventoryInstances(ctx, state, &source, &target)
	} else {
		inventoryErr = e.inventoryPods(ctx, state, &source, &target)
	}
	if inventoryErr != nil {
		return nil, inventoryErr
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

func validateNodes(source, target *corev1.Node) error {
	for _, n := range []*corev1.Node{source, target} {
		if owner := n.Annotations[labelKey]; owner != "" {
			return fmt.Errorf("节点 %s 属于未完成的迁移 %s，请使用原状态文件续跑", n.Name, owner)
		}
		if n.UID == "" || n.Labels[hostnameLabel] != n.Name {
			return errors.New("节点 UID 或 hostname 标签不符合迁移要求")
		}
		if !nodeReady(n) || n.Spec.Unschedulable || n.DeletionTimestamp != nil {
			return fmt.Errorf("节点 %s 必须 Ready、未 cordon 且未删除", n.Name)
		}
		for _, role := range []string{"control-plane", "master", "etcd"} {
			if _, ok := n.Labels["node-role.kubernetes.io/"+role]; ok {
				return fmt.Errorf("不支持 Server 节点 %s 的迁移", n.Name)
			}
		}
	}
	if source.Status.NodeInfo.Architecture == "" || source.Status.NodeInfo.Architecture != target.Status.NodeInfo.Architecture ||
		source.Status.NodeInfo.OperatingSystem != linuxOS || target.Status.NodeInfo.OperatingSystem != linuxOS {
		return errors.New("源和目标必须是相同 CPU 架构的 Linux 节点")
	}
	return nil
}

func (e *Engine) inspectWorkload(ctx context.Context, p *corev1.Pod, owner *metav1.OwnerReference) (*Workload, error) {
	if owner.Kind == "ReplicaSet" {
		var rs appsv1.ReplicaSet
		if err := e.Client.Get(ctx, types.NamespacedName{Namespace: p.Namespace, Name: owner.Name}, &rs); err != nil {
			return nil, err
		}
		if rs.UID != owner.UID {
			return nil, errors.New("ReplicaSet UID 已改变")
		}
		owner = metav1.GetControllerOf(&rs)
	}
	if owner == nil {
		return nil, fmt.Errorf("检查 Pod %s/%s 的 owner 不支持", p.Namespace, p.Name)
	}
	w := &Workload{Kind: owner.Kind, Namespace: p.Namespace, Name: owner.Name,
		UID: owner.UID, OriginalPods: []types.UID{p.UID}, Phase: phasePlanned}
	switch owner.Kind {
	case kindDeployment:
		var d appsv1.Deployment
		if err := e.Client.Get(ctx, types.NamespacedName{Namespace: p.Namespace, Name: owner.Name}, &d); err != nil {
			return nil, err
		}
		if d.UID != owner.UID {
			return nil, errors.New("检查 Deployment UID 已改变")
		}
		w.Template, w.Replicas, w.Owner = *d.Spec.Template.DeepCopy(), replicas(d.Spec.Replicas), metav1.GetControllerOf(&d)
	case kindStatefulSet:
		var s appsv1.StatefulSet
		if err := e.Client.Get(ctx, types.NamespacedName{Namespace: p.Namespace, Name: owner.Name}, &s); err != nil {
			return nil, err
		}
		if s.UID != owner.UID {
			return nil, errors.New("检查 StatefulSet UID 已改变")
		}
		if pol := s.Spec.PersistentVolumeClaimRetentionPolicy; pol != nil &&
			(pol.WhenScaled == appsv1.DeletePersistentVolumeClaimRetentionPolicyType || pol.WhenDeleted == appsv1.DeletePersistentVolumeClaimRetentionPolicyType) {
			return nil, errors.New("检查 StatefulSet PVC 删除/缩容策略必须为 Retain")
		}
		w.Template, w.Replicas, w.Owner = *s.Spec.Template.DeepCopy(), replicas(s.Spec.Replicas), metav1.GetControllerOf(&s)
	default:
		return nil, fmt.Errorf("暂不支持活动 %s %s/%s，不能静默跳过", owner.Kind, p.Namespace, owner.Name)
	}
	if w.Owner != nil && !supportedOwner(w.Owner) {
		return nil, fmt.Errorf("控制器 %s/%s 由未支持的 %s 管理", w.Namespace, w.Name, w.Owner.Kind)
	}
	if w.Replicas != 1 && !(w.Kind == kindDeployment && w.Namespace == systemNamespace) {
		return nil, fmt.Errorf("%s 当前 %d 副本，首版不自动迁移多副本控制器", workloadKey(w), w.Replicas)
	}
	return w, nil
}

func (e *Engine) inspectVolume(ctx context.Context, state *State, w *Workload, pod *corev1.Pod, source *corev1.Node) error {
	var claims []string
	for _, v := range pod.Spec.Volumes {
		if v.HostPath != nil || v.Ephemeral != nil {
			return errors.New("业务直接 hostPath / ephemeral 卷需要单独迁移方案")
		}
		if v.PersistentVolumeClaim != nil {
			claims = append(claims, v.PersistentVolumeClaim.ClaimName)
		}
	}
	if len(claims) == 0 {
		return nil
	}
	if len(claims) != 1 || w.Replicas != 1 {
		return errors.New("首版仅支持单副本、单 PVC 控制器")
	}
	var pvc corev1.PersistentVolumeClaim
	if err := e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: claims[0]}, &pvc); err != nil {
		return err
	}
	pv, err := e.inspectClaim(ctx, &pvc, source)
	if err != nil {
		return err
	}
	var consumers corev1.PodList
	if err := e.Client.List(ctx, &consumers, client.InNamespace(w.Namespace)); err != nil {
		return err
	}
	for _, other := range consumers.Items {
		if other.UID == pod.UID {
			continue
		}
		for _, v := range other.Spec.Volumes {
			if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == pvc.Name {
				return fmt.Errorf("PVC 还被 Pod %s 引用", other.Name)
			}
		}
	}
	index := len(state.Workloads)
	identifier := fmt.Sprintf("%s-%d", state.ID, index)
	w.Volume = &Volume{PVC: pvc, PV: *pv, TemporaryPVC: "km-migrate-" + identifier,
		CopyID: "km-" + identifier, Phase: phasePlanned}
	return nil
}

func (e *Engine) inventoryPods(ctx context.Context, state *State, source, target *corev1.Node) error {
	var pods corev1.PodList
	if err := e.Client.List(ctx, &pods); err != nil {
		return err
	}
	seen := make(map[string]*Workload)
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Spec.NodeName != source.Name || terminalPod(p) {
			continue
		}
		owner := metav1.GetControllerOf(p)
		if owner == nil {
			return fmt.Errorf("检查 Pod %s/%s 无控制器，不能自动迁移", p.Namespace, p.Name)
		}
		if owner.Kind == kindDaemonSet {
			state.DaemonSets = append(state.DaemonSets, WorkloadIdentity{p.Namespace, owner.Name, owner.UID})
			continue
		}
		w, err := e.inspectWorkload(ctx, p, owner)
		if err != nil {
			return err
		}
		key := workloadKey(w)
		if previous := seen[key]; previous != nil {
			previous.OriginalPods = append(previous.OriginalPods, p.UID)
			continue
		}
		placement := w.Template.Spec.DeepCopy()
		// A pin to the node being replaced moves with the workload. Other
		// selectors remain requirements on the target, never copied as labels.
		if placement.NodeSelector[hostnameLabel] == source.Name {
			placement.NodeSelector[hostnameLabel] = target.Name
		}
		if err := validatePlacement(placement, target); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		if err := e.inspectVolume(ctx, state, w, p, source); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		if w.Volume != nil && w.Template.Spec.NodeSelector[hostnameLabel] == source.Name {
			return fmt.Errorf("%s 的持久控制器固定到源 hostname，需要先提供独立的模板切换策略", key)
		}
		if w.Volume != nil && w.Owner != nil && w.Owner.Kind == kindApp {
			return fmt.Errorf("%s 的持久卷由 App 协调，须先提供停写策略", key)
		}
		if w.Kind == kindStatefulSet && w.Volume == nil && (w.Owner == nil || w.Owner.Kind != kindAlertmanager) {
			return fmt.Errorf("%s 没有 PVC，无法保证有状态数据保留", key)
		}
		seen[key] = w
		state.Workloads = append(state.Workloads, w)
	}
	if err := e.rejectHPA(ctx, state.Workloads); err != nil {
		return err
	}
	if err := e.checkEvictionBudgets(ctx, state); err != nil {
		return err
	}
	sort.Slice(state.Workloads, func(i, j int) bool {
		return workloadKey(state.Workloads[i]) < workloadKey(state.Workloads[j])
	})
	return nil
}

func (e *Engine) checkEvictionBudgets(ctx context.Context, state *State) error {
	var budgets policyv1.PodDisruptionBudgetList
	if err := e.Client.List(ctx, &budgets); err != nil {
		return err
	}
	for _, w := range state.Workloads {
		if w.Volume != nil || w.Kind != kindDeployment || !((w.Owner != nil && w.Owner.Kind == kindApp) || (w.Namespace == systemNamespace && w.Replicas > 1)) {
			continue
		}
		pods, err := e.workloadPods(ctx, w)
		if err != nil {
			return err
		}
		for _, budget := range budgets.Items {
			if budget.Namespace != w.Namespace || budget.Status.DisruptionsAllowed > 0 {
				continue
			}
			selector, err := metav1.LabelSelectorAsSelector(budget.Spec.Selector)
			if err != nil {
				return err
			}
			for _, pod := range pods {
				if pod.Spec.NodeName == state.Source.Name && selector.Matches(labels.Set(pod.Labels)) {
					return fmt.Errorf("%s 的 Eviction 被 PDB %s 阻止", workloadKey(w), budget.Name)
				}
			}
		}
	}
	return nil
}

func (e *Engine) operator(ctx context.Context, w *Workload) (*unstructured.Unstructured, error) {
	if w.Owner == nil || w.Owner.Kind == kindApp {
		return nil, nil
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.FromAPIVersionAndKind(w.Owner.APIVersion, w.Owner.Kind))
	if err := e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Owner.Name}, obj); err != nil {
		return nil, err
	}
	if obj.GetUID() != w.Owner.UID {
		return nil, errors.New("检查 Operator 父资源 UID 改变")
	}
	return obj, nil
}

func (e *Engine) pauseOperator(ctx context.Context, w *Workload) error {
	obj, err := e.operator(ctx, w)
	if err != nil || obj == nil {
		return err
	}
	value, _, err := unstructured.NestedBool(obj.Object, "spec", "paused")
	if err != nil {
		return err
	}
	if w.OperatorPaused == nil {
		w.OperatorPaused = pointer(value)
		if err = e.checkpoint(); err != nil {
			return err
		}
	}
	if value {
		return nil
	}
	if err = unstructured.SetNestedField(obj.Object, true, "spec", "paused"); err != nil {
		return err
	}
	return e.Client.Update(ctx, obj)
}

func (e *Engine) guardPaused(ctx context.Context, w *Workload) error {
	obj, err := e.operator(ctx, w)
	if err != nil || obj == nil {
		return err
	}
	value, _, err := unstructured.NestedBool(obj.Object, "spec", "paused")
	if err != nil {
		return err
	}
	if !value || w.OperatorPaused == nil {
		return errors.New("检查 Operator 未暂停或原 paused 值尚未持久化")
	}
	return nil
}

func (e *Engine) restoreOperator(ctx context.Context, w *Workload) error {
	obj, err := e.operator(ctx, w)
	if err != nil || obj == nil {
		return err
	}
	if w.OperatorPaused == nil {
		return errors.New("检查 Operator 原状态未保存")
	}
	if err = unstructured.SetNestedField(obj.Object, *w.OperatorPaused, "spec", "paused"); err != nil {
		return err
	}
	return e.Client.Update(ctx, obj)
}

func (e *Engine) verifyOperatorRestored(obj *unstructured.Unstructured, w *Workload) error {
	value, _, err := unstructured.NestedBool(obj.Object, "spec", "paused")
	if err != nil {
		return err
	}
	if w.OperatorPaused == nil || value != *w.OperatorPaused {
		return errors.New("检查 Operator paused 状态未恢复")
	}
	return nil
}

func (e *Engine) waitWorkload(ctx context.Context, w *Workload, targetOnly bool) error {
	lastLog := time.Time{}
	return e.wait(ctx, "工作负载 "+workloadKey(w)+" Ready", func(ctx context.Context) (bool, error) {
		if _, _, err := e.getWorkload(ctx, w, w.Volume != nil); err != nil {
			return false, err
		}
		pods, err := e.workloadPods(ctx, w)
		if err != nil {
			return false, err
		}
		if len(pods) != int(w.Replicas) {
			return false, nil
		}
		for i := range pods {
			p := &pods[i]
			if !podReady(p) {
				if time.Since(lastLog) >= 15*time.Second {
					reason := ""
					for _, c := range p.Status.ContainerStatuses {
						if c.State.Waiting != nil {
							reason += c.Name + ":" + c.State.Waiting.Reason + " "
						}
						if c.State.Terminated != nil {
							reason += fmt.Sprintf("%s:exit=%d ", c.Name, c.State.Terminated.ExitCode)
						}
					}
					e.logf("Pod 等待就绪 pod=%s/%s node=%s phase=%s terminating=%t containers=%s", p.Namespace, p.Name, p.Spec.NodeName, p.Status.Phase, p.DeletionTimestamp != nil, reason)
					lastLog = time.Now()
				}
				return false, nil
			}
			if targetOnly && p.Spec.NodeName != e.State.Target.Name {
				// Deployment rollouts can briefly expose only the original Ready
				// Pod before its replacement is created. Wait for its removal.
				if p.Spec.NodeName == e.State.Source.Name && slices.Contains(w.OriginalPods, p.UID) {
					return false, nil
				}
				return false, fmt.Errorf("检查 Pod %s 调度到 %s，期望 %s", p.Name, p.Spec.NodeName, e.State.Target.Name)
			}
			if !targetOnly && p.Spec.NodeName == e.State.Source.Name {
				return false, nil
			}
		}
		return true, nil
	})
}

func targetTemplate(w *Workload, node string) corev1.PodTemplateSpec {
	template := *w.Template.DeepCopy()
	if template.Spec.NodeSelector == nil {
		template.Spec.NodeSelector = make(map[string]string)
	}
	template.Spec.NodeSelector[hostnameLabel] = node
	return template
}

func (e *Engine) moveDeployment(ctx context.Context, w *Workload) error {
	obj, count, err := e.getWorkload(ctx, w, false)
	if err != nil {
		return err
	}
	if count != w.Replicas {
		return errors.New("迁移期间副本数改变")
	}
	d, ok := obj.(*appsv1.Deployment)
	if !ok {
		return errors.New("迁移对象不是 Deployment")
	}
	wanted := targetTemplate(w, e.State.Target.Name)
	if !reflect.DeepEqual(d.Spec.Template, w.Template) && !reflect.DeepEqual(d.Spec.Template, wanted) {
		return errors.New("检查 Deployment 模板发生并发变化")
	}
	if reflect.DeepEqual(d.Spec.Template, wanted) {
		return nil
	}
	d.Spec.Template = wanted
	return e.Client.Update(ctx, d)
}

func (e *Engine) moveSystemReplicas(ctx context.Context, w *Workload) error {
	if _, count, err := e.getWorkload(ctx, w, true); err != nil {
		return err
	} else if count != w.Replicas {
		return errors.New("系统副本数改变")
	}
	pods, err := e.workloadPods(ctx, w)
	if err != nil {
		return err
	}
	for i := range pods {
		p := &pods[i]
		if p.Spec.NodeName != e.State.Source.Name || p.DeletionTimestamp != nil {
			continue
		}
		eviction := &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: p.Name, Namespace: p.Namespace},
			DeleteOptions: &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &p.UID}}}
		if err := e.Client.SubResource("eviction").Create(ctx, p, eviction); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	if err = e.waitWorkload(ctx, w, false); err != nil {
		return err
	}
	pods, err = e.workloadPods(ctx, w)
	if err != nil {
		return err
	}
	targetCount := 0
	for _, p := range pods {
		if p.Spec.NodeName == e.State.Target.Name {
			targetCount++
		}
	}
	if targetCount < len(w.OriginalPods) {
		return errors.New("系统替换副本没有调度到指定目标节点，需要检查调度约束")
	}
	return nil
}

func (e *Engine) moveAlertmanager(ctx context.Context, w *Workload) error {
	if w.Owner == nil || w.Owner.Kind != kindAlertmanager {
		return errors.New("无 PVC StatefulSet 没有已验证的迁移策略")
	}
	pods, err := e.workloadPods(ctx, w)
	if err != nil {
		return err
	}
	for _, p := range pods {
		if p.Spec.NodeName != e.State.Source.Name {
			continue
		}
		if net.ParseIP(p.Status.PodIP) == nil {
			return errors.New("检查 Alertmanager Pod IP 无效")
		}
		body, err := httpGet(ctx, net.JoinHostPort(p.Status.PodIP, "9093"), "/api/v2/silences")
		if err != nil {
			return err
		}
		var silences []json.RawMessage
		if err = json.Unmarshal(body, &silences); err != nil {
			return err
		}
		if len(silences) != 0 {
			return errors.New("检查 Alertmanager 存在 silence，需先提供其持久化/迁移策略")
		}
	}
	obj, err := e.operator(ctx, w)
	if err != nil {
		return err
	}
	selector, _, err := unstructured.NestedStringMap(obj.Object, "spec", "nodeSelector")
	if err != nil {
		return err
	}
	if selector == nil {
		selector = map[string]string{}
	}
	if selector[hostnameLabel] == e.State.Target.Name {
		return nil
	}
	selector[hostnameLabel] = e.State.Target.Name
	if err = unstructured.SetNestedStringMap(obj.Object, selector, "spec", "nodeSelector"); err != nil {
		return err
	}
	return e.Client.Update(ctx, obj)
}

func (e *Engine) moveWorkload(ctx context.Context, w *Workload) error {
	if w.Volume != nil {
		return e.waitWorkload(ctx, w, true)
	}
	if w.Phase == phaseStarted {
		return e.waitWorkload(ctx, w, w.Replicas == 1)
	}
	if w.Phase != phasePlanned && w.Phase != phaseMoving {
		return errors.New("未知工作负载迁移阶段")
	}
	if err := e.guard(ctx, true); err != nil {
		return err
	}
	w.Phase = phaseMoving
	if err := e.checkpoint(); err != nil {
		return err
	}
	var err error
	switch {
	case w.Kind == kindStatefulSet:
		err = e.moveAlertmanager(ctx, w)
	case w.Owner != nil && w.Owner.Kind == kindApp:
		if w.Template.Spec.NodeSelector[hostnameLabel] == e.State.Source.Name {
			// Preserve the existing explicit placement decision when replacing
			// its host. An eviction alone would pin the replacement to the old
			// node forever. UID, template drift and replica guards still apply.
			err = e.moveDeployment(ctx, w)
		} else {
			err = e.moveSystemReplicas(ctx, w)
		}
	case w.Namespace == systemNamespace && w.Replicas > 1:
		err = e.moveSystemReplicas(ctx, w)
	default:
		err = e.moveDeployment(ctx, w)
	}
	if err != nil {
		return err
	}
	if err = e.waitWorkload(ctx, w, w.Replicas == 1); err != nil {
		return err
	}
	w.Phase = phaseStarted
	return e.checkpoint()
}

func httpGet(ctx context.Context, address, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+path, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: healthRequestTimeout, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer closeReadOnly(response.Body)
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("健康请求 %s 返回 %d", address, response.StatusCode)
	}
	const maxHealthBody = 1 << 20
	return io.ReadAll(io.LimitReader(response.Body, maxHealthBody))
}

func desiredTargetLabels(node *corev1.Node, values map[string]string) error {
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}
	for key, value := range values {
		if old, exists := node.Labels[key]; exists && old != value {
			return fmt.Errorf("目标标签 %s 已是 %q，拒绝覆盖为 %q", key, old, value)
		}
		node.Labels[key] = value
	}
	return nil
}

func (e *Engine) ensureTargetLabels(ctx context.Context) error {
	if len(e.State.Options.TargetLabels) == 0 {
		return nil
	}
	var node corev1.Node
	if err := e.Client.Get(ctx, types.NamespacedName{Name: e.State.Target.Name}, &node); err != nil {
		return err
	}
	if node.UID != e.State.Target.UID {
		return fmt.Errorf("目标节点 UID 改变")
	}
	missing := false
	for key := range e.State.Options.TargetLabels {
		if _, ok := node.Labels[key]; !ok {
			missing = true
		}
	}
	if err := desiredTargetLabels(&node, e.State.Options.TargetLabels); err != nil {
		return err
	}
	if !missing {
		return nil
	}
	e.logf("补充目标节点计划标签 node=%s labels=%v；完成后保留以满足调度约束", node.Name, e.State.Options.TargetLabels)
	return e.Client.Update(ctx, &node)
}

func (e *Engine) setReplicas(ctx context.Context, w *Workload, count int32) error {
	obj, current, err := e.getWorkload(ctx, w, true)
	if err != nil {
		return err
	}
	if current != 0 && current != w.Replicas {
		return errors.New("副本数被其他客户端改变")
	}
	switch typed := obj.(type) {
	case *appsv1.Deployment:
		typed.Spec.Replicas = &count
	case *appsv1.StatefulSet:
		typed.Spec.Replicas = &count
	}
	return e.Client.Update(ctx, obj)
}

func (e *Engine) stopVolume(ctx context.Context, w *Workload) error {
	if !oneOf(w.Volume.Phase, phasePrepared, phasePrecopyReady, phaseStopping) {
		return nil
	}
	if e.State.Options.Precopy && (w.Volume.Precopy == nil || (!w.Volume.Precopy.Ready && !w.Volume.Precopy.Converged)) {
		return errors.New("在线预同步尚未达到切换条件，禁止停止源服务")
	}
	// Ready 只允许开始停写。最后一轮结束后源 Pod 和卷消费者仍可能变化，
	// 因此停写前重新检查，不能仅相信持久化的预同步完成标记。
	if e.State.Options.Precopy && w.Volume.Phase == phasePrecopyReady {
		if err := e.copyBindings(ctx, w); err != nil {
			return err
		}
		if _, err := e.liveSource(ctx, w); err != nil {
			return err
		}
		if released, err := e.copyMountsReleased(ctx, w); err != nil {
			return err
		} else if !released {
			return errors.New("复制助手或目标卷消费者尚未退出，禁止停止源服务")
		}
	}
	if err := e.guard(ctx, true); err != nil {
		return err
	}
	if err := e.pauseOperator(ctx, w); err != nil {
		return err
	}
	obj, count, err := e.getWorkload(ctx, w, true)
	if err != nil {
		return err
	}
	if count != 0 && count != w.Replicas {
		return errors.New("控制器副本数发生并发变更")
	}
	w.Volume.Phase = phaseStopping
	if err = e.checkpoint(); err != nil {
		return err
	}
	if count != 0 {
		e.logf("停止写入 resource=%s replicas=%d->0；等待原 Pod 完全退出", workloadKey(w), count)
		if err = e.setReplicas(ctx, w, 0); err != nil {
			return err
		}
		obj, _, err = e.getWorkload(ctx, w, true)
		if err != nil {
			return err
		}
	}
	if w.StoppedGeneration != 0 && obj.GetGeneration() != w.StoppedGeneration {
		return errors.New("停止写入阶段发生并发修改")
	}
	w.StoppedGeneration = obj.GetGeneration()
	if err = e.checkpoint(); err != nil {
		return err
	}
	if err = e.wait(ctx, "业务 Pod 退出", func(ctx context.Context) (bool, error) { ps, err := e.workloadPods(ctx, w); return len(ps) == 0, err }); err != nil {
		return err
	}
	if err = e.noMounts(ctx, w); err != nil {
		return err
	}
	w.Volume.Phase = phaseStopped
	return e.checkpoint()
}

func (e *Engine) startVolume(ctx context.Context, w *Workload) error {
	v := w.Volume
	if v.Phase != phaseSwitched && v.Phase != phaseStarting {
		return nil
	}
	if err := e.guard(ctx, true); err != nil {
		return err
	}
	var claim corev1.PersistentVolumeClaim
	if err := e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: v.PVC.Name}, &claim); err != nil {
		return err
	}
	if claim.Status.Phase != corev1.ClaimBound || claim.Spec.VolumeName != v.TargetPV.Name || claim.Labels[labelKey] != e.State.ID {
		return errors.New("启动前目标 PVC 绑定不正确")
	}
	// Persist before restoring replicas: a timeout cannot prove no writes happened.
	v.Phase = phaseStarting
	if err := e.checkpoint(); err != nil {
		return err
	}
	if err := e.setReplicas(ctx, w, w.Replicas); err != nil {
		return err
	}
	if err := e.waitWorkload(ctx, w, true); err != nil {
		return err
	}
	if err := e.restoreOperator(ctx, w); err != nil {
		return err
	}
	v.Phase = phaseStarted
	w.Phase = phaseStarted
	return e.checkpoint()
}

// Each batch finishes before another starts. A failed volume therefore prevents
// new services from stopping while its already-running peers finish normally.
func (e *Engine) migrateVolumes(ctx context.Context) error {
	limit := e.State.Options.PVConcurrency
	if limit < 1 || limit > 16 {
		return errors.New("PV 并发数必须在 1 到 16 之间")
	}
	e.logf("有状态迁移 pvConcurrency=%d rsyncBandwidthLimitKiB=%d precopy=%t；同一批次可能同时停止多个服务", limit, e.State.Options.RsyncBandwidthLimit, e.State.Options.Precopy)
	for _, batch := range volumeBatches(e.State.Workloads, limit) {
		if err := e.runVolumeBatch(ctx, batch, func(ctx context.Context, worker *Engine, w *Workload) error {
			return worker.migrateVolume(ctx, w)
		}); err != nil {
			return err
		}
	}
	return nil
}

func volumeBatches(workloads []*Workload, limit int) [][]*Workload {
	var batches [][]*Workload
	var batch []*Workload
	controllers := map[string]bool{}
	flush := func() {
		if len(batch) > 0 {
			batches = append(batches, batch)
			batch = nil
			controllers = map[string]bool{}
		}
	}
	for _, w := range workloads {
		if w.Volume == nil || w.Volume.Phase == phaseStarted {
			continue
		}
		// Managed services can depend on the same reconciler. Keep their existing
		// pause/restore ordering, even when ordinary volumes use concurrency.
		managed := w.Owner != nil && w.Owner.Kind != kindApp
		controller := workloadKey(w)
		if managed || controllers[controller] {
			flush()
		}
		batch = append(batch, w)
		controllers[controller] = true
		if managed || len(batch) == limit {
			flush()
		}
	}
	flush()
	return batches
}

func (e *Engine) migrateVolume(ctx context.Context, w *Workload) error {
	e.logf("迁移卷 %s/%s，阶段 %s", w.Namespace, w.Volume.PVC.Name, w.Volume.Phase)
	for _, step := range []struct {
		name string
		run  func(context.Context, *Workload) error
	}{{"online-precopy", e.precopyVolume}, {"stop-writes", e.stopVolume}, {"copy-and-verify", e.copyVolume}, {"switch-pvc", e.switchVolume}, {"start-workload", e.startVolume}} {
		if step.name == "online-precopy" && !e.State.Options.Precopy {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.stage(ctx, step.name, workloadKey(w), func(ctx context.Context) error { return step.run(ctx, w) }); err != nil {
			return fmt.Errorf("%s 停留在 %s: %w", workloadKey(w), w.Volume.Phase, err)
		}
	}
	return e.waitWorkload(ctx, w, true)
}

// Workers own independent workload snapshots. Only the coordinator merges a
// worker's own checkpoint; marshaling the shared state never races with writes.
func (e *Engine) runVolumeBatch(ctx context.Context, batch []*Workload, run func(context.Context, *Engine, *Workload) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(batch) == 1 {
		return run(ctx, e, batch[0])
	}
	var mu sync.Mutex
	var saveFailure error
	output := &lockedWriter{dst: e.Output}
	type task struct {
		engine   *Engine
		workload *Workload
	}
	tasks := make([]task, 0, len(batch))
	for _, original := range batch {
		index := -1
		for i, w := range e.State.Workloads {
			if w == original {
				index = i
				break
			}
		}
		if index < 0 {
			return errors.New("并发工作负载不属于当前迁移计划")
		}
		own, err := cloneWorkload(original)
		if err != nil {
			return err
		}
		state := *e.State
		state.Workloads = append([]*Workload(nil), e.State.Workloads...)
		state.Workloads[index] = own
		state.Events = nil
		worker := &Engine{Client: e.Client, Hosts: e.Hosts, Copier: e.Copier, Output: output, State: &state}
		if _, external := e.Copier.(*processCopier); external {
			worker.UseDefaultCopier()
		}
		mergedEvents := 0
		worker.Save = func(s *State) error {
			mu.Lock()
			defer mu.Unlock()
			if saveFailure != nil {
				return saveFailure
			}
			copy, err := cloneWorkload(own)
			if err != nil {
				saveFailure = err
				return err
			}
			e.State.Workloads[index] = copy
			e.State.Events = append(e.State.Events, s.Events[mergedEvents:]...)
			mergedEvents = len(s.Events)
			saveFailure = e.checkpoint()
			return saveFailure
		}
		tasks = append(tasks, task{worker, own})
	}
	e.logf("并发批次开始 volumes=%d；每卷独立校验和启动，批次结束前不停止后续服务", len(tasks))
	errs := make([]error, len(tasks))
	var wg sync.WaitGroup
	for i, t := range tasks {
		wg.Go(func() { errs[i] = run(ctx, t.engine, t.workload) })
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		e.logf("并发批次失败；已等待批次内任务退出，不再迁移后续卷；检查各卷阶段后 resume")
		return err
	}
	e.logf("并发批次完成 volumes=%d", len(tasks))
	return nil
}

func cloneWorkload(w *Workload) (*Workload, error) {
	data, err := json.Marshal(w)
	if err != nil {
		return nil, err
	}
	var copy Workload
	if err := json.Unmarshal(data, &copy); err != nil {
		return nil, err
	}
	return &copy, nil
}

type lockedWriter struct {
	mu  sync.Mutex
	dst io.Writer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dst == nil {
		return len(p), nil
	}
	return w.dst.Write(p)
}

func (e *Engine) waitTargetDaemonSets(ctx context.Context) error {
	if len(e.State.DaemonSets) == 0 {
		return nil
	}
	names := make([]string, 0, len(e.State.DaemonSets))
	for _, ds := range e.State.DaemonSets {
		names = append(names, ds.Namespace+"/"+ds.Name)
	}
	e.logf("检查目标系统 DaemonSet node=%s controllers=%s", e.State.Target.Name, strings.Join(names, ","))
	return e.wait(ctx, "目标系统 DaemonSet Ready", func(ctx context.Context) (bool, error) {
		var pods corev1.PodList
		if err := e.Client.List(ctx, &pods); err != nil {
			return false, err
		}
		for _, ds := range e.State.DaemonSets {
			ready := false
			for i := range pods.Items {
				p := &pods.Items[i]
				if p.Namespace == ds.Namespace && p.Spec.NodeName == e.State.Target.Name && ownedBy(p, ds.UID) && podReady(p) {
					ready = true
					break
				}
			}
			if !ready {
				return false, nil
			}
		}
		return true, nil
	})
}
