package nodemigrate

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func workloadVolumes(w *Workload) []*Volume {
	if w.Volume != nil {
		return []*Volume{w.Volume}
	}
	return w.Volumes
}

// A view reuses the existing, checkpointed per-volume copier without changing
// the instance's ownership or creating a second persisted copy of a Volume.
func volumeView(w *Workload, v *Volume) *Workload {
	view := *w
	view.Volume = v
	return &view
}

func volumeViews(s *State) []*Workload {
	var result []*Workload
	for _, w := range s.Workloads {
		for _, v := range workloadVolumes(w) {
			result = append(result, volumeView(w, v))
		}
	}
	return result
}

func selectedNamespace(o Options, ns string) bool {
	if len(o.Namespaces) > 0 {
		return slices.Contains(o.Namespaces, ns)
	}
	return ns != systemNamespace && !slices.Contains(o.ExcludeNamespaces, ns)
}

func validatePlanNodes(source, target *corev1.Node, mode string) error {
	if mode != ModeWorkloads {
		return errors.New("只支持业务 Pod/PVC 迁移计划")
	}
	for _, n := range []*corev1.Node{source, target} {
		if n.UID == "" || n.Labels[hostnameLabel] != n.Name || !nodeReady(n) || n.DeletionTimestamp != nil {
			return errors.New("源和目标节点必须具有稳定身份且 Ready")
		}
		if n.Annotations[labelKey] != "" {
			return errors.New("节点有未完成的迁移，请使用原状态文件续跑")
		}
	}
	if target.Spec.Unschedulable {
		return errors.New("目标节点不可调度")
	}
	if source.Status.NodeInfo.Architecture == "" || source.Status.NodeInfo.Architecture != target.Status.NodeInfo.Architecture || source.Status.NodeInfo.OperatingSystem != linuxOS || target.Status.NodeInfo.OperatingSystem != linuxOS {
		return errors.New("源和目标必须是相同 CPU 架构的 Linux 节点")
	}
	return nil
}

func (e *Engine) inventoryInstances(ctx context.Context, state *State, source, target *corev1.Node) error {
	var pods corev1.PodList
	if err := e.Client.List(ctx, &pods); err != nil {
		return err
	}
	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[i].Namespace+"/"+pods.Items[i].Name < pods.Items[j].Namespace+"/"+pods.Items[j].Name
	})
	seenClaims := map[types.UID]bool{}
	selected := map[string]bool{}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Spec.NodeName != source.Name || terminalPod(p) || !selectedNamespace(state.Options, p.Namespace) {
			continue
		}
		owner := metav1.GetControllerOf(p)
		if p.Annotations[corev1.MirrorPodAnnotationKey] != "" {
			continue
		}
		if owner != nil && owner.Kind == kindDaemonSet {
			continue
		}
		if owner == nil || !podReady(p) {
			return fmt.Errorf("源实例 %s/%s 必须有控制器且 Ready", p.Namespace, p.Name)
		}
		w := &Workload{Namespace: p.Namespace, OriginalPods: []types.UID{p.UID}, Phase: phasePlanned, Instance: &Instance{Pod: *p.DeepCopy()}}
		switch owner.Kind {
		case kindStatefulSet:
			var s appsv1.StatefulSet
			if err := e.Client.Get(ctx, types.NamespacedName{Namespace: p.Namespace, Name: owner.Name}, &s); err != nil {
				return err
			}
			if s.UID != owner.UID || s.DeletionTimestamp != nil {
				return errors.New("源 StatefulSet 身份改变")
			}
			if policy := s.Spec.PersistentVolumeClaimRetentionPolicy; policy != nil && (policy.WhenDeleted != appsv1.RetainPersistentVolumeClaimRetentionPolicyType || policy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType) {
				return errors.New("StatefulSet PVC 策略必须为 Retain")
			}
			w.Kind, w.Name, w.UID, w.Replicas, w.Template, w.Owner = kindStatefulSet, s.Name, s.UID, replicas(s.Spec.Replicas), *s.Spec.Template.DeepCopy(), metav1.GetControllerOf(&s)
			w.Instance.StatefulSetSpec = s.Spec.DeepCopy()
		case "ReplicaSet":
			var rs appsv1.ReplicaSet
			if err := e.Client.Get(ctx, types.NamespacedName{Namespace: p.Namespace, Name: owner.Name}, &rs); err != nil {
				return err
			}
			if rs.UID != owner.UID || rs.DeletionTimestamp != nil {
				return errors.New("源 ReplicaSet 身份改变")
			}
			dOwner := metav1.GetControllerOf(&rs)
			if dOwner == nil || dOwner.Kind != kindDeployment {
				return errors.New("实例必须由 Deployment/ReplicaSet 或 StatefulSet 管理")
			}
			var d appsv1.Deployment
			if err := e.Client.Get(ctx, types.NamespacedName{Namespace: p.Namespace, Name: dOwner.Name}, &d); err != nil {
				return err
			}
			if d.UID != dOwner.UID || d.DeletionTimestamp != nil || replicas(rs.Spec.Replicas) != replicas(d.Spec.Replicas) || !equality.Semantic.DeepEqual(rs.Spec.Template.Spec, d.Spec.Template.Spec) {
				return errors.New("Deployment 正在滚动更新或身份改变")
			}
			w.Kind, w.Name, w.UID, w.Replicas, w.Template, w.Owner = kindDeployment, d.Name, d.UID, replicas(d.Spec.Replicas), *d.Spec.Template.DeepCopy(), metav1.GetControllerOf(&d)
			w.Instance.ReplicaSet, w.Instance.DeploymentSpec = rs.DeepCopy(), d.Spec.DeepCopy()
		default:
			return fmt.Errorf("不支持实例的控制器 %s", owner.Kind)
		}
		if w.Replicas < 1 {
			return errors.New("源控制器副本数无效")
		}
		if w.Owner != nil && !supportedOwner(w.Owner) && !(w.Owner.Kind == "NebulaCluster" && w.Owner.APIVersion == "apps.nebula-graph.io/v1alpha1") {
			return errors.New("未支持的控制器父资源")
		}
		placement := w.Template.Spec.DeepCopy()
		if w.Kind == kindDeployment && placement.NodeSelector[hostnameLabel] == source.Name {
			placement.NodeSelector[hostnameLabel] = target.Name
		}
		if placement.NodeName != "" {
			return errors.New("控制器固定 nodeName 不支持逐实例迁移")
		}
		if err := validatePlacement(placement, target); err != nil {
			return fmt.Errorf("%s: %w", workloadKey(w), err)
		}
		for _, mount := range p.Spec.Volumes {
			if mount.HostPath != nil || mount.Ephemeral != nil {
				return errors.New("直接 hostPath/ephemeral 不支持逐实例迁移")
			}
			if mount.PersistentVolumeClaim == nil {
				continue
			}
			if w.Kind != kindStatefulSet {
				return errors.New("逐实例模式暂不支持带 PVC 的 Deployment")
			}
			var claim corev1.PersistentVolumeClaim
			if err := e.Client.Get(ctx, types.NamespacedName{Namespace: p.Namespace, Name: mount.PersistentVolumeClaim.ClaimName}, &claim); err != nil {
				return err
			}
			if seenClaims[claim.UID] {
				return errors.New("多个实例或挂载共享 PVC，需单独迁移方案")
			}
			seenClaims[claim.UID] = true
			for _, o := range claim.OwnerReferences {
				if o.UID != w.UID && (w.Owner == nil || o.UID != w.Owner.UID) {
					return errors.New("PVC owner 不是原 StatefulSet 或其父资源")
				}
			}
			// Reuse the claim checks; the validated owners are preserved on rebind.
			checked := claim.DeepCopy()
			checked.OwnerReferences = nil
			pv, err := e.inspectClaim(ctx, checked, source)
			if err != nil {
				return err
			}
			for _, other := range pods.Items {
				if other.Namespace != p.Namespace || other.UID == p.UID {
					continue
				}
				for _, v := range other.Spec.Volumes {
					if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == claim.Name {
						return errors.New("PVC 被其他 Pod 引用")
					}
				}
			}
			id := "km-" + state.ID + "-" + strconv.FormatInt(int64(volumeCount(state)+len(w.Volumes)), 36)
			if len(id) > 24 {
				return errors.New("卷数量超过复制操作 ID 长度上限")
			}
			w.Volumes = append(w.Volumes, &Volume{PVC: claim, PV: *pv, CopyID: id, TemporaryPVC: "km-migrate-" + strings.TrimPrefix(id, "km-"), Phase: phasePlanned})
		}
		if w.Kind == kindStatefulSet && len(w.Volumes) == 0 {
			return errors.New("无 PVC 的 StatefulSet 需要单独的数据保留方案")
		}
		state.Workloads = append(state.Workloads, w)
		selected[w.Namespace] = true
	}
	if len(state.Workloads) == 0 {
		return errors.New("按命名空间规则筛选后，源节点没有可迁移的业务实例")
	}
	// Persist the resolved snapshot. Resume must not discover new namespaces
	// or expand the confirmed workload set after planning.
	if len(state.Options.Namespaces) == 0 {
		for ns := range selected {
			state.Options.Namespaces = append(state.Options.Namespaces, ns)
		}
		sort.Strings(state.Options.Namespaces)
	}
	if err := e.rejectHPA(ctx, state.Workloads); err != nil {
		return err
	}
	return nil
}

func (e *Engine) instanceController(ctx context.Context, w *Workload) error {
	obj, count, err := e.getWorkload(ctx, w, true)
	if err != nil {
		return err
	}
	if count != w.Replicas {
		return errors.New("控制器副本数改变")
	}
	if s, ok := obj.(*appsv1.StatefulSet); ok {
		if w.Instance.StatefulSetSpec == nil || !equality.Semantic.DeepEqual(s.Spec, *w.Instance.StatefulSetSpec) {
			return errors.New("StatefulSet spec 改变")
		}
		return nil
	}
	d := obj.(*appsv1.Deployment)
	if w.Instance.DeploymentSpec == nil || !equality.Semantic.DeepEqual(d.Spec, *w.Instance.DeploymentSpec) {
		return errors.New("Deployment spec 改变")
	}
	if w.Instance.ReplicaSet == nil {
		return errors.New("ReplicaSet 快照缺失")
	}
	var rs appsv1.ReplicaSet
	if err := e.Client.Get(ctx, client.ObjectKeyFromObject(w.Instance.ReplicaSet), &rs); err != nil {
		return err
	}
	if rs.UID != w.Instance.ReplicaSet.UID || rs.DeletionTimestamp != nil || !ownedBy(&rs, w.UID) || !equality.Semantic.DeepEqual(rs.Spec, w.Instance.ReplicaSet.Spec) {
		return errors.New("ReplicaSet 身份/spec 改变")
	}
	return nil
}

func (e *Engine) liveInstance(ctx context.Context, w *Workload) (types.UID, error) {
	if err := e.instanceController(ctx, w); err != nil {
		return "", err
	}
	var pod corev1.Pod
	if err := e.Client.Get(ctx, client.ObjectKeyFromObject(&w.Instance.Pod), &pod); err != nil {
		return "", err
	}
	if pod.UID != w.Instance.Pod.UID || pod.Spec.NodeName != e.State.Source.Name || !podReady(&pod) {
		return "", errors.New("原实例身份/位置改变或未 Ready")
	}
	return pod.UID, nil
}
