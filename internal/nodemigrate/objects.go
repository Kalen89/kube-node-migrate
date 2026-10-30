package nodemigrate

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultWaitTimeout = 10 * time.Minute
	pollInterval       = 2 * time.Second
	hostnameLabel      = "kubernetes.io/hostname"
)

func volumePath(pv *corev1.PersistentVolume) (string, error) {
	var path string
	if pv.Spec.Local != nil {
		path = pv.Spec.Local.Path
	}
	if pv.Spec.HostPath != nil {
		path = pv.Spec.HostPath.Path
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return "", errors.New("只支持具有明确绝对目录的 local / hostPath PV")
	}
	return path, nil
}

func pvOnNode(pv *corev1.PersistentVolume, name string) bool {
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return false
	}
	terms := pv.Spec.NodeAffinity.Required.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchFields) != 0 || len(terms[0].MatchExpressions) != 1 {
		return false
	}
	r := terms[0].MatchExpressions[0]
	return r.Key == hostnameLabel && r.Operator == corev1.NodeSelectorOpIn && len(r.Values) == 1 && r.Values[0] == name
}

func (e *Engine) localPathClass(ctx context.Context, name string) error {
	var sc storagev1.StorageClass
	if err := e.Client.Get(ctx, types.NamespacedName{Name: name}, &sc); err != nil {
		return err
	}
	if sc.Provisioner != "rancher.io/local-path" || sc.VolumeBindingMode == nil || *sc.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
		return errors.New("只支持 rancher.io/local-path + WaitForFirstConsumer")
	}
	return nil
}

func validatePlacement(spec *corev1.PodSpec, target *corev1.Node) error {
	for _, constraint := range spec.TopologySpreadConstraints {
		if constraint.WhenUnsatisfiable == corev1.DoNotSchedule {
			return errors.New("硬 topology spread 约束需要单独的迁移调度方案")
		}
	}
	if spec.NodeName != "" && spec.NodeName != target.Name {
		return errors.New("检查 Pod 模板固定了其他 nodeName")
	}
	for k, v := range spec.NodeSelector {
		if target.Labels[k] != v {
			return fmt.Errorf("目标节点不满足 nodeSelector %s=%s", k, v)
		}
	}
	for _, taint := range target.Spec.Taints {
		if taint.Effect != corev1.TaintEffectNoSchedule && taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		tolerated := false
		for _, tol := range spec.Tolerations {
			if tol.ToleratesTaint(&taint) {
				tolerated = true
				break
			}
		}
		if !tolerated {
			return fmt.Errorf("业务 Pod 不容忍目标污点 %s", taint.Key)
		}
	}
	if spec.Affinity == nil {
		return nil
	}
	a := spec.Affinity
	if (a.PodAffinity != nil && len(a.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0) ||
		(a.PodAntiAffinity != nil && len(a.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 0) {
		return errors.New("必需 Pod 亲和/反亲和约束需要人工迁移方案")
	}
	if a.NodeAffinity == nil || a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return nil
	}
	for _, term := range a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		matches := len(term.MatchExpressions)+len(term.MatchFields) > 0
		for _, r := range term.MatchExpressions {
			matches = matches && matchRequirement(r, target.Labels)
		}
		for _, r := range term.MatchFields {
			matches = matches && matchRequirement(r, map[string]string{"metadata.name": target.Name})
		}
		if matches {
			return nil
		}
	}
	return errors.New("目标节点不满足必需 NodeAffinity")
}

func matchRequirement(r corev1.NodeSelectorRequirement, labels map[string]string) bool {
	value, exists := labels[r.Key]
	in := false
	for _, item := range r.Values {
		if item == value {
			in = true
		}
	}
	switch r.Operator {
	case corev1.NodeSelectorOpIn:
		return exists && in
	case corev1.NodeSelectorOpNotIn:
		return !exists || !in
	case corev1.NodeSelectorOpExists:
		return exists
	case corev1.NodeSelectorOpDoesNotExist:
		return !exists
	case corev1.NodeSelectorOpGt, corev1.NodeSelectorOpLt:
		if !exists || len(r.Values) != 1 {
			return false
		}
		a, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return false
		}
		b, err := strconv.ParseInt(r.Values[0], 10, 64)
		if err != nil {
			return false
		}
		if r.Operator == corev1.NodeSelectorOpGt {
			return a > b
		}
		return a < b
	default:
		return false
	}
}

func (e *Engine) wait(ctx context.Context, what string, check func(context.Context) (bool, error)) error {
	timeout := e.State.Options.WaitTimeout
	if timeout <= 0 {
		timeout = defaultWaitTimeout
	}
	started, last := time.Now(), time.Time{}
	if err := wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		done, err := check(ctx)
		if !done && err == nil && time.Since(last) >= 15*time.Second {
			e.logf("等待中 resource=%s elapsed=%s timeout=%s", what, time.Since(started).Round(time.Second), timeout)
			last = time.Now()
		}
		return done, err
	}); err != nil {
		return fmt.Errorf("等待%s失败，保留状态后使用 resume: %w", what, err)
	}
	return nil
}

func (e *Engine) guard(ctx context.Context, sourceRequired bool) error {
	var system corev1.Namespace
	if err := e.Client.Get(ctx, types.NamespacedName{Name: systemNamespace}, &system); err != nil {
		return err
	}
	if system.UID != e.State.ClusterUID {
		return errors.New("集群 UID 已改变，拒绝操作")
	}
	identities := []NodeIdentity{e.State.Target}
	if sourceRequired {
		identities = append(identities, e.State.Source)
	}
	for _, identity := range identities {
		var n corev1.Node
		if err := e.Client.Get(ctx, types.NamespacedName{Name: identity.Name}, &n); err != nil {
			return err
		}
		if n.UID != identity.UID || !nodeReady(&n) {
			return fmt.Errorf("节点 %s UID 改变或不 Ready", identity.Name)
		}
		if owner := n.Annotations[labelKey]; owner != "" && owner != e.State.ID {
			return errors.New("节点正由另一迁移操作占用")
		}
		if identity.Name == e.State.Target.Name && (n.Spec.Unschedulable || n.DeletionTimestamp != nil) {
			return errors.New("目标节点被 cordon 或正在删除")
		}
		entry := e.State.TargetConfig
		if identity.Name == e.State.Source.Name {
			entry = e.State.SourceConfig
		}
		if !configAddressMatches(entry, &n) {
			return fmt.Errorf("节点 %s 的实际 IP 改变", identity.Name)
		}
		if entry.Architecture != n.Status.NodeInfo.Architecture {
			return fmt.Errorf("节点 %s 的 CPU 架构改变", identity.Name)
		}
		for _, role := range []string{"control-plane", "master", "etcd"} {
			if e.State.Options.Mode == ModeWorkloads {
				break
			}
			if _, ok := n.Labels["node-role.kubernetes.io/"+role]; ok {
				return errors.New("迁移中的 Agent 被变更为 Server")
			}
		}
	}
	return nil
}

func (e *Engine) getWorkload(ctx context.Context, w *Workload, strictTemplate bool) (client.Object, int32, error) {
	key := types.NamespacedName{Namespace: w.Namespace, Name: w.Name}
	var obj client.Object
	var template corev1.PodTemplateSpec
	var count int32
	switch w.Kind {
	case kindDeployment:
		d := &appsv1.Deployment{}
		if err := e.Client.Get(ctx, key, d); err != nil {
			return nil, 0, err
		}
		obj, template, count = d, d.Spec.Template, replicas(d.Spec.Replicas)
	case kindStatefulSet:
		s := &appsv1.StatefulSet{}
		if err := e.Client.Get(ctx, key, s); err != nil {
			return nil, 0, err
		}
		obj, template, count = s, s.Spec.Template, replicas(s.Spec.Replicas)
		if p := s.Spec.PersistentVolumeClaimRetentionPolicy; p != nil &&
			(p.WhenDeleted == appsv1.DeletePersistentVolumeClaimRetentionPolicyType || p.WhenScaled == appsv1.DeletePersistentVolumeClaimRetentionPolicyType) {
			return nil, 0, errors.New("检查 StatefulSet PVC Retain 策略被改变")
		}
	default:
		return nil, 0, errors.New("未知控制器类型")
	}
	if obj.GetUID() != w.UID || obj.GetDeletionTimestamp() != nil {
		return nil, 0, errors.New("控制器 UID 改变或正在删除")
	}
	if !equality.Semantic.DeepEqual(metav1.GetControllerOf(obj), w.Owner) {
		return nil, 0, errors.New("控制器父资源归属改变")
	}
	if strictTemplate && !equality.Semantic.DeepEqual(template, w.Template) {
		return nil, 0, errors.New("控制器模板发生并发变化")
	}
	return obj, count, nil
}

func (e *Engine) workloadPods(ctx context.Context, w *Workload) ([]corev1.Pod, error) {
	uids := map[types.UID]bool{w.UID: true}
	if w.Kind == kindDeployment {
		var sets appsv1.ReplicaSetList
		if err := e.Client.List(ctx, &sets, client.InNamespace(w.Namespace)); err != nil {
			return nil, err
		}
		for _, rs := range sets.Items {
			if ownedBy(&rs, w.UID) {
				uids[rs.UID] = true
			}
		}
	}
	var pods corev1.PodList
	if err := e.Client.List(ctx, &pods, client.InNamespace(w.Namespace)); err != nil {
		return nil, err
	}
	var result []corev1.Pod
	for _, p := range pods.Items {
		if owner := metav1.GetControllerOf(&p); owner != nil && uids[owner.UID] {
			result = append(result, p)
		}
	}
	return result, nil
}

func ownedBy(obj metav1.Object, uid types.UID) bool {
	owner := metav1.GetControllerOf(obj)
	return owner != nil && owner.UID == uid
}

func podReady(p *corev1.Pod) bool {
	if p.DeletionTimestamp != nil {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (e *Engine) claimUsers(ctx context.Context, namespace string, names ...string) ([]corev1.Pod, error) {
	var pods corev1.PodList
	if err := e.Client.List(ctx, &pods, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var users []corev1.Pod
	for _, p := range pods.Items {
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim == nil {
				continue
			}
			for _, name := range names {
				if v.PersistentVolumeClaim.ClaimName == name {
					users = append(users, p)
					break
				}
			}
		}
	}
	return users, nil
}

func (e *Engine) noMounts(ctx context.Context, w *Workload) error {
	users, err := e.claimUsers(ctx, w.Namespace, w.Volume.PVC.Name, w.Volume.TemporaryPVC)
	if err != nil {
		return err
	}
	if len(users) != 0 {
		return fmt.Errorf("PVC 仍被 %d 个 Pod 引用，不可移除保护 finalizer", len(users))
	}
	return nil
}

func helperTolerations(taints []corev1.Taint) []corev1.Toleration {
	var result []corev1.Toleration
	for _, taint := range taints {
		if strings.HasPrefix(taint.Key, "node.kubernetes.io/") {
			continue
		}
		result = append(result, corev1.Toleration{Key: taint.Key, Value: taint.Value, Operator: corev1.TolerationOpEqual, Effect: taint.Effect})
	}
	return result
}
