package nodemigrate

import (
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func validateInstanceState(s *State, w *Workload) error {
	i := w.Instance
	if i == nil || w.Volume != nil || !selectedNamespace(s.Options, w.Namespace) || i.Pod.Name == "" || i.Pod.UID == "" || i.Pod.Namespace != w.Namespace || i.Pod.Spec.NodeName != s.Source.Name || len(w.OriginalPods) != 1 || w.OriginalPods[0] != i.Pod.UID || !oneOf(w.Phase, phasePlanned, phasePrepared, phaseStopping, phaseStopped, phaseVerified, phaseSwitching, phaseStarting, phaseStarted) {
		return errors.New("无效逐实例检查点")
	}
	o := metav1.GetControllerOf(&i.Pod)
	if o == nil {
		return errors.New("实例原控制器身份缺失")
	}
	if w.Kind == kindStatefulSet {
		if i.StatefulSetSpec == nil || i.ReplicaSet != nil || i.DeploymentSpec != nil || o.Kind != kindStatefulSet || o.UID != w.UID || len(w.Volumes) == 0 {
			return errors.New("无效 StatefulSet 实例快照")
		}
	} else if i.ReplicaSet == nil || i.DeploymentSpec == nil || i.StatefulSetSpec != nil || o.Kind != "ReplicaSet" || o.UID != i.ReplicaSet.UID || !ownedBy(i.ReplicaSet, w.UID) || len(w.Volumes) != 0 {
		return errors.New("无效 Deployment/ReplicaSet 实例快照")
	}
	seen := map[string]bool{}
	for _, v := range w.Volumes {
		if v == nil || seen[v.PVC.Name] {
			return errors.New("实例含空或重复卷")
		}
		seen[v.PVC.Name] = true
		if oneOf(v.Phase, phaseSwitched, phaseStarted) && v.NewPVCUID == "" {
			return errors.New("改绑卷缺少重建 PVC UID")
		}
		if oneOf(w.Phase, phaseVerified, phaseSwitching, phaseStarting, phaseStarted) && (!validDigest(v.SourceDigest) || !validDigest(v.TargetDigest) || *v.SourceDigest != *v.TargetDigest) {
			return errors.New("实例尚未全部校验，不能切换或启动")
		}
		if oneOf(w.Phase, phaseStarting, phaseStarted) && !oneOf(v.Phase, phaseSwitched, phaseStarted) {
			return errors.New("实例尚未全部改绑，不能启动")
		}
		if v.Phase == phaseStarted && w.Phase != phaseStarted {
			return errors.New("卷已启动但实例阶段不匹配")
		}
	}
	claims := map[string]bool{}
	for _, mount := range i.Pod.Spec.Volumes {
		if mount.PersistentVolumeClaim != nil {
			claims[mount.PersistentVolumeClaim.ClaimName] = true
		}
	}
	if len(claims) != len(seen) {
		return errors.New("实例卷清单与原 Pod PVC 不匹配")
	}
	for name := range claims {
		if !seen[name] {
			return errors.New("实例漏记原 Pod PVC")
		}
	}
	if i.ReplacementName != "" && i.ReplicaSet == nil {
		return errors.New("StatefulSet 不得改变实例名称")
	}
	if i.ReplacementName != "" && i.ReplacementName != holdName(s, w) {
		return errors.New("替换实例名称不属于本次操作")
	}
	if w.Phase == phaseStarted && (i.ReplacementUID == "" || i.ReadyAt.IsZero()) {
		return fmt.Errorf("完成实例缺少目标身份/时间")
	}
	return nil
}
