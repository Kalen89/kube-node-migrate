package nodemigrate

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (e *Engine) retain(ctx context.Context, expected *corev1.PersistentVolume) error {
	var current corev1.PersistentVolume
	if err := e.Client.Get(ctx, types.NamespacedName{Name: expected.Name}, &current); err != nil {
		return err
	}
	if current.UID != expected.UID {
		return errors.New("PV UID 改变，拒绝变更回收策略")
	}
	if current.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain {
		return nil
	}
	current.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
	return e.Client.Update(ctx, &current)
}

func claimSpec(source *corev1.PersistentVolumeClaim, pv string) corev1.PersistentVolumeClaimSpec {
	return corev1.PersistentVolumeClaimSpec{AccessModes: append([]corev1.PersistentVolumeAccessMode(nil), source.Spec.AccessModes...),
		Resources: *source.Spec.Resources.DeepCopy(), StorageClassName: source.Spec.StorageClassName, VolumeMode: pointer(corev1.PersistentVolumeFilesystem), VolumeName: pv}
}

func (e *Engine) prepareVolume(ctx context.Context, w *Workload) error {
	v := w.Volume
	if v.Phase != phasePlanned && v.Phase != phasePreparing {
		return nil
	}
	if err := e.guard(ctx, true); err != nil {
		return err
	}
	if _, _, err := e.getWorkload(ctx, w, true); err != nil {
		return err
	}
	var original corev1.PersistentVolumeClaim
	if err := e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: v.PVC.Name}, &original); err != nil {
		return err
	}
	if original.UID != v.PVC.UID || original.Spec.VolumeName != v.PV.Name {
		return errors.New("源 PVC 绑定已改变")
	}
	v.Phase = phasePreparing
	if err := e.checkpoint(); err != nil {
		return err
	}
	if err := e.retain(ctx, &v.PV); err != nil {
		return err
	}
	dest, err := e.prepareClaim(ctx, w)
	if err != nil {
		return err
	}
	key := client.ObjectKeyFromObject(dest)
	helper, err := e.bindingHelper(ctx, w)
	if err != nil {
		return err
	}
	if err = e.wait(ctx, "目标 PVC Bound", func(ctx context.Context) (bool, error) {
		if err := e.Client.Get(ctx, key, dest); err != nil {
			return false, err
		}
		if dest.UID != v.TemporaryUID {
			return false, errors.New("目标 PVC UID 改变")
		}
		return dest.Status.Phase == corev1.ClaimBound, nil
	}); err != nil {
		return err
	}
	var pv corev1.PersistentVolume
	if err = e.Client.Get(ctx, types.NamespacedName{Name: dest.Spec.VolumeName}, &pv); err != nil {
		return err
	}
	if pv.Name == v.PV.Name || !pvOnNode(&pv, e.State.Target.Name) || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != dest.UID {
		return errors.New("目标 PV 节点或 claimRef 不匹配")
	}
	if _, err = volumePath(&pv); err != nil {
		return err
	}
	if err = e.retain(ctx, &pv); err != nil {
		return err
	}
	v.TargetPV = pv.DeepCopy()
	e.logf("目标卷已绑定 pvc=%s/%s targetPVC=%s targetPV=%s node=%s reclaim=Retain", w.Namespace, v.PVC.Name, v.TemporaryPVC, pv.Name, e.State.Target.Name)
	if err = e.checkpoint(); err != nil {
		return err
	}
	if err = e.Client.Delete(ctx, helper, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &helper.UID}}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err = e.wait(ctx, "绑定助手退出", func(ctx context.Context) (bool, error) {
		var p corev1.Pod
		err := e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: helper.Name}, &p)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}); err != nil {
		return err
	}
	v.Phase = phasePrepared
	return e.checkpoint()
}

func (e *Engine) stopped(ctx context.Context, w *Workload) error {
	if w.Instance != nil {
		return e.instanceStopped(ctx, w)
	}
	obj, count, err := e.getWorkload(ctx, w, true)
	if err != nil {
		return err
	}
	if count != 0 {
		return errors.New("服务副本数不再是 0")
	}
	if w.StoppedGeneration != 0 && obj.GetGeneration() != w.StoppedGeneration {
		return errors.New("停止写入后控制器 generation 改变")
	}
	pods, err := e.workloadPods(ctx, w)
	if err != nil {
		return err
	}
	if len(pods) != 0 {
		return errors.New("业务 Pod 尚未完全退出")
	}
	return nil
}

func (e *Engine) copyVolume(ctx context.Context, w *Workload) error {
	v := w.Volume
	if v.Phase != phaseStopped && v.Phase != phaseCopying && v.Phase != phaseCopied {
		return nil
	}
	if err := e.guard(ctx, true); err != nil {
		return err
	}
	if err := e.stopped(ctx, w); err != nil {
		return err
	}
	if err := e.copyBindings(ctx, w); err != nil {
		return err
	}
	if v.FinalSyncRetry || (v.Phase == phaseCopied && v.TargetDigest != nil && v.SourceDigest != nil && *v.SourceDigest != *v.TargetDigest) {
		if err := e.retryFinalSync(ctx, w); err != nil {
			return err
		}
	}
	if v.Phase != phaseCopied {
		if v.SourceDigest == nil && !e.State.Options.ParallelDigest {
			e.logf("开始扫描源卷摘要 pvc=%s/%s node=%s；只读检查文件内容、属主与权限", w.Namespace, v.PVC.Name, e.State.Source.Name)
			path, err := volumePath(&v.PV)
			if err != nil {
				return err
			}
			if err = e.noMounts(ctx, w); err != nil {
				return err
			}
			if err := e.stage(ctx, "source-digest", workloadKey(w)+"/"+v.PVC.Name, func(ctx context.Context) error {
				summary, err := e.filesystem(ctx, e.State.SourceConfig, path, "digest")
				if err != nil {
					return err
				}
				v.SourceDigest = &summary.Digest
				e.logf("源卷摘要完成 pvc=%s/%s bytes=%d entries=%d sha256=%s", w.Namespace, v.PVC.Name, summary.Bytes, summary.Entries, summary.SHA256)
				return nil
			}); err != nil {
				return err
			}
		}
		v.Phase = phaseCopying
		if err := e.checkpoint(); err != nil {
			return err
		}
		if e.Copier == nil {
			return errors.New("复制执行器未初始化")
		}
		if err := e.stage(ctx, "final-sync", workloadKey(w)+"/"+v.PVC.Name, func(ctx context.Context) error {
			if err := e.Copier.Copy(ctx, e.State, w); err != nil {
				return err
			}
			e.logf("卷复制完成 pvc=%s/%s；等待助手释放后校验目标文件内容", w.Namespace, v.PVC.Name)
			if err := e.stopped(ctx, w); err != nil {
				return err
			}
			return e.wait(ctx, "复制助手释放 PVC", func(ctx context.Context) (bool, error) {
				users, err := e.claimUsers(ctx, w.Namespace, v.PVC.Name, v.TemporaryPVC)
				return len(users) == 0, err
			})
		}); err != nil {
			return err
		}
		v.Phase = phaseCopied
		if err := e.checkpoint(); err != nil {
			return err
		}
	}
	if e.State.Options.ParallelDigest {
		if err := e.verifyParallelDigests(ctx, w); err != nil {
			return err
		}
		v.Phase = phaseVerified
		return e.checkpoint()
	}
	path, err := volumePath(v.TargetPV)
	if err != nil {
		return err
	}
	e.logf("开始校验目标卷摘要 pvc=%s/%s node=%s；校验通过前不切换绑定", w.Namespace, v.PVC.Name, e.State.Target.Name)
	if err := e.stage(ctx, "target-digest", workloadKey(w)+"/"+v.PVC.Name, func(ctx context.Context) error {
		summary, err := e.filesystem(ctx, e.State.TargetConfig, path, "digest")
		if err != nil {
			return err
		}
		v.TargetDigest = &summary.Digest
		if v.SourceDigest == nil || *v.SourceDigest != *v.TargetDigest {
			v.FinalSyncRetry = true
			return errors.New("源/目标文件摘要不一致，禁止切换")
		}
		e.logf("卷摘要一致 pvc=%s/%s bytes=%d entries=%d sha256=%s；允许切换绑定", w.Namespace, v.PVC.Name, summary.Bytes, summary.Entries, summary.SHA256)
		return nil
	}); err != nil {
		return err
	}
	v.Phase = phaseVerified
	return e.checkpoint()
}

func (e *Engine) copyBindings(ctx context.Context, w *Workload) error {
	v := w.Volume
	for _, expected := range []struct {
		name string
		uid  types.UID
		pv   string
	}{
		{v.PVC.Name, v.PVC.UID, v.PV.Name}, {v.TemporaryPVC, v.TemporaryUID, v.TargetPV.Name},
	} {
		var claim corev1.PersistentVolumeClaim
		if err := e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: expected.name}, &claim); err != nil {
			return err
		}
		if claim.UID != expected.uid || claim.Spec.VolumeName != expected.pv || claim.Status.Phase != corev1.ClaimBound {
			return errors.New("复制前 PVC 绑定身份改变")
		}
		if expected.name == v.TemporaryPVC && claim.Labels[labelKey] != e.State.ID {
			return errors.New("复制目标 PVC 不属于本次迁移")
		}
		var pv corev1.PersistentVolume
		if err := e.Client.Get(ctx, types.NamespacedName{Name: expected.pv}, &pv); err != nil {
			return err
		}
		original := &v.PV
		if expected.name == v.TemporaryPVC {
			original = v.TargetPV
		}
		if pv.UID != original.UID || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != expected.uid || pv.Spec.ClaimRef.Namespace != w.Namespace || pv.Spec.ClaimRef.Name != expected.name {
			return errors.New("复制前 PV 身份或 claimRef 改变")
		}
	}
	return nil
}

func (e *Engine) switchVolume(ctx context.Context, w *Workload) error {
	v := w.Volume
	if w.Instance != nil {
		for _, other := range w.Volumes {
			if !oneOf(other.Phase, phaseVerified, phaseSwitching, phaseSwitched) || !validDigest(other.SourceDigest) || !validDigest(other.TargetDigest) || *other.SourceDigest != *other.TargetDigest {
				return errors.New("同一实例的全部 PVC 必须校验成功后才允许任何改绑")
			}
		}
	}
	if v.Phase != phaseVerified && v.Phase != phaseSwitching {
		return nil
	}
	if err := e.guard(ctx, true); err != nil {
		return err
	}
	if err := e.stopped(ctx, w); err != nil {
		return err
	}
	if err := e.noMounts(ctx, w); err != nil {
		return err
	}
	if v.SourceDigest == nil || v.TargetDigest == nil || *v.SourceDigest != *v.TargetDigest {
		return errors.New("未保存数据校验结果")
	}
	if err := e.retain(ctx, &v.PV); err != nil {
		return err
	}
	if err := e.retain(ctx, v.TargetPV); err != nil {
		return err
	}
	var target corev1.PersistentVolume
	if err := e.Client.Get(ctx, types.NamespacedName{Name: v.TargetPV.Name}, &target); err != nil {
		return err
	}
	allowed := map[types.UID]bool{v.TemporaryUID: true}
	var currentClaim corev1.PersistentVolumeClaim
	err := e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: v.PVC.Name}, &currentClaim)
	if err == nil && currentClaim.Labels[labelKey] == e.State.ID && currentClaim.Spec.VolumeName == target.Name {
		allowed[currentClaim.UID] = true
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if ref := target.Spec.ClaimRef; ref != nil && (ref.Namespace != w.Namespace || !allowed[ref.UID]) {
		return errors.New("目标 PV 被其他 claim 占用，保留原 PVC")
	}
	v.Phase = phaseSwitching
	e.logf("切换 PVC 绑定 pvc=%s/%s sourcePV=%s targetPV=%s；保留原 PVC 名称与旧 PV 数据", w.Namespace, v.PVC.Name, v.PV.Name, v.TargetPV.Name)
	if err := e.checkpoint(); err != nil {
		return err
	}
	var temporary corev1.PersistentVolumeClaim
	err = e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: v.TemporaryPVC}, &temporary)
	if err == nil {
		if temporary.UID != v.TemporaryUID || temporary.Labels[labelKey] != e.State.ID {
			return errors.New("临时 PVC 身份改变")
		}
		if err = e.deleteClaim(ctx, &temporary); err != nil {
			return err
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	if err = e.bindClaim(ctx, w, v.TargetPV); err != nil {
		return err
	}
	v.Phase = phaseSwitched
	return e.checkpoint()
}

func (e *Engine) deleteClaim(ctx context.Context, claim *corev1.PersistentVolumeClaim) error {
	if err := e.Client.Delete(ctx, claim, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &claim.UID}}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return e.wait(ctx, "PVC 正常删除", func(ctx context.Context) (bool, error) {
		var current corev1.PersistentVolumeClaim
		err := e.Client.Get(ctx, client.ObjectKeyFromObject(claim), &current)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if current.UID != claim.UID {
			return false, errors.New("PVC 名称已被其他对象使用")
		}
		return false, nil
	})
}

func (e *Engine) bindClaim(ctx context.Context, w *Workload, desired *corev1.PersistentVolume) error {
	v := w.Volume
	var current corev1.PersistentVolumeClaim
	key := types.NamespacedName{Namespace: w.Namespace, Name: v.PVC.Name}
	err := e.Client.Get(ctx, key, &current)
	if err == nil && current.UID == v.PVC.UID && desired.Name == v.PV.Name {
		return nil
	}
	if err == nil && current.UID == v.PVC.UID {
		if err = e.deleteClaim(ctx, &current); err != nil {
			return err
		}
		err = apierrors.NewNotFound(corev1.Resource("persistentvolumeclaims"), key.Name)
	}
	if apierrors.IsNotFound(err) {
		labels := make(map[string]string)
		for k, value := range v.PVC.Labels {
			labels[k] = value
		}
		labels[labelKey] = e.State.ID
		annotations := make(map[string]string)
		for k, value := range v.PVC.Annotations {
			if !strings.HasPrefix(k, "pv.kubernetes.io/") && !strings.HasPrefix(k, "volume.kubernetes.io/") && k != "kubectl.kubernetes.io/last-applied-configuration" {
				annotations[k] = value
			}
		}
		current = corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Labels: labels, Annotations: annotations, OwnerReferences: append([]metav1.OwnerReference(nil), v.PVC.OwnerReferences...)}, Spec: claimSpec(&v.PVC, desired.Name)}
		if err = e.Client.Create(ctx, &current); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if current.Spec.VolumeName != desired.Name || current.Labels[labelKey] != e.State.ID {
		return errors.New("同名 PVC 不属于本次切换或绑定到其他 PV")
	}
	if v.NewPVCUID != "" && current.UID != v.NewPVCUID {
		return errors.New("重建 PVC UID 改变")
	}
	v.NewPVCUID = current.UID
	if err = e.checkpoint(); err != nil {
		return err
	}
	var pv corev1.PersistentVolume
	if err = e.Client.Get(ctx, types.NamespacedName{Name: desired.Name}, &pv); err != nil {
		return err
	}
	if pv.UID != desired.UID || pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		return errors.New("目标 PV 身份或 Retain 改变")
	}
	if ref := pv.Spec.ClaimRef; ref != nil {
		allowed := ref.Namespace == w.Namespace && (ref.UID == v.PVC.UID || ref.UID == v.TemporaryUID || ref.UID == current.UID)
		if !allowed {
			return errors.New("PV 已被其他 PVC 占用，拒绝覆盖 claimRef")
		}
	}
	pv.Spec.ClaimRef = &corev1.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: w.Namespace, Name: current.Name, UID: current.UID}
	if err = e.Client.Update(ctx, &pv); err != nil {
		return err
	}
	return e.wait(ctx, "原名称 PVC 绑定新 PV", func(ctx context.Context) (bool, error) {
		var c corev1.PersistentVolumeClaim
		if err := e.Client.Get(ctx, key, &c); err != nil {
			return false, err
		}
		if c.UID != current.UID || c.Spec.VolumeName != desired.Name {
			return false, fmt.Errorf("PVC 绑定身份改变")
		}
		return c.Status.Phase == corev1.ClaimBound, nil
	})
}

func (e *Engine) bindingHelper(ctx context.Context, w *Workload) (*corev1.Pod, error) {
	v := w.Volume
	helperName := "bind-" + v.TemporaryPVC
	var helper corev1.Pod
	err := e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: helperName}, &helper)
	if apierrors.IsNotFound(err) {
		secrets, secretErr := e.pullSecrets(ctx, w.Namespace, &w.Template.Spec)
		if secretErr != nil {
			return nil, secretErr
		}
		helper = corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: helperName, Namespace: w.Namespace, Labels: map[string]string{labelKey: e.State.ID}},
			Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, TerminationGracePeriodSeconds: pointer(int64(0)), AutomountServiceAccountToken: pointer(false),
				NodeSelector: map[string]string{hostnameLabel: e.State.Target.Name}, Tolerations: helperTolerations(e.State.TargetTaints), ImagePullSecrets: secrets,
				Containers: []corev1.Container{{Name: "bind", Image: e.State.Options.HelperImage, Resources: serviceResources(e.State.Options.HelperResources), Command: []string{"sh", "-c", "sleep 86400"},
					VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}}}},
				Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: v.TemporaryPVC}}}}}}
		if err = e.Client.Create(ctx, &helper); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else if helper.Labels[labelKey] != e.State.ID || helper.Spec.NodeSelector[hostnameLabel] != e.State.Target.Name {
		return nil, errors.New("绑定助手不属于本次迁移")
	}
	if !safeHelperVolumes(&helper, v.TemporaryPVC) {
		return nil, errors.New("绑定助手 PVC 配置漂移")
	}
	return &helper, nil
}

func (e *Engine) prepareClaim(ctx context.Context, w *Workload) (*corev1.PersistentVolumeClaim, error) {
	v := w.Volume
	var dest corev1.PersistentVolumeClaim
	key := types.NamespacedName{Namespace: w.Namespace, Name: v.TemporaryPVC}
	err := e.Client.Get(ctx, key, &dest)
	if apierrors.IsNotFound(err) {
		dest = corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Labels: map[string]string{labelKey: e.State.ID}}, Spec: claimSpec(&v.PVC, "")}
		if err = e.Client.Create(ctx, &dest); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else if dest.Labels[labelKey] != e.State.ID {
		return nil, errors.New("临时 PVC 不属于本次迁移")
	}
	if v.TemporaryUID != "" && dest.UID != v.TemporaryUID {
		return nil, errors.New("临时 PVC UID 改变")
	}
	v.TemporaryUID = dest.UID
	if err = e.checkpoint(); err != nil {
		return nil, err
	}
	return &dest, nil
}
