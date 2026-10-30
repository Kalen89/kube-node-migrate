package nodemigrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (e *Engine) preflightInstances(ctx context.Context) error {
	if err := e.guard(ctx, true); err != nil {
		return err
	}
	if err := e.checkPendingPlacement(ctx); err != nil {
		return err
	}
	if e.Hosts == nil {
		return errors.New("迁移镜像缓存/卷校验需要 SSH 执行器")
	}
	probes, err := e.admissionProbes(ctx)
	if err != nil {
		return err
	}
	if err := e.identifyExecutor(ctx); err != nil {
		return err
	}
	for _, ns := range e.State.Options.Namespaces {
		for _, resource := range []string{"pods", "persistentvolumeclaims"} {
			for i, probe := range probes {
				review := &authorizationv1.SelfSubjectAccessReview{Spec: authorizationv1.SelfSubjectAccessReviewSpec{ResourceAttributes: &authorizationv1.ResourceAttributes{Namespace: ns, Verb: "create", Resource: resource}}}
				if err := probe.Create(ctx, review); err != nil {
					return fmt.Errorf("API Server %d 验证 impersonation 权限: %w", i+1, err)
				}
				if !review.Status.Allowed {
					return errors.New("准入探测身份没有 CREATE 权限，不能用 RBAC 拒绝冒充准入保护生效")
				}
			}
		}
	}
	for _, w := range e.State.Workloads {
		if err := e.instanceController(ctx, w); err != nil {
			return err
		}
		if w.Kind == kindStatefulSet && w.Phase != phaseStarted {
			if err := e.patchReconcileAnnotation(ctx, w, false, client.DryRunAll); err != nil {
				return fmt.Errorf("StatefulSet 协调元数据 patch dry-run: %w", err)
			}
		}
		p, b := e.holdObjects(w)
		var existing admissionv1.ValidatingAdmissionPolicy
		err := e.Client.Get(ctx, client.ObjectKeyFromObject(p), &existing)
		if apierrors.IsNotFound(err) {
			if err = e.Client.Create(ctx, p, client.DryRunAll); err != nil {
				return fmt.Errorf("准入 Policy dry-run: %w", err)
			}
		} else if err != nil {
			return err
		} else if existing.Labels[labelKey] != e.State.ID || !sameHoldPolicy(&existing, p) || (w.Instance.PolicyUID != "" && existing.UID != w.Instance.PolicyUID) {
			return errors.New("恢复前 Policy 身份/内容改变")
		}
		var binding admissionv1.ValidatingAdmissionPolicyBinding
		err = e.Client.Get(ctx, client.ObjectKeyFromObject(b), &binding)
		if apierrors.IsNotFound(err) {
			if err = e.Client.Create(ctx, b, client.DryRunAll); err != nil {
				return fmt.Errorf("准入 Binding dry-run: %w", err)
			}
		} else if err != nil {
			return err
		} else if binding.Labels[labelKey] != e.State.ID || !equality.Semantic.DeepEqual(binding.Spec, b.Spec) || (w.Instance.BindingUID != "" && binding.UID != w.Instance.BindingUID) {
			return errors.New("恢复前 Binding 身份/内容改变")
		}
	}
	if HasPendingVolumes(e.State) {
		for _, host := range []Host{e.State.SourceConfig, e.State.TargetConfig} {
			if _, err := e.filesystem(ctx, host, "/", "free"); err != nil {
				return err
			}
		}
		for _, view := range volumeViews(e.State) {
			if view.Volume.Phase == phaseStarted {
				continue
			}
			path, err := volumePath(&view.Volume.PV)
			if err != nil {
				return err
			}
			if _, err = e.filesystem(ctx, e.State.SourceConfig, path, "probe"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) instancePeers(ctx context.Context, w *Workload, originalPresent bool) error {
	if err := e.instanceController(ctx, w); err != nil {
		return err
	}
	pods, err := e.workloadPods(ctx, w)
	if err != nil {
		return err
	}
	peers := 0
	for _, p := range pods {
		if terminalPod(&p) || p.UID == w.Instance.Pod.UID {
			continue
		}
		if !podReady(&p) {
			return fmt.Errorf("其他副本 %s 未 Ready", p.Name)
		}
		peers++
	}
	if peers != int(w.Replicas)-1 {
		return errors.New("同组副本数量改变，禁止停止下一实例")
	}
	if originalPresent {
		_, err = e.liveInstance(ctx, w)
	}
	return err
}

func (e *Engine) instanceStopped(ctx context.Context, w *Workload) error {
	if err := e.guard(ctx, true); err != nil {
		return err
	}
	if err := e.instanceController(ctx, w); err != nil {
		return err
	}
	if err := e.checkHold(ctx, w); err != nil {
		return err
	}
	var pod corev1.Pod
	err := e.Client.Get(ctx, client.ObjectKeyFromObject(&w.Instance.Pod), &pod)
	if err == nil {
		return errors.New("所选实例仍存在或被提前重建，禁止复制/改绑")
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	output, err := e.Hosts.Run(ctx, e.State.SourceConfig, runtimeCLI+" ps -o json")
	if err != nil {
		return err
	}
	var running struct {
		Containers []struct {
			Labels map[string]string `json:"labels"`
		} `json:"containers"`
	}
	if err = json.Unmarshal([]byte(output), &running); err != nil {
		return errors.New("无法解析源运行时容器状态")
	}
	for _, c := range running.Containers {
		if c.Labels["io.kubernetes.pod.uid"] == string(w.Instance.Pod.UID) {
			return errors.New("源实例容器仍在运行")
		}
	}
	return nil
}

func (e *Engine) stopInstance(ctx context.Context, w *Workload) (result error) {
	// A failed activation/eviction must not disable healing of the still-live
	// original. Once termination begins, retain the hold and fail closed.
	defer func() {
		if result == nil {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		var p corev1.Pod
		if err := e.Client.Get(cleanup, client.ObjectKeyFromObject(&w.Instance.Pod), &p); err == nil && p.UID == w.Instance.Pod.UID && p.DeletionTimestamp == nil && podReady(&p) {
			result = errors.Join(result, e.removeHold(cleanup, w))
		}
	}()
	if err := e.instancePeers(ctx, w, false); err != nil {
		return err
	}
	var original corev1.Pod
	lookupErr := e.Client.Get(ctx, client.ObjectKeyFromObject(&w.Instance.Pod), &original)
	if lookupErr != nil && !apierrors.IsNotFound(lookupErr) {
		return lookupErr
	}
	online := lookupErr == nil && original.DeletionTimestamp == nil
	if lookupErr == nil && (original.UID != w.Instance.Pod.UID || original.Spec.NodeName != e.State.Source.Name) {
		return errors.New("原实例已经替换")
	}
	if online {
		if err := e.checkInstancePlacement(ctx, w); err != nil {
			return err
		}
	}
	for _, v := range w.Volumes {
		if e.State.Options.Precopy && (v.Precopy == nil || (!v.Precopy.Ready && !v.Precopy.Converged)) {
			return errors.New("全部 PVC 预同步策略完成后才允许停写")
		}
		view := volumeView(w, v)
		if err := e.copyBindings(ctx, view); err != nil {
			return err
		}
		if !online {
			continue
		}
		if _, err := e.liveInstance(ctx, w); err != nil {
			return err
		}
		if released, err := e.copyConsumersReleased(ctx, view, original.UID); err != nil {
			return err
		} else if !released {
			return errors.New("复制助手或目标卷消费者未释放")
		}
	}
	if online {
		// Online copying may run for hours. Recheck live capacity and the
		// remaining rsync growth/scratch before disrupting the original Pod.
		if err := e.checkCapacity(ctx); err != nil {
			return err
		}
		if len(w.Volumes) != 0 {
			if err := e.checkSpace(ctx); err != nil {
				return err
			}
		}
	}
	if err := e.ensureHold(ctx, w); err != nil {
		return err
	}
	var pod corev1.Pod
	err := e.Client.Get(ctx, client.ObjectKeyFromObject(&w.Instance.Pod), &pod)
	if err == nil {
		if pod.UID != w.Instance.Pod.UID || pod.Spec.NodeName != e.State.Source.Name {
			return errors.New("源实例已替换")
		}
		if pod.DeletionTimestamp == nil {
			if !podReady(&pod) {
				return errors.New("源实例已不 Ready")
			}
			w.Instance.EvictionRequestedAt = time.Now().UTC()
			if err = e.checkpoint(); err != nil {
				return err
			}
			eviction := &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}, DeleteOptions: &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}}}
			if err = e.Client.SubResource("eviction").Create(ctx, &pod, eviction); err != nil {
				return err
			}
			w.Instance.EvictedAt = time.Now().UTC()
			if err = e.checkpoint(); err != nil {
				return err
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	} else if w.Instance.EvictionRequestedAt.IsZero() {
		return errors.New("原实例在驱逐前消失，保留现场")
	}
	if err = e.wait(ctx, "原实例正常退出", func(ctx context.Context) (bool, error) {
		var p corev1.Pod
		err := e.Client.Get(ctx, client.ObjectKeyFromObject(&w.Instance.Pod), &p)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}); err != nil {
		return err
	}
	if err = e.instanceStopped(ctx, w); err != nil {
		return err
	}
	w.Instance.StoppedAt = time.Now().UTC()
	w.Phase = phaseStopped
	return e.checkpoint()
}

func (e *Engine) startInstance(ctx context.Context, w *Workload) error {
	if err := e.instanceController(ctx, w); err != nil {
		return err
	}
	for _, v := range w.Volumes {
		if v.Phase != phaseSwitched {
			return errors.New("实例全部 PVC 改绑完成后才允许启动")
		}
	}
	name := w.Instance.Pod.Name
	ownerUID := w.UID
	if rs := w.Instance.ReplicaSet; rs != nil {
		ownerUID = rs.UID
		if w.Instance.ReplacementName == "" {
			w.Instance.ReplacementName = holdName(e.State, w)
			if err := e.checkpoint(); err != nil {
				return err
			}
		}
		name = w.Instance.ReplacementName
		var pod corev1.Pod
		err := e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: name}, &pod)
		if apierrors.IsNotFound(err) {
			template := rs.Spec.Template.DeepCopy()
			if template.Labels == nil {
				template.Labels = map[string]string{}
			}
			template.Labels[labelKey] = e.State.ID
			pod = corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: w.Namespace, Labels: template.Labels, Annotations: template.Annotations, OwnerReferences: []metav1.OwnerReference{*metav1.GetControllerOf(&w.Instance.Pod)}}, Spec: template.Spec}
			if pod.Spec.NodeSelector == nil {
				pod.Spec.NodeSelector = map[string]string{}
			}
			pod.Spec.NodeSelector[hostnameLabel] = e.State.Target.Name
			if err = e.Client.Create(ctx, &pod); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if !ownedBy(&pod, rs.UID) || pod.Labels[labelKey] != e.State.ID || pod.Spec.NodeSelector[hostnameLabel] != e.State.Target.Name {
			return errors.New("目标替换 Pod 不属于本次迁移")
		}
	} else {
		if err := e.removeHold(ctx, w); err != nil {
			return err
		}
	}
	if err := e.waitTargetInstance(ctx, w, name, ownerUID); err != nil {
		return err
	}
	if w.Instance.ReadyAt.IsZero() {
		w.Instance.ReadyAt = time.Now().UTC()
		if err := e.checkpoint(); err != nil {
			return err
		}
	}
	if err := e.removeHold(ctx, w); err != nil {
		return err
	}
	if err := e.waitInstanceReplicas(ctx, w); err != nil {
		return err
	}
	w.Phase = phaseStarted
	for _, v := range w.Volumes {
		v.Phase = phaseStarted
	}
	return e.checkpoint()
}

func (e *Engine) waitInstanceReplicas(ctx context.Context, w *Workload) error {
	return e.wait(ctx, "同组全部副本 Ready", func(ctx context.Context) (bool, error) {
		if err := e.instanceController(ctx, w); err != nil {
			return false, err
		}
		pods, err := e.workloadPods(ctx, w)
		if err != nil {
			return false, err
		}
		n := 0
		for _, p := range pods {
			if terminalPod(&p) {
				continue
			}
			n++
			if !podReady(&p) {
				return false, nil
			}
		}
		return n == int(w.Replicas), nil
	})
}

func (e *Engine) applyInstances(ctx context.Context) error {
	if e.State.Phase == phaseValidated && e.State.SourceRestored {
		return e.Verify(ctx)
	}
	if e.State.Phase == phaseValidated {
		if err := e.validateInstances(ctx); err != nil {
			return err
		}
		return e.finishInstances(ctx)
	}
	if err := e.guard(ctx, true); err != nil {
		return err
	}
	if err := e.claimNodes(ctx); err != nil {
		return err
	}
	for _, step := range []struct {
		name string
		run  func(context.Context) error
	}{{"instance-preflight", e.preflightInstances}, {"capacity-preflight", e.checkCapacity}, {"health-before", e.checkHealth}, {"image-prewarm", e.warmImages}} {
		if err := e.stage(ctx, step.name, "", step.run); err != nil {
			return err
		}
	}
	if e.Copier == nil && HasPendingVolumes(e.State) {
		return errors.New("复制执行器未初始化")
	}
	if _, external := e.Copier.(*processCopier); external && HasPendingVolumes(e.State) {
		if err := CheckCopyTool(ctx, e.State.Options); err != nil {
			return err
		}
	}
	for _, w := range e.State.Workloads {
		if err := e.instanceController(ctx, w); err != nil {
			return err
		}
		for _, v := range w.Volumes {
			if err := e.prepareVolume(ctx, volumeView(w, v)); err != nil {
				return err
			}
		}
	}
	if err := e.stage(ctx, "disk-preflight", "", e.checkSpace); err != nil {
		return err
	}
	if err := e.cordonSource(ctx); err != nil {
		return err
	}
	for _, w := range e.State.Workloads {
		if w.Phase == phaseStarted {
			if err := e.validateInstance(ctx, w); err != nil {
				return err
			}
			continue
		}
		e.logf("逐实例迁移 resource=%s phase=%s volumes=%d", workloadKey(w), w.Phase, len(w.Volumes))
		if err := e.refreshOnlinePrecopy(ctx, w); err != nil {
			return err
		}
		if w.Phase == phasePlanned {
			if err := e.instancePeers(ctx, w, true); err != nil {
				return err
			}
			for _, v := range w.Volumes {
				view := volumeView(w, v)
				if err := e.stage(ctx, "online-precopy", workloadKey(w)+"/"+v.PVC.Name, func(ctx context.Context) error { return e.precopyVolume(ctx, view) }); err != nil {
					return err
				}
			}
			w.Phase = phasePrepared
			if err := e.checkpoint(); err != nil {
				return err
			}
		}
		if oneOf(w.Phase, phasePrepared, phaseStopping) {
			w.Phase = phaseStopping
			if err := e.checkpoint(); err != nil {
				return err
			}
			if err := e.stage(ctx, "stop-writes", workloadKey(w), func(ctx context.Context) error { return e.stopInstance(ctx, w) }); err != nil {
				return err
			}
		}
		if w.Phase == phaseStopped {
			for _, v := range w.Volumes {
				if oneOf(v.Phase, phasePrepared, phasePrecopyReady) {
					v.Phase = phaseStopped
					if err := e.checkpoint(); err != nil {
						return err
					}
				}
				if err := e.copyVolume(ctx, volumeView(w, v)); err != nil {
					return err
				}
			}
			w.Phase = phaseVerified
			if err := e.checkpoint(); err != nil {
				return err
			}
		}
		if oneOf(w.Phase, phaseVerified, phaseSwitching) {
			w.Phase = phaseSwitching
			if err := e.checkpoint(); err != nil {
				return err
			}
			for _, v := range w.Volumes {
				if err := e.switchVolume(ctx, volumeView(w, v)); err != nil {
					return err
				}
			}
			w.Phase = phaseStarting
			if err := e.checkpoint(); err != nil {
				return err
			}
		}
		if w.Phase == phaseStarting {
			if err := e.stage(ctx, "start-workload", workloadKey(w), func(ctx context.Context) error { return e.startInstance(ctx, w) }); err != nil {
				return err
			}
		}
	}
	if err := e.validateInstances(ctx); err != nil {
		return err
	}
	if err := e.checkHealth(ctx); err != nil {
		return err
	}
	e.State.Phase = phaseValidated
	if err := e.checkpoint(); err != nil {
		return err
	}
	return e.finishInstances(ctx)
}

func (e *Engine) validateInstance(ctx context.Context, w *Workload) error {
	if w.Phase != phaseStarted {
		return errors.New("实例迁移尚未完成")
	}
	if err := e.waitInstanceReplicas(ctx, w); err != nil {
		return err
	}
	name := w.Instance.Pod.Name
	if w.Instance.ReplacementName != "" {
		name = w.Instance.ReplacementName
	}
	var pod corev1.Pod
	if err := e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: name}, &pod); err != nil {
		return err
	}
	if pod.UID != w.Instance.ReplacementUID || !podReady(&pod) || pod.Spec.NodeName != e.State.Target.Name {
		return errors.New("完成实例的 UID/就绪/节点改变")
	}
	for _, v := range w.Volumes {
		if v.Phase != phaseStarted || !validDigest(v.SourceDigest) || !validDigest(v.TargetDigest) || *v.SourceDigest != *v.TargetDigest {
			return errors.New("完成实例缺少全部卷校验")
		}
		var pvc corev1.PersistentVolumeClaim
		if err := e.Client.Get(ctx, client.ObjectKeyFromObject(&v.PVC), &pvc); err != nil {
			return err
		}
		if pvc.UID != v.NewPVCUID || pvc.Status.Phase != corev1.ClaimBound || v.TargetPV == nil || pvc.Spec.VolumeName != v.TargetPV.Name || pvc.Labels[labelKey] != e.State.ID {
			return errors.New("完成 PVC 绑定改变")
		}
		pvs := []*corev1.PersistentVolume{v.TargetPV}
		if e.State.Phase != phaseValidated {
			pvs = append(pvs, &v.PV)
		}
		for _, saved := range pvs {
			var pv corev1.PersistentVolume
			if err := e.Client.Get(ctx, client.ObjectKeyFromObject(saved), &pv); err != nil {
				return err
			}
			if pv.UID != saved.UID || pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
				return errors.New("新/旧 PV 身份或 Retain 改变")
			}
			if saved == v.TargetPV && (!pvOnNode(&pv, e.State.Target.Name) || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID) {
				return errors.New("目标 PV 节点/claimRef 改变")
			}
		}
	}
	return nil
}

func (e *Engine) validateInstances(ctx context.Context) error {
	if err := e.guard(ctx, false); err != nil {
		return err
	}
	for _, w := range e.State.Workloads {
		if err := e.validateInstance(ctx, w); err != nil {
			return fmt.Errorf("%s: %w", workloadKey(w), err)
		}
	}
	var policies admissionv1.ValidatingAdmissionPolicyList
	var bindings admissionv1.ValidatingAdmissionPolicyBindingList
	if err := e.Client.List(ctx, &policies, client.MatchingLabels{labelKey: e.State.ID}); err != nil {
		return err
	}
	if err := e.Client.List(ctx, &bindings, client.MatchingLabels{labelKey: e.State.ID}); err != nil {
		return err
	}
	if len(policies.Items)+len(bindings.Items) != 0 {
		return errors.New("本次迁移仍有准入保护残留")
	}
	return nil
}

func (e *Engine) finishInstances(ctx context.Context) error {
	if e.State.SourceRestored {
		return e.verifyInstanceCompletion(ctx)
	}
	for _, identity := range []NodeIdentity{e.State.Source, e.State.Target} {
		var n corev1.Node
		if err := e.Client.Get(ctx, types.NamespacedName{Name: identity.Name}, &n); err != nil {
			return err
		}
		if n.UID != identity.UID {
			return errors.New("完成前 Node UID 改变")
		}
		if n.Annotations[labelKey] != "" && n.Annotations[labelKey] != e.State.ID {
			return errors.New("节点被其他迁移占用")
		}
		delete(n.Annotations, labelKey)
		if identity.Name == e.State.Source.Name {
			n.Spec.Unschedulable = e.State.SourceUnschedulable
		}
		if err := e.Client.Update(ctx, &n); err != nil {
			return err
		}
	}
	e.State.SourceRestored = true
	return e.checkpoint()
}

func (e *Engine) verifyInstanceCompletion(ctx context.Context) error {
	if !e.State.SourceRestored {
		return errors.New("尚未完成源调度状态及节点标记恢复，请 resume")
	}
	// Scheduling and lifecycle decisions made by the user after completion are
	// outside the migration contract; never undo a later cordon or retirement.
	return nil
}
