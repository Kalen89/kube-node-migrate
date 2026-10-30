package nodemigrate

import (
	"context"
	"errors"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A saved pre-copy estimate expires while the original Pod keeps writing.
// Never return an already terminating/stopped instance to online copying.
func (e *Engine) refreshOnlinePrecopy(ctx context.Context, w *Workload) error {
	if !e.State.Options.Precopy || !oneOf(w.Phase, phasePrepared, phaseStopping) {
		return nil
	}
	var original corev1.Pod
	err := e.Client.Get(ctx, client.ObjectKeyFromObject(&w.Instance.Pod), &original)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if original.UID != w.Instance.Pod.UID || original.Spec.NodeName != e.State.Source.Name {
		return errors.New("恢复预同步前原实例身份或节点已改变")
	}
	if original.DeletionTimestamp != nil {
		return nil
	}
	if !podReady(&original) {
		return errors.New("原实例尚在线但未 Ready，拒绝重新预同步或停写")
	}
	for _, v := range w.Volumes {
		if !oneOf(v.Phase, phasePrepared, phasePrecopying, phasePrecopyReady) {
			return errors.New("在线实例的卷已进入离线阶段，拒绝重置事务")
		}
		if v.Precopy != nil {
			v.Precopy.Ready, v.Precopy.Converged = false, false
			v.Phase = phasePrecopying
		}
	}
	w.Phase = phasePlanned
	e.logf("源实例仍在线，刷新全部卷的预同步策略 resource=%s", workloadKey(w))
	return e.checkpoint()
}
