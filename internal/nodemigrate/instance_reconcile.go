package nodemigrate

import (
	"context"
	"errors"
	"strings"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func reconcileAnnotation(s *State, w *Workload) string {
	return "kube-node-migrate.io/reconcile-" + strings.TrimPrefix(holdName(s, w), "km-move-")
}

// A CREATE hold can put the controller in exponential backoff. Deleting the
// policy is not a StatefulSet informer event. Updating only top-level metadata
// requeues the original controller without changing its template or replicas.
// The operation/instance-specific marker can be recovered after interruption.
func (e *Engine) patchReconcileAnnotation(ctx context.Context, w *Workload, clear bool, opts ...client.PatchOption) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		obj, count, err := e.getWorkload(ctx, w, true)
		if err != nil {
			return err
		}
		sts, ok := obj.(*appsv1.StatefulSet)
		if !ok || count != w.Replicas || w.Instance.StatefulSetSpec == nil || !equality.Semantic.DeepEqual(sts.Spec, *w.Instance.StatefulSetSpec) {
			return errors.New("触发协调前 StatefulSet 身份或 spec 改变")
		}
		key := reconcileAnnotation(e.State, w)
		value, present := sts.Annotations[key]
		if present && !strings.HasPrefix(value, e.State.ID+"/") {
			return errors.New("协调注解被其他客户端占用，拒绝覆盖")
		}
		if clear && !present {
			return nil
		}
		before := sts.DeepCopy()
		if clear {
			delete(sts.Annotations, key)
		} else {
			if sts.Annotations == nil {
				sts.Annotations = map[string]string{}
			}
			sts.Annotations[key] = e.State.ID + "/" + time.Now().UTC().Format(time.RFC3339Nano)
		}
		return e.Client.Patch(ctx, sts, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}), opts...)
	})
}

func (e *Engine) requestStatefulSetReconcile(ctx context.Context, w *Workload) error {
	for _, v := range w.Volumes {
		if v.Phase != phaseSwitched {
			return errors.New("全部 PVC 改绑完成前禁止触发目标实例协调")
		}
	}
	// API absence is checked again on retries; never wake a held instance.
	for _, obj := range []client.Object{&admissionv1.ValidatingAdmissionPolicy{}, &admissionv1.ValidatingAdmissionPolicyBinding{}} {
		err := e.Client.Get(ctx, types.NamespacedName{Name: holdName(e.State, w)}, obj)
		if err == nil {
			return errors.New("实例准入保护仍存在，禁止触发目标实例协调")
		}
		if !apierrors.IsNotFound(err) {
			return err
		}
	}
	if err := e.patchReconcileAnnotation(ctx, w, false); err != nil {
		return err
	}
	e.logf("已触发原 StatefulSet 协调 resource=%s；Pod 创建后清理本次临时顶层注解", workloadKey(w))
	return nil
}
