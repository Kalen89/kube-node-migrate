package nodemigrate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (e *Engine) targetInstance(ctx context.Context, w *Workload, name string, owner types.UID) (*corev1.Pod, error) {
	var pod corev1.Pod
	err := e.Client.Get(ctx, client.ObjectKey{Namespace: w.Namespace, Name: name}, &pod)
	if apierrors.IsNotFound(err) {
		if w.Instance.ReplacementUID != "" {
			return nil, errors.New("已经观察的目标实例消失，拒绝自动创建另一个实例")
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !ownedBy(&pod, owner) || pod.UID == w.Instance.Pod.UID || pod.DeletionTimestamp != nil || (pod.Spec.NodeName != "" && pod.Spec.NodeName != e.State.Target.Name) {
		return nil, errors.New("目标实例身份/节点不符或正在删除")
	}
	if w.Instance.ReplacementUID != "" && pod.UID != w.Instance.ReplacementUID {
		return nil, errors.New("目标实例再次被替换")
	}
	if terminalPod(&pod) {
		return nil, fmt.Errorf("目标实例已终止 phase=%s", pod.Status.Phase)
	}
	if w.Instance.ReplacementUID == "" {
		w.Instance.ReplacementUID = pod.UID
		if err = e.checkpoint(); err != nil {
			return nil, err
		}
	}
	return &pod, nil
}

func containersRunning(pod *corev1.Pod) bool {
	if podReady(pod) {
		return true
	}
	for _, container := range pod.Spec.Containers {
		running := false
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == container.Name && status.State.Running != nil {
				running = true
			}
		}
		if !running {
			return false
		}
	}
	return len(pod.Spec.Containers) > 0
}

// All waits share one timeout: splitting progress must not multiply downtime.
func (e *Engine) waitTargetInstance(ctx context.Context, w *Workload, name string, owner types.UID) error {
	timeout := e.State.Options.WaitTimeout
	if timeout <= 0 {
		timeout = defaultWaitTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	lastReconcile := time.Time{}
	for _, step := range []struct {
		stage string
		ready func(*corev1.Pod) bool
	}{
		{"wait-pod-created", func(*corev1.Pod) bool { return true }},
		{"wait-pod-scheduled", func(p *corev1.Pod) bool { return p.Spec.NodeName == e.State.Target.Name }},
		{"wait-container-started", containersRunning},
		{"wait-pod-ready", podReady},
	} {
		lastDetail := ""
		if err := e.stage(ctx, step.stage, workloadKey(w), func(ctx context.Context) error {
			return e.wait(ctx, strings.TrimPrefix(stageTitle(step.stage), "等待")+" "+name, func(ctx context.Context) (bool, error) {
				pod, err := e.targetInstance(ctx, w, name, owner)
				if err != nil {
					return false, err
				}
				if pod == nil {
					// Informer propagation can briefly outlive policy deletion. Retry
					// the metadata notification, not the eviction or data transfer.
					if w.Kind == kindStatefulSet && time.Since(lastReconcile) >= 15*time.Second {
						if err := e.stage(ctx, "statefulset-reconcile", workloadKey(w), func(ctx context.Context) error { return e.requestStatefulSetReconcile(ctx, w) }); err != nil {
							return false, err
						}
						lastReconcile = time.Now()
						pod, err = e.targetInstance(ctx, w, name, owner)
						if err != nil {
							return false, err
						}
					}
					if pod == nil {
						return false, nil
					}
				}
				if step.ready(pod) {
					return true, nil
				}
				// Reasons are useful; arbitrary API error messages may contain URLs
				// or other private configuration, so they are not logged here.
				detail := fmt.Sprintf("phase=%s node=%s", pod.Status.Phase, pod.Spec.NodeName)
				for _, c := range pod.Status.Conditions {
					if (c.Type == corev1.PodScheduled || c.Type == corev1.PodReady) && c.Status != corev1.ConditionTrue {
						detail += fmt.Sprintf(" %s=%s reason=%s", c.Type, c.Status, c.Reason)
					}
				}
				for _, c := range append(append([]corev1.ContainerStatus(nil), pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...) {
					if c.State.Waiting != nil {
						detail += fmt.Sprintf(" container=%s reason=%s", c.Name, c.State.Waiting.Reason)
					}
				}
				if detail != lastDetail {
					e.logf("目标实例等待详情 stage=%s pod=%s/%s %s", step.stage, w.Namespace, name, detail)
					lastDetail = detail
				}
				return false, nil
			})
		}); err != nil {
			return err
		}
		if step.stage == "wait-pod-created" && w.Kind == kindStatefulSet {
			if err := e.patchReconcileAnnotation(ctx, w, true); err != nil {
				return err
			}
		}
	}
	return nil
}
