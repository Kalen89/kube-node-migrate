package nodemigrate

import (
	"context"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// copyRuntime 仅采信当前 Job 所有的成功 Pod 中唯一 rsync 容器的时间。
// 该时间含 rsync 自身的连接、扫描和传输，不是纯网络时间或停机预测。
// Pod 已清理、多次尝试或状态缺失时返回 nil，禁止把未知耗时当作达标。
func (c *processCopier) copyRuntime(ctx context.Context, job *batchv1.Job) (*int64, error) {
	if job.Status.Failed > 0 {
		return nil, nil
	}
	var pods corev1.PodList
	if err := c.engine.Client.List(ctx, &pods, client.InNamespace(job.Namespace), client.MatchingLabels{"job-name": job.Name}); err != nil {
		return nil, err
	}
	var runtime *int64
	owned := 0
	for _, pod := range pods.Items {
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.Kind != "Job" || owner.UID != job.UID || owner.Name != job.Name {
			continue
		}
		owned++
		if pod.Status.Phase != corev1.PodSucceeded || len(pod.Spec.Containers) != 1 {
			continue
		}
		for _, status := range pod.Status.ContainerStatuses {
			end := status.State.Terminated
			if status.Name == pod.Spec.Containers[0].Name && end != nil && end.ExitCode == 0 && !end.StartedAt.IsZero() && end.FinishedAt.After(end.StartedAt.Time) {
				millis := end.FinishedAt.Sub(end.StartedAt.Time).Milliseconds()
				runtime = &millis
			}
		}
	}
	if owned != 1 {
		return nil, nil
	}
	return runtime, nil
}
