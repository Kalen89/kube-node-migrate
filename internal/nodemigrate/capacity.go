package nodemigrate

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CapacityReport describes scheduler requests, not a guarantee of runtime peaks.
type CapacityReport struct {
	Allocatable corev1.ResourceList `json:"allocatable"`
	Existing    corev1.ResourceList `json:"existing"`
	Incoming    corev1.ResourceList `json:"incoming"`
	Reserve     corev1.ResourceList `json:"reserve"`
	Required    corev1.ResourceList `json:"required"`
	Warnings    []string            `json:"warnings,omitempty"`
}

func addResources(dst, src corev1.ResourceList) {
	for key, q := range src {
		n := dst[key]
		n.Add(q)
		dst[key] = n
	}
}
func maxResources(dst, src corev1.ResourceList) {
	for key, q := range src {
		if old := dst[key]; old.Cmp(q) < 0 {
			dst[key] = q.DeepCopy()
		}
	}
}

// podRequests includes init sidecars, sequential init peaks and Pod overhead.
func podRequests(spec *corev1.PodSpec) corev1.ResourceList {
	return podResources(spec, false)
}

func podResources(spec *corev1.PodSpec, limits bool) corev1.ResourceList {
	sum, prefix, peak := corev1.ResourceList{}, corev1.ResourceList{}, corev1.ResourceList{}
	resources := func(c corev1.Container) corev1.ResourceList {
		if limits {
			return c.Resources.Limits
		}
		return c.Resources.Requests
	}
	for _, c := range spec.Containers {
		addResources(sum, resources(c))
	}
	for _, c := range spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			addResources(prefix, resources(c))
			maxResources(peak, prefix)
		} else {
			step := prefix.DeepCopy()
			addResources(step, resources(c))
			maxResources(peak, step)
		}
	}
	addResources(sum, prefix)
	maxResources(sum, peak)
	for key, overhead := range spec.Overhead {
		if q := sum[key]; !limits || q.Sign() > 0 {
			addResources(sum, corev1.ResourceList{key: overhead})
		}
	}
	return sum
}

func capacityReserve(o Options) (corev1.ResourceList, error) {
	cpu, memory, pods := o.ReserveCPU, o.ReserveMemory, o.ReservePods
	if cpu == "" {
		cpu = "200m"
	}
	if memory == "" {
		memory = "256Mi"
	}
	if pods == 0 {
		pods = 2
	}
	c, err := resource.ParseQuantity(cpu)
	if err != nil || c.Sign() < 0 {
		return nil, fmt.Errorf("reserve-cpu 无效")
	}
	m, err := resource.ParseQuantity(memory)
	if err != nil || m.Sign() < 0 {
		return nil, fmt.Errorf("reserve-memory 无效")
	}
	if pods < 2 {
		return nil, fmt.Errorf("reserve-pods 至少为 2，用于迁移助手")
	}
	if c.Cmp(resource.MustParse("200m")) < 0 || m.Cmp(resource.MustParse("256Mi")) < 0 {
		return nil, fmt.Errorf("迁移助手至少预留 CPU 200m、内存 256Mi")
	}
	reserve := corev1.ResourceList{corev1.ResourceCPU: c, corev1.ResourceMemory: m, corev1.ResourcePods: *resource.NewQuantity(pods, resource.DecimalSI)}
	// User reserves are totals, with an automatically raised minimum for copies.
	minimum := corev1.ResourceList{}
	for range max(1, o.PVConcurrency) {
		addResources(minimum, serviceResources(o.RsyncResources).Requests)
		addResources(minimum, corev1.ResourceList{corev1.ResourcePods: resource.MustParse("1")})
	}
	maxResources(minimum, serviceResources(o.HelperResources).Requests)
	maxResources(reserve, minimum)
	return reserve, nil
}

func (e *Engine) checkCapacity(ctx context.Context) error {
	if e.State.Options.Mode == ModeWorkloads {
		if err := e.checkPendingPlacement(ctx); err != nil {
			return err
		}
	}
	var node corev1.Node
	if err := e.Client.Get(ctx, types.NamespacedName{Name: e.State.Target.Name}, &node); err != nil {
		return err
	}
	for _, condition := range node.Status.Conditions {
		if condition.Status == corev1.ConditionTrue && (condition.Type == corev1.NodeMemoryPressure || condition.Type == corev1.NodeDiskPressure || condition.Type == corev1.NodePIDPressure) {
			return fmt.Errorf("目标节点处于 %s", condition.Type)
		}
	}
	reserve, err := capacityReserve(e.State.Options)
	if err != nil {
		return err
	}
	report := &CapacityReport{Allocatable: node.Status.Allocatable.DeepCopy(), Existing: corev1.ResourceList{}, Incoming: corev1.ResourceList{}, Reserve: reserve, Required: corev1.ResourceList{}}
	e.State.Capacity = report
	var pods corev1.PodList
	if err := e.Client.List(ctx, &pods); err != nil {
		return err
	}
	assistants := corev1.ResourceList{}
	for _, p := range pods.Items {
		if terminalPod(&p) {
			continue
		}
		request := podRequests(&p.Spec)
		request[corev1.ResourcePods] = resource.MustParse("1")
		switch p.Spec.NodeName {
		case e.State.Target.Name:
			if e.warmPod(&p) {
				continue // Prewarm leftovers are removed before the queue starts.
			}
			if e.assistantPod(&p) {
				addResources(assistants, request)
				continue // Already represented by the total helper reserve.
			}
			addResources(report.Existing, request)
		case e.State.Source.Name:
			if e.State.Options.Mode == ModeWorkloads {
				continue // The confirmed instance snapshots are counted below.
			}
			owner := metav1.GetControllerOf(&p)
			if owner != nil && owner.Kind == kindDaemonSet {
				continue
			} // The target counterpart is counted above.
			addResources(report.Incoming, request)
		}
	}
	maxResources(report.Reserve, assistants)
	if e.State.Options.Mode == ModeWorkloads {
		for _, w := range e.State.Workloads {
			if w.Instance == nil {
				continue
			}
			replacement := targetPod(w, pods.Items, node.Name)
			if replacement != nil && replacement.Spec.NodeName == node.Name {
				continue
			}
			request := podRequests(&w.Instance.Pod.Spec)
			if replacement != nil {
				request = podRequests(&replacement.Spec)
			}
			request[corev1.ResourcePods] = resource.MustParse("1")
			addResources(report.Incoming, request)
		}
	}
	// One workload at a time rolls out; reserve its largest extra Pod as well.
	for _, w := range e.State.Workloads {
		if e.State.Options.Mode != ModeWorkloads && w.Kind == kindDeployment && w.Volume == nil && w.Phase != phaseStarted {
			q := podRequests(&w.Template.Spec)
			q[corev1.ResourcePods] = resource.MustParse("2")
			maxResources(report.Reserve, q)
		}
	}
	addResources(report.Required, report.Existing)
	addResources(report.Required, report.Incoming)
	addResources(report.Required, report.Reserve)
	for _, key := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourcePods} {
		if q := report.Allocatable[key]; q.Sign() <= 0 {
			return fmt.Errorf("目标节点缺少有效 allocatable %s", key)
		}
	}
	e.logf("容量预检 node=%s existing=[%s] incoming=[%s] reserve=[%s] required=[%s] allocatable=[%s]", node.Name, formatResources(report.Existing), formatResources(report.Incoming), formatResources(report.Reserve), formatResources(report.Required), formatResources(report.Allocatable))
	for key, required := range report.Required {
		available := report.Allocatable[key]
		if required.Cmp(available) > 0 {
			return fmt.Errorf("目标容量不足 resource=%s required=%s allocatable=%s", key, required.String(), available.String())
		}
	}
	warming, sourceWarming, err := e.prewarmCapacity(ctx)
	if err != nil {
		return err
	}
	if HasPendingVolumes(e.State) {
		if err := e.checkSourceCopyCapacity(ctx, pods.Items, sourceWarming); err != nil {
			return err
		}
	}
	namespaces := map[string]bool{}
	for _, w := range e.State.Workloads {
		namespaces[w.Namespace] = true
	}
	for namespace := range namespaces {
		var quotas corev1.ResourceQuotaList
		if err := e.Client.List(ctx, &quotas, client.InNamespace(namespace)); err != nil {
			return err
		}
		var ranges corev1.LimitRangeList
		if err := e.Client.List(ctx, &ranges, client.InNamespace(namespace)); err != nil {
			return err
		}
		if len(quotas.Items) > 0 || len(ranges.Items) > 0 {
			report.Warnings = append(report.Warnings, fmt.Sprintf("namespace %s 有 %d 个 ResourceQuota、%d 个 LimitRange；检查无 scope 配额与助手资源范围，其他准入以 API 为准", namespace, len(quotas.Items), len(ranges.Items)))
		}
		if len(quotas.Items)+len(ranges.Items) == 0 {
			continue
		}
		if err := e.namespaceCapacity(
			namespace,
			quotas.Items,
			ranges.Items,
			pods.Items,
			warming[namespace],
		); err != nil {
			return err
		}
	}
	report.Warnings = append(report.Warnings, "容量按 requests 计算；未配置 requests、运行峰值及其他调度约束仍需关注")
	for _, warning := range report.Warnings {
		e.logf("容量提示 %s", warning)
	}
	return nil
}

func formatResources(values corev1.ResourceList) string {
	parts := make([]string, 0, len(values))
	for key, q := range values {
		parts = append(parts, string(key)+"="+q.String())
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

func subtractResources(dst, src corev1.ResourceList) {
	for key, q := range src {
		n := dst[key]
		n.Sub(q)
		dst[key] = n
	}
}

func podNodeRequests(spec *corev1.PodSpec) corev1.ResourceList {
	q := podRequests(spec)
	q[corev1.ResourcePods] = resource.MustParse("1")
	return q
}

func pendingCopy(w *Workload) bool {
	for _, v := range workloadVolumes(w) {
		if !oneOf(v.Phase, phaseVerified, phaseSwitching, phaseSwitched, phaseStarted) {
			return true
		}
	}
	return false
}

func sourcePod(w *Workload, pods []corev1.Pod, source string) *corev1.Pod {
	for i := range pods {
		p := &pods[i]
		if terminalPod(p) || p.Spec.NodeName != source || p.Namespace != w.Namespace {
			continue
		}
		for _, uid := range w.OriginalPods {
			if p.UID == uid {
				return p
			}
		}
	}
	return nil
}

func (e *Engine) onlineCopy(w *Workload, original *corev1.Pod) bool {
	if !e.State.Options.Precopy {
		return false
	}
	return w.Instance == nil || original != nil && original.DeletionTimestamp == nil && oneOf(w.Phase, phasePlanned, phasePrepared, phaseStopping)
}

func (e *Engine) checkSourceCopyCapacity(ctx context.Context, pods []corev1.Pod, sourceWarming bool) error {
	var source corev1.Node
	if err := e.Client.Get(ctx, types.NamespacedName{Name: e.State.Source.Name}, &source); err != nil {
		return err
	}
	current, required := corev1.ResourceList{}, corev1.ResourceList{}
	for _, p := range pods {
		if p.Spec.NodeName == source.Name && !terminalPod(&p) {
			if e.warmPod(&p) {
				continue // warmImages removes leftovers before creating a helper.
			}
			addResources(current, podNodeRequests(&p.Spec))
		}
	}
	if sourceWarming {
		peak := current.DeepCopy()
		addResources(peak, podNodeRequests(&corev1.PodSpec{Containers: []corev1.Container{{Resources: serviceResources(e.State.Options.HelperResources)}}}))
		maxResources(required, peak)
	}
	sshd := podNodeRequests(&corev1.PodSpec{Containers: []corev1.Container{{Resources: serviceResources(e.State.Options.SSHDResources)}}})
	if e.State.Options.Mode != ModeWorkloads {
		for range max(1, e.State.Options.PVConcurrency) {
			addResources(current, sshd)
		}
		maxResources(required, current)
	} else {
		for _, w := range e.State.Workloads {
			original := sourcePod(w, pods, source.Name)
			if pendingCopy(w) {
				peak := current.DeepCopy()
				// Online rounds overlap the original. Offline rounds run only
				// after this selected instance exits; unrelated Pods remain.
				if original != nil && !e.onlineCopy(w, original) {
					subtractResources(peak, podNodeRequests(&original.Spec))
				}
				// A resumed sshd already consumes capacity in current.
				for _, p := range pods {
					if p.Spec.NodeName == source.Name && !terminalPod(&p) && copyPod(w, &p) {
						subtractResources(peak, podNodeRequests(&p.Spec))
					}
				}
				addResources(peak, sshd)
				maxResources(required, peak)
			}
			if original != nil {
				subtractResources(current, podNodeRequests(&original.Spec))
			}
			for _, p := range pods {
				if p.Spec.NodeName == source.Name && !terminalPod(&p) && copyPod(w, &p) {
					subtractResources(current, podNodeRequests(&p.Spec))
				}
			}
		}
	}
	for key, need := range required {
		available := source.Status.Allocatable[key]
		if need.Cmp(available) > 0 {
			return fmt.Errorf("源节点缺少复制助手容量 resource=%s required=%s allocatable=%s；调整 sshd.resources 或释放源端容量", key, need.String(), available.String())
		}
	}
	e.logf("源端复制容量预检 node=%s required=[%s]；按在线/停写阶段计算峰值", source.Name, formatResources(required))
	return nil
}

func (e *Engine) warmPod(p *corev1.Pod) bool {
	return p.Labels[labelKey] == e.State.ID && strings.HasPrefix(p.Name, "km-migration-warm-"+e.State.ID+"-")
}

func copyPod(w *Workload, p *corev1.Pod) bool {
	if p.Namespace != w.Namespace {
		return false
	}
	for _, v := range workloadVolumes(w) {
		if p.Labels["app.kubernetes.io/instance"] == "pv-migrate-"+v.CopyID+"-clusterip" {
			return true
		}
	}
	return false
}

func (e *Engine) assistantPod(p *corev1.Pod) bool {
	for _, w := range e.State.Workloads {
		if copyPod(w, p) {
			return true
		}
		for _, v := range workloadVolumes(w) {
			if p.Namespace == w.Namespace && p.Labels[labelKey] == e.State.ID && p.Name == "bind-"+v.TemporaryPVC {
				return true
			}
		}
	}
	return false
}
