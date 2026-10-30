package nodemigrate

import (
	"context"
	"fmt"
	"net"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Refresh placement before applying a saved plan and before every eviction.
func (e *Engine) checkPendingPlacement(ctx context.Context) error {
	var target corev1.Node
	if err := e.Client.Get(ctx, types.NamespacedName{Name: e.State.Target.Name}, &target); err != nil {
		return err
	}
	var pods corev1.PodList
	if err := e.Client.List(ctx, &pods); err != nil {
		return err
	}
	for _, w := range e.State.Workloads {
		if w.Phase == phaseStarted {
			continue
		}
		if err := e.instancePlacement(w, &target, pods.Items); err != nil {
			return err
		}
		// Earlier instances will remain on the target when later ones move.
		if targetPod(w, pods.Items, target.Name) == nil {
			pods.Items = append(pods.Items, corev1.Pod{Spec: *w.Template.Spec.DeepCopy(), ObjectMeta: w.Template.ObjectMeta})
			pods.Items[len(pods.Items)-1].Namespace = w.Namespace
			pods.Items[len(pods.Items)-1].Name = workloadKey(w)
			pods.Items[len(pods.Items)-1].Spec.NodeName = target.Name
		}
	}
	return e.refreshHelperTaints(ctx, &target)
}

func (e *Engine) checkInstancePlacement(ctx context.Context, w *Workload) error {
	var target corev1.Node
	if err := e.Client.Get(ctx, types.NamespacedName{Name: e.State.Target.Name}, &target); err != nil {
		return err
	}
	var pods corev1.PodList
	if err := e.Client.List(ctx, &pods); err != nil {
		return err
	}
	if err := e.instancePlacement(w, &target, pods.Items); err != nil {
		return err
	}
	return e.refreshHelperTaints(ctx, &target)
}

// Helpers tolerate current custom taints; the business spec above is always
// validated using its own original tolerations before these snapshots change.
func (e *Engine) refreshHelperTaints(ctx context.Context, target *corev1.Node) error {
	var source corev1.Node
	if err := e.Client.Get(ctx, types.NamespacedName{Name: e.State.Source.Name}, &source); err != nil {
		return err
	}
	if source.UID != e.State.Source.UID || !nodeReady(&source) || source.DeletionTimestamp != nil {
		return fmt.Errorf("源节点身份改变、未 Ready 或正在删除")
	}
	e.State.SourceTaints = source.Spec.DeepCopy().Taints
	e.State.TargetTaints = target.Spec.DeepCopy().Taints
	return nil
}

func (e *Engine) instancePlacement(w *Workload, target *corev1.Node, pods []corev1.Pod) error {
	unavailable := !nodeReady(target) || target.Spec.Unschedulable || target.DeletionTimestamp != nil
	if target.UID != e.State.Target.UID || unavailable {
		return fmt.Errorf("目标节点身份改变、未 Ready 或不可调度")
	}
	spec := w.Template.Spec.DeepCopy()
	if w.Kind == kindDeployment && spec.NodeSelector[hostnameLabel] == e.State.Source.Name {
		spec.NodeSelector[hostnameLabel] = target.Name
	}
	if err := validatePlacement(spec, target); err != nil {
		return fmt.Errorf("%s: %w", workloadKey(w), err)
	}
	replacement := targetPod(w, pods, target.Name)
	for i := range pods {
		p := &pods[i]
		if p == replacement || p.Spec.NodeName != target.Name || terminalPod(p) {
			continue
		}
		if port, ok := conflictingHostPort(spec, &p.Spec); ok {
			return fmt.Errorf("%s 的 hostPort %s/%d 与目标 Pod %s/%s 冲突", workloadKey(w), port.Protocol, port.HostPort, p.Namespace, p.Name)
		}
	}
	return nil
}

// Match only the selected instance, never an arbitrary peer of its controller.
func targetPod(w *Workload, pods []corev1.Pod, target string) *corev1.Pod {
	if w.Instance == nil {
		return nil
	}
	i := w.Instance
	name, owner := i.Pod.Name, w.UID
	if i.ReplicaSet != nil {
		name, owner = i.ReplacementName, i.ReplicaSet.UID
	}
	for n := range pods {
		p := &pods[n]
		originalOrUnrelated := p.Namespace != w.Namespace || p.UID == i.Pod.UID || !ownedBy(p, owner)
		if originalOrUnrelated || terminalPod(p) {
			continue
		}
		matches := name != "" && p.Name == name
		if i.ReplacementUID != "" {
			matches = p.UID == i.ReplacementUID
		}
		if matches && (p.Spec.NodeName == target || p.Spec.NodeName == "" && p.Spec.NodeSelector[hostnameLabel] == target) {
			return p
		}
	}
	return nil
}

func conflictingHostPort(a, b *corev1.PodSpec) (corev1.ContainerPort, bool) {
	ports := func(spec *corev1.PodSpec) []corev1.ContainerPort {
		var result []corev1.ContainerPort
		for _, c := range append(append([]corev1.Container(nil), spec.Containers...), spec.InitContainers...) {
			for _, p := range c.Ports {
				if p.HostPort > 0 {
					if p.Protocol == "" {
						p.Protocol = corev1.ProtocolTCP
					}
					result = append(result, p)
				}
			}
		}
		return result
	}
	for _, x := range ports(a) {
		for _, y := range ports(b) {
			samePort := x.HostPort == y.HostPort && x.Protocol == y.Protocol
			overlappingIP := hostIPsOverlap(x.HostIP, y.HostIP)
			if samePort && overlappingIP {
				return x, true
			}
		}
	}
	return corev1.ContainerPort{}, false
}

// Empty/0.0.0.0 matches all scheduler HostPortInfo entries. An explicit IPv6
// wildcard also overlaps specific IPv6 binds, without rejecting IPv4-only binds.
func hostIPsOverlap(a, b string) bool {
	if a == b || a == "" || b == "" || a == "0.0.0.0" || b == "0.0.0.0" {
		return true
	}
	x, y := net.ParseIP(a), net.ParseIP(b)
	if x == nil || y == nil {
		return false
	}
	return x.Equal(y) || (x.To4() == nil && y.To4() == nil && (x.IsUnspecified() || y.IsUnspecified()))
}
