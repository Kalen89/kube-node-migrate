package nodemigrate

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// The prewarm queue is serial across namespaces and hosts. Cache each host's
// inventory once so quota and source-node checks share the same observation.
func (e *Engine) prewarmCapacity(ctx context.Context) (map[string]bool, bool, error) {
	groups, err := e.imageGroups(ctx, false)
	if err != nil {
		return nil, false, err
	}
	warming := map[string]bool{}
	caches := map[string]map[string]bool{}
	source := false
	for _, group := range groups {
		host := group.host
		if host.Name == "" {
			host = e.State.TargetConfig
		}
		cache, known := caches[host.Name]
		if !known {
			cache = map[string]bool{}
			if e.Hosts != nil {
				cache, err = e.imageCacheFor(ctx, host)
				if err != nil {
					return nil, false, err
				}
			}
			caches[host.Name] = cache
		}
		for image := range group.images {
			ref, err := normalizeImage(image)
			if err != nil {
				return nil, false, err
			}
			if !cache[ref] {
				warming[group.namespace] = true
				source = source || host.Name == e.State.Source.Name
			}
		}
	}
	return warming, source, nil
}

func podQuotaUsage(spec *corev1.PodSpec) corev1.ResourceList {
	usage := corev1.ResourceList{corev1.ResourcePods: resource.MustParse("1"), "count/pods": resource.MustParse("1")}
	for key, value := range podRequests(spec) {
		usage[key] = value.DeepCopy()
		usage[corev1.ResourceName("requests."+string(key))] = value.DeepCopy()
	}
	for key, value := range podResources(spec, true) {
		usage[corev1.ResourceName("limits."+string(key))] = value
	}
	return usage
}

// Namespace quota concerns objects that coexist, not reserved node headroom.
// Helpers are serial: warming/binding, online copy, offline copy, replacement.
func (e *Engine) namespaceCapacity(
	namespace string,
	quotas []corev1.ResourceQuota,
	ranges []corev1.LimitRange,
	pods []corev1.Pod,
	warming bool,
) error {
	binding, copying := false, false
	for _, w := range e.State.Workloads {
		if w.Namespace != namespace || w.Phase == phaseStarted {
			continue
		}
		copying = copying || pendingCopy(w)
		for _, v := range workloadVolumes(w) {
			binding = binding || oneOf(v.Phase, phasePlanned, phasePreparing)
		}
	}
	services := map[string]corev1.ResourceList{}
	for name, configured := range map[string]*corev1.ResourceRequirements{"helper": e.State.Options.HelperResources, "sshd": e.State.Options.SSHDResources, "rsync": e.State.Options.RsyncResources} {
		if name == "helper" && !warming && !binding || name != "helper" && !copying {
			continue
		}
		spec := corev1.PodSpec{Containers: []corev1.Container{{Name: name, Resources: serviceResources(configured)}}}
		if err := namespacePodLimits(
			namespace,
			name,
			&spec,
			ranges,
		); err != nil {
			return err
		}
		services[name] = podQuotaUsage(&spec)
	}
	extra := corev1.ResourceList{}
	removed := corev1.ResourceList{}
	warmRemoved := corev1.ResourceList{}
	bindRemoved := corev1.ResourceList{}
	for _, p := range pods {
		if p.Namespace != namespace {
			continue
		}
		usage := podQuotaUsage(&p.Spec)
		if terminalPod(&p) {
			usage = corev1.ResourceList{"count/pods": resource.MustParse("1")}
		}
		if e.warmPod(&p) {
			addResources(removed, usage)
			addResources(warmRemoved, usage)
			addResources(bindRemoved, usage)
			continue
		}
		for _, w := range e.State.Workloads {
			if copyPod(w, &p) {
				addResources(removed, usage)
				break
			}
			for _, v := range workloadVolumes(w) {
				if w.Namespace == namespace && p.Labels[labelKey] == e.State.ID && p.Name == "bind-"+v.TemporaryPVC {
					addResources(removed, usage)
					addResources(bindRemoved, usage)
				}
			}
		}
	}
	if warming {
		peak := services["helper"].DeepCopy()
		subtractResources(peak, warmRemoved)
		maxResources(extra, peak)
	}
	if binding {
		peak := services["helper"].DeepCopy()
		subtractResources(peak, bindRemoved)
		maxResources(extra, peak)
	}
	copies := corev1.ResourceList{}
	for range max(1, e.State.Options.PVConcurrency) {
		addResources(copies, services["sshd"])
		addResources(copies, services["rsync"])
	}
	// The global serial queue preserves completed replacements when the next
	// instance moves. Delta is relative to the quota's current Used snapshot.
	delta := corev1.ResourceList{}
	subtractResources(delta, removed)
	for _, w := range e.State.Workloads {
		if w.Namespace != namespace || w.Phase == phaseStarted {
			continue
		}
		original := sourcePod(w, pods, e.State.Source.Name)
		if pendingCopy(w) && e.onlineCopy(w, original) {
			peak := delta.DeepCopy()
			addResources(peak, copies)
			maxResources(extra, peak)
		}
		if original != nil {
			subtractResources(delta, podQuotaUsage(&original.Spec))
		}
		if pendingCopy(w) {
			peak := delta.DeepCopy()
			addResources(peak, copies)
			maxResources(extra, peak)
		}
		if targetPod(w, pods, e.State.Target.Name) == nil {
			spec := w.Template.Spec.DeepCopy()
			if err := namespacePodLimits(
				namespace,
				workloadKey(w),
				spec,
				ranges,
			); err != nil {
				return err
			}
			usage := podQuotaUsage(spec)
			if w.Instance != nil {
				// Preserve admission-injected sidecars/overhead from the original
				// snapshot as well as any current LimitRange defaults.
				maxResources(usage, podQuotaUsage(&w.Instance.Pod.Spec))
			}
			addResources(delta, usage)
		}
		maxResources(extra, delta)
	}
	for _, w := range volumeViews(e.State) {
		if w.Namespace != namespace || w.Volume.TemporaryUID != "" || w.Volume.Phase == phaseStarted {
			continue
		}
		storage := w.Volume.PVC.Spec.Resources.Requests[corev1.ResourceStorage]
		for _, r := range ranges {
			for _, limit := range r.Spec.Limits {
				if limit.Type != corev1.LimitTypePersistentVolumeClaim {
					continue
				}
				if min, ok := limit.Min[corev1.ResourceStorage]; ok && storage.Cmp(min) < 0 {
					return fmt.Errorf("PVC %s/%s 低于 LimitRange %s 的最小 storage", namespace, w.Volume.PVC.Name, r.Name)
				}
				if max, ok := limit.Max[corev1.ResourceStorage]; ok && storage.Cmp(max) > 0 {
					return fmt.Errorf("PVC %s/%s 超过 LimitRange %s 的最大 storage", namespace, w.Volume.PVC.Name, r.Name)
				}
			}
		}
		increment := corev1.ResourceList{corev1.ResourcePersistentVolumeClaims: resource.MustParse("1"), "count/persistentvolumeclaims": resource.MustParse("1"), corev1.ResourceRequestsStorage: storage}
		if class := w.Volume.PVC.Spec.StorageClassName; class != nil {
			increment[corev1.ResourceName(*class+".storageclass.storage.k8s.io/requests.storage")] = storage
			increment[corev1.ResourceName(*class+".storageclass.storage.k8s.io/persistentvolumeclaims")] = resource.MustParse("1")
		}
		addResources(extra, increment)
	}
	for _, quota := range quotas {
		if len(quota.Spec.Scopes) > 0 || quota.Spec.ScopeSelector != nil {
			continue // Scoped admission remains authoritative.
		}
		if len(quota.Status.Hard) == 0 {
			return fmt.Errorf("ResourceQuota %s/%s 尚无已同步的状态", namespace, quota.Name)
		}
		for key, needed := range extra {
			hard, limited := quota.Status.Hard[key]
			if !limited || needed.Sign() <= 0 {
				continue
			}
			used, known := quota.Status.Used[key]
			if !known {
				return fmt.Errorf("ResourceQuota %s/%s 缺少 %s 使用状态", namespace, quota.Name, key)
			}
			required := used.DeepCopy()
			required.Add(needed)
			if required.Cmp(hard) > 0 {
				return fmt.Errorf("ResourceQuota %s/%s 不足 resource=%s used=%s extra=%s hard=%s", namespace, quota.Name, key, used.String(), needed.String(), hard.String())
			}
		}
	}
	return nil
}

func namespacePodLimits(namespace, name string, spec *corev1.PodSpec, ranges []corev1.LimitRange) error {
	containers := make([]*corev1.Container, 0, len(spec.Containers)+len(spec.InitContainers))
	for i := range spec.Containers {
		containers = append(containers, &spec.Containers[i])
	}
	for i := range spec.InitContainers {
		containers = append(containers, &spec.InitContainers[i])
	}
	for _, c := range containers {
		if c.Resources.Requests == nil {
			c.Resources.Requests = corev1.ResourceList{}
		}
		if c.Resources.Limits == nil {
			c.Resources.Limits = corev1.ResourceList{}
		}
		for _, r := range ranges {
			for _, limit := range r.Spec.Limits {
				if limit.Type != corev1.LimitTypeContainer {
					continue
				}
				for key, value := range limit.Default {
					if _, ok := c.Resources.Limits[key]; !ok {
						c.Resources.Limits[key] = value.DeepCopy()
					}
				}
				for key, value := range limit.DefaultRequest {
					if _, ok := c.Resources.Requests[key]; !ok {
						c.Resources.Requests[key] = value.DeepCopy()
					}
				}
			}
		}
	}
	for _, r := range ranges {
		for _, limit := range r.Spec.Limits {
			if limit.Type == corev1.LimitTypeContainer {
				for _, c := range containers {
					if err := checkLimitResources(c.Resources.Requests, c.Resources.Limits, limit); err != nil {
						return fmt.Errorf("%s 超过 LimitRange %s/%s 的容器约束: %w", name, namespace, r.Name, err)
					}
				}
			} else if limit.Type == corev1.LimitTypePod {
				if err := checkLimitResources(podRequests(spec), podResources(spec, true), limit); err != nil {
					return fmt.Errorf("%s 超过 LimitRange %s/%s 的 Pod 约束: %w", name, namespace, r.Name, err)
				}
			}
		}
	}
	return nil
}

func checkLimitResources(requests, limits corev1.ResourceList, rule corev1.LimitRangeItem) error {
	for key, min := range rule.Min {
		req, lim := requests[key], limits[key]
		if req.Cmp(min) < 0 || lim.Sign() > 0 && lim.Cmp(min) < 0 {
			return fmt.Errorf("最小 %s=%s", key, min.String())
		}
	}
	for key, max := range rule.Max {
		req, lim := requests[key], limits[key]
		if lim.Sign() <= 0 || req.Cmp(max) > 0 || lim.Cmp(max) > 0 {
			return fmt.Errorf("最大 %s=%s", key, max.String())
		}
	}
	for key, ratio := range rule.MaxLimitRequestRatio {
		req, lim := requests[key], limits[key]
		bound := req.DeepCopy()
		bound.AsDec().Mul(bound.AsDec(), ratio.AsDec())
		if req.Sign() <= 0 || lim.Sign() <= 0 || lim.AsDec().Cmp(bound.AsDec()) > 0 {
			return fmt.Errorf("%s limit/request 比例=%s", key, ratio.String())
		}
	}
	return nil
}
