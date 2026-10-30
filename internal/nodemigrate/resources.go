package nodemigrate

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// DefaultServiceResources returns independent defaults for each migration service.
func DefaultServiceResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("512Mi")},
	}
}

// Merge individual quantities so a partial override retains the other defaults.
// Missing resources in older checkpoints preserve their original behavior.
func serviceResources(configured *corev1.ResourceRequirements) corev1.ResourceRequirements {
	result := DefaultServiceResources()
	if configured != nil {
		for key, value := range configured.Requests {
			result.Requests[key] = value.DeepCopy()
		}
		for key, value := range configured.Limits {
			result.Limits[key] = value.DeepCopy()
		}
	}
	return result
}

func normalizeServiceResources(o *Options) {
	helper, sshd, rsync := serviceResources(o.HelperResources), serviceResources(o.SSHDResources), serviceResources(o.RsyncResources)
	o.HelperResources, o.SSHDResources, o.RsyncResources = &helper, &sshd, &rsync
}

func validateServiceResources(o Options) error {
	for name, configured := range map[string]*corev1.ResourceRequirements{"helper": o.HelperResources, "sshd": o.SSHDResources, "rsync": o.RsyncResources} {
		if configured != nil && len(configured.Claims) != 0 {
			return fmt.Errorf("%s.resources 不支持资源 claims", name)
		}
		r := serviceResources(configured)
		for kind, values := range map[string]corev1.ResourceList{"requests": r.Requests, "limits": r.Limits} {
			for key, value := range values {
				if key != corev1.ResourceCPU && key != corev1.ResourceMemory {
					return fmt.Errorf("%s.resources.%s 仅支持 cpu 和 memory", name, kind)
				}
				if value.Sign() <= 0 {
					return fmt.Errorf("%s.resources.%s.%s 必须为正数", name, kind, key)
				}
			}
		}
		for key, request := range r.Requests {
			if request.Cmp(r.Limits[key]) > 0 {
				return fmt.Errorf("%s.resources.requests.%s 不能超过 limits", name, key)
			}
		}
	}
	return nil
}
