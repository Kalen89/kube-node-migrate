package nodemigrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func normalizeImage(value string) (string, error) {
	ref, err := name.ParseReference(value, name.WeakValidation, name.WithDefaultRegistry("docker.io"))
	if err != nil {
		return "", err
	}
	return strings.Replace(ref.Name(), "index.docker.io/", "docker.io/", 1), nil
}

func parseImageCache(output string) map[string]bool {
	images := make(map[string]bool)
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		// containerd may discard compressed layers after unpacking. Runtime
		// readiness depends on the unpacked snapshot, not exportable content.
		if len(fields) < 7 || (fields[3] != "complete" && fields[3] != "incomplete") || fields[len(fields)-1] != "true" {
			continue
		}
		ref, err := normalizeImage(fields[0])
		if err == nil {
			images[ref] = true
		}
	}
	return images
}

func (e *Engine) imageCache(ctx context.Context) (map[string]bool, error) {
	return e.imageCacheFor(ctx, e.State.TargetConfig)
}

func (e *Engine) imageCacheFor(ctx context.Context, host Host) (map[string]bool, error) {
	if e.Hosts == nil {
		return nil, errors.New("SSH 执行器未初始化")
	}
	output, err := e.Hosts.Run(ctx, host, "k3s ctr -n k8s.io images check")
	if err != nil {
		return nil, err
	}
	return parseImageCache(output), nil
}

type imageGroup struct {
	host      Host
	namespace string
	secrets   []corev1.LocalObjectReference
	images    map[string]bool
}

func (e *Engine) imageGroups(ctx context.Context, resolveSecrets bool) ([]imageGroup, error) {
	groups := make(map[string]*imageGroup)
	groupFor := func(host Host, namespace string, secrets []corev1.LocalObjectReference) *imageGroup {
		keyBytes, _ := json.Marshal(secrets)
		key := host.Name + "/" + namespace + "/" + string(keyBytes)
		if groups[key] == nil {
			groups[key] = &imageGroup{host: host, namespace: namespace, secrets: secrets, images: make(map[string]bool)}
		}
		return groups[key]
	}
	for _, w := range e.State.Workloads {
		if w.Phase == phaseStarted {
			continue
		}
		var secrets []corev1.LocalObjectReference
		if resolveSecrets {
			var err error
			secrets, err = e.pullSecrets(ctx, w.Namespace, &w.Template.Spec)
			if err != nil {
				return nil, err
			}
		}
		sort.Slice(secrets, func(i, j int) bool { return secrets[i].Name < secrets[j].Name })
		group := groupFor(e.State.TargetConfig, w.Namespace, secrets)
		containers := append(append([]corev1.Container(nil), w.Template.Spec.InitContainers...), w.Template.Spec.Containers...)
		for _, container := range containers {
			group.images[container.Image] = true
		}
		if pendingCopy(w) {
			for _, image := range []string{e.State.Options.HelperImage, e.State.Options.RsyncImage} {
				if image == "" {
					return nil, errors.New("迁移助手镜像尚未配置")
				}
				group.images[image] = true
			}
			if e.State.Options.SSHDImage == "" {
				return nil, errors.New("迁移 SSH 助手镜像尚未配置")
			}
			groupFor(e.State.SourceConfig, w.Namespace, secrets).images[e.State.Options.SSHDImage] = true
		}
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]imageGroup, 0, len(keys))
	for _, key := range keys {
		result = append(result, *groups[key])
	}
	return result, nil
}

func (e *Engine) warmImages(ctx context.Context) error {
	if err := e.cleanupWarmPods(ctx); err != nil {
		return err
	}
	groups, err := e.imageGroups(ctx, true)
	if err != nil {
		return err
	}
	for i, group := range groups {
		available, err := e.imageCacheFor(ctx, group.host)
		if err != nil {
			return err
		}
		var missing []string
		for image := range group.images {
			ref, err := normalizeImage(image)
			if err != nil {
				return err
			}
			if !available[ref] {
				missing = append(missing, image)
			}
		}
		if len(missing) == 0 {
			e.logf("所需镜像已缓存 node=%s namespace=%s count=%d", group.host.Name, group.namespace, len(group.images))
			continue
		}
		sort.Strings(missing)
		for j, image := range missing {
			e.logf("预热所需镜像 %d/%d node=%s namespace=%s image=%s", j+1, len(missing), group.host.Name, group.namespace, image)
			if err := e.warmGroup(ctx, group, []string{image}, fmt.Sprintf("%d-%d", i, j)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) warmGroup(ctx context.Context, group imageGroup, images []string, index string) error {
	if group.host.Name == "" {
		group.host = e.State.TargetConfig
	}
	taints := e.State.TargetTaints
	if group.host.Name == e.State.Source.Name {
		taints = e.State.SourceTaints
	}
	name := fmt.Sprintf("km-migration-warm-%s-%s", e.State.ID, index)
	key := types.NamespacedName{Namespace: group.namespace, Name: name}
	var pod corev1.Pod
	err := e.Client.Get(ctx, key, &pod)
	if apierrors.IsNotFound(err) {
		containers := make([]corev1.Container, 0, len(images))
		for i, image := range images {
			containers = append(containers, corev1.Container{Name: fmt.Sprintf("cache-%d", i), Image: image, ImagePullPolicy: corev1.PullIfNotPresent,
				// Deliberately never execute the application's ENTRYPOINT, even in shell-less images.
				Command: []string{"/__qtcore_image_cache_only__"}, Resources: serviceResources(e.State.Options.HelperResources)})
		}
		pod = corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: group.namespace, Labels: map[string]string{labelKey: e.State.ID}},
			Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: pointer(false),
				TerminationGracePeriodSeconds: pointer(int64(0)), NodeSelector: map[string]string{hostnameLabel: group.host.Name},
				Tolerations: append(helperTolerations(taints), corev1.Toleration{Key: "node.kubernetes.io/unschedulable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}), ImagePullSecrets: group.secrets, Containers: containers}}
		if err = e.Client.Create(ctx, &pod); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if pod.Labels[labelKey] != e.State.ID || pod.Spec.NodeSelector[hostnameLabel] != group.host.Name {
		return errors.New("镜像预热 Pod 不属于本次迁移")
	}
	e.logf("在 %s 认证预热 %d 个镜像，不执行应用入口", group.namespace, len(images))
	if err = e.wait(ctx, "所需镜像运行文件系统已解包", func(ctx context.Context) (bool, error) {
		cache, err := e.imageCacheFor(ctx, group.host)
		if err != nil {
			return false, err
		}
		for _, image := range images {
			ref, err := normalizeImage(image)
			if err != nil {
				return false, err
			}
			if !cache[ref] {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	} // Keep the owned Pod for inspection/resume after a failed pull.
	if err = e.Client.Delete(ctx, &pod, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return e.wait(ctx, "预热 Pod 清理", func(ctx context.Context) (bool, error) {
		var current corev1.Pod
		err := e.Client.Get(ctx, key, &current)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
}

func (e *Engine) cleanupWarmPods(ctx context.Context) error {
	var pods corev1.PodList
	if err := e.Client.List(ctx, &pods, client.MatchingLabels{labelKey: e.State.ID}); err != nil {
		return err
	}
	for _, pod := range pods.Items {
		if !strings.HasPrefix(pod.Name, "km-migration-warm-"+e.State.ID+"-") {
			continue
		}
		if (pod.Spec.NodeSelector[hostnameLabel] != e.State.Target.Name && pod.Spec.NodeSelector[hostnameLabel] != e.State.Source.Name) || !safeHelperVolumes(&pod, "") {
			return errors.New("残留预热 Pod 发生配置漂移")
		}
		if err := e.Client.Delete(ctx, &pod, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		key := client.ObjectKeyFromObject(&pod)
		if err := e.wait(ctx, "上次预热 Pod 退出", func(ctx context.Context) (bool, error) {
			var current corev1.Pod
			err := e.Client.Get(ctx, key, &current)
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}); err != nil {
			return err
		}
	}
	return nil
}

// Admission webhooks can add read-only ConfigMaps (for example time zones).
// The binding helper may also mount its one expected temporary claim.
func safeHelperVolumes(pod *corev1.Pod, claim string) bool {
	claims := 0
	allowed := make(map[string]bool)
	for _, volume := range pod.Spec.Volumes {
		switch {
		case claim != "" && volume.Name == "data" && volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claim:
			claims++
			allowed[volume.Name] = false
		case volume.ConfigMap != nil:
			allowed[volume.Name] = true
		default:
			return false
		}
	}
	if (claim != "" && claims != 1) || (claim == "" && claims != 0) {
		return false
	}
	for _, container := range append(append([]corev1.Container(nil), pod.Spec.InitContainers...), pod.Spec.Containers...) {
		if len(container.VolumeDevices) != 0 {
			return false
		}
		for _, mount := range container.VolumeMounts {
			readOnly, exists := allowed[mount.Name]
			if !exists || (readOnly && !mount.ReadOnly) {
				return false
			}
		}
	}
	return true
}
