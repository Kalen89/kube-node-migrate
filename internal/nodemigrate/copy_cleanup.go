package nodemigrate

import (
	"context"
	"errors"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func copyRelease(w *Workload) string { return "pv-migrate-" + w.Volume.CopyID + "-clusterip" }

// An occupied name is a recovery question, never permission to uninstall it.
func (c *processCopier) requireAbsentRelease(ctx context.Context, w *Workload) error {
	release := copyRelease(w)
	var secrets corev1.SecretList
	if err := c.engine.Client.List(ctx, &secrets, client.InNamespace(w.Namespace), client.MatchingLabels{"owner": "helm", "name": release}); err != nil {
		return err
	}
	if len(secrets.Items) > 0 {
		return fmt.Errorf("复制 Release %s 已存在但没有可接续的 Job，保留现场并拒绝覆盖", release)
	}
	objects, err := c.releaseObjects(ctx, w)
	if err != nil {
		return err
	}
	if len(objects) > 0 {
		return errors.New("复制名称被已有辅助资源占用，拒绝覆盖")
	}
	return nil
}

func (c *processCopier) releaseObjects(ctx context.Context, w *Workload) ([]client.Object, error) {
	release := copyRelease(w)
	var result []client.Object
	for _, list := range []client.ObjectList{&batchv1.JobList{}, &appsv1.DeploymentList{}, &corev1.ServiceList{}, &corev1.SecretList{}, &corev1.ServiceAccountList{}, &networkingv1.NetworkPolicyList{}} {
		if err := c.engine.Client.List(ctx, list, client.InNamespace(w.Namespace), client.MatchingLabels{"app.kubernetes.io/instance": release}); err != nil {
			return nil, err
		}
		items, err := meta.ExtractList(list)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			obj, ok := item.(client.Object)
			if !ok {
				return nil, errors.New("辅助资源类型无效")
			}
			if obj.GetName() != release+"-rsync" && obj.GetName() != release+"-sshd" {
				return nil, fmt.Errorf("Release 含未知辅助资源 %s，拒绝自动清理", obj.GetName())
			}
			if obj.GetAnnotations()["meta.helm.sh/release-name"] != release || obj.GetAnnotations()["meta.helm.sh/release-namespace"] != w.Namespace || obj.GetLabels()["app.kubernetes.io/managed-by"] != "Helm" {
				return nil, errors.New("辅助资源 Helm 归属不符，拒绝清理")
			}
			result = append(result, obj)
		}
	}
	return result, nil
}

// cleanupRelease deletes only an exact, recorded operation using UID/RV guards.
// It avoids pv-migrate cleanup's operation-prefix matching entirely.
func (c *processCopier) cleanupRelease(ctx context.Context, _ *State, w *Workload) error {
	if w.Volume.CopyJobUID == "" || w.Volume.CopyJobName == "" {
		return errors.New("缺少已记录的复制 Job 身份，拒绝自动清理")
	}
	objects, err := c.releaseObjects(ctx, w)
	if err != nil {
		return err
	}
	release := copyRelease(w)
	var releases corev1.SecretList
	if err = c.engine.Client.List(ctx, &releases, client.InNamespace(w.Namespace), client.MatchingLabels{"owner": "helm", "name": release}); err != nil {
		return err
	}
	for i := range releases.Items {
		s := &releases.Items[i]
		if s.Type != "helm.sh/release.v1" || !strings.HasPrefix(s.Name, "sh.helm.release.v1."+release+".v") {
			return errors.New("Helm Release Secret 类型或名称不符，拒绝清理")
		}
		objects = append(objects, s)
	}
	intent := w.Volume.CopyCleanup
	if intent == nil {
		// Missing Jobs cannot establish ownership of an unknown partial release.
		var job batchv1.Job
		err := c.engine.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Volume.CopyJobName}, &job)
		if apierrors.IsNotFound(err) && len(objects) == 0 {
			return nil
		}
		if err != nil {
			return fmt.Errorf("清理前无法验证原复制 Job: %w", err)
		}
		if job.UID != w.Volume.CopyJobUID {
			return errors.New("复制 Job UID 改变，拒绝清理")
		}
		intent = &CopyCleanupIntent{Parents: []types.UID{job.UID}}
		foundJob := false
		for _, obj := range objects {
			kind := cleanupResourceKind(obj)
			if obj.GetUID() == "" || obj.GetResourceVersion() == "" || kind == "" {
				return errors.New("辅助资源身份缺失，拒绝清理")
			}
			intent.Resources = append(intent.Resources, CopyCleanupResource{Kind: kind, Name: obj.GetName(), UID: obj.GetUID(), ResourceVersion: obj.GetResourceVersion()})
			if kind == "Job" && obj.GetUID() == job.UID && obj.GetName() == job.Name {
				foundJob = true
			}
			if kind == "Deployment" {
				intent.Parents = append(intent.Parents, obj.GetUID())
			}
		}
		if !foundJob {
			return errors.New("原复制 Job 缺少有效 Helm 归属，拒绝清理")
		}
		w.Volume.CopyCleanup = intent
	}
	if err := validateCopyCleanup(w); err != nil {
		return err
	}
	// A same-name replacement or newly injected release object is never adopted.
	known := map[string]CopyCleanupResource{}
	for _, saved := range intent.Resources {
		known[saved.Kind+"/"+saved.Name] = saved
	}
	for _, obj := range objects {
		saved, ok := known[cleanupResourceKind(obj)+"/"+obj.GetName()]
		if !ok || saved.UID != obj.GetUID() {
			return errors.New("清理中的 Release 资源身份改变，拒绝清理")
		}
	}
	for i := range intent.Resources {
		saved := &intent.Resources[i]
		obj := cleanupResourceObject(saved.Kind)
		err := c.engine.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: saved.Name}, obj)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		if obj.GetUID() != saved.UID || !cleanupResourceOwned(obj, release, w.Namespace) {
			return errors.New("已记录的清理资源 UID 或 Helm 归属改变")
		}
		// Controllers may update status or deletion metadata. Revalidate the exact
		// UID and ownership, then checkpoint the new RV before deleting it.
		saved.ResourceVersion = obj.GetResourceVersion()
	}
	if err = c.engine.checkpoint(); err != nil {
		return err
	}
	// Stop only the exact copy helpers, before controller GC can select the
	// template's 30-second grace. No application Pod or force-delete is used.
	parents := map[types.UID]bool{}
	for _, uid := range intent.Parents {
		parents[uid] = true
	}
	var sets appsv1.ReplicaSetList
	selector := client.MatchingLabels{"app.kubernetes.io/instance": release}
	if err = c.engine.Client.List(ctx, &sets, client.InNamespace(w.Namespace), selector); err != nil {
		return err
	}
	for _, rs := range sets.Items {
		if o := metav1.GetControllerOf(&rs); o != nil && parents[o.UID] {
			parents[rs.UID] = true
		}
	}
	var pods corev1.PodList
	if err = c.engine.Client.List(ctx, &pods, client.InNamespace(w.Namespace), selector); err != nil {
		return err
	}
	for _, p := range pods.Items {
		if o := metav1.GetControllerOf(&p); o == nil || !parents[o.UID] {
			return errors.New("复制助手 Pod 归属不符，拒绝清理")
		}
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.UID == "" {
			return errors.New("复制助手 Pod UID 缺失")
		}
		err = c.engine.Client.Delete(ctx, p, &client.DeleteOptions{GracePeriodSeconds: pointer(int64(5)), Preconditions: &metav1.Preconditions{UID: &p.UID, ResourceVersion: &p.ResourceVersion}})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	for i := range intent.Resources {
		if err := c.deleteRecordedResource(ctx, w, &intent.Resources[i]); err != nil {
			return err
		}
	}
	if err = c.engine.wait(ctx, "已记录复制资源清理", func(ctx context.Context) (bool, error) {
		for _, saved := range intent.Resources {
			obj := cleanupResourceObject(saved.Kind)
			err := c.engine.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: saved.Name}, obj)
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return false, err
			}
			if obj.GetUID() != saved.UID {
				return false, errors.New("清理期间资源被替换")
			}
			return false, nil
		}
		return true, nil
	}); err != nil {
		return err
	}
	if err = c.requireAbsentRelease(ctx, w); err != nil {
		return err
	}
	w.Volume.CopyCleanup = nil
	return c.engine.checkpoint()
}

// Helper Pod exit can update its Deployment after cleanup was checkpointed.
// Refresh only the already recorded UID and Helm owner on a bounded conflict
// retry; persist its new RV before deletion, never adopt a replacement object.
func (c *processCopier) deleteRecordedResource(ctx context.Context, w *Workload, saved *CopyCleanupResource) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		obj := cleanupResourceObject(saved.Kind)
		if err := c.engine.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: saved.Name}, obj); err != nil {
			return client.IgnoreNotFound(err)
		}
		if obj.GetUID() != saved.UID || !cleanupResourceOwned(obj, copyRelease(w), w.Namespace) {
			return errors.New("已记录的清理资源 UID 或 Helm 归属改变")
		}
		saved.ResourceVersion = obj.GetResourceVersion()
		if err := c.engine.checkpoint(); err != nil {
			return err
		}
		policy := metav1.DeletePropagationForeground
		err := c.engine.Client.Delete(ctx, obj, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &saved.UID, ResourceVersion: &saved.ResourceVersion}, PropagationPolicy: &policy})
		if apierrors.IsConflict(err) {
			c.engine.logf("复制助手清理版本冲突，重新校验身份后重试 kind=%s namespace=%s name=%s", saved.Kind, w.Namespace, saved.Name)
		}
		return client.IgnoreNotFound(err)
	})
}

func cleanupResourceKind(obj client.Object) string {
	switch obj.(type) {
	case *batchv1.Job:
		return "Job"
	case *appsv1.Deployment:
		return "Deployment"
	case *corev1.Service:
		return "Service"
	case *corev1.Secret:
		return "Secret"
	case *corev1.ServiceAccount:
		return "ServiceAccount"
	case *networkingv1.NetworkPolicy:
		return "NetworkPolicy"
	}
	return ""
}

func cleanupResourceObject(kind string) client.Object {
	switch kind {
	case "Job":
		return &batchv1.Job{}
	case "Deployment":
		return &appsv1.Deployment{}
	case "Service":
		return &corev1.Service{}
	case "Secret":
		return &corev1.Secret{}
	case "ServiceAccount":
		return &corev1.ServiceAccount{}
	case "NetworkPolicy":
		return &networkingv1.NetworkPolicy{}
	}
	return nil
}

func cleanupResourceOwned(obj client.Object, release, namespace string) bool {
	if s, ok := obj.(*corev1.Secret); ok && s.Type == "helm.sh/release.v1" {
		return strings.HasPrefix(s.Name, "sh.helm.release.v1."+release+".v") && s.Labels["owner"] == "helm" && s.Labels["name"] == release
	}
	return (obj.GetName() == release+"-rsync" || obj.GetName() == release+"-sshd") && obj.GetLabels()["app.kubernetes.io/instance"] == release &&
		obj.GetLabels()["app.kubernetes.io/managed-by"] == "Helm" && obj.GetAnnotations()["meta.helm.sh/release-name"] == release && obj.GetAnnotations()["meta.helm.sh/release-namespace"] == namespace
}

func validateCopyCleanup(w *Workload) error {
	v := w.Volume
	if v.CopyCleanup == nil {
		return nil
	}
	if v.CopyJobUID == "" || v.CopyJobName == "" {
		return errors.New("复制清理意图缺少原操作身份或阶段无效")
	}
	parents := map[types.UID]bool{v.CopyJobUID: true}
	seen := map[string]bool{}
	foundJob := false
	for _, saved := range v.CopyCleanup.Resources {
		obj := cleanupResourceObject(saved.Kind)
		key := saved.Kind + "/" + saved.Name
		if obj == nil || saved.UID == "" || saved.ResourceVersion == "" || seen[key] {
			return errors.New("复制清理资源身份无效或重复")
		}
		seen[key] = true
		if saved.Kind == "Job" && saved.Name == v.CopyJobName && saved.UID == v.CopyJobUID {
			foundJob = true
		}
		if saved.Kind == "Deployment" {
			parents[saved.UID] = true
		}
		if saved.Name != copyRelease(w)+"-sshd" && saved.Name != copyRelease(w)+"-rsync" && !(saved.Kind == "Secret" && strings.HasPrefix(saved.Name, "sh.helm.release.v1."+copyRelease(w)+".v")) {
			return errors.New("复制清理资源名称不属于本次操作")
		}
	}
	if !foundJob || len(v.CopyCleanup.Parents) != len(parents) {
		return errors.New("复制清理控制器身份缺失")
	}
	for _, uid := range v.CopyCleanup.Parents {
		if !parents[uid] {
			return errors.New("复制清理控制器身份不符")
		}
		delete(parents, uid)
	}
	return nil
}
