package nodemigrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func holdName(s *State, w *Workload) string {
	hash := sha256.Sum256([]byte(workloadKey(w)))
	return "km-move-" + s.ID + "-" + hex.EncodeToString(hash[:6])
}

func holdMessage(s *State, w *Workload) string {
	return "Migration hold " + holdName(s, w) + ": instance not verified"
}

func celString(value string) string { b, _ := json.Marshal(value); return string(b) }

func (e *Engine) holdObjects(w *Workload) (*admissionv1.ValidatingAdmissionPolicy, *admissionv1.ValidatingAdmissionPolicyBinding) {
	name := holdName(e.State, w)
	metadata := metav1.ObjectMeta{Name: name, Labels: map[string]string{labelKey: e.State.ID}}
	podMatch := "object.metadata.name == " + celString(w.Instance.Pod.Name)
	if rs := w.Instance.ReplicaSet; rs != nil {
		podMatch = "has(object.metadata.ownerReferences) && object.metadata.ownerReferences.exists(r, has(r.controller) && r.controller && r.uid == " + celString(string(rs.UID)) + ")"
	}
	match := "(request.resource.resource == 'pods' && (" + podMatch + "))"
	var claims []string
	for _, v := range w.Volumes {
		claims = append(claims, v.PVC.Name)
	}
	if len(claims) > 0 {
		b, _ := json.Marshal(claims)
		match += " || (request.resource.resource == 'persistentvolumeclaims' && object.metadata.name in " + string(b) + ")"
	}
	policy := &admissionv1.ValidatingAdmissionPolicy{ObjectMeta: metadata,
		Spec: admissionv1.ValidatingAdmissionPolicySpec{
			FailurePolicy:    pointer(admissionv1.Fail),
			MatchConstraints: &admissionv1.MatchResources{MatchPolicy: pointer(admissionv1.Equivalent), ResourceRules: []admissionv1.NamedRuleWithOperations{{RuleWithOperations: admissionv1.RuleWithOperations{Operations: []admissionv1.OperationType{admissionv1.Create}, Rule: admissionv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods", "persistentvolumeclaims"}, Scope: pointer(admissionv1.AllScopes)}}}}},
			MatchConditions:  []admissionv1.MatchCondition{{Name: "selected-namespace", Expression: "request.namespace == " + celString(w.Namespace)}, {Name: "selected-instance", Expression: match}},
			Validations:      []admissionv1.Validation{{Expression: "request.userInfo.username == " + celString(e.State.ExecutorUser), Message: holdMessage(e.State, w), Reason: pointer(metav1.StatusReasonForbidden)}},
		}}
	binding := &admissionv1.ValidatingAdmissionPolicyBinding{ObjectMeta: *metadata.DeepCopy(), Spec: admissionv1.ValidatingAdmissionPolicyBindingSpec{PolicyName: name, ValidationActions: []admissionv1.ValidationAction{admissionv1.Deny}}}
	return policy, binding
}

func (e *Engine) identifyExecutor(ctx context.Context) error {
	review := &authenticationv1.SelfSubjectReview{}
	if err := e.Client.Create(ctx, review); err != nil {
		return fmt.Errorf("查询迁移认证身份: %w", err)
	}
	user := review.Status.UserInfo.Username
	if user == "" || strings.HasPrefix(user, "system:serviceaccount:kube-system:") || user == "system:kube-controller-manager" {
		return errors.New("迁移身份不能与 Kubernetes 控制器身份共用")
	}
	if e.State.ExecutorUser != "" && e.State.ExecutorUser != user {
		return errors.New("续跑身份与原准入保护的迁移身份不同")
	}
	e.State.ExecutorUser = user
	return nil
}

func sameHoldPolicy(got, want *admissionv1.ValidatingAdmissionPolicy) bool {
	// Only server defaults may be additional. Rule/condition/validation counts
	// are exact, so a broadened scope or extra denial can never be adopted.
	return got.Spec.MatchConstraints != nil && len(got.Spec.MatchConstraints.ResourceRules) == 1 && len(got.Spec.MatchConstraints.ExcludeResourceRules) == 0 &&
		emptySelector(got.Spec.MatchConstraints.NamespaceSelector) && emptySelector(got.Spec.MatchConstraints.ObjectSelector) && len(got.Spec.Variables) == 0 && len(got.Spec.AuditAnnotations) == 0 &&
		equality.Semantic.DeepEqual(want.Spec.MatchConstraints.ResourceRules, got.Spec.MatchConstraints.ResourceRules) &&
		equality.Semantic.DeepEqual(want.Spec.Validations, got.Spec.Validations) && equality.Semantic.DeepEqual(want.Spec.MatchConditions, got.Spec.MatchConditions) &&
		len(got.Spec.MatchConditions) == 2 && len(got.Spec.Validations) == 1 && got.Spec.ParamKind == nil &&
		equality.Semantic.DeepDerivative(want.Spec, got.Spec)
}

func emptySelector(selector *metav1.LabelSelector) bool {
	return selector == nil || (len(selector.MatchLabels) == 0 && len(selector.MatchExpressions) == 0)
}

func (e *Engine) ensureHold(ctx context.Context, w *Workload) error {
	policy, binding := e.holdObjects(w)
	var current admissionv1.ValidatingAdmissionPolicy
	err := e.Client.Get(ctx, client.ObjectKeyFromObject(policy), &current)
	if apierrors.IsNotFound(err) {
		err = e.Client.Create(ctx, policy)
		current = *policy
	}
	if err != nil {
		return err
	}
	if current.Labels[labelKey] != e.State.ID || !sameHoldPolicy(&current, policy) || (w.Instance.PolicyUID != "" && current.UID != w.Instance.PolicyUID) {
		return errors.New("准入 Policy 身份或规则改变")
	}
	w.Instance.PolicyUID = current.UID
	if err = e.checkpoint(); err != nil {
		return err
	}
	var active admissionv1.ValidatingAdmissionPolicyBinding
	err = e.Client.Get(ctx, client.ObjectKeyFromObject(binding), &active)
	if apierrors.IsNotFound(err) {
		err = e.Client.Create(ctx, binding)
		active = *binding
	}
	if err != nil {
		return err
	}
	if active.Labels[labelKey] != e.State.ID || !equality.Semantic.DeepEqual(active.Spec, binding.Spec) || (w.Instance.BindingUID != "" && active.UID != w.Instance.BindingUID) {
		return errors.New("准入 Binding 身份或规则改变")
	}
	w.Instance.BindingUID = active.UID
	if err = e.checkpoint(); err != nil {
		return err
	}
	clients, err := e.admissionProbes(ctx)
	if err != nil {
		return err
	}
	e.logf("验证全部 API Server 的准入保护 resource=%s endpoints=%d", workloadKey(w), len(clients))
	return e.wait(ctx, "验证逐实例准入保护实际生效", func(ctx context.Context) (bool, error) {
		probes := []client.Object{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: w.Instance.Pod.Name, Namespace: w.Namespace, OwnerReferences: append([]metav1.OwnerReference(nil), w.Instance.Pod.OwnerReferences...)}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "probe", Image: e.State.Options.HelperImage}}, RestartPolicy: corev1.RestartPolicyNever}}}
		for _, v := range w.Volumes {
			probes = append(probes, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: v.PVC.Name, Namespace: w.Namespace}, Spec: claimSpec(&v.PVC, "")})
		}
		for _, server := range clients {
			for _, probe := range probes {
				err := server.Create(ctx, probe.DeepCopyObject().(client.Object), client.DryRunAll)
				if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), holdMessage(e.State, w)) {
					return false, nil
				}
			}
		}
		return true, nil
	})
}

func (e *Engine) checkHold(ctx context.Context, w *Workload) error {
	policy, binding := e.holdObjects(w)
	var p admissionv1.ValidatingAdmissionPolicy
	var b admissionv1.ValidatingAdmissionPolicyBinding
	if err := e.Client.Get(ctx, client.ObjectKeyFromObject(policy), &p); err != nil {
		return err
	}
	if err := e.Client.Get(ctx, client.ObjectKeyFromObject(binding), &b); err != nil {
		return err
	}
	if p.UID != w.Instance.PolicyUID || b.UID != w.Instance.BindingUID || p.Labels[labelKey] != e.State.ID || b.Labels[labelKey] != e.State.ID || !sameHoldPolicy(&p, policy) || !equality.Semantic.DeepEqual(b.Spec, binding.Spec) {
		return errors.New("实例保护消失或发生漂移，禁止继续")
	}
	return nil
}

func (e *Engine) removeHold(ctx context.Context, w *Workload) error {
	name := holdName(e.State, w)
	wantedPolicy, wantedBinding := e.holdObjects(w)
	for _, item := range []struct {
		obj client.Object
		uid types.UID
	}{{&admissionv1.ValidatingAdmissionPolicyBinding{}, w.Instance.BindingUID}, {&admissionv1.ValidatingAdmissionPolicy{}, w.Instance.PolicyUID}} {
		err := e.Client.Get(ctx, types.NamespacedName{Name: name}, item.obj)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		if item.obj.GetUID() != item.uid || item.obj.GetLabels()[labelKey] != e.State.ID {
			return errors.New("拒绝删除身份改变的准入保护")
		}
		switch current := item.obj.(type) {
		case *admissionv1.ValidatingAdmissionPolicy:
			if !sameHoldPolicy(current, wantedPolicy) {
				return errors.New("拒绝删除规则漂移的准入 Policy")
			}
		case *admissionv1.ValidatingAdmissionPolicyBinding:
			if !equality.Semantic.DeepEqual(current.Spec, wantedBinding.Spec) {
				return errors.New("拒绝删除规则漂移的准入 Binding")
			}
		}
		rv := item.obj.GetResourceVersion()
		if err = e.Client.Delete(ctx, item.obj, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &item.uid, ResourceVersion: &rv}}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err = e.wait(ctx, "移除实例准入保护", func(ctx context.Context) (bool, error) {
			err := e.Client.Get(ctx, types.NamespacedName{Name: name}, item.obj)
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}); err != nil {
			return err
		}
	}
	w.Instance.BindingUID, w.Instance.PolicyUID = "", ""
	return e.checkpoint()
}

func (e *Engine) admissionProbes(ctx context.Context) ([]client.Client, error) {
	probes := []client.Client{e.AdmissionProbe}
	if e.AdmissionProbes != nil {
		var err error
		probes, err = e.AdmissionProbes(ctx)
		if err != nil {
			return nil, err
		}
	}
	if len(probes) == 0 {
		return nil, errors.New("缺少独立身份的准入 dry-run 客户端")
	}
	for _, probe := range probes {
		if probe == nil {
			return nil, errors.New("缺少独立身份的准入 dry-run 客户端")
		}
	}
	return probes, nil
}
