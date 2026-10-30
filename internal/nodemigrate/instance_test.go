package nodemigrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// HA fixture: the lowest ordinal is on the source, two peers stay running.
// Copy and API controllers are simulated; no real host or cluster is modified.
func instanceFixture(t *testing.T) (*Engine, *Workload) {
	t.Helper()
	e, w, _ := fixture(t)
	w.Replicas = 3
	var sts appsv1.StatefulSet
	require.NoError(t, e.Client.Get(context.Background(), types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &sts))
	sts.Spec.Replicas = pointer(int32(3))
	second := *w.Volume
	second.PVC = *w.Volume.PVC.DeepCopy()
	second.PVC.Name = "logs-db-0"
	second.PVC.UID = "logs-original"
	second.PVC.Spec.VolumeName = "logs-old"
	second.CopyID = "km-" + e.State.ID + "-1"
	second.TemporaryPVC = "km-migrate-" + e.State.ID + "-1"
	second.TemporaryUID = "logs-temporary"
	second.PV = *testPV("logs-old", "old", second.PVC.Name, second.PVC.UID)
	second.TargetPV = testPV("logs-new", "new", second.TemporaryPVC, second.TemporaryUID)
	temporary := second.PVC.DeepCopy()
	temporary.Name = second.TemporaryPVC
	temporary.UID = second.TemporaryUID
	temporary.Spec.VolumeName = second.TargetPV.Name
	temporary.Labels = map[string]string{labelKey: e.State.ID}
	for _, o := range []client.Object{&second.PVC, &second.PV, second.TargetPV, temporary} {
		require.NoError(t, e.Client.Create(context.Background(), o))
	}
	w.Template.Spec.Volumes = append(w.Template.Spec.Volumes, corev1.Volume{Name: "logs", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: second.PVC.Name}}})
	sts.Spec.Template = *w.Template.DeepCopy()
	require.NoError(t, e.Client.Update(context.Background(), &sts))
	var original corev1.Pod
	require.NoError(t, e.Client.Get(context.Background(), types.NamespacedName{Namespace: w.Namespace, Name: "db-0"}, &original))
	original.Spec.Volumes = w.Template.Spec.Volumes
	require.NoError(t, e.Client.Update(context.Background(), &original))
	w.Instance = &Instance{Pod: *original.DeepCopy(), StatefulSetSpec: sts.Spec.DeepCopy()}
	w.Volumes = []*Volume{w.Volume, &second}
	w.Volume = nil
	for _, name := range []string{"db-1", "db-2"} {
		peer := original.DeepCopy()
		peer.Name = name
		peer.UID = types.UID(name + "-uid")
		peer.ResourceVersion = ""
		peer.Spec.NodeName = "peer"
		peer.Spec.Volumes = nil
		require.NoError(t, e.Client.Create(context.Background(), peer))
	}
	e.State.Options.Mode = ModeWorkloads
	e.State.Options.SourcePolicy = ""
	e.State.Options.Namespaces = []string{w.Namespace}
	e.State.ExecutorUser = "system:admin"
	e.State.Options.Precopy = true
	e.State.Options.PrecopyRounds = 1
	e.State.SourceRoles = map[string]string{"node-role.kubernetes.io/control-plane": "true", "node-role.kubernetes.io/etcd": "true"}
	var source corev1.Node
	require.NoError(t, e.Client.Get(context.Background(), types.NamespacedName{Name: "old"}, &source))
	for k, v := range e.State.SourceRoles {
		source.Labels[k] = v
	}
	require.NoError(t, e.Client.Update(context.Background(), &source))
	base := e.Client.(client.WithWatch)
	e.Client = interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if review, ok := obj.(*authenticationv1.SelfSubjectReview); ok {
				review.Status.UserInfo.Username = "system:admin"
				return nil
			}
			options := &client.CreateOptions{}
			for _, o := range opts {
				o.ApplyToCreate(options)
			}
			if len(options.DryRun) > 0 {
				return nil
			}
			if obj.GetUID() == "" {
				obj.SetUID(types.UID("created-" + obj.GetName()))
			}
			if p, ok := obj.(*corev1.PersistentVolumeClaim); ok {
				p.Status.Phase = corev1.ClaimBound
			}
			if p, ok := obj.(*corev1.Pod); ok && p.Spec.NodeSelector[hostnameLabel] == e.State.Target.Name && metav1.GetControllerOf(p) != nil && metav1.GetControllerOf(p).Kind == "ReplicaSet" {
				p.Spec.NodeName = e.State.Target.Name
				p.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
			}
			return c.Create(ctx, obj, opts...)
		},
		SubResourceCreate: func(ctx context.Context, c client.Client, sub string, obj client.Object, body client.Object, opts ...client.SubResourceCreateOption) error {
			if sub != "eviction" {
				return errors.New("unexpected subresource")
			}
			return c.Delete(ctx, obj, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: pointer(obj.GetUID())}})
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if err := c.Delete(ctx, obj, opts...); err != nil {
				return err
			}
			if _, ok := obj.(*admissionv1.ValidatingAdmissionPolicyBinding); ok && w.Kind == kindStatefulSet {
				var exists corev1.Pod
				if err := c.Get(ctx, client.ObjectKeyFromObject(&w.Instance.Pod), &exists); apierrors.IsNotFound(err) {
					for _, v := range w.Volumes {
						var claim corev1.PersistentVolumeClaim
						if err := c.Get(ctx, client.ObjectKeyFromObject(&v.PVC), &claim); err != nil || claim.Spec.VolumeName != v.TargetPV.Name {
							return errors.New("controller attempted start before all PVCs rebound")
						}
					}
					replacement := w.Instance.Pod.DeepCopy()
					replacement.UID = "replacement-pod"
					replacement.ResourceVersion = ""
					replacement.Spec.NodeName = e.State.Target.Name
					return c.Create(ctx, replacement)
				}
			}
			return nil
		},
	})
	e.AdmissionProbe = interceptor.NewClient(base, interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if review, ok := obj.(*authorizationv1.SelfSubjectAccessReview); ok {
			review.Status.Allowed = true
			return nil
		}
		options := &client.CreateOptions{}
		for _, o := range opts {
			o.ApplyToCreate(options)
		}
		if len(options.DryRun) == 0 {
			return errors.New("probe must never persist an object")
		}
		return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, obj.GetName(), errors.New(holdMessage(e.State, w)))
	}})
	previous := e.Hosts
	e.Hosts = hostFunc(func(ctx context.Context, h Host, cmd string) (string, error) {
		if strings.HasSuffix(cmd, "ps -o json") {
			return `{"containers":[]}`, nil
		}
		return previous.Run(ctx, h, cmd)
	})
	return e, w
}

func TestInstanceMigrationKeepsControllerAndPeersAcrossTwoVolumes(t *testing.T) {
	e, w := instanceFixture(t)
	ctx := context.Background()
	saved := 0
	e.Save = func(s *State) error { saved++; return validateState(s) }
	require.NoError(t, e.Apply(ctx, true))
	require.Greater(t, saved, 10)
	require.Equal(t, phaseValidated, e.State.Phase)
	require.True(t, e.State.SourceRestored)
	for _, v := range w.Volumes {
		require.Equal(t, phaseStarted, v.Phase)
		require.NotEqual(t, v.PVC.UID, v.NewPVCUID)
		require.Equal(t, v.SourceDigest, v.TargetDigest)
	}
	for _, name := range []string{"db-1", "db-2"} {
		var peer corev1.Pod
		require.NoError(t, e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: name}, &peer))
		require.Equal(t, types.UID(name+"-uid"), peer.UID)
	}
	var sts appsv1.StatefulSet
	require.NoError(t, e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &sts))
	require.Equal(t, *w.Instance.StatefulSetSpec, sts.Spec)
	require.Equal(t, w.UID, sts.UID)
	require.NoError(t, e.Verify(ctx))
}

func TestInstanceSecondVolumeFailureNeverRebindsFirst(t *testing.T) {
	e, w := instanceFixture(t)
	e.Copier = copierFunc(func(_ context.Context, _ *State, view *Workload) error {
		if view.Volume == w.Volumes[1] && view.Volume.Phase == phaseCopying {
			return errors.New("disk error")
		}
		return nil
	})
	require.ErrorContains(t, e.Apply(context.Background(), true), "disk error")
	require.Equal(t, phaseStopped, w.Phase)
	require.NoError(t, e.checkHold(context.Background(), w))
	for _, v := range w.Volumes {
		var original corev1.PersistentVolumeClaim
		require.NoError(t, e.Client.Get(context.Background(), client.ObjectKeyFromObject(&v.PVC), &original))
		require.Equal(t, v.PVC.UID, original.UID)
	}
	// The held source stays stopped; a new run resumes and verifies all volumes.
	e.Copier = copierFunc(func(context.Context, *State, *Workload) error { return nil })
	require.NoError(t, e.Apply(context.Background(), true))
	require.NoError(t, e.Verify(context.Background()))
}

func TestInstanceResumeAfterPartialRebindDoesNotStartEarly(t *testing.T) {
	e, w := instanceFixture(t)
	failed := false
	baseSave := e.Save
	e.Save = func(s *State) error {
		if w.Volumes[0].Phase == phaseSwitched && w.Volumes[1].Phase != phaseSwitched && !failed {
			failed = true
			return errors.New("checkpoint unavailable")
		}
		return baseSave(s)
	}
	require.ErrorContains(t, e.Apply(context.Background(), true), "checkpoint unavailable")
	require.Equal(t, phaseSwitching, w.Phase)
	var pod corev1.Pod
	require.True(t, apierrors.IsNotFound(e.Client.Get(context.Background(), client.ObjectKeyFromObject(&w.Instance.Pod), &pod)))
	require.NoError(t, e.checkHold(context.Background(), w))
	e.Save = baseSave
	require.NoError(t, e.Apply(context.Background(), true))
	require.NoError(t, e.Verify(context.Background()))
}

func TestInstanceProtectionFailureLeavesOriginalRunning(t *testing.T) {
	e, w := instanceFixture(t)
	e.AdmissionProbe = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
		if review, ok := obj.(*authorizationv1.SelfSubjectAccessReview); ok {
			review.Status.Allowed = true
			return nil
		}
		return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "db-0", errors.New("RBAC denied"))
	}})
	e.State.Options.WaitTimeout = time.Millisecond
	require.Error(t, e.Apply(context.Background(), true))
	var pod corev1.Pod
	require.NoError(t, e.Client.Get(context.Background(), client.ObjectKeyFromObject(&w.Instance.Pod), &pod))
	require.True(t, podReady(&pod))
	var policies admissionv1.ValidatingAdmissionPolicyList
	require.NoError(t, e.Client.List(context.Background(), &policies))
	require.Empty(t, policies.Items)
}

func TestInstanceDeploymentMovesOneReplicaWithoutRollout(t *testing.T) {
	e, w := instanceFixture(t)
	ctx := context.Background()
	var oldPods corev1.PodList
	require.NoError(t, e.Client.List(ctx, &oldPods))
	for i := range oldPods.Items {
		require.NoError(t, e.Client.Delete(ctx, &oldPods.Items[i]))
	}
	w.Kind, w.Name, w.UID, w.Replicas, w.Owner = kindDeployment, "app", "app-uid", 2, nil
	w.Volume, w.Volumes = nil, nil
	template := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "demo"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "db:v1"}}}}
	w.Template = template
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: w.Name, Namespace: w.Namespace, UID: w.UID}, Spec: appsv1.DeploymentSpec{Replicas: pointer(int32(2)), Template: template}}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "app-rs", Namespace: w.Namespace, UID: "rs-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: kindDeployment, Name: d.Name, UID: d.UID, Controller: pointer(true)}}}, Spec: appsv1.ReplicaSetSpec{Replicas: pointer(int32(2)), Template: template}}
	require.NoError(t, e.Client.Create(ctx, d))
	require.NoError(t, e.Client.Create(ctx, rs))
	basePod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "app-pod-0", Namespace: w.Namespace, UID: "app-source-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: pointer(true)}}, Labels: map[string]string{"app": "demo"}}, Spec: *template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	basePod.Spec.NodeName = e.State.Source.Name
	require.NoError(t, e.Client.Create(ctx, basePod))
	peer := basePod.DeepCopy()
	peer.Name = "app-pod-1"
	peer.UID = "app-peer-uid"
	peer.ResourceVersion = ""
	peer.Spec.NodeName = "peer"
	require.NoError(t, e.Client.Create(ctx, peer))
	w.Instance = &Instance{Pod: *basePod.DeepCopy(), ReplicaSet: rs.DeepCopy(), DeploymentSpec: d.Spec.DeepCopy()}
	w.OriginalPods = []types.UID{basePod.UID}
	require.NoError(t, e.Apply(ctx, true))
	require.NoError(t, e.Verify(ctx))
	var after appsv1.Deployment
	require.NoError(t, e.Client.Get(ctx, client.ObjectKeyFromObject(d), &after))
	require.Equal(t, d.Spec, after.Spec)
	var samePeer corev1.Pod
	require.NoError(t, e.Client.Get(ctx, client.ObjectKeyFromObject(peer), &samePeer))
	require.Equal(t, peer.UID, samePeer.UID)
	var replacement corev1.Pod
	require.NoError(t, e.Client.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Instance.ReplacementName}, &replacement))
	require.True(t, ownedBy(&replacement, rs.UID))
	require.Equal(t, e.State.Target.Name, replacement.Spec.NodeName)
}

func TestInstanceRuntimeProofRejectsOldRunningContainer(t *testing.T) {
	e, w := instanceFixture(t)
	previous := e.Hosts
	e.Hosts = hostFunc(func(ctx context.Context, h Host, cmd string) (string, error) {
		if strings.HasSuffix(cmd, "ps -o json") {
			return fmt.Sprintf(`{"containers":[{"labels":{"io.kubernetes.pod.uid":%q}}]}`, w.Instance.Pod.UID), nil
		}
		return previous.Run(ctx, h, cmd)
	})
	require.ErrorContains(t, e.Apply(context.Background(), true), "仍在运行")
	require.NoError(t, e.checkHold(context.Background(), w))
	for _, v := range w.Volumes {
		require.NotEqual(t, phaseSwitched, v.Phase)
	}
	// Resume a serialized checkpoint with the source Pod already absent.
	path := t.TempDir() + "/state.json"
	require.NoError(t, SaveState(path, e.State))
	loaded, err := LoadState(path)
	require.NoError(t, err)
	e.State = loaded
	e.Hosts = previous
	require.NoError(t, e.Apply(context.Background(), true))
	require.NoError(t, e.Verify(context.Background()))
}

func TestInstanceCompletedResumeDoesNotUndoLaterCordon(t *testing.T) {
	e, _ := instanceFixture(t)
	ctx := context.Background()
	require.NoError(t, e.Apply(ctx, true))
	var source corev1.Node
	require.NoError(t, e.Client.Get(ctx, types.NamespacedName{Name: e.State.Source.Name}, &source))
	source.Spec.Unschedulable = true
	require.NoError(t, e.Client.Update(ctx, &source))
	require.NoError(t, e.Apply(ctx, true))
	require.NoError(t, e.Client.Get(ctx, types.NamespacedName{Name: source.Name}, &source))
	require.True(t, source.Spec.Unschedulable)
}

func TestInstanceHoldCannotExpandScope(t *testing.T) {
	e, w := instanceFixture(t)
	p, b := e.holdObjects(w)
	require.Equal(t, "request.namespace == \"prod\"", p.Spec.MatchConditions[0].Expression)
	require.Equal(t, []admissionv1.OperationType{admissionv1.Create}, p.Spec.MatchConstraints.ResourceRules[0].Operations)
	require.Contains(t, p.Spec.MatchConditions[1].Expression, "logs-db-0")
	require.Equal(t, []admissionv1.ValidationAction{admissionv1.Deny}, b.Spec.ValidationActions)
	require.Equal(t, metav1.StatusReasonForbidden, *p.Spec.Validations[0].Reason)
	broader := p.DeepCopy()
	broader.Spec.MatchConditions[0].Expression = "true"
	require.False(t, sameHoldPolicy(broader, p))
	broader = p.DeepCopy()
	broader.Spec.MatchConstraints.ResourceRules[0].Resources = append(broader.Spec.MatchConstraints.ResourceRules[0].Resources, "secrets")
	require.False(t, sameHoldPolicy(broader, p))
	broader = p.DeepCopy()
	broader.Spec.MatchConstraints.ObjectSelector = &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "app", Operator: metav1.LabelSelectorOpDoesNotExist}}}
	require.False(t, sameHoldPolicy(broader, p))
	defaulted := p.DeepCopy()
	defaulted.Spec.MatchConstraints.NamespaceSelector = &metav1.LabelSelector{}
	defaulted.Spec.MatchConstraints.ObjectSelector = &metav1.LabelSelector{}
	require.True(t, sameHoldPolicy(defaulted, p))
}

func TestInstancePlanIncludesOnlySelectedNamespaceAndPreservesServer(t *testing.T) {
	e, w := instanceFixture(t)
	// Only the lowest ordinal is on the source; never aggregate all replicas.
	state, err := e.Plan(context.Background(), e.State.Options)
	require.NoError(t, err)
	require.Len(t, state.Workloads, 1)
	require.Len(t, state.Workloads[0].Volumes, 2)
	require.Equal(t, w.Instance.Pod.UID, state.Workloads[0].Instance.Pod.UID)
	require.Empty(t, state.SourceRoles)
	legacy := state.Options
	legacy.Mode = "agent"
	legacy.Namespaces = nil
	legacy.SourcePolicy = "cordon"
	_, err = e.Plan(context.Background(), legacy)
	require.ErrorContains(t, err, "只迁移业务")
}

func TestInstanceStateAndSummaryPreserveIdentityWithoutSecrets(t *testing.T) {
	e, w := instanceFixture(t)
	w.Instance.Pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "PASSWORD", Value: "do-not-export-this"}}
	data, err := json.Marshal(Summary(e.State))
	require.NoError(t, err)
	require.NotContains(t, string(data), "do-not-export-this")
	require.Contains(t, string(data), "logs-db-0")
	require.NoError(t, validateState(e.State))
	w.Instance.Pod.UID = "replaced"
	require.Error(t, validateState(e.State))
}
