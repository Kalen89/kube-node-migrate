package nodemigrate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type hostFunc func(context.Context, Host, string) (string, error)

func (f hostFunc) Run(ctx context.Context, n Host, command string) (string, error) {
	return f(ctx, n, command)
}

func (f hostFunc) Filesystem(ctx context.Context, n Host, _ string, path, mode string) (string, error) {
	return f(ctx, n, "filesystem --path "+shellQuote(path)+" --mode "+shellQuote(mode))
}

type copierFunc func(context.Context, *State, *Workload) error

// Legacy component harness exercises copy/checkpoint primitives retained by
// the instance workflow. It is not a supported execution mode or CLI path.
func applyLegacyComponents(e *Engine, ctx context.Context, allow bool) error {
	if !allow {
		return e.Apply(ctx, false)
	}
	if err := e.guard(ctx, true); err != nil {
		return err
	}
	return e.withLease(ctx, e.apply)
}

func (f copierFunc) Copy(ctx context.Context, s *State, w *Workload) error { return f(ctx, s, w) }

func testNode(name, ip string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid"), Labels: map[string]string{hostnameLabel: name}},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16"), corev1.ResourceMemory: resource.MustParse("32Gi"), corev1.ResourcePods: resource.MustParse("110")}, NodeInfo: corev1.NodeSystemInfo{Architecture: "amd64", OperatingSystem: "linux"}, Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: ip}}, Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
}

func testState() *State {
	return &State{Version: stateVersion, ID: "0123456789abcdef", ClusterUID: "cluster-uid", CreatedAt: time.Now().UTC(), Phase: phasePlanned,
		Source: NodeIdentity{"old", "old-uid"}, Target: NodeIdentity{"new", "new-uid"},
		SourceConfig: Host{Name: "old", IP: "10.0.0.1", User: "root", Port: 22, Architecture: "amd64"},
		TargetConfig: Host{Name: "new", IP: "10.0.0.2", User: "root", Port: 22, Architecture: "amd64"},
		Options:      Options{PVConcurrency: 1, Source: "old", Target: "new", StatePath: "/tmp/private/state.json", Kubeconfig: "/tmp/kubeconfig", SSHUser: "root", SSHPort: 22, SourcePolicy: "cordon", CopyTimeout: time.Hour, WaitTimeout: 100 * time.Millisecond, HelperImage: "helper:v1", SSHDImage: "sshd:v3.7.0", RsyncImage: "rsync:v3.7.0"}}
}

func testPV(name, node, claim string, uid types.UID) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid")}, Spec: corev1.PersistentVolumeSpec{
		PersistentVolumeSource:        corev1.PersistentVolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/data/" + name}},
		PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
		ClaimRef:                      &corev1.ObjectReference{Namespace: "prod", Name: claim, UID: uid},
		NodeAffinity:                  &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: hostnameLabel, Operator: corev1.NodeSelectorOpIn, Values: []string{node}}}}}}}}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}}
}

func fixture(t *testing.T) (*Engine, *Workload, Options) {
	t.Helper()
	s := testState()
	claim := corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-db-0", Namespace: "prod", UID: "source-claim"}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "old-pv", StorageClassName: pointer("local-path"), AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}}}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	temporary := claim.DeepCopy()
	temporary.Name = "km-migrate-" + s.ID + "-0"
	temporary.UID = "temporary-uid"
	temporary.Spec.VolumeName = "new-pv"
	temporary.Labels = map[string]string{labelKey: s.ID}
	oldPV, newPV := testPV("old-pv", "old", claim.Name, claim.UID), testPV("new-pv", "new", temporary.Name, temporary.UID)
	template := corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "db", Image: "db:v1"}}, Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim.Name}}}}}}
	w := &Workload{Kind: "StatefulSet", Namespace: "prod", Name: "db", UID: "db-uid", Replicas: 1, Template: template, OriginalPods: []types.UID{"db-0-uid"}, Phase: phasePlanned,
		Volume: &Volume{PVC: claim, PV: *oldPV, TemporaryPVC: temporary.Name, TemporaryUID: temporary.UID, TargetPV: newPV, CopyID: "km-" + s.ID + "-0", Phase: "prepared"}}
	s.Workloads = []*Workload{w}
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: w.Name, Namespace: w.Namespace, UID: w.UID, Generation: 1}, Spec: appsv1.StatefulSetSpec{Replicas: pointer(int32(1)), Template: template}}
	pod := businessPod(w, "old")
	cfg := s.Options
	objects := []client.Object{testNode("old", "10.0.0.1"), testNode("new", "10.0.0.2"), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: s.ClusterUID}}, sts, pod, &claim, temporary, oldPV, newPV,
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "prod"}},
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "local-path"}, Provisioner: "rancher.io/local-path", VolumeBindingMode: pointer(storagev1.VolumeBindingWaitForFirstConsumer)}}
	c := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objects...).Build()
	e := &Engine{Client: c, State: s, Save: func(*State) error { return nil }, Copier: copierFunc(func(context.Context, *State, *Workload) error { return nil })}
	e.Hosts = hostFunc(func(_ context.Context, _ Host, command string) (string, error) {
		if strings.HasPrefix(command, "k3s ctr") {
			return "docker.io/library/db:v1 type digest complete (3/3) 1MiB true\ndocker.io/library/helper:v1 type digest complete (3/3) 1MiB true\ndocker.io/library/sshd:v3.7.0 type digest complete (3/3) 1MiB true\ndocker.io/library/rsync:v3.7.0 type digest complete (3/3) 1MiB true", nil
		}
		return `{"version":2,"sha256":"` + strings.Repeat("a", 64) + `","entries":2,"bytes":8,"device":7,"blockSize":4096,"freeBytes":1048576,"freeInodes":100,"inventory":[{"path":"Lg==","type":16384,"size":4096,"allocatedBytes":4096},{"path":"ZGF0YQ==","type":32768,"size":8,"allocatedBytes":4096}]}`, nil
	})
	return e, w, cfg
}

func businessPod(w *Workload, node string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: w.Name + "-0", Namespace: w.Namespace, UID: types.UID(w.Name + "-0-uid"), OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: w.Kind, Name: w.Name, UID: w.UID, Controller: pointer(true)}}}, Spec: corev1.PodSpec{NodeName: node, Containers: w.Template.Spec.Containers, Volumes: w.Template.Spec.Volumes}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.42.0.9", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
}

func TestPlanInventoryIsReadOnlyAndRejectsUnsafeObjects(t *testing.T) {
	for _, tc := range []struct {
		name  string
		alter func(*testing.T, *Engine, *Workload, Options)
		want  string
	}{
		{name: "local-path", alter: func(*testing.T, *Engine, *Workload, Options) {}},
		{name: "ownerless", want: "必须有控制器", alter: func(t *testing.T, e *Engine, _ *Workload, _ Options) {
			var p corev1.Pod
			require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: "prod", Name: "db-0"}, &p))
			p.OwnerReferences = nil
			require.NoError(t, e.Client.Update(t.Context(), &p))
		}},
		{name: "server-workloads", alter: func(t *testing.T, e *Engine, _ *Workload, _ Options) {
			var n corev1.Node
			require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: "old"}, &n))
			n.Labels["node-role.kubernetes.io/control-plane"] = "true"
			require.NoError(t, e.Client.Update(t.Context(), &n))
		}},
		{name: "hpa", want: "HPA", alter: func(t *testing.T, e *Engine, w *Workload, _ Options) {
			require.NoError(t, e.Client.Create(t.Context(), &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: w.Namespace}, Spec: autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: w.Kind, Name: w.Name}}}))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, w, cfg := fixture(t)
			cfg.SourcePolicy = ""
			tc.alter(t, e, w, cfg)
			e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
				return errors.New("plan modified cluster")
			}, Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
				return errors.New("plan modified cluster")
			}, Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
				return errors.New("plan modified cluster")
			}})
			s, err := e.Plan(t.Context(), cfg)
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
				return
			}
			require.NoError(t, err)
			require.Len(t, s.Workloads, 1)
			require.Equal(t, w.Volume.PVC.UID, s.Workloads[0].Volumes[0].PVC.UID)
			require.Equal(t, phasePlanned, s.Workloads[0].Volumes[0].Phase)
		})
	}
}

func simulateControllers(e *Engine, workloads ...*Workload) {
	e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if err := c.Delete(ctx, obj, opts...); err != nil {
				return err
			}
			if claim, ok := obj.(*corev1.PersistentVolumeClaim); ok {
				var pv corev1.PersistentVolume
				if err := c.Get(ctx, types.NamespacedName{Name: claim.Spec.VolumeName}, &pv); err != nil {
					return err
				}
				pv.Status.Phase = corev1.VolumeReleased
				return c.Status().Update(ctx, &pv)
			}
			return nil
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetUID() == "" {
				obj.SetUID(types.UID(obj.GetName() + "-created"))
			}
			if p, ok := obj.(*corev1.PersistentVolumeClaim); ok {
				p.Status.Phase = corev1.ClaimBound
			}
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if sts, ok := obj.(*appsv1.StatefulSet); ok {
				sts.Generation++
				if err := c.Update(ctx, obj, opts...); err != nil {
					return err
				}
				var w *Workload
				for _, candidate := range workloads {
					if candidate.Namespace == sts.Namespace && candidate.Name == sts.Name {
						w = candidate
						break
					}
				}
				if w == nil {
					return errors.New("unexpected simulated controller")
				}
				p := businessPod(w, "old")
				if replicas(sts.Spec.Replicas) == 0 {
					return client.IgnoreNotFound(c.Delete(ctx, p))
				}
				p.Spec.NodeName = "new"
				p.UID = "new-pod-uid"
				return c.Create(ctx, p)
			}
			return c.Update(ctx, obj, opts...)
		},
	})
}

func TestApplyPreservesData(t *testing.T) {
	e, w, _ := fixture(t)
	simulateControllers(e, w)
	var phases []string
	e.Save = func(s *State) error { phases = append(phases, w.Volume.Phase); return nil }
	require.NoError(t, applyLegacyComponents(e, t.Context(), true))
	require.Equal(t, "validated", e.State.Phase)
	require.Contains(t, phases, "starting")
	require.Equal(t, phaseStarted, w.Volume.Phase)
	var old corev1.PersistentVolume
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: "old-pv"}, &old))
	require.Equal(t, corev1.PersistentVolumeReclaimRetain, old.Spec.PersistentVolumeReclaimPolicy)
	var claim corev1.PersistentVolumeClaim
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Volume.PVC.Name}, &claim))
	require.NotEqual(t, w.Volume.PVC.UID, claim.UID)
	require.Equal(t, "new-pv", claim.Spec.VolumeName)

}

func TestCopyFailureNeverDeletesOriginalClaim(t *testing.T) {
	e, w, _ := fixture(t)
	simulateControllers(e, w)
	e.Copier = copierFunc(func(context.Context, *State, *Workload) error { return errors.New("disk full") })
	require.ErrorContains(t, applyLegacyComponents(e, t.Context(), true), "disk full")
	require.Equal(t, "copying", w.Volume.Phase)
	var c corev1.PersistentVolumeClaim
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Volume.PVC.Name}, &c))
	require.Equal(t, w.Volume.PVC.UID, c.UID)
	var sts appsv1.StatefulSet
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &sts))
	require.Zero(t, *sts.Spec.Replicas)
}

func stoppedFixture(t *testing.T) (*Engine, *Workload) {
	t.Helper()
	e, w, _ := fixture(t)
	simulateControllers(e, w)
	require.NoError(t, e.stopVolume(t.Context(), w))
	return e, w
}

func TestTargetTakeoverBlocksBeforeOriginalClaimDeletion(t *testing.T) {
	e, w := stoppedFixture(t)
	d := &Digest{SHA256: strings.Repeat("a", 64), Entries: 1}
	w.Volume.SourceDigest = d
	w.Volume.TargetDigest = d
	w.Volume.Phase = "verified"
	var target corev1.PersistentVolume
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: "new-pv"}, &target))
	target.Spec.ClaimRef.UID = "foreign"
	require.NoError(t, e.Client.Update(t.Context(), &target))
	require.ErrorContains(t, e.switchVolume(t.Context(), w), "保留原 PVC")
	var original corev1.PersistentVolumeClaim
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Volume.PVC.Name}, &original))
	require.Equal(t, w.Volume.PVC.UID, original.UID)
}

func TestStartingCheckpointMustSucceedBeforeRestoringReplicas(t *testing.T) {
	e, w := stoppedFixture(t)
	d := &Digest{SHA256: strings.Repeat("a", 64), Entries: 1}
	w.Volume.SourceDigest = d
	w.Volume.TargetDigest = d
	w.Volume.Phase = "verified"
	require.NoError(t, e.switchVolume(t.Context(), w))
	e.Save = func(*State) error { return errors.New("state disk full") }
	require.ErrorContains(t, e.startVolume(t.Context(), w), "state disk full")
	var sts appsv1.StatefulSet
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &sts))
	require.Zero(t, *sts.Spec.Replicas)
}

func TestLeaseRejectsSameOperationFromAnotherClient(t *testing.T) {
	e, _, _ := fixture(t)
	l, err := e.acquireLease(t.Context())
	require.NoError(t, err)
	_, err = e.acquireLease(t.Context())
	require.ErrorContains(t, err, "另一个")
	require.NoError(t, e.releaseLease(t.Context(), l.UID, "foreign-holder"))
	var existing coordinationv1.Lease
	require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(l), &existing))
	require.ErrorContains(t, e.renewLease(t.Context(), l.UID, "foreign-holder"), "所有权")
	require.NoError(t, e.releaseLease(t.Context(), l.UID, *l.Spec.HolderIdentity))
}

func TestNoMountsBlocksEvenTerminatingConsumer(t *testing.T) {
	e, w, _ := fixture(t)
	require.ErrorContains(t, e.noMounts(t.Context(), w), "引用")
	require.ErrorContains(t, applyLegacyComponents(e, t.Context(), false), "allow-downtime")
}

func TestImageCacheRequiresUnpackedSnapshot(t *testing.T) {
	cache := parseImageCache("busybox:1.34.1 type hash complete (3/3) 1MiB true\ndiscarded:v1 type hash incomplete (1/3) 1MiB true\npacked:v1 type hash complete (3/3) 1MiB false\nmissing:v1 type hash incomplete (1/3) 1MiB false\nunknown:v1 type hash unavailable (1/?) 1MiB true\n")
	require.True(t, cache["docker.io/library/busybox:1.34.1"])
	require.True(t, cache["docker.io/library/discarded:v1"])
	require.Len(t, cache, 2)
}

func TestWarmVolumesAllowsOnlyReadOnlyConfigMaps(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{
		Volumes:    []corev1.Volume{{Name: "timezone", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{}}}},
		Containers: []corev1.Container{{VolumeMounts: []corev1.VolumeMount{{Name: "timezone", ReadOnly: true}}}},
	}}
	require.True(t, safeHelperVolumes(pod, ""))
	pod.Spec.Containers[0].VolumeMounts[0].ReadOnly = false
	require.False(t, safeHelperVolumes(pod, ""))
	pod.Spec.Containers[0].VolumeMounts[0].ReadOnly = true
	pod.Spec.Volumes[0].VolumeSource = corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "business"}}
	require.False(t, safeHelperVolumes(pod, ""))
	pod.Spec.Volumes[0].VolumeSource = corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/data"}}
	require.False(t, safeHelperVolumes(pod, ""))
	pod.Spec.Volumes[0] = corev1.Volume{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "temporary"}}}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "timezone", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{}}})
	pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "data"}, {Name: "timezone", ReadOnly: true}}
	require.True(t, safeHelperVolumes(pod, "temporary"))
	require.False(t, safeHelperVolumes(pod, "wrong-claim"))
}

func TestLegacyOperatorPauseAndRestoreOriginalValue(t *testing.T) {
	for _, original := range []bool{false, true} {
		t.Run(fmt.Sprint(original), func(t *testing.T) {
			e, w, _ := fixture(t)
			w.Owner = &metav1.OwnerReference{APIVersion: "monitoring.coreos.com/v1", Kind: "Prometheus", Name: "obs", UID: "obs-uid"}
			obj := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "monitoring.coreos.com/v1", "kind": "Prometheus", "metadata": map[string]any{"name": "obs", "namespace": w.Namespace, "uid": "obs-uid"}, "spec": map[string]any{"paused": original}}}
			require.NoError(t, e.Client.Create(t.Context(), obj))
			require.NoError(t, e.pauseOperator(t.Context(), w))
			require.Equal(t, original, *w.OperatorPaused)
			require.NoError(t, e.guardPaused(t.Context(), w))
			require.NoError(t, e.restoreOperator(t.Context(), w))
			current, err := e.operator(t.Context(), w)
			require.NoError(t, err)
			require.NoError(t, e.verifyOperatorRestored(current, w))
		})
	}
}

func TestSuspendedMigrationProtectsNodesAfterLeaseRelease(t *testing.T) {
	e, _, cfg := fixture(t)
	cfg.SourcePolicy = ""
	require.NoError(t, e.claimNodes(t.Context()))
	_, err := e.Plan(t.Context(), cfg)
	require.ErrorContains(t, err, "未完成")
	other := *e.State
	other.ID = "fedcba9876543210"
	second := &Engine{Client: e.Client, State: &other}
	require.Error(t, second.claimNodes(t.Context()))
}

func TestLeaseReleasesAfterCancellation(t *testing.T) {
	e, _, _ := fixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	err := e.withLease(ctx, func(ctx context.Context) error { cancel(); <-ctx.Done(); return ctx.Err() })
	require.ErrorIs(t, err, context.Canceled)
	var lease coordinationv1.Lease
	err = e.Client.Get(t.Context(), types.NamespacedName{Namespace: "kube-system", Name: leaseName}, &lease)
	require.True(t, apierrors.IsNotFound(err))
}
