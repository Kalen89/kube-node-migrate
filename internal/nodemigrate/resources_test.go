package nodemigrate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestServiceResourcesValidateAndInheritDefaults(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configured corev1.ResourceRequirements
		valid      bool
	}{
		{"partial", corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m")}}, true},
		{"zero", corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")}}, false},
		{"negative", corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("-1Mi")}}, false},
		{"request-over-limit", corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}}, false},
		{"unsupported", corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := testState().Options
			o.RsyncResources = &tc.configured
			err := ValidateOptions(o)
			if !tc.valid {
				require.ErrorContains(t, err, "rsync.resources")
				return
			}
			require.NoError(t, err)
			actual := serviceResources(o.RsyncResources)
			require.Equal(t, "250m", actual.Requests.Cpu().String())
			require.Equal(t, "128Mi", actual.Requests.Memory().String())
			require.Equal(t, "512Mi", actual.Limits.Memory().String())
			actual.Requests[corev1.ResourceCPU] = resource.MustParse("500m")
			require.Equal(t, "250m", tc.configured.Requests.Cpu().String())
		})
	}
}

func TestConfiguredResourcesReachBindingPodAndMoverValues(t *testing.T) {
	e, w, _ := fixture(t)
	e.State.Options.StatePath = filepath.Join(t.TempDir(), "state.json")
	e.State.Options.HelperResources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m")}}
	e.State.Options.SSHDResources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")}}
	e.State.Options.RsyncResources = &corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")}}
	helper, err := e.bindingHelper(t.Context(), w)
	require.NoError(t, err)
	require.Equal(t, w.Namespace, helper.Namespace)
	require.Equal(t, "250m", helper.Spec.Containers[0].Resources.Requests.Cpu().String())
	require.Equal(t, "128Mi", helper.Spec.Containers[0].Resources.Requests.Memory().String())
	path, err := (&processCopier{engine: e}).values(t.Context(), e.State, w)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var values struct {
		SSHD struct {
			Resources corev1.ResourceRequirements `json:"resources"`
		} `json:"sshd"`
		Rsync struct {
			Resources corev1.ResourceRequirements `json:"resources"`
		} `json:"rsync"`
	}
	require.NoError(t, json.Unmarshal(data, &values))
	require.Equal(t, "500m", values.SSHD.Resources.Requests.Cpu().String())
	require.Equal(t, "2Gi", values.Rsync.Resources.Limits.Memory().String())
	require.Equal(t, "100m", values.Rsync.Resources.Requests.Cpu().String())
}

func TestCapacityAndQuotaUseConfiguredServiceResources(t *testing.T) {
	e, w, _ := fixture(t)
	e.State.Options.HelperResources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")}}
	e.State.Options.SSHDResources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")}}
	e.State.Options.RsyncResources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1500m")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}}
	reserve, err := capacityReserve(e.State.Options)
	require.NoError(t, err)
	require.Equal(t, "3", reserve.Cpu().String())
	quota := corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "cpu"}, Status: corev1.ResourceQuotaStatus{
		Hard: corev1.ResourceList{corev1.ResourceLimitsCPU: resource.MustParse("3")}, Used: corev1.ResourceList{corev1.ResourceLimitsCPU: resource.MustParse("0")},
	}}
	require.ErrorContains(t, e.namespaceCapacity(w.Namespace, []corev1.ResourceQuota{quota}, nil, nil, true), "limits.cpu")
	var source corev1.Node
	require.NoError(t, e.Client.Get(t.Context(), types.NamespacedName{Name: e.State.Source.Name}, &source))
	source.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("250m")
	require.NoError(t, e.Client.Status().Update(t.Context(), &source))
	require.ErrorContains(t, e.checkSourceCopyCapacity(t.Context(), nil, false), "源节点缺少")
}

func TestLimitRangeChecksEachServiceUsedInTheNamespace(t *testing.T) {
	e, w, _ := fixture(t)
	e.State.Options.SSHDResources = &corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}}
	ranges := []corev1.LimitRange{{ObjectMeta: metav1.ObjectMeta{Name: "cpu"}, Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{Type: corev1.LimitTypeContainer, Max: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}}}}}
	require.ErrorContains(t, e.namespaceCapacity(w.Namespace, nil, ranges, nil, true), "sshd")
	require.NoError(t, e.namespaceCapacity("namespace-without-pvc", nil, ranges, nil, true))
}

func TestResourceSettingsAreSnapshottedAndOldWorkloadStatesLoad(t *testing.T) {
	e, _ := instanceFixture(t)
	o := e.State.Options
	o.HelperResources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m")}}
	s, err := e.Plan(t.Context(), o)
	require.NoError(t, err)
	o.HelperResources.Requests[corev1.ResourceCPU] = resource.MustParse("900m")
	require.Equal(t, "250m", s.Options.HelperResources.Requests.Cpu().String())
	require.Equal(t, "512Mi", s.Options.RsyncResources.Limits.Memory().String())
	path := filepath.Join(t.TempDir(), "state.json")
	require.NoError(t, SaveState(path, s))
	loaded, err := LoadState(path)
	require.NoError(t, err)
	require.Equal(t, "250m", loaded.Options.HelperResources.Requests.Cpu().String())
	// Format 6 had no resource configuration and must retain the old defaults.
	s.Version = 6
	s.Options.HelperResources, s.Options.SSHDResources, s.Options.RsyncResources = nil, nil, nil
	require.NoError(t, SaveState(path, s))
	loaded, err = LoadState(path)
	require.NoError(t, err)
	require.Equal(t, stateVersion, loaded.Version)
	require.Equal(t, "100m", loaded.Options.HelperResources.Requests.Cpu().String())
	require.NoError(t, CheckExecutionState(loaded))
}
