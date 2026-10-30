// Package nodemigrate moves K3s workloads and local volumes with resumable cutovers.
package nodemigrate

import (
	"context"
	"io"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/Kalen89/kube-node-migrate/internal/filesystem"
)

const (
	phasePreparing      = "preparing"
	phasePrepared       = "prepared"
	phasePrecopying     = "precopying"
	phasePrecopyReady   = "precopy-ready"
	phaseStopping       = "stopping"
	phaseStopped        = "stopped"
	phaseCopying        = "copying"
	phaseCopied         = "copied"
	phaseVerified       = "verified"
	phaseSwitching      = "switching"
	phaseSwitched       = "switched"
	phaseStarting       = "starting"
	phaseMoving         = "moving"
	phaseVolumesMoved   = "volumes-moved"
	phaseWorkloadsMoved = "workloads-moved"
	phaseValidated      = "validated"
	phaseRetiring       = "isolating"
	phaseRetired        = "isolated"
	kindDeployment      = "Deployment"
	kindStatefulSet     = "StatefulSet"
	kindDaemonSet       = "DaemonSet"
	kindApp             = "App"
	kindAlertmanager    = "Alertmanager"
	systemNamespace     = "kube-system"
	linuxOS             = "linux"
	copyToolName        = "pv-migrate"
	stateVersion        = 8
	ModeWorkloads       = "workloads"
	labelKey            = "kube-node-migrate.io/operation"
	MigrationLabel      = "kube-node-migrate.io/migration"
	leaseName           = "kube-node-migration"
	phasePlanned        = "planned"
	phaseStarted        = "started"
	sha256HexLength     = 64
)

// Options identifies the source, destination, offline dependencies and timeouts.
type Options struct {
	Mode                string            `json:"mode,omitempty"`
	Namespaces          []string          `json:"namespaces,omitempty"`
	ExcludeNamespaces   []string          `json:"excludeNamespaces,omitempty"`
	SSHKnownHosts       string            `json:"sshKnownHosts,omitempty"`
	Source              string            `json:"source"`
	Target              string            `json:"target"`
	StatePath           string            `json:"statePath"`
	Kubeconfig          string            `json:"kubeconfig"`
	Context             string            `json:"context"`
	HelperImage         string            `json:"helperImage"`
	SSHDImage           string            `json:"sshdImage"`
	RsyncImage          string            `json:"rsyncImage"`
	Archive             string            `json:"archive,omitempty"`
	SSHUser             string            `json:"sshUser"`
	SSHPort             int               `json:"sshPort"`
	SSHKey              string            `json:"sshKey,omitempty"`
	SourcePolicy        string            `json:"sourcePolicy"`
	CopyBinary          string            `json:"copyBinary"`
	FilesystemHelper    string            `json:"filesystemHelper"`
	WaitTimeout         time.Duration     `json:"waitTimeout"`
	CopyTimeout         time.Duration     `json:"copyTimeout"`
	TargetLabels        map[string]string `json:"targetLabels,omitempty"`
	ReserveCPU          string            `json:"reserveCPU,omitempty"`
	ReserveMemory       string            `json:"reserveMemory,omitempty"`
	ReservePods         int64             `json:"reservePods,omitempty"`
	HealthChecks        []HealthCheck     `json:"healthChecks,omitempty"`
	PVConcurrency       int               `json:"pvConcurrency"`
	RsyncBandwidthLimit int64             `json:"rsyncBandwidthLimit,omitempty"`
	ParallelDigest      bool              `json:"parallelDigest,omitempty"`
	Precopy             bool              `json:"precopy,omitempty"`
	PrecopyRounds       int               `json:"precopyRounds,omitempty"`
	PrecopyThreshold    time.Duration     `json:"precopyThreshold,omitempty"`

	HelperResources *corev1.ResourceRequirements `json:"helperResources,omitempty"`
	SSHDResources   *corev1.ResourceRequirements `json:"sshdResources,omitempty"`
	RsyncResources  *corev1.ResourceRequirements `json:"rsyncResources,omitempty"`
}

// NodeIdentity prevents a resumed operation from acting on a replacement object.
type NodeIdentity struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}

// WorkloadIdentity identifies a controller across namespaces.
type WorkloadIdentity struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	UID       types.UID `json:"uid"`
}

// Digest records a complete stopped filesystem scan without storing file contents.
type Digest = filesystem.Digest

// StateVersion returns the current executable checkpoint format.
func StateVersion() int { return stateVersion }

// Volume retains both binding identities until an explicit later cleanup.
type Volume struct {
	NewPVCUID         types.UID                    `json:"newPVCUID,omitempty"`
	PVC               corev1.PersistentVolumeClaim `json:"pvc"`
	PV                corev1.PersistentVolume      `json:"pv"`
	TemporaryPVC      string                       `json:"temporaryPVC"`
	TemporaryUID      types.UID                    `json:"temporaryUID,omitempty"`
	TargetPV          *corev1.PersistentVolume     `json:"targetPV,omitempty"`
	CopyID            string                       `json:"copyID"`
	CopyJobName       string                       `json:"copyJobName,omitempty"`
	CopyJobUID        types.UID                    `json:"copyJobUID,omitempty"`
	CopySucceeded     bool                         `json:"copySucceeded,omitempty"`
	CopyRuntimeMillis *int64                       `json:"copyRuntimeMillis,omitempty"`
	CopyCleanup       *CopyCleanupIntent           `json:"copyCleanup,omitempty"`
	FinalSyncRetry    bool                         `json:"finalSyncRetry,omitempty"`
	SourceDigest      *Digest                      `json:"sourceDigest,omitempty"`
	TargetDigest      *Digest                      `json:"targetDigest,omitempty"`
	Phase             string                       `json:"phase"`
	Precopy           *PrecopyProgress             `json:"precopy,omitempty"`
}

// CopyCleanupIntent authorizes recovery of an already verified cleanup only.
type CopyCleanupIntent struct {
	Resources []CopyCleanupResource `json:"resources"`
	Parents   []types.UID           `json:"parents"`
}

type CopyCleanupResource struct {
	Kind            string    `json:"kind"`
	Name            string    `json:"name"`
	UID             types.UID `json:"uid"`
	ResourceVersion string    `json:"resourceVersion"`
}

// PrecopyProgress 仅记录在线预同步是否完成既定策略，不证明数据一致。
// Ready 允许进入停写；切换卷仍要求源写进程退出、最终同步及摘要校验通过。
type PrecopyProgress struct {
	RoundStartedAt time.Time      `json:"roundStartedAt,omitempty"`
	Rounds         []PrecopyRound `json:"rounds,omitempty"`
	// Converged 为兼容旧检查点保留；新轮次中仅表示显式耗时阈值达标。
	// 该字段不证明变化量在下降，更不证明数据库或文件副本一致。
	Converged bool `json:"converged"`
	// Ready 表示成功轮数或显式阈值策略完成，允许重新检查后进入停写。
	Ready bool `json:"ready"`
}

// PrecopyRound includes copy setup and cleanup time, not just network transfer.
type PrecopyRound struct {
	StartedAt     time.Time `json:"startedAt"`
	FinishedAt    time.Time `json:"finishedAt"`
	ElapsedMillis int64     `json:"elapsedMillis"`
	// SyncMillis 来自成功 rsync 容器的运行时间，含扫描、连接及传输，
	// 不含 Pod 调度、镜像拉取和清理；nil 表示未知，不能当作 0 或达标。
	SyncMillis *int64 `json:"syncMillis,omitempty"`
}

// Workload stores the original desired state for drift checks and safe recovery.
type Workload struct {
	Instance          *Instance              `json:"instance,omitempty"`
	Volumes           []*Volume              `json:"volumes,omitempty"`
	Kind              string                 `json:"kind"`
	Namespace         string                 `json:"namespace"`
	Name              string                 `json:"name"`
	UID               types.UID              `json:"uid"`
	Replicas          int32                  `json:"replicas"`
	Template          corev1.PodTemplateSpec `json:"template"`
	Owner             *metav1.OwnerReference `json:"owner,omitempty"`
	OperatorPaused    *bool                  `json:"operatorPaused,omitempty"`
	OriginalPods      []types.UID            `json:"originalPods"`
	StoppedGeneration int64                  `json:"stoppedGeneration,omitempty"`
	Volume            *Volume                `json:"volume,omitempty"`
	Phase             string                 `json:"phase"`
}

// Instance records one original Pod, not an entire controller's desired state.
// Controller specs and Pod snapshots are private checkpoint data.
type Instance struct {
	Pod                 corev1.Pod              `json:"pod"`
	ReplicaSet          *appsv1.ReplicaSet      `json:"replicaSet,omitempty"`
	DeploymentSpec      *appsv1.DeploymentSpec  `json:"deploymentSpec,omitempty"`
	StatefulSetSpec     *appsv1.StatefulSetSpec `json:"statefulSetSpec,omitempty"`
	PolicyUID           types.UID               `json:"policyUID,omitempty"`
	BindingUID          types.UID               `json:"bindingUID,omitempty"`
	ReplacementName     string                  `json:"replacementName,omitempty"`
	ReplacementUID      types.UID               `json:"replacementUID,omitempty"`
	EvictedAt           time.Time               `json:"evictedAt,omitempty"`
	EvictionRequestedAt time.Time               `json:"evictionRequestedAt,omitempty"`
	StoppedAt           time.Time               `json:"stoppedAt,omitempty"`
	ReadyAt             time.Time               `json:"readyAt,omitempty"`
}

// State is private operational data: controller snapshots may contain inline env.
// Store it only in a 0700 directory and a 0600 file; never print the full value.
type State struct {
	ActiveStage         *StageEvent        `json:"activeStage,omitempty"`
	ExecutorUser        string             `json:"executorUser,omitempty"`
	SourceRoles         map[string]string  `json:"sourceRoles,omitempty"`
	SourceUnschedulable bool               `json:"sourceUnschedulable,omitempty"`
	SourceRestored      bool               `json:"sourceRestored,omitempty"`
	Version             int                `json:"version"`
	ID                  string             `json:"id"`
	ClusterUID          types.UID          `json:"clusterUID"`
	CreatedAt           time.Time          `json:"createdAt"`
	UpdatedAt           time.Time          `json:"updatedAt"`
	Source              NodeIdentity       `json:"source"`
	Target              NodeIdentity       `json:"target"`
	SourceConfig        Host               `json:"sourceConfig"`
	TargetConfig        Host               `json:"targetConfig"`
	Options             Options            `json:"options"`
	Workloads           []*Workload        `json:"workloads"`
	DaemonSets          []WorkloadIdentity `json:"daemonSets"`
	SourceTaints        []corev1.Taint     `json:"sourceTaints,omitempty"`
	TargetTaints        []corev1.Taint     `json:"targetTaints,omitempty"`
	Phase               string             `json:"phase"`
	Capacity            *CapacityReport    `json:"capacity,omitempty"`
	Events              []StageEvent       `json:"events,omitempty"`
	HealthBefore        bool               `json:"healthBefore,omitempty"`
}

// Hosts executes only on the two authenticated hosts selected by the plan.
// Implementations must permit concurrent calls when PVConcurrency is above one.
type Hosts interface {
	Run(context.Context, Host, string) (string, error)
}

// Copier starts or rejoins a persisted PVC copy and cleans up only its own helpers.
// Custom implementations must permit concurrent calls on distinct volumes.
type Copier interface {
	Copy(context.Context, *State, *Workload) error
}

// Engine shares the installation client's authentication and namespace selection.
type Engine struct {
	Client client.Client
	// AdmissionProbe uses a different, impersonated identity for server dry-runs.
	// It never creates a real Pod or PVC and must not bypass admission.
	AdmissionProbe client.Client
	// AdmissionProbes refreshes the directly addressed HA API server clients.
	// Every client must observe the hold before the original instance is evicted.
	AdmissionProbes func(context.Context) ([]client.Client, error)
	Hosts           Hosts
	Copier          Copier
	Output          io.Writer
	State           *State
	Save            func(*State) error
}
