package nodemigrate

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	privateDirMode  = 0o700
	privateFileMode = 0o600
)

// OpenState locks a private state location; the caller must close the returned lock.
func OpenState(path string) (*os.File, error) {
	if path == "" {
		return nil, errors.New("必须指定 --state，迁移状态用于断点恢复")
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, privateDirMode); err != nil {
		return nil, err
	}
	info, err := os.Lstat(parent)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("迁移状态目录必须是权限为 0700 的真实目录: %s", parent)
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, privateFileMode)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		closeReadOnly(lock)
		return nil, fmt.Errorf("迁移状态正被另一个进程使用: %w", err)
	}
	return lock, nil
}

// SaveState atomically persists a checkpoint before the next irreversible API call.
func SaveState(path string, state *State) error {
	if state == nil {
		return errors.New("迁移状态未初始化")
	}
	state.UpdatedAt = time.Now().UTC()
	if err := writePrivateJSON(path, state); err != nil {
		return err
	}
	// The private checkpoint is authoritative and must be durable first.
	if err := SaveReport(path, state); err != nil {
		return fmt.Errorf("检查点已保存，但脱敏报告刷新失败: %w", err)
	}
	return nil
}

func writePrivateJSON(path string, value any) error {
	return writeAtomicJSON(path, value, true)
}

func writeAtomicJSON(path string, value any, durable bool) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".node-migration-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer removeTemporary(name)
	if _, err = f.Write(append(data, '\n')); err == nil && durable {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	if !durable {
		return nil
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer closeReadOnly(dir)
	return dir.Sync()
}

// LoadState rejects unknown schemas and files exposed to other users.
func LoadState(path string) (*State, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("迁移状态必须是权限为 0600 的普通文件")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.Version == 2 || state.Version == 3 {
		state.Version = 5 // Historical node-replacement checkpoints are read-only.
		// Older tools always migrated one volume at a time without a bandwidth cap.
		state.Options.PVConcurrency = 1
		state.Options.RsyncBandwidthLimit = 0
		state.Options.Precopy = false
		state.Options.PrecopyRounds = 0
		state.Options.PrecopyThreshold = 0
	}
	if state.Version == 4 {
		state.Version = 5
	}
	if state.Version == 6 || state.Version == 7 {
		// Original workload checkpoints used the same fixed service resources.
		state.Version = stateVersion
	}
	if state.Version != stateVersion && state.Version != 5 {
		return nil, fmt.Errorf("迁移状态格式 %d 与当前格式 %d 不兼容，请使用对应版本处理该任务", state.Version, stateVersion)
	}
	if state.ID == "" || state.ClusterUID == "" {
		return nil, errors.New("迁移状态版本或身份无效")
	}
	if err := validateState(&state); err != nil {
		return nil, err
	}
	if state.Version == stateVersion {
		normalizeServiceResources(&state.Options)
	}
	return &state, nil
}

func validateState(s *State) error {
	if err := ValidateOptions(s.Options); err != nil {
		return err
	}
	if s.Options.Mode == ModeWorkloads && len(s.Options.Namespaces) == 0 {
		return errors.New("逐实例检查点缺少已经确认的命名空间清单")
	}
	if s.Options.Mode == ModeWorkloads && (!oneOf(s.Phase, phasePlanned, phaseValidated) || (s.SourceRestored && s.Phase != phaseValidated)) {
		return errors.New("workloads 检查点不得进入整机隔离阶段")
	}
	if !oneOf(s.SourceConfig.Architecture, "amd64", "arm64") || s.SourceConfig.Architecture != s.TargetConfig.Architecture {
		return errors.New("迁移状态缺少匹配的节点架构")
	}
	id, err := hex.DecodeString(s.ID)
	if err != nil || len(id) != operationRandomBytes || s.Source.Name == "" || s.Target.Name == "" || s.Source.Name == s.Target.Name || s.Source.UID == "" || s.Target.UID == "" || s.SourceConfig.Addr() == s.TargetConfig.Addr() {
		return errors.New("迁移状态中的节点身份无效")
	}
	if !oneOf(s.Phase, phasePlanned, phaseVolumesMoved, phaseWorkloadsMoved, phaseValidated, phaseRetiring, phaseRetired) {
		return errors.New("未知整机迁移阶段")
	}
	seen := make(map[string]bool)
	volumeIDs := map[string]bool{}
	for _, w := range s.Workloads {
		if w == nil || w.UID == "" || w.Name == "" || w.Namespace == "" || w.Replicas < 1 || !oneOf(w.Kind, kindDeployment, kindStatefulSet) {
			return errors.New("无效工作负载状态")
		}
		if s.Options.Mode == ModeWorkloads {
			if err := validateInstanceState(s, w); err != nil {
				return err
			}
		} else if w.Instance != nil || len(w.Volumes) != 0 || !oneOf(w.Phase, phasePlanned, phaseMoving, phaseStarted) {
			return errors.New("旧模式含不兼容的逐实例状态")
		}
		key := workloadKey(w)
		if seen[key] {
			return errors.New("迁移状态包含重复工作负载")
		}
		seen[key] = true
		for _, v := range workloadVolumes(w) {
			if v == nil {
				return errors.New("卷状态为空")
			}
			for _, id := range []string{"pvc:" + string(v.PVC.UID), "pv:" + string(v.PV.UID), "copy:" + v.CopyID} {
				if volumeIDs[id] {
					return errors.New("不同实例共享源卷或复制操作身份")
				}
				volumeIDs[id] = true
			}
			if err := validateVolumeState(s, volumeView(w, v)); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
		}
	}
	return nil
}

func validateVolumeState(s *State, w *Workload) error {
	v := w.Volume
	if !oneOf(v.Phase, phasePlanned, phasePreparing, phasePrepared, phasePrecopying, phasePrecopyReady, phaseStopping, phaseStopped, phaseCopying, phaseCopied, phaseVerified, phaseSwitching, phaseSwitched, phaseStarting, phaseStarted) {
		return errors.New("未知卷迁移阶段")
	}
	if (w.Instance == nil && w.Replicas != 1) || v.PVC.UID == "" || v.PVC.Name == "" || v.PVC.Namespace != w.Namespace || v.PV.UID == "" || v.PVC.Spec.VolumeName != v.PV.Name || !pvOnNode(&v.PV, s.Source.Name) {
		return errors.New("源 PVC/PV 身份不完整")
	}
	if _, err := volumePath(&v.PV); err != nil {
		return err
	}
	if !strings.HasPrefix(v.CopyID, "km-"+s.ID+"-") || len(v.CopyID) > 24 || v.TemporaryPVC != "km-migrate-"+strings.TrimPrefix(v.CopyID, "km-") {
		return errors.New("复制操作身份无效")
	}
	if !oneOf(v.Phase, phasePlanned, phasePreparing) && (v.TemporaryUID == "" || v.TargetPV == nil) {
		return errors.New("目标卷身份缺失")
	}
	if v.TargetPV != nil {
		if v.TargetPV.UID == "" || v.TargetPV.Name == v.PV.Name || !pvOnNode(v.TargetPV, s.Target.Name) {
			return errors.New("目标 PV 身份无效")
		}
		if _, err := volumePath(v.TargetPV); err != nil {
			return err
		}
	}
	if oneOf(v.Phase, phaseVerified, phaseSwitching, phaseSwitched, phaseStarting, phaseStarted) && (!validDigest(v.SourceDigest) || !validDigest(v.TargetDigest) || *v.SourceDigest != *v.TargetDigest) {
		return errors.New("已校验阶段缺少匹配的数据摘要")
	}
	if err := validatePrecopyState(s.Options, v); err != nil {
		return err
	}
	if err := validateCopyRecoveryState(w); err != nil {
		return err
	}
	return nil
}

func oneOf(value string, choices ...string) bool {
	for _, choice := range choices {
		if value == choice {
			return true
		}
	}
	return false
}

func (e *Engine) checkpoint() error {
	if e.Save == nil {
		return errors.New("迁移状态持久化未初始化")
	}
	return e.Save(e.State)
}
