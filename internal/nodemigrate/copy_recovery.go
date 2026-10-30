package nodemigrate

import (
	"context"
	"errors"
)

func (e *Engine) retryFinalSync(ctx context.Context, w *Workload) error {
	v := w.Volume
	if v.Phase != phaseCopied || v.NewPVCUID != "" || (w.Instance != nil && (w.Phase != phaseStopped || w.Instance.ReplacementUID != "")) {
		return errors.New("目标卷已经进入切换或启动阶段，禁止重新同步")
	}
	for _, other := range w.Volumes {
		if other.NewPVCUID != "" || !oneOf(other.Phase, phasePrepared, phasePrecopyReady, phaseStopped, phaseCopying, phaseCopied, phaseVerified) {
			return errors.New("同一实例已有卷进入切换，禁止重新同步")
		}
		view := volumeView(w, other)
		if err := e.copyBindings(ctx, view); err != nil {
			return err
		}
		if err := e.noMounts(ctx, view); err != nil {
			return err
		}
	}
	if err := e.noMounts(ctx, w); err != nil {
		return err
	}
	if v.CopyCleanup != nil {
		return errors.New("复制清理尚未完成，禁止重新同步")
	}
	v.FinalSyncRetry = false
	v.CopyJobName, v.CopyJobUID, v.CopySucceeded = "", "", false
	v.CopyRuntimeMillis = nil
	v.SourceDigest, v.TargetDigest = nil, nil
	v.Phase = phaseCopying
	return e.checkpoint()
}

// validateCopyRecoveryState checks persisted retry/cleanup authorization.
func validateCopyRecoveryState(w *Workload) error {
	v := w.Volume
	if err := validateCopyCleanup(w); err != nil {
		return err
	}
	if v.CopyCleanup != nil && !oneOf(v.Phase, phasePrecopying, phaseCopying, phaseCopied) {
		return errors.New("复制清理意图阶段无效")
	}
	if v.FinalSyncRetry {
		if v.Phase != phaseCopied || !validDigest(v.SourceDigest) || !validDigest(v.TargetDigest) || *v.SourceDigest == *v.TargetDigest || v.NewPVCUID != "" {
			return errors.New("最终同步重试意图与卷状态不符")
		}
		if w.Instance != nil && (w.Phase != phaseStopped || w.Instance.ReplacementUID != "") {
			return errors.New("最终同步重试实例已经进入切换或启动阶段")
		}
		for _, other := range w.Volumes {
			if other.NewPVCUID != "" || !oneOf(other.Phase, phasePrepared, phasePrecopyReady, phaseStopped, phaseCopying, phaseCopied, phaseVerified) {
				return errors.New("最终同步重试的同实例卷已经进入切换")
			}
		}
	}
	return nil
}
