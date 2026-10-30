package nodemigrate

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

func validatePrecopyState(o Options, v *Volume) error {
	p := v.Precopy
	if !o.Precopy {
		if p != nil || oneOf(v.Phase, phasePrecopying, phasePrecopyReady) {
			return errors.New("未启用在线预同步却存在预同步状态")
		}
		return nil
	}
	if oneOf(v.Phase, phasePlanned, phasePreparing, phasePrepared) {
		if p != nil {
			return errors.New("准备阶段含不匹配的预同步记录")
		}
		return nil
	}
	if p == nil {
		return errors.New("缺少在线预同步记录")
	}
	for _, r := range p.Rounds {
		if r.StartedAt.IsZero() || r.FinishedAt.Before(r.StartedAt) || r.ElapsedMillis < 0 || r.ElapsedMillis != r.FinishedAt.Sub(r.StartedAt).Milliseconds() {
			return errors.New("在线预同步轮次耗时无效")
		}
		if r.SyncMillis != nil && (*r.SyncMillis < 0 || *r.SyncMillis > r.ElapsedMillis) {
			return errors.New("在线预同步实际同步耗时无效")
		}
	}
	if p.Ready || p.Converged {
		if len(p.Rounds) == 0 || !p.RoundStartedAt.IsZero() {
			return errors.New("在线预同步收敛记录不完整")
		}
		last := p.Rounds[len(p.Rounds)-1]
		// 旧检查点仅有 Converged，保守沿用旧的整轮耗时证明；
		// 新检查点必须明确记录 Ready，并按当前策略验证成功轮数或实际同步耗时。
		if o.PrecopyThreshold == 0 {
			if !p.Ready || p.Converged || len(p.Rounds) < o.PrecopyRounds {
				return errors.New("在线预同步成功轮数不足或状态不符")
			}
		} else if p.Ready && (!p.Converged || last.SyncMillis == nil || time.Duration(*last.SyncMillis)*time.Millisecond > o.PrecopyThreshold) {
			return errors.New("在线预同步实际同步耗时未满足切换条件")
		} else if !p.Ready && last.FinishedAt.Sub(last.StartedAt) > o.PrecopyThreshold {
			return errors.New("在线预同步末轮耗时超出切换条件")
		}
	}
	if v.Phase != phasePrecopying && !p.Ready && !p.Converged {
		return errors.New("在线预同步未完成，不能进入停写及切换阶段")
	}
	if oneOf(v.Phase, phasePrecopying, phasePrecopyReady) && (v.SourceDigest != nil || v.TargetDigest != nil) {
		return errors.New("在线预同步不能保存停写后的数据摘要")
	}
	return nil
}

func (e *Engine) precopyVolume(ctx context.Context, w *Workload) error {
	v, o := w.Volume, e.State.Options
	if !o.Precopy || !oneOf(v.Phase, phasePrepared, phasePrecopying, phasePrecopyReady) {
		return nil
	}
	if err := e.guard(ctx, true); err != nil {
		return err
	}
	if err := e.copyBindings(ctx, w); err != nil {
		return err
	}
	if _, err := e.liveSource(ctx, w); err != nil {
		return err
	}
	if v.Phase == phasePrecopyReady {
		// Re-entering here means execution stopped before stop-writes. The source
		// may have run for hours since the old estimate, so refresh it on resume.
		v.Phase = phasePrecopying
		v.Precopy.Converged = false
		v.Precopy.Ready = false
		if err := e.checkpoint(); err != nil {
			return err
		}
	}
	if e.Copier == nil {
		return errors.New("复制执行器未初始化")
	}
	if v.Precopy == nil {
		v.Precopy = &PrecopyProgress{}
		v.Phase = phasePrecopying
		if err := e.checkpoint(); err != nil {
			return err
		}
	}
	p := v.Precopy
	for attempt := 1; attempt <= o.PrecopyRounds; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.RoundStartedAt.IsZero() {
			p.RoundStartedAt = time.Now().UTC()
			if err := e.checkpoint(); err != nil {
				return err
			}
		}
		e.logf("在线预同步开始 pvc=%s/%s round=%d attempt=%d/%d threshold=%s；源 Pod 保持运行，目标副本不启动", w.Namespace, v.PVC.Name, len(p.Rounds)+1, attempt, o.PrecopyRounds, o.PrecopyThreshold)
		if err := e.Copier.Copy(ctx, e.State, w); err != nil {
			return fmt.Errorf("在线预同步失败，未执行源服务停写: %w", err)
		}
		if err := e.waitCopyReleased(ctx, w); err != nil {
			return err
		}
		round := PrecopyRound{StartedAt: p.RoundStartedAt, FinishedAt: time.Now().UTC()}
		round.ElapsedMillis = round.FinishedAt.Sub(round.StartedAt).Milliseconds()
		p.Rounds = append(p.Rounds, round)
		p.RoundStartedAt = time.Time{}
		round.SyncMillis = v.CopyRuntimeMillis
		if round.SyncMillis != nil && (*round.SyncMillis < 0 || *round.SyncMillis > round.ElapsedMillis) {
			round.SyncMillis = nil // 时钟异常时不使用该测量判断阈值。
		}
		p.Rounds[len(p.Rounds)-1] = round
		// 持续写入时，成功轮次只说明本轮 rsync 正常完成，不代表副本一致。
		// 默认按成功轮数结束预同步；只有用户显式设置阈值才要求达标。
		// 达标仍只是停机成本参考，不表示脏数据为零或数据库已可启动。
		p.Converged = o.PrecopyThreshold > 0 && round.SyncMillis != nil && time.Duration(*round.SyncMillis)*time.Millisecond <= o.PrecopyThreshold
		p.Ready = p.Converged || (o.PrecopyThreshold == 0 && attempt == o.PrecopyRounds)
		// The same owned release name can be reused only after all helpers are
		// gone. Persist this reset together with the completed round so resume
		// never mistakes a pre-copy success for the final offline copy.
		v.CopyJobName, v.CopyJobUID, v.CopySucceeded = "", "", false
		v.CopyRuntimeMillis = nil
		if p.Ready {
			v.Phase = phasePrecopyReady
		}
		if err := e.checkpoint(); err != nil {
			return err
		}
		syncTime, overhead := "unknown", "unknown"
		if round.SyncMillis != nil {
			syncTime = (time.Duration(*round.SyncMillis) * time.Millisecond).String()
			overhead = (time.Duration(round.ElapsedMillis-*round.SyncMillis) * time.Millisecond).String()
		}
		e.logf("在线预同步本轮成功 pvc=%s/%s round=%d elapsed=%s sync=%s setupCleanup=%s ready=%t thresholdMet=%t；源仍在写入，尚未证明副本一致", w.Namespace, v.PVC.Name, len(p.Rounds), round.FinishedAt.Sub(round.StartedAt).Round(time.Millisecond), syncTime, overhead, p.Ready, p.Converged)
		if p.Ready {
			e.logf("预同步策略完成；允许进入停写阶段，禁止跳过源进程退出、最终同步和摘要校验")
			return nil
		}
	}
	return fmt.Errorf("在线预同步 %d 轮后仍未达到显式阈值 %s，或缺少实际同步耗时；源服务保持运行，后续批次不启动，可使用 resume 再尝试", o.PrecopyRounds, o.PrecopyThreshold)
}

// liveSource allows exactly the original running source Pod. A replacement,
// reschedule or a replica/template change requires a fresh operator decision.
func (e *Engine) liveSource(ctx context.Context, w *Workload) (types.UID, error) {
	if w.Instance != nil {
		return e.liveInstance(ctx, w)
	}
	_, count, err := e.getWorkload(ctx, w, true)
	if err != nil {
		return "", err
	}
	if count != 1 {
		return "", errors.New("在线预同步要求源服务维持单副本运行")
	}
	pods, err := e.workloadPods(ctx, w)
	if err != nil {
		return "", err
	}
	if len(pods) != 1 || pods[0].Spec.NodeName != e.State.Source.Name || !podReady(&pods[0]) || !slices.Contains(w.OriginalPods, pods[0].UID) {
		return "", errors.New("在线预同步的原始源 Pod 已替换、未就绪或调度位置改变")
	}
	return pods[0].UID, nil
}

// copyMountsReleased permits only the live source Pod during pre-copy. Target
// consumers and leftover copy helpers must be gone before another round/cutover.
func (e *Engine) copyMountsReleased(ctx context.Context, w *Workload) (bool, error) {
	var allowed types.UID
	if oneOf(w.Volume.Phase, phasePrecopying, phasePrecopyReady) {
		var err error
		allowed, err = e.liveSource(ctx, w)
		if err != nil {
			return false, err
		}
	}
	return e.copyConsumersReleased(ctx, w, allowed)
}

func (e *Engine) copyConsumersReleased(ctx context.Context, w *Workload, allowed types.UID) (bool, error) {
	users, err := e.claimUsers(ctx, w.Namespace, w.Volume.PVC.Name, w.Volume.TemporaryPVC)
	if err != nil {
		return false, err
	}
	for _, p := range users {
		if allowed == "" || p.UID != allowed {
			return false, nil
		}
		for _, vol := range p.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == w.Volume.TemporaryPVC {
				return false, errors.New("源业务 Pod 不得挂载预同步目标卷")
			}
		}
	}
	return true, nil
}

func (e *Engine) waitCopyReleased(ctx context.Context, w *Workload) error {
	return e.wait(ctx, "复制助手释放卷", func(ctx context.Context) (bool, error) {
		return e.copyMountsReleased(ctx, w)
	})
}
