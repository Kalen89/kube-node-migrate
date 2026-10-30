package nodemigrate

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// StageEvent contains resource identities and timings, never controller snapshots.
type StageEvent struct {
	Stage         string    `json:"stage"`
	Resource      string    `json:"resource,omitempty"`
	StartedAt     time.Time `json:"startedAt"`
	FinishedAt    time.Time `json:"finishedAt"`
	Status        string    `json:"status"`
	ElapsedMillis int64     `json:"elapsedMillis"`
}

func (e *Engine) stage(ctx context.Context, stage, resource string, run func(context.Context) error) error {
	started := time.Now().UTC()
	previous := e.State.ActiveStage
	e.State.ActiveStage = &StageEvent{Stage: stage, Resource: resource, StartedAt: started, Status: "running"}
	e.logf("阶段开始 stage=%s resource=%s action=%s", stage, resource, stageTitle(stage))
	err := e.checkpoint()
	if err == nil {
		err = run(ctx)
	} else {
		e.logf("保存阶段开始检查点失败 stage=%s resource=%s；不执行该阶段: %v", stage, resource, err)
	}
	event := StageEvent{Stage: stage, Resource: resource, StartedAt: started, FinishedAt: time.Now().UTC(), Status: "succeeded"}
	event.ElapsedMillis = event.FinishedAt.Sub(started).Milliseconds()
	if err != nil {
		event.Status = "failed"
	}
	e.State.ActiveStage = previous
	e.State.Events = append(e.State.Events, event)
	saved := e.checkpoint()
	if saved != nil {
		e.State.Events[len(e.State.Events)-1].Status = "failed"
		e.logf("保存阶段检查点失败 stage=%s resource=%s；不进入下一阶段: %v", stage, resource, saved)
	}
	err = errors.Join(err, saved)
	if err != nil {
		e.logf("阶段失败 stage=%s resource=%s elapsed=%s error=%v；修复后使用 resume，状态=%s", stage, resource, time.Since(started).Round(time.Millisecond), err, e.State.Phase)
	} else {
		e.logf("阶段完成 stage=%s resource=%s elapsed=%s", stage, resource, time.Since(started).Round(time.Millisecond))
	}
	return err
}

func stageTitle(stage string) string {
	titles := map[string]string{
		"target-labels":          "准备目标标签",
		"target-daemonsets":      "等待目标系统 DaemonSet 就绪",
		"capacity-preflight":     "检查目标资源容量",
		"health-before":          "迁移前健康检查",
		"image-prewarm":          "预热源目标所需镜像",
		"prepare-volume":         "准备目标数据卷",
		"disk-preflight":         "检查磁盘空间与 inode",
		"online-precopy":         "在线预同步，源服务保持运行",
		"stop-writes":            "停止服务写入",
		"copy-and-verify":        "复制并校验数据",
		"switch-pvc":             "切换 PVC 绑定",
		"start-workload":         "启动目标服务",
		"source-digest":          "扫描停止写入后的源卷摘要",
		"final-sync":             "执行停写后的最终同步并释放复制助手",
		"target-digest":          "扫描目标卷摘要并比较源目标内容",
		"parallel-digests":       "并发读取源目标完整摘要并比较内容",
		"statefulset-reconcile":  "解除保护后触发 StatefulSet 协调",
		"wait-pod-created":       "等待目标 Pod 创建",
		"wait-pod-scheduled":     "等待目标 Pod 调度",
		"wait-container-started": "等待目标容器启动",
		"wait-pod-ready":         "等待目标 Pod Ready",
		"move-workload":          "迁移工作负载",
		"validate-workloads":     "验证全部工作负载",
		"health-after":           "迁移后健康检查",
	}
	if title := titles[stage]; title != "" {
		return title
	}
	return stage
}

// Summary is safe to share: it intentionally excludes Options and pod templates.
func Summary(s *State) any {
	type volume struct {
		PVC, Phase     string
		Source, Target *Digest
		Match          bool
		Precopy        *PrecopyProgress `json:"precopy,omitempty"`
	}
	type workload struct {
		Kind, Namespace, Name, Phase  string
		Instance                      string `json:"instance,omitempty"`
		EvictedAt, StoppedAt, ReadyAt time.Time
		PVC, VolumePhase              string
		Volume                        *volume   `json:"volume,omitempty"`
		Volumes                       []*volume `json:"volumes,omitempty"`
	}
	items := make([]workload, 0, len(s.Workloads))
	for _, w := range s.Workloads {
		if w == nil {
			continue
		}
		item := workload{Kind: w.Kind, Namespace: w.Namespace, Name: w.Name, Phase: w.Phase}
		if v := w.Volume; v != nil {
			item.PVC, item.VolumePhase = v.PVC.Name, v.Phase
			item.Volume = &volume{PVC: v.PVC.Name, Phase: v.Phase, Source: v.SourceDigest, Target: v.TargetDigest, Match: v.SourceDigest != nil && v.TargetDigest != nil && *v.SourceDigest == *v.TargetDigest, Precopy: v.Precopy}
		}
		if i := w.Instance; i != nil {
			item.Instance, item.EvictedAt, item.StoppedAt, item.ReadyAt = i.Pod.Name, i.EvictedAt, i.StoppedAt, i.ReadyAt
			for _, v := range w.Volumes {
				if v == nil {
					continue
				}
				item.Volumes = append(item.Volumes, &volume{PVC: v.PVC.Name, Phase: v.Phase, Source: v.SourceDigest, Target: v.TargetDigest, Match: v.SourceDigest != nil && v.TargetDigest != nil && *v.SourceDigest == *v.TargetDigest, Precopy: v.Precopy})
			}
		}
		items = append(items, item)
	}
	return struct {
		ID, Source, Target, Phase string
		UpdatedAt                 time.Time
		Capacity                  *CapacityReport
		Workloads                 []workload
		Events                    []StageEvent
		ActiveStage               *StageEvent
		PVConcurrency             int
		RsyncBandwidthLimit       int64
		Precopy                   bool
		PrecopyRounds             int
		PrecopyThreshold          time.Duration
		ParallelDigest            bool
		Namespaces                []string
		SourceRestored            bool
	}{s.ID, s.Source.Name, s.Target.Name, s.Phase, s.UpdatedAt, s.Capacity, items, s.Events, s.ActiveStage, s.Options.PVConcurrency, s.Options.RsyncBandwidthLimit, s.Options.Precopy, s.Options.PrecopyRounds, s.Options.PrecopyThreshold, s.Options.ParallelDigest, s.Options.Namespaces, s.SourceRestored}
}

// SaveReport atomically replaces a redacted, rebuildable observation report.
// It does not fsync: only the private checkpoint authorizes resumed execution.
func SaveReport(path string, s *State) error {
	if s == nil {
		return fmt.Errorf("迁移状态未初始化")
	}
	return writeAtomicJSON(path+".report.json", Summary(s), false)
}
