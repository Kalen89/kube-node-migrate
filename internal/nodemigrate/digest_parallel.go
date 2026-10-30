package nodemigrate

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// verifyParallelDigests overlaps two read-only scans only after final rsync and
// helper exit. Workers never mutate checkpoints or write shared progress output.
// Default scans stay serial: both nodes may share the same physical disks.
func (e *Engine) verifyParallelDigests(ctx context.Context, w *Workload) error {
	v := w.Volume
	source, err := volumePath(&v.PV)
	if err != nil {
		return err
	}
	target, err := volumePath(v.TargetPV)
	if err != nil {
		return err
	}
	if err := e.noMounts(ctx, w); err != nil {
		return err
	}
	return e.stage(ctx, "parallel-digests", workloadKey(w)+"/"+v.PVC.Name, func(ctx context.Context) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		type result struct {
			index  int
			digest *Digest
			event  StageEvent
			err    error
		}
		results := make(chan result, 2)
		for i, scan := range []struct {
			host        Host
			path, stage string
		}{
			{e.State.SourceConfig, source, "source-digest"},
			{e.State.TargetConfig, target, "target-digest"},
		} {
			go func() {
				started := time.Now().UTC()
				summary, err := e.filesystem(ctx, scan.host, scan.path, "digest")
				event := StageEvent{Stage: scan.stage, Resource: workloadKey(w) + "/" + v.PVC.Name, StartedAt: started, FinishedAt: time.Now().UTC(), Status: "succeeded"}
				event.ElapsedMillis = event.FinishedAt.Sub(started).Milliseconds()
				var digest *Digest
				if err != nil {
					event.Status = "failed"
					err = fmt.Errorf("%s: %w", scan.stage, err)
					cancel()
				} else {
					digest = &summary.Digest
				}
				results <- result{index: i, digest: digest, event: event, err: err}
			}()
		}
		var scans [2]result
		// Join both workers even on cancellation; no scan can outlive the stage.
		for range 2 {
			result := <-results
			scans[result.index] = result
		}
		for _, scan := range scans {
			e.State.Events = append(e.State.Events, scan.event)
		}
		if err := errors.Join(scans[0].err, scans[1].err); err != nil {
			return err
		}
		v.SourceDigest, v.TargetDigest = scans[0].digest, scans[1].digest
		if *v.SourceDigest != *v.TargetDigest {
			v.FinalSyncRetry = true
			return errors.New("源/目标文件摘要不一致，禁止切换")
		}
		e.logf("并行卷摘要一致 pvc=%s/%s bytes=%d entries=%d sha256=%s", w.Namespace, v.PVC.Name, v.SourceDigest.Bytes, v.SourceDigest.Entries, v.SourceDigest.SHA256)
		return nil
	})
}
