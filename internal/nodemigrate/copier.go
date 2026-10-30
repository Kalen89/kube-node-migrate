package nodemigrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const copierVersion = "v3.7.0"

func rsyncExtraArgs(o Options) string {
	args := "--numeric-ids"
	if o.RsyncBandwidthLimit > 0 {
		args += " --bwlimit=" + strconv.FormatInt(o.RsyncBandwidthLimit, 10)
	}
	return args
}

type processCopier struct{ engine *Engine }

// UseDefaultCopier selects the fixed offline pv-migrate protocol implementation.
func (e *Engine) UseDefaultCopier() { e.Copier = &processCopier{engine: e} }

func (c *processCopier) command(ctx context.Context, state *State, args ...string) error {
	cmd := exec.CommandContext(ctx, state.Options.CopyBinary, args...)
	cmd.Env = append(os.Environ(), "HELM_DRIVER=secret")
	cmd.Stdout, cmd.Stderr = c.engine.Output, c.engine.Output
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("复制工具失败，保留操作 %s 的资源用于 resume: %w", state.ID, err)
	}
	return nil
}

func moverImage(reference string) (map[string]any, error) {
	tag, err := name.NewTag(reference, name.WeakValidation)
	if err != nil {
		return nil, err
	}
	if tag.TagStr() != copierVersion {
		return nil, fmt.Errorf("复制镜像必须匹配工具版本 %s", copierVersion)
	}
	return map[string]any{"repository": tag.Repository.Name(), "tag": copierVersion, "pullPolicy": "IfNotPresent"}, nil
}

func (c *processCopier) values(ctx context.Context, state *State, w *Workload) (string, error) {
	sshd, err := moverImage(state.Options.SSHDImage)
	if err != nil {
		return "", err
	}
	rsync, err := moverImage(state.Options.RsyncImage)
	if err != nil {
		return "", err
	}
	secrets, err := c.engine.pullSecrets(ctx, w.Namespace, &w.Template.Spec)
	if err != nil {
		return "", err
	}
	values := map[string]any{
		"sshd":  map[string]any{"image": sshd, "resources": serviceResources(state.Options.SSHDResources), "imagePullSecrets": secrets, "tolerations": helperTolerations(state.SourceTaints), "networkPolicy": map[string]any{"enabled": false}},
		"rsync": map[string]any{"image": rsync, "resources": serviceResources(state.Options.RsyncResources), "imagePullSecrets": secrets, "tolerations": helperTolerations(state.TargetTaints), "networkPolicy": map[string]any{"enabled": false}},
	}
	if state.Options.Mode == ModeWorkloads {
		sshdValues := values["sshd"].(map[string]any)
		tolerations := helperTolerations(state.SourceTaints)
		tolerations = append(tolerations, corev1.Toleration{Key: "node.kubernetes.io/unschedulable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule})
		sshdValues["tolerations"] = tolerations
	}
	data, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	path := filepath.Join(filepath.Dir(state.Options.StatePath), w.Volume.CopyID+".values.json")
	if err = os.WriteFile(path, data, privateFileMode); err != nil {
		return "", err
	}
	return path, nil
}

func (c *processCopier) findJob(ctx context.Context, w *Workload) (*batchv1.Job, error) {
	v := w.Volume
	release := "pv-migrate-" + v.CopyID + "-clusterip"
	var jobs batchv1.JobList
	if err := c.engine.Client.List(ctx, &jobs, client.InNamespace(w.Namespace), client.MatchingLabels{"app.kubernetes.io/instance": release}); err != nil {
		return nil, err
	}
	if len(jobs.Items) == 0 {
		return nil, nil
	}
	if len(jobs.Items) != 1 {
		return nil, errors.New("复制操作对应多个 Job，拒绝猜测")
	}
	job := &jobs.Items[0]
	if v.CopyJobUID != "" && (job.UID != v.CopyJobUID || job.Name != v.CopyJobName) {
		return nil, errors.New("复制 Job UID 改变")
	}
	v.CopyJobName, v.CopyJobUID = job.Name, job.UID
	return job, c.engine.checkpoint()
}

func (c *processCopier) Copy(ctx context.Context, state *State, w *Workload) error {
	e := c.engine
	limit := state.Options.CopyTimeout
	if limit <= 0 {
		limit = 12 * time.Hour
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	if w.Volume.CopyCleanup != nil {
		if err := c.cleanup(ctx, state, w); err != nil {
			return err
		}
		if err := e.waitCopyReleased(ctx, w); err != nil {
			return err
		}
		if w.Volume.CopySucceeded {
			return nil
		}
		w.Volume.CopyJobName, w.Volume.CopyJobUID = "", ""
		w.Volume.CopyRuntimeMillis = nil
		if err := e.checkpoint(); err != nil {
			return err
		}
	}
	job, err := c.findJob(ctx, w)
	if err != nil {
		return err
	}
	if job != nil && jobFailed(job) {
		if err = c.cleanup(ctx, state, w); err != nil {
			return err
		}
		if err = e.waitCopyReleased(ctx, w); err != nil {
			return err
		}
		w.Volume.CopyJobName, w.Volume.CopyJobUID = "", ""
		w.Volume.CopyRuntimeMillis = nil
		if err = e.checkpoint(); err != nil {
			return err
		}
		job = nil
	}
	if job == nil {
		if w.Volume.CopySucceeded {
			if err = c.cleanup(ctx, state, w); err != nil {
				return err
			}
			// A finished job may have been cleaned before the checkpoint; digest verification still follows.
			return e.waitCopyReleased(ctx, w)
		}
		if err = e.copyBindings(ctx, w); err != nil {
			return err
		}
		if released, err := e.copyMountsReleased(ctx, w); err != nil {
			return err
		} else if !released {
			return errors.New("存在未释放的复制助手或额外卷消费者，拒绝启动复制")
		}
		if err = c.requireAbsentRelease(ctx, w); err != nil {
			return err
		}
		w.Volume.CopyJobName, w.Volume.CopyJobUID = "", ""
		if err = e.checkpoint(); err != nil {
			return err
		}
		path, err := c.values(ctx, state, w)
		if err != nil {
			return err
		}
		if err = c.command(ctx, state, copyArgs(state, w, path)...); err != nil {
			return err
		}
		if err = e.wait(ctx, "复制 Job 创建", func(ctx context.Context) (bool, error) {
			var err error
			job, err = c.findJob(ctx, w)
			return job != nil, err
		}); err != nil {
			return err
		}
	}
	key := types.NamespacedName{Namespace: w.Namespace, Name: job.Name}
	uid := job.UID
	started, last := time.Now(), time.Time{}
	if err = wait.PollUntilContextCancel(ctx, pollInterval, true, func(ctx context.Context) (bool, error) {
		var current batchv1.Job
		if err := e.Client.Get(ctx, key, &current); err != nil {
			return false, err
		}
		if current.UID != uid {
			return false, errors.New("运行中的复制 Job 身份改变")
		}
		if time.Since(last) >= 15*time.Second {
			total := uint64(0)
			if w.Volume.SourceDigest != nil {
				total = w.Volume.SourceDigest.Bytes
			}
			e.logf("数据复制进行中 pvc=%s/%s job=%s active=%d succeeded=%d failed=%d totalBytes=%d elapsed=%s", w.Namespace, w.Volume.PVC.Name, current.Name, current.Status.Active, current.Status.Succeeded, current.Status.Failed, total, time.Since(started).Round(time.Second))
			last = time.Now()
		}
		for _, condition := range current.Status.Conditions {
			if condition.Status != corev1.ConditionTrue {
				continue
			}
			if condition.Type == batchv1.JobFailed {
				return false, fmt.Errorf("复制 Job 失败: %s；请检查 Job 后处理该 copyID", condition.Reason)
			}
			if condition.Type == batchv1.JobComplete {
				// 在清理 Pod 前保存 rsync 容器的运行时间；调度与清理开销
				// 不参与显式预同步阈值判断，缺失时间保持 unknown。
				var err error
				w.Volume.CopyRuntimeMillis, err = c.copyRuntime(ctx, &current)
				if err != nil {
					return false, err
				}
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		return err
	}
	w.Volume.CopySucceeded = true
	if err = e.checkpoint(); err != nil {
		return err
	}
	if err = c.cleanup(ctx, state, w); err != nil {
		return err
	}
	if err = e.wait(ctx, "复制 Job 清理", func(ctx context.Context) (bool, error) {
		var current batchv1.Job
		err := e.Client.Get(ctx, key, &current)
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}); err != nil {
		return err
	}
	return e.waitCopyReleased(ctx, w)
}

func copyArgs(state *State, w *Workload, valuesPath string) []string {
	extra := rsyncExtraArgs(state.Options)
	if (state.Options.Precopy || state.Options.Mode == ModeWorkloads) && w.Volume.Phase != phasePrecopying {
		// An online round can copy inconsistent bytes while a file changes.
		// The offline pass must compare content even if size/mtime match.
		extra += " --checksum"
	}
	args := []string{"--source-namespace", w.Namespace, "--source", w.Volume.PVC.Name,
		"--dest-namespace", w.Namespace, "--dest", w.Volume.TemporaryPVC, "--strategies", "clusterip",
		"--rsync-extra-args", extra, "--helm-timeout", "10m", "--helm-values", valuesPath,
		"--source-kubeconfig", state.Options.Kubeconfig, "--dest-kubeconfig", state.Options.Kubeconfig,
		"--id", w.Volume.CopyID, "--detach", "--no-cleanup", "--no-cleanup-on-failure"}
	if w.Volume.Phase == phasePrecopying {
		args = append(args, "--ignore-mounted")
	}
	if state.Options.Precopy || state.Options.Mode == ModeWorkloads {
		args = append(args, "--dest-delete-extraneous-files")
	}
	if state.Options.Context != "" {
		args = append(args, "--source-context", state.Options.Context, "--dest-context", state.Options.Context)
	}
	return args
}

func jobFailed(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (c *processCopier) cleanup(ctx context.Context, state *State, w *Workload) error {
	return c.cleanupRelease(ctx, state, w)
}

// CheckCopyTool checks the version and image contracts before any service is stopped.
func CheckCopyTool(ctx context.Context, options Options) error {
	if options.CopyBinary == "" {
		return errors.New("离线 pv-migrate 二进制尚未加载")
	}
	output, err := exec.CommandContext(ctx, options.CopyBinary, "--version").CombinedOutput()
	if err != nil {
		return fmt.Errorf("无法执行复制工具: %w", err)
	}
	fields := strings.Fields(strings.TrimSpace(string(output)))
	if len(fields) < 3 || fields[0] != "pv-migrate" || fields[1] != "version" || strings.TrimPrefix(fields[2], "v") != strings.TrimPrefix(copierVersion, "v") {
		return fmt.Errorf("复制工具必须是 %s", copierVersion)
	}
	if _, err = moverImage(options.SSHDImage); err != nil {
		return err
	}
	_, err = moverImage(options.RsyncImage)
	return err
}
