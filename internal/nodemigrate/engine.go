package nodemigrate

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func (e *Engine) logf(format string, args ...any) {
	if e.Output != nil {
		if _, err := fmt.Fprintf(e.Output, "[%s] [node-migration %s] "+format+"\n", append([]any{time.Now().UTC().Format(time.RFC3339), e.State.ID}, args...)...); err != nil {
			logger.Debugf("write migration progress: %v", err)
		}
	}
}

// Apply moves all supported workloads; retiring the source remains a separate action.
func (e *Engine) Apply(ctx context.Context, allowDowntime bool) error {
	if !allowDowntime {
		return errors.New("该操作会停止并重建所选业务实例，请检查 plan 并指定 --allow-downtime")
	}
	if e.State == nil {
		return errors.New("未加载迁移状态")
	}
	if err := CheckExecutionState(e.State); err != nil {
		return err
	}
	// ActiveStage is observation only; a crashed attempt is not a parent stage.
	e.State.ActiveStage = nil
	return e.withLease(ctx, e.applyInstances)
}

func (e *Engine) cordonSource(ctx context.Context) error {
	var node corev1.Node
	if err := e.Client.Get(ctx, types.NamespacedName{Name: e.State.Source.Name}, &node); err != nil {
		return err
	}
	if node.UID != e.State.Source.UID {
		return errors.New("源 Node UID 改变")
	}
	if node.Spec.Unschedulable {
		return nil
	}
	node.Spec.Unschedulable = true
	return e.Client.Update(ctx, &node)
}

// Validate checks the selected workload instances using the current contract.
func (e *Engine) Validate(ctx context.Context) error {
	if err := CheckExecutionState(e.State); err != nil {
		return err
	}
	return e.validateInstances(ctx)
}
