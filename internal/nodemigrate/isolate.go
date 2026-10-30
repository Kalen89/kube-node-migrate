package nodemigrate

import (
	"context"
	"errors"
)

// runtimeCLI is read-only here: only the selected original Pod UID is checked.
const runtimeCLI = "k3s crictl --runtime-endpoint unix:///run/k3s/containerd/containerd.sock --timeout 30s"

// CheckExecutionState never converts an old node-replacement transaction into
// a different lifecycle contract. Historical checkpoints remain readable by status.
func CheckExecutionState(s *State) error {
	if s == nil {
		return errors.New("未加载迁移状态")
	}
	if s.Version != stateVersion || s.Options.Mode != ModeWorkloads || s.Options.SourcePolicy != "" || len(s.Options.TargetLabels) != 0 {
		return errors.New("旧版节点迁移检查点仅支持 status 查看；请使用对应旧版本处理原任务，新工具只迁移业务 Pod/PVC")
	}
	return nil
}

// Verify checks the selected target instances; it does not require the old
// machine to stay online after a completed migration or check its node role.
func (e *Engine) Verify(ctx context.Context) error {
	if err := CheckExecutionState(e.State); err != nil {
		return err
	}
	if err := e.validateInstances(ctx); err != nil {
		return err
	}
	if e.State.Phase != phaseValidated {
		return errors.New("迁移未完成，请使用 resume")
	}
	if err := e.verifyInstanceCompletion(ctx); err != nil {
		return err
	}
	return e.checkHealth(ctx)
}
