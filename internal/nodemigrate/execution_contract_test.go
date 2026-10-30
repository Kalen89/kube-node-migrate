package nodemigrate

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrivatePlanRejectsHistoricalExecutionContracts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options Options
	}{
		{name: "legacy-agent", options: Options{Mode: "agent"}},
		{name: "missing-workloads-mode", options: Options{}},
		{name: "node-lifecycle-policy", options: Options{Mode: ModeWorkloads, SourcePolicy: "cordon"}},
		{name: "node-label-writes", options: Options{Mode: ModeWorkloads, TargetLabels: map[string]string{"business": "true"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// No API client: rejection must happen before any cluster operation.
			e := &Engine{}
			_, err := e.plan(t.Context(), tc.options)
			require.ErrorContains(t, err, "只支持业务 Pod/PVC")
			require.Nil(t, e.State)
		})
	}
}

func TestValidateRejectsHistoricalCheckpointWithoutClusterAccess(t *testing.T) {
	e := &Engine{State: &State{Version: 5, Options: Options{Mode: "agent"}}}
	require.ErrorContains(t, e.Validate(t.Context()), "旧版节点迁移检查点仅支持 status")
}
