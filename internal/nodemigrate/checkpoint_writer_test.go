package nodemigrate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckpointWriterCoalescesObservationsAndPersistsTransactions(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "state.json")
	s := &State{ID: "operation", Phase: phasePlanned, Options: Options{Kubeconfig: "private-credential-path"}, Workloads: []*Workload{{Instance: &Instance{}, Volumes: []*Volume{{Phase: phasePrepared}}}}}
	save := NewCheckpointWriter(path)
	require.NoError(t, save(s))
	initial, err := os.Stat(path)
	require.NoError(t, err)
	privateBefore, err := os.ReadFile(path)
	require.NoError(t, err)

	s.ActiveStage = &StageEvent{Stage: "source-digest", Status: "running"}
	s.Events = append(s.Events, StageEvent{Stage: "final-sync", Status: "succeeded"})
	require.NoError(t, save(s))
	observed, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, os.SameFile(initial, observed), "pure observations must not replace the private checkpoint")
	privateAfter, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, privateBefore, privateAfter)
	report, err := os.ReadFile(path + ".report.json")
	require.NoError(t, err)
	require.Contains(t, string(report), "source-digest")
	require.Contains(t, string(report), "final-sync")
	require.NotContains(t, string(report), "private-credential-path")

	// A change through a nested pointer is still a transaction change.
	s.Workloads[0].Volumes[0].Phase = phaseCopying
	require.NoError(t, save(s))
	transaction, err := os.Stat(path)
	require.NoError(t, err)
	require.False(t, os.SameFile(observed, transaction))
	var persisted State
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &persisted))
	require.Equal(t, phaseCopying, persisted.Workloads[0].Volumes[0].Phase)
	require.NotNil(t, persisted.ActiveStage)

	// The standalone API always writes, even if only observations have changed.
	s.Events = append(s.Events, StageEvent{Stage: "target-digest", Status: "succeeded"})
	require.NoError(t, SaveState(path, s))
	alwaysWritten, err := os.Stat(path)
	require.NoError(t, err)
	require.False(t, os.SameFile(transaction, alwaysWritten))
}

func TestCheckpointWriterPrivateFailureDoesNotPublishOrAdvanceCache(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "state.json")
	s := &State{ID: "operation", Phase: phasePlanned}
	save := NewCheckpointWriter(path)
	require.NoError(t, save(s))
	before, err := os.ReadFile(path + ".report.json")
	require.NoError(t, err)
	// A directory prevents the atomic private-file replacement even as root.
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Mkdir(path, 0o700))
	s.Phase = phaseValidated
	s.ActiveStage = &StageEvent{Stage: "should-not-publish", Status: "running"}
	require.Error(t, save(s))
	after, err := os.ReadFile(path + ".report.json")
	require.NoError(t, err)
	require.Equal(t, before, after)

	require.NoError(t, os.Remove(path))
	require.NoError(t, save(s), "retry must persist the previously failed transaction")
	var persisted State
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &persisted))
	require.Equal(t, phaseValidated, persisted.Phase)
	report, err := os.ReadFile(path + ".report.json")
	require.NoError(t, err)
	require.Contains(t, string(report), "should-not-publish")
}

func TestCheckpointWriterReportFailureInvalidatesEarlierFingerprint(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "state.json")
	s := &State{ID: "operation", Phase: phasePlanned}
	save := NewCheckpointWriter(path)
	require.NoError(t, save(s))
	reportPath := path + ".report.json"
	require.NoError(t, os.Remove(reportPath))
	require.NoError(t, os.Mkdir(reportPath, 0o700))
	s.Phase = phaseValidated
	require.ErrorContains(t, save(s), "检查点已保存")
	before, err := os.Stat(path)
	require.NoError(t, err)
	// Private persistence succeeded but the report failed. Returning to the old
	// transaction must not reuse its stale cached fingerprint and leave new data.
	s.Phase = phasePlanned
	require.NoError(t, os.Remove(reportPath))
	require.NoError(t, save(s))
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.False(t, os.SameFile(before, after))
	var persisted State
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &persisted))
	require.Equal(t, phasePlanned, persisted.Phase)
}
