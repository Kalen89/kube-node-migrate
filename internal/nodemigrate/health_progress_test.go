package nodemigrate

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
)

func TestHealthBeforeAndAfterUseConfiguredStatusAndDoNotFollowRedirects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ok" {
			w.WriteHeader(200)
		} else {
			http.Redirect(w, r, "/ok", http.StatusFound)
		}
	}))
	defer server.Close()
	e, _, _ := fixture(t)
	e.State.Options.HealthChecks = []HealthCheck{{Name: "service", URL: server.URL + "/ok"}}
	require.NoError(t, e.checkHealth(t.Context()))
	e.State.Options.HealthChecks[0].URL = server.URL + "/redirect"
	require.ErrorContains(t, e.checkHealth(t.Context()), "deadline exceeded")
}

func TestAutomatedMigrationRecordsStagesAndTargetLabel(t *testing.T) {
	e, w, _ := fixture(t)
	simulateControllers(e, w)
	e.State.Options.TargetLabels = map[string]string{MigrationLabel: "true"}
	var log bytes.Buffer
	e.Output = &log
	require.NoError(t, applyLegacyComponents(e, t.Context(), true))
	require.Equal(t, phaseValidated, e.State.Phase)
	require.Contains(t, log.String(), "停止写入")
	require.Contains(t, log.String(), "卷摘要一致")
	require.NotEmpty(t, e.State.Events)
	require.NotNil(t, e.State.Capacity)
	require.Equal(t, w.Volume.SourceDigest, w.Volume.TargetDigest)
}

func TestMissingTargetDaemonSetBlocksBeforeStoppingWorkload(t *testing.T) {
	e, w, _ := fixture(t)
	e.State.DaemonSets = []WorkloadIdentity{{Namespace: w.Namespace, Name: "agent", UID: "ds-uid"}}
	require.ErrorContains(t, applyLegacyComponents(e, t.Context(), true), "目标系统 DaemonSet")
	require.Equal(t, phasePrepared, w.Volume.Phase)
}

func TestHealthConfigRejectsURLsThatCouldExposeCredentials(t *testing.T) {
	for _, url := range []string{"file:///etc/passwd", "https://user:password@example.com/", "https://example.com/?token=secret", "https://example.com/#private"} {
		require.Error(t, validateHealthChecks([]HealthCheck{{Name: "health", URL: url}}))
	}
}

func TestStageLogsAndReportContainProgressWithoutSecrets(t *testing.T) {
	e, w, _ := fixture(t)
	w.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "PASSWORD", Value: "do-not-report"}}
	var output bytes.Buffer
	e.Output = &output
	require.NoError(t, e.stage(t.Context(), "copy-and-verify", "app/db", func(context.Context) error { return nil }))
	require.ErrorContains(t, e.stage(t.Context(), "start-workload", "app/db", func(context.Context) error { return errors.New("not ready") }), "not ready")
	require.Contains(t, output.String(), "阶段开始 stage=copy-and-verify resource=app/db")
	require.Contains(t, output.String(), "阶段完成 stage=copy-and-verify")
	require.Contains(t, output.String(), "阶段失败 stage=start-workload")
	require.Contains(t, output.String(), "resume")
	path := filepath.Join(t.TempDir(), "state.json")
	require.NoError(t, SaveReport(path, e.State))
	data, err := os.ReadFile(path + ".report.json")
	require.NoError(t, err)
	require.NotContains(t, string(data), "PASSWORD")
	require.NotContains(t, string(data), "do-not-report")
	require.Contains(t, string(data), "copy-and-verify")
	require.Contains(t, string(data), "failed")
	info, err := os.Stat(path + ".report.json")
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.Len(t, e.State.Events, 2)
}

func TestAuditLogIsPrivateAppendOnlyAndRejectsAliases(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	log, err := OpenLog(path)
	require.NoError(t, err)
	_, err = log.WriteString("first\n")
	require.NoError(t, err)
	require.NoError(t, log.Close())
	log, err = OpenLog(path)
	require.NoError(t, err)
	_, err = log.WriteString("second\n")
	require.NoError(t, err)
	require.NoError(t, log.Close())
	data, err := os.ReadFile(path + ".log")
	require.NoError(t, err)
	require.Equal(t, "first\nsecond\n", string(data))
	require.NoError(t, os.Symlink(path+".log", filepath.Join(dir, "alias.log")))
	_, err = OpenLog(filepath.Join(dir, "alias"))
	require.Error(t, err)
	require.NoError(t, os.Link(path+".log", filepath.Join(dir, "hard.log")))
	_, err = OpenLog(filepath.Join(dir, "hard"))
	require.Error(t, err)
	require.NoError(t, unix.Mkfifo(filepath.Join(dir, "fifo.log"), 0o600))
	_, err = OpenLog(filepath.Join(dir, "fifo"))
	require.Error(t, err)
}

func TestUnhealthyBaselineBlocksBeforeStoppingService(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer server.Close()
	e, w, _ := fixture(t)
	e.State.Options.HealthChecks = []HealthCheck{{Name: "login", URL: server.URL}}
	require.ErrorContains(t, applyLegacyComponents(e, t.Context(), true), "健康检查")
	require.Equal(t, phasePrepared, w.Volume.Phase)
}

func TestCheckpointFailureIsLoggedAsFailedStage(t *testing.T) {
	e, _, _ := fixture(t)
	var output bytes.Buffer
	e.Output = &output
	e.Save = func(*State) error { return errors.New("state disk full") }
	require.ErrorContains(t, e.stage(t.Context(), "prepare-volume", "app/db", func(context.Context) error { return nil }), "state disk full")
	require.Equal(t, "failed", e.State.Events[0].Status)
	require.Contains(t, output.String(), "保存阶段检查点失败")
	require.NotContains(t, output.String(), "阶段完成")
}
