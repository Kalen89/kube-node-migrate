package nodemigrate

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HealthCheck is an operator-selected HTTP endpoint, stable across Pod replacement.
type HealthCheck struct {
	Name               string `json:"name"`
	URL                string `json:"url"`
	ExpectedStatus     int    `json:"expectedStatus,omitempty"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify,omitempty"`
}

func validateHealthChecks(checks []HealthCheck) error {
	seen := map[string]bool{}
	for _, check := range checks {
		u, err := url.Parse(check.URL)
		if check.Name == "" || strings.ContainsAny(check.Name, "\r\n\t") || seen[check.Name] || err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("健康检查名称或 HTTP URL 无效；URL 不允许凭据、查询参数和 fragment")
		}
		if check.ExpectedStatus != 0 && (check.ExpectedStatus < 100 || check.ExpectedStatus > 599) {
			return fmt.Errorf("健康检查 %s 的 expectedStatus 无效", check.Name)
		}
		seen[check.Name] = true
	}
	return nil
}

func (e *Engine) checkHealth(ctx context.Context) error {
	if len(e.State.Options.HealthChecks) == 0 {
		e.logf("未配置额外 HTTP 健康检查；工作负载仍按 Pod Ready 验证")
		return nil
	}
	for _, check := range e.State.Options.HealthChecks {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: check.InsecureSkipVerify} // Explicit per-endpoint operator choice for private certificates.
		httpClient := &http.Client{Timeout: 10 * time.Second, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		expected := check.ExpectedStatus
		if expected == 0 {
			expected = http.StatusOK
		}
		err := e.wait(ctx, "健康检查 "+check.Name, func(ctx context.Context) (bool, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, check.URL, nil)
			if err != nil {
				return false, err
			}
			response, err := httpClient.Do(req)
			if err != nil {
				e.logf("健康检查等待 name=%s url=%s error=%v", check.Name, check.URL, err)
				return false, nil
			}
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			closeReadOnly(response.Body)
			e.logf("健康检查 name=%s url=%s status=%d expected=%d", check.Name, check.URL, response.StatusCode, expected)
			return response.StatusCode == expected, nil
		})
		transport.CloseIdleConnections()
		if err != nil {
			return err
		}
	}
	return nil
}
