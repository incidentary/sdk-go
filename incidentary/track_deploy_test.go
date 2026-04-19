package incidentary

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// captureServer is a tiny httptest wrapper that records what the SDK posted.
type captureServer struct {
	server  *httptest.Server
	mu      sync.Mutex
	path    string
	method  string
	headers http.Header
	body    map[string]interface{}
	calls   int32
}

func newCaptureServer(t *testing.T, status int) *captureServer {
	t.Helper()
	cs := &captureServer{}
	cs.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&cs.calls, 1)
		raw, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		cs.mu.Lock()
		cs.path = r.URL.Path
		cs.method = r.Method
		cs.headers = r.Header.Clone()
		_ = json.Unmarshal(raw, &cs.body)
		cs.mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(func() { cs.server.Close() })
	return cs
}

func baseConfig(url string) TrackDeployConfig {
	return TrackDeployConfig{
		BaseURL: url,
		APIKey:  "ik_test_deadbeef",
	}
}

func TestTrackDeployPostsToDeploysEndpoint(t *testing.T) {
	cs := newCaptureServer(t, http.StatusAccepted)

	err := TrackDeploy(baseConfig(cs.server.URL), TrackDeployOptions{
		Service:         "payments-api",
		Version:         "1.2.3",
		CommitSHA:       "abc1234",
		CommitMessage:   "fix rounding",
		Branch:          "main",
		DeployedByName:  "Ada",
		DeployedByEmail: "ada@example.com",
		Environment:     "staging",
		DiffURL:         "https://github.com/org/repo/compare/abc1234",
		Metadata:        map[string]interface{}{"pipeline": "ci-123"},
	})
	if err != nil {
		t.Fatalf("TrackDeploy returned non-nil error: %v", err)
	}

	if cs.path != "/api/v1/deploys" {
		t.Errorf("expected path /api/v1/deploys, got %q", cs.path)
	}
	if cs.method != "POST" {
		t.Errorf("expected method POST, got %q", cs.method)
	}
	want := map[string]string{
		"service_name":      "payments-api",
		"version":           "1.2.3",
		"commit_sha":        "abc1234",
		"commit_message":    "fix rounding",
		"branch":            "main",
		"deployed_by_name":  "Ada",
		"deployed_by_email": "ada@example.com",
		"environment":       "staging",
		"deploy_source":     "sdk",
		"diff_url":          "https://github.com/org/repo/compare/abc1234",
	}
	for k, v := range want {
		got, ok := cs.body[k]
		if !ok || got != v {
			t.Errorf("body[%q] = %v, want %q", k, got, v)
		}
	}
}

func TestTrackDeployAttachesAuthorizationAndContentType(t *testing.T) {
	cs := newCaptureServer(t, http.StatusAccepted)

	cfg := baseConfig(cs.server.URL)
	cfg.APIKey = "ik_live_xyz"
	_ = TrackDeploy(cfg, TrackDeployOptions{Service: "payments-api"})

	if got := cs.headers.Get("Authorization"); got != "Bearer ik_live_xyz" {
		t.Errorf("Authorization header = %q, want %q", got, "Bearer ik_live_xyz")
	}
	if got := cs.headers.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type header = %q, want %q", got, "application/json")
	}
}

func TestTrackDeployEnvironmentDefaultsToProduction(t *testing.T) {
	cs := newCaptureServer(t, http.StatusAccepted)
	_ = TrackDeploy(baseConfig(cs.server.URL), TrackDeployOptions{Service: "payments-api"})
	if got := cs.body["environment"]; got != "production" {
		t.Errorf("environment = %v, want production", got)
	}
}

func TestTrackDeployAlwaysSetsDeploySourceToSdk(t *testing.T) {
	cs := newCaptureServer(t, http.StatusAccepted)
	_ = TrackDeploy(baseConfig(cs.server.URL), TrackDeployOptions{Service: "payments-api"})
	if got := cs.body["deploy_source"]; got != "sdk" {
		t.Errorf("deploy_source = %v, want sdk", got)
	}
}

func TestTrackDeployDeployedAtDefaultsToNowRFC3339(t *testing.T) {
	cs := newCaptureServer(t, http.StatusAccepted)
	before := time.Now().UTC().Add(-time.Second)
	_ = TrackDeploy(baseConfig(cs.server.URL), TrackDeployOptions{Service: "payments-api"})
	after := time.Now().UTC().Add(time.Second)

	raw, _ := cs.body["deployed_at"].(string)
	if raw == "" {
		t.Fatalf("deployed_at missing or empty")
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatalf("deployed_at %q not RFC3339: %v", raw, err)
	}
	if parsed.Before(before) || parsed.After(after) {
		t.Errorf("deployed_at %v outside [%v, %v]", parsed, before, after)
	}
}

func TestTrackDeployDeployedAtPassesThroughCustomValue(t *testing.T) {
	cs := newCaptureServer(t, http.StatusAccepted)
	fixed := time.Date(2026, 4, 1, 9, 30, 0, 0, time.UTC)
	_ = TrackDeploy(baseConfig(cs.server.URL), TrackDeployOptions{
		Service:    "payments-api",
		DeployedAt: fixed,
	})
	raw, _ := cs.body["deployed_at"].(string)
	if !strings.HasPrefix(raw, "2026-04-01T09:30:00") {
		t.Errorf("deployed_at = %q, want prefix 2026-04-01T09:30:00", raw)
	}
}

func TestTrackDeployOmitsUnsetOptionalFields(t *testing.T) {
	cs := newCaptureServer(t, http.StatusAccepted)
	_ = TrackDeploy(baseConfig(cs.server.URL), TrackDeployOptions{Service: "payments-api"})

	forbidden := []string{
		"version", "commit_sha", "commit_message", "branch",
		"deployed_by_name", "deployed_by_email", "diff_url",
	}
	for _, k := range forbidden {
		if _, ok := cs.body[k]; ok {
			t.Errorf("unexpected body key %q", k)
		}
	}
}

func TestTrackDeployTrimsTrailingSlashFromBaseURL(t *testing.T) {
	cs := newCaptureServer(t, http.StatusAccepted)

	cfg := baseConfig(cs.server.URL + "/")
	_ = TrackDeploy(cfg, TrackDeployOptions{Service: "payments-api"})

	if cs.path != "/api/v1/deploys" {
		t.Errorf("path = %q, want /api/v1/deploys (no double slash)", cs.path)
	}
}

func TestTrackDeployNetworkErrorDoesNotReturnError(t *testing.T) {
	// Point at an invalid URL — http.Client should error, TrackDeploy must
	// swallow it and return nil.
	err := TrackDeploy(
		TrackDeployConfig{
			BaseURL: "http://127.0.0.1:1/",
			APIKey:  "ik",
			Timeout: 100 * time.Millisecond,
		},
		TrackDeployOptions{Service: "payments-api"},
	)
	if err != nil {
		t.Errorf("TrackDeploy must be fail-open on network error, got: %v", err)
	}
}

func TestTrackDeployHTTPErrorStatusDoesNotReturnError(t *testing.T) {
	cs := newCaptureServer(t, http.StatusServiceUnavailable)
	err := TrackDeploy(baseConfig(cs.server.URL), TrackDeployOptions{Service: "payments-api"})
	if err != nil {
		t.Errorf("TrackDeploy must be fail-open on HTTP 503, got: %v", err)
	}
	if atomic.LoadInt32(&cs.calls) != 1 {
		t.Errorf("expected 1 request, got %d", cs.calls)
	}
}

func TestTrackDeployEmptyServiceReturnsEarly(t *testing.T) {
	cs := newCaptureServer(t, http.StatusAccepted)
	err := TrackDeploy(baseConfig(cs.server.URL), TrackDeployOptions{Service: ""})
	if err != nil {
		t.Errorf("TrackDeploy must not error on empty service, got: %v", err)
	}
	if atomic.LoadInt32(&cs.calls) != 0 {
		t.Errorf("expected no request when service is empty, got %d", cs.calls)
	}
}

func TestTrackDeployHonorsCustomHTTPClient(t *testing.T) {
	cs := newCaptureServer(t, http.StatusAccepted)
	var custom int32
	client := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			atomic.AddInt32(&custom, 1)
			return http.DefaultTransport.RoundTrip(r)
		}),
	}
	cfg := baseConfig(cs.server.URL)
	cfg.HTTPClient = client

	_ = TrackDeploy(cfg, TrackDeployOptions{Service: "payments-api"})
	if atomic.LoadInt32(&custom) != 1 {
		t.Errorf("expected custom HTTP client to be used exactly once, got %d", custom)
	}
}

// roundTripFunc is declared in transport_wrap_test.go — reuse it here.
