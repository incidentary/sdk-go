package incidentary_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/incidentary/sdk-go/incidentary"
)

func makeClient(overrides func(*incidentary.Config)) *incidentary.Client {
	cfg := incidentary.DefaultConfig("key", "svc")
	cfg.PreArmThresholdHigh = 10
	cfg.PreArmThresholdLow = 2
	cfg.PreArmMinDurationMs = 0
	cfg.PreArmTTLMs = 200
	cfg.PreArmCooldownMs = 0
	cfg.PreArmEnableSlowSuccess = false
	cfg.PreArmEnableInFlight = false
	cfg.PreArmEnableRetry = false
	if overrides != nil {
		overrides(&cfg)
	}
	return incidentary.New(cfg)
}

func TestClientInitialModeNormal(t *testing.T) {
	client := makeClient(nil)
	if client.GetMode() != incidentary.ModeNormal {
		t.Fatalf("expected NORMAL mode, got %s", client.GetMode())
	}
}

func TestClientPreArmTransitionWithReasonMetadata(t *testing.T) {
	client := makeClient(nil)
	for i := 0; i < 8; i++ {
		client.RecordRequest(200)
	}
	for i := 0; i < 2; i++ {
		client.RecordRequest(500)
	}

	if client.GetMode() != incidentary.ModePreArmed {
		t.Fatalf("expected PRE_ARMED mode, got %s", client.GetMode())
	}

	debug := client.GetPreArmDebugState()
	if debug.ActivePreArmWindow == nil {
		t.Fatalf("expected active prearm window")
	}
	if len(debug.ActivePreArmWindow.Reasons) == 0 {
		t.Fatalf("expected prearm reasons")
	}
	if debug.ActivePreArmWindow.Reasons[0].TriggerType != incidentary.TriggerErrorRate5xx {
		t.Fatalf("expected first reason to be error_rate_5xx")
	}
}

func TestIncidentTransitionsPreserveBindMetadata(t *testing.T) {
	client := makeClient(nil)
	for i := 0; i < 8; i++ {
		client.RecordRequest(200)
	}
	for i := 0; i < 2; i++ {
		client.RecordRequest(500)
	}

	client.EscalateToIncidentWithID("inc_123")
	if client.GetMode() != incidentary.ModeIncident {
		t.Fatalf("expected INCIDENT mode, got %s", client.GetMode())
	}

	debug := client.GetPreArmDebugState()
	if debug.ActivePreArmWindow == nil || debug.ActivePreArmWindow.BoundIncidentID != "inc_123" {
		t.Fatalf("expected bound incident id")
	}
	if debug.Counters["prearm_bind_total"] != 1 {
		t.Fatalf("expected prearm_bind_total=1")
	}

	client.CloseIncident()
	if client.GetMode() != incidentary.ModeNormal {
		t.Fatalf("expected NORMAL mode after close")
	}
}

func TestPreArmExpiresSilentlyWithoutBind(t *testing.T) {
	client := makeClient(func(cfg *incidentary.Config) {
		cfg.PreArmTTLMs = 40
		cfg.PreArmMinDurationMs = 0
	})
	for i := 0; i < 8; i++ {
		client.RecordRequest(200)
	}
	for i := 0; i < 2; i++ {
		client.RecordRequest(500)
	}
	if client.GetMode() != incidentary.ModePreArmed {
		t.Fatalf("expected PRE_ARMED")
	}

	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) && client.GetMode() != incidentary.ModeNormal {
		time.Sleep(10 * time.Millisecond)
	}
	if client.GetMode() != incidentary.ModeNormal {
		t.Fatalf("expected mode to return to NORMAL after ttl")
	}

	debug := client.GetPreArmDebugState()
	if debug.Counters["prearm_expire_total"] < 1 {
		t.Fatalf("expected expire counter increment")
	}
	if len(debug.RecentPreArmWindows) < 1 {
		t.Fatalf("expected at least one recent prearm window")
	}
}

func TestDetailCaptureChangesBetweenNormalAndPreArmed(t *testing.T) {
	client := makeClient(func(cfg *incidentary.Config) {
		cfg.PreArmDetailCapturePayloadEnabled = true
		cfg.PreArmDetailMaxPayloadBytes = 64
	})

	base := &incidentary.SkeletonCe{CeID: "ce-1", TraceID: "trace-1", ServiceID: "svc", Kind: incidentary.KindHTTPIn}
	normal := client.AttachDetailToEvent(base, &incidentary.CeDetail{Method: "GET", RouteKey: "/orders/:id"})
	if normal.Detail != nil {
		t.Fatalf("did not expect detail in NORMAL mode")
	}
	client.EscalateToIncident()

	withDetail := client.AttachDetailToEvent(base, &incidentary.CeDetail{
		Method:         "POST",
		RouteKey:       "/charges/:id/capture",
		PayloadSnippet: `{"password":"secret","token":"abc"}`,
	})
	if withDetail.Detail == nil {
		t.Fatalf("expected detail in PRE_ARMED")
	}
	if !bytes.Contains([]byte(withDetail.Detail.PayloadSnippet), []byte("redacted")) {
		t.Fatalf("expected redacted payload snippet, got %q", withDetail.Detail.PayloadSnippet)
	}
	if bytes.Contains([]byte(withDetail.Detail.PayloadSnippet), []byte("secret")) {
		t.Fatalf("expected payload to hide raw secret, got %q", withDetail.Detail.PayloadSnippet)
	}
}

func TestMiddlewareCapturesInboundRequest(t *testing.T) {
	client := makeClient(nil)

	handler := incidentary.Middleware(client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "2")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	if res.Code != http.StatusTeapot {
		t.Fatalf("expected status %d, got %d", http.StatusTeapot, res.Code)
	}
}

func TestInstrumentedDoRecordsRetryKeyQuality(t *testing.T) {
	client := makeClient(func(cfg *incidentary.Config) {
		cfg.PreArmEnableRetry = true
	})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}))
	defer ts.Close()

	retryAttempt := 2
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/charges/123/capture?expand=true", bytes.NewReader([]byte("{}")))
	resp, err := incidentary.InstrumentedDo(client, ts.Client(), &incidentary.TraceContext{
		TraceID: "trace-1",
		CeID:    "ce-parent",
	}, req, &incidentary.OutboundRetryMetadata{
		RetryAttempt:      &retryAttempt,
		RouteTemplate:     "/charges/:id/capture",
		DownstreamService: "billing",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 status")
	}

	debug := client.GetPreArmDebugState()
	if debug.RetryKeyQualityTotal["route_template"] < 1 {
		t.Fatalf("expected route_template quality usage")
	}
}

func TestTransportPausesAfterFreeCELimit(t *testing.T) {
	var calls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":"ce_limit_reached","limit_type":"ce","plan":"free","limit":200000}`)
	}))
	defer ts.Close()

	transport := incidentary.NewTransport(ts.URL, "key", "svc", "test", 200, nil)
	event := &incidentary.SkeletonCe{
		CeID:       "ce-1",
		TraceID:    "trace-1",
		ServiceID:  "svc",
		WallTsNs:   time.Now().UnixNano(),
		Kind:       incidentary.KindHTTPIn,
		StatusCode: 200,
		DurationNs: 1000,
	}

	transport.UploadBatchWithMode([]*incidentary.SkeletonCe{event}, incidentary.ModeNormal, "")
	time.Sleep(50 * time.Millisecond)
	if transport.IsHealthy() {
		t.Fatalf("expected transport to pause after free-plan CE limit")
	}

	transport.UploadBatchWithMode([]*incidentary.SkeletonCe{event}, incidentary.ModeNormal, "")
	time.Sleep(50 * time.Millisecond)
	if calls != 1 {
		t.Fatalf("expected paused transport to suppress further uploads, got %d requests", calls)
	}
}

func TestRecordEventWrappersEmitQueueJobWebhookVocabulary(t *testing.T) {
	type eventRow struct {
		EventType  string                 `json:"event_type"`
		Kind       string                 `json:"kind"`
		StatusCode int                    `json:"status_code"`
		ParentID   *string                `json:"parent_id"`
		Attributes map[string]interface{} `json:"attributes"`
	}
	type ingestBatch struct {
		Events []eventRow `json:"events"`
	}
	type ingestResult struct {
		batch ingestBatch
		err   error
	}

	batches := make(chan ingestResult, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			batches <- ingestResult{err: err}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var parsed ingestBatch
		if err := json.Unmarshal(body, &parsed); err != nil {
			batches <- ingestResult{err: err}
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		batches <- ingestResult{batch: parsed}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := makeClient(func(cfg *incidentary.Config) {
		cfg.APIURL = server.URL
	})

	baseNs := time.Now().UnixNano()
	status202 := 202
	client.RecordQueuePublish(incidentary.RecordEventOptions{TraceID: "trace-evt", WallTsNs: baseNs})
	client.RecordQueueConsume(incidentary.RecordEventOptions{TraceID: "trace-evt", WallTsNs: baseNs + 1})
	client.RecordJobStart(incidentary.RecordEventOptions{
		TraceID:    "trace-evt",
		ParentCeID: "ce-parent",
		WallTsNs:   baseNs + 2,
	})
	client.RecordJobEnd(incidentary.RecordEventOptions{TraceID: "trace-evt", WallTsNs: baseNs + 3})
	client.RecordWebhookIn(incidentary.RecordEventOptions{TraceID: "trace-evt", WallTsNs: baseNs + 4})
	client.RecordWebhookOut(incidentary.RecordEventOptions{
		TraceID:    "trace-evt",
		WallTsNs:   baseNs + 5,
		Status:     &status202,
		EventAttrs: map[string]interface{}{"endpoint": "payments"},
	})

	client.FlushToBackend(nil)

	select {
	case result := <-batches:
		if result.err != nil {
			t.Fatalf("failed to capture ingest batch: %v", result.err)
		}
		batch := result.batch
		if len(batch.Events) != 6 {
			t.Fatalf("expected 6 events, got %d", len(batch.Events))
		}

		byType := make(map[string]eventRow, len(batch.Events))
		for _, event := range batch.Events {
			byType[event.EventType] = event
		}

		assertEvent := func(eventType, expectedKind string, expectedStatus int) {
			ev, ok := byType[eventType]
			if !ok {
				t.Fatalf("missing event_type %s", eventType)
			}
			if ev.Kind != expectedKind {
				t.Fatalf("expected kind %s for %s, got %s", expectedKind, eventType, ev.Kind)
			}
			if ev.StatusCode != expectedStatus {
				t.Fatalf("expected status_code %d for %s, got %d", expectedStatus, eventType, ev.StatusCode)
			}
		}

		assertEvent("queue_publish", "QUEUE_PUBLISH", 0)
		assertEvent("queue_consume", "QUEUE_CONSUME", 0)
		assertEvent("job_start", "JOB", 0)
		assertEvent("job_end", "JOB", 0)
		assertEvent("webhook_in", "HTTP_SERVER", 200)
		assertEvent("webhook_out", "HTTP_CLIENT", 202)

		if byType["job_start"].ParentID == nil || *byType["job_start"].ParentID != "ce-parent" {
			t.Fatalf("expected job_start parent_id to be ce-parent")
		}
		if byType["webhook_out"].Attributes["endpoint"] != "payments" {
			t.Fatalf("expected webhook_out attributes endpoint to be payments")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for ingest batch")
	}
}
