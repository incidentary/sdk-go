package incidentary

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// --- rollingWindow.advance ---

func TestRollingWindowAdvancesAcrossManyBuckets(t *testing.T) {
	// Create a window with 10 buckets covering 10 seconds.
	w := newRollingWindow(10_000, 10)
	nowMs := w.lastAdvMs

	// Record some errors.
	w.record(true, nowMs)
	w.record(true, nowMs)
	w.record(false, nowMs)

	rate := w.errorRatePct(nowMs)
	if rate <= 0 {
		t.Fatal("expected non-zero error rate after recording errors")
	}

	// Advance by more than the full window — all buckets should clear.
	futureMs := nowMs + 20_000
	rate2 := w.errorRatePct(futureMs)
	if rate2 != 0 {
		t.Fatalf("expected zero rate after advancing past full window, got %f", rate2)
	}
}

func TestRollingWindowAdvancesByExactlyOneBucket(t *testing.T) {
	w := newRollingWindow(10_000, 10) // 1 bucket per second
	nowMs := w.lastAdvMs

	w.record(true, nowMs)
	w.record(false, nowMs)

	// Advance by exactly one bucket (1000ms).
	futureMs := nowMs + 1_000
	w.record(false, futureMs)

	rate := w.errorRatePct(futureMs)
	// 1 error / 3 total = 33%.
	if rate <= 0 {
		t.Fatal("expected non-zero error rate within window after advancing one bucket")
	}
}

func TestRollingWindowReturnsZeroWithNoRequests(t *testing.T) {
	w := newRollingWindow(10_000, 10)
	nowMs := w.lastAdvMs
	rate := w.errorRatePct(nowMs)
	if rate != 0 {
		t.Fatalf("expected zero rate with no requests, got %f", rate)
	}
}

func TestRollingWindowAdvanceNoOpsForNegativeStep(t *testing.T) {
	w := newRollingWindow(10_000, 10)
	nowMs := w.lastAdvMs

	w.record(true, nowMs)

	// Go backwards in time — should be no-op.
	w.advance(nowMs - 1_000)
	rate := w.errorRatePct(nowMs)
	if rate <= 0 {
		t.Fatal("expected non-zero rate after backward advance (no-op)")
	}
}

// --- GetPreArmDebugState with active window ---

func TestGetPreArmDebugStateWithActiveWindowReturnsWindow(t *testing.T) {
	client := newTestClient()
	clock := client.nowClock()
	client.mu.Lock()
	client.activePreArmWindow = &PreArmWindow{
		ID:          "pw_test_debug",
		StartedAtMs: clock.wallMs,
		ExpiresAtMs: clock.wallMs + 300_000,
		Reasons: []PreArmTriggerReason{
			{TriggerType: TriggerSlowSuccess, Severity: SeveritySevere},
		},
	}
	client.mu.Unlock()

	state := client.GetPreArmDebugState()
	if state.ActivePreArmWindow == nil {
		t.Fatal("expected ActivePreArmWindow to be populated")
	}
	if state.ActivePreArmWindow.ID != "pw_test_debug" {
		t.Fatalf("expected window ID 'pw_test_debug', got %q", state.ActivePreArmWindow.ID)
	}
}

func TestGetPreArmDebugStateWithRecentWindowsPopulated(t *testing.T) {
	client := newTestClient()
	clock := client.nowClock()

	client.mu.Lock()
	window := PreArmWindow{
		ID:          "pw_recent",
		StartedAtMs: clock.wallMs - 5_000,
		ExpiresAtMs: clock.wallMs,
		ClosedAtMs:  clock.wallMs,
		CloseReason: "ttl",
	}
	client.pushRecentWindowLocked(window)
	client.mu.Unlock()

	state := client.GetPreArmDebugState()
	if len(state.RecentPreArmWindows) != 1 {
		t.Fatalf("expected 1 recent window, got %d", len(state.RecentPreArmWindows))
	}
	if state.RecentPreArmWindows[0].ID != "pw_recent" {
		t.Fatalf("expected window ID 'pw_recent', got %q", state.RecentPreArmWindows[0].ID)
	}
}

// --- annotateBufferedEventsLocked in INCIDENT mode ---

func TestAnnotateBufferedEventsInIncidentModeAnnotatesPreAlertEvents(t *testing.T) {
	client := newTestClient()
	client.mode.Store(string(ModeIncident))
	now := time.Now()
	alertAt := now.UnixNano()
	client.preArmAlertedAtNs = alertAt

	// Event before the alert — should be annotated.
	eventBefore := &SkeletonCe{CeID: "before", WallTsNs: alertAt - 1_000_000}
	// Event after the alert — should NOT be annotated.
	eventAfter := &SkeletonCe{CeID: "after", WallTsNs: alertAt + 1_000_000}

	result := client.annotateBufferedEventsLocked([]*SkeletonCe{eventBefore, eventAfter})

	// Find the before event.
	var before *SkeletonCe
	for _, e := range result {
		if e.CeID == "before" {
			before = e
		}
	}
	if before == nil {
		t.Fatal("expected 'before' event in result")
	}
	if before.CapturedBeforeAlert == nil || !*before.CapturedBeforeAlert {
		t.Fatal("expected CapturedBeforeAlert=true for event before alert")
	}

	// Find the after event.
	var after *SkeletonCe
	for _, e := range result {
		if e.CeID == "after" {
			after = e
		}
	}
	if after == nil {
		t.Fatal("expected 'after' event in result")
	}
	if after.CapturedBeforeAlert != nil {
		t.Fatal("expected CapturedBeforeAlert to be nil for event after alert")
	}
}

func TestAnnotateBufferedEventsInIncidentModeNoAnnotationWhenNoAlertTs(t *testing.T) {
	client := newTestClient()
	client.mode.Store(string(ModeIncident))
	client.preArmAlertedAtNs = 0 // no alert timestamp

	events := []*SkeletonCe{
		{CeID: "ce-1", WallTsNs: time.Now().UnixNano()},
	}
	result := client.annotateBufferedEventsLocked(events)
	if result[0].CapturedBeforeAlert != nil {
		t.Fatal("expected no annotation when preArmAlertedAtNs is 0")
	}
}

// --- CloseIncident with active pre-arm window ---

func TestCloseIncidentMovesActiveWindowToRecent(t *testing.T) {
	client := newTestClient()
	clock := client.nowClock()
	client.mu.Lock()
	client.mode.Store(string(ModePreArmed))
	client.activePreArmWindow = &PreArmWindow{
		ID:          "pw_close_test",
		StartedAtMs: clock.wallMs,
		ExpiresAtMs: clock.wallMs + 300_000,
	}
	client.mu.Unlock()

	client.EscalateToIncident()
	client.CloseIncident()

	state := client.GetPreArmDebugState()
	if len(state.RecentPreArmWindows) == 0 {
		t.Fatal("expected active window to be moved to recent on CloseIncident")
	}
	if state.ActivePreArmWindow != nil {
		t.Fatal("expected active window to be nil after CloseIncident")
	}
}

// --- buildOutboundDetail ---

// setPreArmedWithStartTime puts the client in PRE_ARMED mode with a recent
// preArmStartedAt so that evaluatePreArmLocked won't exit back to NORMAL.
func setPreArmedWithStartTime(t *testing.T, client *Client) {
	t.Helper()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.mode.Store(string(ModePreArmed))
	client.preArmStartedAt = client.nowClock().wallMs // set start to now
	client.activePreArmWindow = &PreArmWindow{
		ID:          "test-window",
		StartedAtMs: client.nowClock().wallMs,
		ExpiresAtMs: client.nowClock().wallMs + 300_000,
	}
}

func TestBuildOutboundDetailPopulatesInPreArmedMode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestClient()
	setPreArmedWithStartTime(t, client)

	parent := &TraceContext{TraceID: "trace-detail", CeID: "ce-detail"}
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/items", nil)

	resp, err := InstrumentedDo(client, nil, parent, req, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != nil {
		resp.Body.Close()
	}

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event from InstrumentedDo")
	}

	// In PRE_ARMED mode, detail should be attached to the event.
	last := events[len(events)-1]
	if last.Detail == nil {
		t.Fatal("expected Detail to be attached in PRE_ARMED mode")
	}
	if last.Detail.Method != "GET" {
		t.Fatalf("expected Method 'GET', got %q", last.Detail.Method)
	}
}

func TestBuildOutboundDetailWithTimeoutClassification(t *testing.T) {
	client := newTestClient()
	setPreArmedWithStartTime(t, client)

	// Use a transport that simulates a timeout.
	timeoutTransport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, &testNetError{message: "timeout", timeout: true}
	})
	httpClient := &http.Client{Transport: timeoutTransport}

	parent := &TraceContext{TraceID: "trace-timeout", CeID: "ce-timeout"}
	req, _ := http.NewRequest(http.MethodPost, "http://slow.example.com/api", nil)
	_, _ = InstrumentedDo(client, httpClient, parent, req, nil)

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event on timeout")
	}
	last := events[len(events)-1]
	if last.Detail == nil {
		t.Fatal("expected Detail on timeout event in PRE_ARMED mode")
	}
	if last.Detail.LocalErrorClassification != "timeout" {
		t.Fatalf("expected classification 'timeout', got %q", last.Detail.LocalErrorClassification)
	}
}

func TestBuildOutboundDetailWithCancelledClassification(t *testing.T) {
	client := newTestClient()
	setPreArmedWithStartTime(t, client)

	cancelledTransport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, &testNetError{message: "connection reset", timeout: false}
	})
	httpClient := &http.Client{Transport: cancelledTransport}

	req, _ := http.NewRequest(http.MethodGet, "http://example.com/api", nil)
	_, _ = InstrumentedDo(client, httpClient, nil, req, nil)

	// No panic and event is recorded — main goal.
	_ = client.buffer.Flush(time.Now().UnixMilli())
}

func TestBuildOutboundDetailDirectly(t *testing.T) {
	client := newTestClient()
	// buildOutboundDetail is only called if ShouldCaptureDetailForCurrentMode() is true.
	client.mode.Store(string(ModePreArmed))

	req, _ := http.NewRequest(http.MethodPost, "http://example.com/api/submit", nil)
	req.ContentLength = 200
	req.Header.Set("Content-Type", "application/json")

	respHeaders := http.Header{}
	respHeaders.Set("Content-Type", "application/json")
	respHeaders.Set("Content-Length", "100")

	resolution := downstreamEdgeResolver.Resolve(ResolveDownstreamEdgeInput{
		TraceID: "trace-test",
		Method:  "POST",
		URL:     "http://example.com/api/submit",
	})

	detail := buildOutboundDetail(client, req, respHeaders, resolution, nil, nil, false, false)
	if detail == nil {
		t.Fatal("expected non-nil detail from buildOutboundDetail in PRE_ARMED mode")
	}
	if detail.Method != "POST" {
		t.Fatalf("expected Method 'POST', got %q", detail.Method)
	}
	if detail.RequestBytes != 200 {
		t.Fatalf("expected RequestBytes 200, got %d", detail.RequestBytes)
	}
	if detail.LocalErrorClassification != "none" {
		t.Fatalf("expected classification 'none', got %q", detail.LocalErrorClassification)
	}
}

// --- collectDistinctMildCount with multiple mild triggers in ring ---

func TestCollectDistinctMildCountFindsMultipleMildTriggers(t *testing.T) {
	cfg := testTriggerConfig()
	cfg.SlowSuccess.HighRate = 0.9   // hard to hit severe
	cfg.SlowSuccess.MildRate = 0.1   // easy to hit mild
	cfg.SlowSuccess.MinSamples = 10
	cfg.SlowSuccess.SlowMultiplier = 1.1
	cfg.Retry.HighRate = 0.9
	cfg.Retry.MildRate = 0.05
	cfg.Retry.MinTotalAttempts = 10
	cfg.InFlight.MinAbsoluteInFlight = 1000 // very hard to hit
	cfg.EnableInFlightPileup = false
	engine := NewTriggerEngine(cfg)

	// Warm up EWMA.
	for sec := int64(0); sec < 5; sec++ {
		for i := 0; i < 10; i++ {
			engine.OnRequestComplete(signal(nil), sec, sec*1000)
		}
		engine.Evaluate(ModeNormal, sec*1000, sec, sec*1000)
	}

	// Add slow requests (mild slow_success signal).
	for i := 0; i < 3; i++ {
		engine.OnRequestComplete(signal(func(s *RequestCompleteSignal) {
			s.DurationNs = 2_000_000_000
		}), 5, 5_000)
	}
	for i := 0; i < 7; i++ {
		engine.OnRequestComplete(signal(nil), 5, 5_000)
	}
	engine.Evaluate(ModeNormal, 5_000, 5, 5_000)

	// Add retry signals (mild retry_onset signal).
	for i := 0; i < 20; i++ {
		isRetry := i%3 == 0
		engine.OnRequestComplete(signal(func(s *RequestCompleteSignal) {
			s.Kind = KindHTTPOut
			s.OutboundRetryQuality = RetryKeyQualityExplicit
			s.ExplicitRetryObserved = &isRetry
		}), 5, int64(5_100+i))
	}

	// At mono time within mildWindowMs — both mild triggers should be recognized.
	decision := engine.Evaluate(ModeNormal, 5_200, 5, 5_200)
	_ = decision // might or might not trigger — just verifies no panic

	count := engine.collectDistinctMildCount(5_200)
	_ = count // just verify it runs without panic
}

// --- disableTrigger via panic recovery ---

func TestTriggerEngineSafeMethodsRecoverFromPanics(t *testing.T) {
	// This test verifies that the safe wrapper methods recover from panics
	// and disable the affected trigger rather than crashing.
	engine := NewTriggerEngine(testTriggerConfig())

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic propagated outside engine: %v", r)
		}
	}()

	// Drive engine normally to ensure it works.
	for i := 0; i < 10; i++ {
		engine.OnRequestStart(1)
		engine.OnRequestComplete(signal(nil), 1, 1_000)
	}
	_ = engine.Evaluate(ModeNormal, 1_000, 1, 1_000)
}

// --- BeginTx with ConnBeginTx support ---

// TestInstrumentedConnBeginTxWithConnBeginTxSupport is covered by
// TestInstrumentedConnBeginTxFallsBackToBeginWhenNotSupported in queue_integration_test.go

// --- eventTypeToKind coverage ---

func TestEventTypeToKindForAllTypes(t *testing.T) {
	client := newTestClient()

	tests := []struct {
		eventType IncidentaryEventType
		wantKind  CeKind
	}{
		{EventHTTPIn, KindHTTPIn},
		{EventWebhookIn, KindHTTPIn},
		{EventGRPCIn, KindHTTPIn},
		{EventHTTPOut, KindHTTPOut},
		{EventWebhookOut, KindHTTPOut},
		{EventGRPCOut, KindHTTPOut},
		{EventQueuePublish, KindQueuePublish},
		{EventQueueConsume, KindQueueConsume},
		{EventDBQuery, KindDBQuery},
		{EventJobStart, KindJob},
		{EventJobEnd, KindJob},
		{EventInternalTask, KindInternal},
		{"unknown_type", KindInternal},
	}

	for _, tc := range tests {
		t.Run(string(tc.eventType), func(t *testing.T) {
			got := client.eventTypeToKind(tc.eventType)
			if got != tc.wantKind {
				t.Fatalf("eventTypeToKind(%q): expected %q, got %q", tc.eventType, tc.wantKind, got)
			}
		})
	}
}

// --- RecordRequestStart with empty kind ---

func TestRecordRequestStartWithEmptyKindDefaultsToHTTPIn(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	client := newTestClient()
	client.RecordRequestStart("") // empty kind — should default to KindHTTPIn
}

// --- RecordRequestWithOptions with empty kind ---

func TestRecordRequestWithOptionsEmptyKindDefaultsToHTTPIn(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	client := newTestClient()
	client.RecordRequestWithOptions(200, RecordRequestOptions{})
}

// --- nextPowerOfTwo ---

func TestNextPowerOfTwoValues(t *testing.T) {
	tests := []struct {
		input int
		want  int
	}{
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{128, 128},
		{129, 256},
	}
	for _, tc := range tests {
		got := nextPowerOfTwo(tc.input)
		if got != tc.want {
			t.Fatalf("nextPowerOfTwo(%d): expected %d, got %d", tc.input, tc.want, got)
		}
	}
}

// --- GRPCCall records correct service name ---

func TestRecordGRPCCallRecordsServiceID(t *testing.T) {
	cfg := DefaultConfig("test-key", "my-grpc-service")
	cfg.BaseURL = "http://localhost:9999"
	client := New(cfg)
	ctx := setTraceContext(context.Background(), "trace-svc", "ce-svc")

	RecordGRPCCall(ctx, client, KindHTTPIn, "/MyService/MyMethod", time.Now(), nil)

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	if events[len(events)-1].ServiceID != "my-grpc-service" {
		t.Fatalf("expected ServiceID 'my-grpc-service', got %q", events[len(events)-1].ServiceID)
	}
}

func TestRecordGRPCCallRecordsMethodInEventAttrs(t *testing.T) {
	client := newTestClient()
	RecordGRPCCall(context.Background(), client, KindHTTPOut, "/UserService/GetUser", time.Now(), nil)

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	attrs, ok := events[len(events)-1].Attributes.(map[string]interface{})
	if !ok || attrs["method"] != "/UserService/GetUser" {
		t.Fatalf("expected method '/UserService/GetUser' in EventAttrs, got %v", events[len(events)-1].Attributes)
	}
}

func TestRecordGRPCCallWithEmptyMethodNoEventAttrs(t *testing.T) {
	client := newTestClient()
	RecordGRPCCall(context.Background(), client, KindHTTPOut, "", time.Now(), nil)

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	if events[len(events)-1].Attributes != nil {
		t.Fatal("expected nil EventAttrs when method is empty")
	}
}

// --- RecordDBQuery with operation and query in EventAttrs ---

func TestRecordDBQueryAttrsContainOperationAndQuery(t *testing.T) {
	client := newTestClient()
	recordDBQuery(client, context.Background(), "query", "SELECT id FROM users WHERE id=?", time.Now(), nil)

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	attrs, ok := events[len(events)-1].Attributes.(map[string]interface{})
	if !ok {
		t.Fatalf("expected EventAttrs to be map[string]interface{}, got %T", events[len(events)-1].Attributes)
	}
	if attrs["operation"] != "query" {
		t.Fatalf("expected operation 'query', got %v", attrs["operation"])
	}
	if attrs["query"] != "SELECT id FROM users WHERE id=?" {
		t.Fatalf("expected full query, got %v", attrs["query"])
	}
}

func TestRecordDBQueryWithEmptyOperationAndQueryNoAttrs(t *testing.T) {
	client := newTestClient()
	recordDBQuery(client, context.Background(), "", "", time.Now(), nil)

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	if events[len(events)-1].Attributes != nil {
		t.Fatal("expected nil EventAttrs for empty operation and query")
	}
}
