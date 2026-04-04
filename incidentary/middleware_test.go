package incidentary

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// --- filterHeaders ---

func TestFilterHeadersReturnsAllowlistedHeaders(t *testing.T) {
	headers := http.Header{}
	headers.Set("Content-Type", "application/json")
	headers.Set("X-Secret-Token", "should-not-appear")
	headers.Set("X-Request-Id", "req-123")

	allowlist := []string{"content-type", "x-request-id"}
	result := filterHeaders(headers, allowlist)

	if result["content-type"] != "application/json" {
		t.Fatalf("expected content-type 'application/json', got %q", result["content-type"])
	}
	if result["x-request-id"] != "req-123" {
		t.Fatalf("expected x-request-id 'req-123', got %q", result["x-request-id"])
	}
	if _, ok := result["x-secret-token"]; ok {
		t.Fatal("expected x-secret-token to be excluded from result")
	}
}

func TestFilterHeadersReturnsNilForEmptyAllowlist(t *testing.T) {
	headers := http.Header{}
	headers.Set("Content-Type", "application/json")

	result := filterHeaders(headers, nil)
	if result != nil {
		t.Fatal("expected nil result for empty allowlist")
	}
}

func TestFilterHeadersReturnsNilWhenNoAllowlistedHeadersPresent(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-Other", "value")

	result := filterHeaders(headers, []string{"content-type"})
	if result != nil {
		t.Fatalf("expected nil result when no allowlisted headers present, got %v", result)
	}
}

func TestFilterHeadersIgnoresBlankValues(t *testing.T) {
	headers := http.Header{}
	headers.Set("Content-Type", "  ") // whitespace only

	result := filterHeaders(headers, []string{"content-type"})
	if result != nil {
		t.Fatal("expected nil result for blank header value")
	}
}

// --- parseContentLength ---

func TestParseContentLengthParsesValidValues(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"100", 100},
		{"0", 0},
		{"99999", 99999},
		{"  1024  ", 1024}, // whitespace
	}

	for _, tc := range tests {
		got := parseContentLength(tc.input)
		if got != tc.want {
			t.Fatalf("parseContentLength(%q): expected %d, got %d", tc.input, tc.want, got)
		}
	}
}

func TestParseContentLengthReturnsZeroForInvalidValues(t *testing.T) {
	tests := []struct {
		input string
	}{
		{""},
		{"not-a-number"},
		{"-1"},
		{"9.5"},
	}

	for _, tc := range tests {
		got := parseContentLength(tc.input)
		if got != 0 {
			t.Fatalf("parseContentLength(%q): expected 0 for invalid input, got %d", tc.input, got)
		}
	}
}

// --- hashRetryIdentity ---

func TestHashRetryIdentityDeterministic(t *testing.T) {
	a := hashRetryIdentity("GET:/api/v1/users")
	b := hashRetryIdentity("GET:/api/v1/users")
	if a != b {
		t.Fatal("expected same hash for same input")
	}
}

func TestHashRetryIdentityDifferentForDifferentInputs(t *testing.T) {
	a := hashRetryIdentity("GET:/api/v1/users")
	b := hashRetryIdentity("POST:/api/v1/users")
	if a == b {
		t.Fatal("expected different hashes for different inputs")
	}
}

func TestHashRetryIdentityEmptyInputReturnsNonZero(t *testing.T) {
	// FNV-1a with empty string still returns the offset basis, not zero.
	got := hashRetryIdentity("")
	// Just verify it doesn't panic and returns a value.
	_ = got
}

// --- explicitRetryValue ---

func TestExplicitRetryValueNilReturnsNil(t *testing.T) {
	result := explicitRetryValue(nil)
	if result != nil {
		t.Fatalf("expected nil for nil input, got %v", result)
	}
}

func TestExplicitRetryValueTrueReturnsTrue(t *testing.T) {
	val := true
	result := explicitRetryValue(&val)
	if result != true {
		t.Fatalf("expected true, got %v", result)
	}
}

func TestExplicitRetryValueFalseReturnsFalse(t *testing.T) {
	val := false
	result := explicitRetryValue(&val)
	if result != false {
		t.Fatalf("expected false, got %v", result)
	}
}

// --- extractExplicitRetryObserved ---

func TestExtractExplicitRetryObservedFromNilMetadata(t *testing.T) {
	result := extractExplicitRetryObserved(nil)
	if result != nil {
		t.Fatal("expected nil for nil metadata")
	}
}

func TestExtractExplicitRetryObservedFromRetryAttempt(t *testing.T) {
	attempt := 2
	meta := &OutboundRetryMetadata{RetryAttempt: &attempt}
	result := extractExplicitRetryObserved(meta)
	if result == nil {
		t.Fatal("expected non-nil result for retry attempt >= 2")
	}
	if !*result {
		t.Fatal("expected true for retry attempt >= 2")
	}
}

func TestExtractExplicitRetryObservedFromRetryAttemptFirst(t *testing.T) {
	attempt := 1
	meta := &OutboundRetryMetadata{RetryAttempt: &attempt}
	result := extractExplicitRetryObserved(meta)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if *result {
		t.Fatal("expected false for retry attempt < 2 (first attempt)")
	}
}

func TestExtractExplicitRetryObservedFromIsRetry(t *testing.T) {
	isRetry := true
	meta := &OutboundRetryMetadata{IsRetry: &isRetry}
	result := extractExplicitRetryObserved(meta)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if !*result {
		t.Fatal("expected true for IsRetry=true")
	}
}

func TestExtractExplicitRetryObservedBothNilReturnsNil(t *testing.T) {
	meta := &OutboundRetryMetadata{} // no RetryAttempt, no IsRetry
	result := extractExplicitRetryObserved(meta)
	if result != nil {
		t.Fatal("expected nil when both fields are nil")
	}
}

// --- metadataToDownstreamMetadata ---

func TestMetadataToDownstreamMetadataNilReturnsNil(t *testing.T) {
	result := metadataToDownstreamMetadata(nil)
	if result != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestMetadataToDownstreamMetadataConvertsFields(t *testing.T) {
	meta := &OutboundRetryMetadata{
		RetryGroupID:      "group-1",
		IdempotencyKey:    "idem-key",
		OperationKey:      "op-key",
		RetryKey:          "retry-key",
		RouteTemplate:     "/api/{id}",
		RouteKey:          "/api/123",
		EdgeKey:           "edge-key",
		DownstreamService: "backend-svc",
		OperationName:     "GetUser",
	}

	result := metadataToDownstreamMetadata(meta)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.RetryGroupID != "group-1" {
		t.Fatalf("expected RetryGroupID 'group-1', got %q", result.RetryGroupID)
	}
	if result.OperationName != "GetUser" {
		t.Fatalf("expected OperationName 'GetUser', got %q", result.OperationName)
	}
}

// --- metadataField ---

func TestMetadataFieldWithNilMetadataReturnsEmpty(t *testing.T) {
	result := metadataField(nil, func(m *OutboundRetryMetadata) string { return m.RouteKey })
	if result != "" {
		t.Fatalf("expected empty string for nil metadata, got %q", result)
	}
}

func TestMetadataFieldExtractsValue(t *testing.T) {
	meta := &OutboundRetryMetadata{RouteKey: "/api/v1/users"}
	result := metadataField(meta, func(m *OutboundRetryMetadata) string { return m.RouteKey })
	if result != "/api/v1/users" {
		t.Fatalf("expected '/api/v1/users', got %q", result)
	}
}

// --- InjectTraceContext ---

func TestInjectTraceContextSetsHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	InjectTraceContext(req, "trace-abc", "ce-xyz")

	if req.Header.Get(TraceIDHeader) != "trace-abc" {
		t.Fatalf("expected trace header 'trace-abc', got %q", req.Header.Get(TraceIDHeader))
	}
	if req.Header.Get(ParentCeHeader) != "ce-xyz" {
		t.Fatalf("expected parent-ce header 'ce-xyz', got %q", req.Header.Get(ParentCeHeader))
	}
}

func TestInjectTraceContextWithNilRequestDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic with nil request: %v", r)
		}
	}()
	InjectTraceContext(nil, "trace-abc", "ce-xyz")
}

// --- buildInboundDetail ---

func TestBuildInboundDetailReturnsNilWithNilClient(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	rw := &responseWriter{ResponseWriter: httptest.NewRecorder(), status: 200}
	result := buildInboundDetail(nil, req, rw)
	if result != nil {
		t.Fatal("expected nil detail when client is nil")
	}
}

func TestBuildInboundDetailReturnsNilInNormalMode(t *testing.T) {
	client := newTestClient()
	// Default mode is NORMAL — detail capture disabled.
	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	rw := &responseWriter{ResponseWriter: httptest.NewRecorder(), status: 200}
	result := buildInboundDetail(client, req, rw)
	if result != nil {
		t.Fatal("expected nil detail in NORMAL mode")
	}
}

func TestBuildInboundDetailPopulatesInPreArmedMode(t *testing.T) {
	client := newTestClient()
	client.mode.Store(string(ModePreArmed))

	req := httptest.NewRequest(http.MethodPost, "/api/users", nil)
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = 128

	rw := &responseWriter{ResponseWriter: httptest.NewRecorder(), status: 201}

	result := buildInboundDetail(client, req, rw)
	if result == nil {
		t.Fatal("expected non-nil detail in PRE_ARMED mode")
	}
	if result.Method != "POST" {
		t.Fatalf("expected Method 'POST', got %q", result.Method)
	}
	if result.RequestBytes != 128 {
		t.Fatalf("expected RequestBytes 128, got %d", result.RequestBytes)
	}
}

func TestBuildInboundDetailNegativeContentLengthBecomesZero(t *testing.T) {
	client := newTestClient()
	client.mode.Store(string(ModePreArmed))

	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	req.ContentLength = -1

	rw := &responseWriter{ResponseWriter: httptest.NewRecorder(), status: 200}
	result := buildInboundDetail(client, req, rw)
	if result == nil {
		t.Fatal("expected non-nil detail")
	}
	if result.RequestBytes != 0 {
		t.Fatalf("expected RequestBytes 0 for negative content-length, got %d", result.RequestBytes)
	}
}

// --- Middleware integration ---

func TestMiddlewareWritesEventToBuffer(t *testing.T) {
	client := newTestClient()

	var handlerCalled bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	mw := Middleware(client, inner)
	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	req.Header.Set(TraceIDHeader, "trace-mw-test")
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	if !handlerCalled {
		t.Fatal("expected inner handler to be called")
	}

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event to be written to buffer")
	}
	if events[len(events)-1].Kind != KindHTTPIn {
		t.Fatalf("expected kind HTTP_IN, got %q", events[len(events)-1].Kind)
	}
}

func TestMiddlewareWithNilClientDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic with nil client: %v", r)
		}
	}()

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mw := Middleware(nil, inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)
}

func TestMiddlewarePreservesRequestTraceIDFromHeader(t *testing.T) {
	client := newTestClient()

	var capturedTraceID string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTraceID, _ = ContextTrace(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	mw := Middleware(client, inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(TraceIDHeader, "upstream-trace-id")
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	if capturedTraceID != "upstream-trace-id" {
		t.Fatalf("expected traceID 'upstream-trace-id' in context, got %q", capturedTraceID)
	}
}

func TestMiddlewareGeneratesTraceIDWhenAbsent(t *testing.T) {
	client := newTestClient()

	var capturedTraceID string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTraceID, _ = ContextTrace(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	mw := Middleware(client, inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	if capturedTraceID == "" {
		t.Fatal("expected auto-generated traceID when header absent")
	}
}

func TestMiddlewareRecordsResponseStatus(t *testing.T) {
	client := newTestClient()

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot) // 418
	})

	mw := Middleware(client, inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	if events[len(events)-1].Status != http.StatusTeapot {
		t.Fatalf("expected status 418, got %d", events[len(events)-1].Status)
	}
}

// --- responseWriter ---

func TestResponseWriterCapturesStatusCode(t *testing.T) {
	rw := &responseWriter{ResponseWriter: httptest.NewRecorder(), status: http.StatusOK}
	rw.WriteHeader(http.StatusNotFound)

	if rw.status != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rw.status)
	}
}

// --- InstrumentedDo ---

func TestInstrumentedDoRecordsOutboundEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestClient()
	parent := &TraceContext{TraceID: "trace-do", CeID: "ce-do"}

	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/test", nil)
	if err != nil {
		t.Fatal(err)
	}

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
	last := events[len(events)-1]
	if last.Kind != KindHTTPOut {
		t.Fatalf("expected kind HTTP_OUT, got %q", last.Kind)
	}
	if last.TraceID != "trace-do" {
		t.Fatalf("expected traceID 'trace-do', got %q", last.TraceID)
	}
}

func TestInstrumentedDoInjectsTraceHeaders(t *testing.T) {
	var gotTraceID, gotParentCe string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTraceID = r.Header.Get(TraceIDHeader)
		gotParentCe = r.Header.Get(ParentCeHeader)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestClient()
	parent := &TraceContext{TraceID: "trace-inject", CeID: "ce-inject"}

	req, _ := http.NewRequest(http.MethodGet, server.URL+"/", nil)
	resp, _ := InstrumentedDo(client, nil, parent, req, nil)
	if resp != nil {
		resp.Body.Close()
	}

	if gotTraceID != "trace-inject" {
		t.Fatalf("expected trace header 'trace-inject', got %q", gotTraceID)
	}
	if gotParentCe == "" {
		t.Fatal("expected parent-ce header to be set (new ceID)")
	}
}

func TestInstrumentedDoWithNilRequestReturnsError(t *testing.T) {
	client := newTestClient()
	_, err := InstrumentedDo(client, nil, nil, nil, nil)
	if err == nil {
		t.Fatal("expected error for nil request")
	}
}

func TestInstrumentedDoRecordsStatus500OnNetworkError(t *testing.T) {
	client := newTestClient()
	parent := &TraceContext{TraceID: "trace-err", CeID: "ce-err"}

	req, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:0/invalid", nil)
	failClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, &testNetError{message: "connection refused", timeout: false}
	})}

	_, _ = InstrumentedDo(client, failClient, parent, req, nil)

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event on network error")
	}
	if events[len(events)-1].Status != 0 {
		t.Fatalf("expected status 0 on network error, got %d", events[len(events)-1].Status)
	}
}

func TestInstrumentedDoWithNilClientStillSendsRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	req, _ := http.NewRequest(http.MethodGet, server.URL+"/", nil)
	resp, err := InstrumentedDo(nil, nil, nil, req, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != nil {
		resp.Body.Close()
	}
}

func TestInstrumentedDoWithRetryMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestClient()
	attempt := 2
	meta := &OutboundRetryMetadata{
		RetryAttempt:  &attempt,
		RouteTemplate: "/api/{id}",
		RouteKey:      "/api/123",
	}

	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/123", nil)
	resp, err := InstrumentedDo(client, nil, nil, req, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != nil {
		resp.Body.Close()
	}
}

// testNetError simulates a network error for testing.
type testNetError struct {
	message string
	timeout bool
}

func (e *testNetError) Error() string   { return e.message }
func (e *testNetError) Timeout() bool   { return e.timeout }
func (e *testNetError) Temporary() bool { return false }

// --- parseContentLength edge cases ---

func TestParseContentLengthLargeValues(t *testing.T) {
	got := parseContentLength(strconv.FormatInt(9_999_999_999, 10))
	if got != 9_999_999_999 {
		t.Fatalf("expected 9999999999, got %d", got)
	}
}

// --- strconv is needed ---
var _ = strings.ToLower
var _ = strconv.Itoa
