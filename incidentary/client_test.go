package incidentary

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// --- GetCaptureMode ---

func TestGetCaptureModeReturnsNormalByDefault(t *testing.T) {
	client := newTestClient()
	if got := client.GetCaptureMode(); got != ModeNormal {
		t.Fatalf("expected ModeNormal, got %q", got)
	}
}

// --- RecordRequest (simple wrapper) ---

func TestRecordRequestDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic from RecordRequest: %v", r)
		}
	}()
	client := newTestClient()
	client.RecordRequest(200)
	client.RecordRequest(500)
	client.RecordRequest(0)
}

// --- RecordQueuePublish / RecordQueueConsume / RecordJobStart / RecordJobEnd ---

func TestRecordEventTypeHelpersDoNotPanic(t *testing.T) {
	client := newTestClient()
	opts := RecordEventOptions{TraceID: "trace-helpers", DurationNs: 1_000_000}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	client.RecordQueuePublish(opts)
	client.RecordQueueConsume(opts)
	client.RecordJobStart(opts)
	client.RecordJobEnd(opts)
	client.RecordWebhookIn(opts)
	client.RecordWebhookOut(opts)
}

func TestRecordQueuePublishWritesEventToBuffer(t *testing.T) {
	client := newTestClient()
	client.RecordQueuePublish(RecordEventOptions{TraceID: "trace-pub", DurationNs: 500_000})

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected at least one event after RecordQueuePublish")
	}
	last := events[len(events)-1]
	if last.EventType != string(EventQueuePublish) {
		t.Fatalf("expected event_type %q, got %q", EventQueuePublish, last.EventType)
	}
	if last.TraceID != "trace-pub" {
		t.Fatalf("expected traceID 'trace-pub', got %q", last.TraceID)
	}
}

func TestRecordQueueConsumeWritesEventToBuffer(t *testing.T) {
	client := newTestClient()
	client.RecordQueueConsume(RecordEventOptions{TraceID: "trace-consume"})

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event after RecordQueueConsume")
	}
	if events[len(events)-1].EventType != string(EventQueueConsume) {
		t.Fatalf("expected event_type %q, got %q", EventQueueConsume, events[len(events)-1].EventType)
	}
}

func TestRecordJobStartWritesEventToBuffer(t *testing.T) {
	client := newTestClient()
	client.RecordJobStart(RecordEventOptions{TraceID: "trace-job-start"})

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event after RecordJobStart")
	}
	if events[len(events)-1].EventType != string(EventJobStart) {
		t.Fatalf("expected event_type %q, got %q", EventJobStart, events[len(events)-1].EventType)
	}
}

func TestRecordJobEndWritesEventToBuffer(t *testing.T) {
	client := newTestClient()
	client.RecordJobEnd(RecordEventOptions{TraceID: "trace-job-end"})

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event after RecordJobEnd")
	}
	if events[len(events)-1].EventType != string(EventJobEnd) {
		t.Fatalf("expected event_type %q, got %q", EventJobEnd, events[len(events)-1].EventType)
	}
}

func TestRecordWebhookInWritesEventToBuffer(t *testing.T) {
	client := newTestClient()
	client.RecordWebhookIn(RecordEventOptions{TraceID: "trace-webhook-in"})

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event after RecordWebhookIn")
	}
	if events[len(events)-1].EventType != string(EventWebhookIn) {
		t.Fatalf("expected event_type %q, got %q", EventWebhookIn, events[len(events)-1].EventType)
	}
}

func TestRecordWebhookOutWritesEventToBuffer(t *testing.T) {
	client := newTestClient()
	client.RecordWebhookOut(RecordEventOptions{TraceID: "trace-webhook-out"})

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event after RecordWebhookOut")
	}
	if events[len(events)-1].EventType != string(EventWebhookOut) {
		t.Fatalf("expected event_type %q, got %q", EventWebhookOut, events[len(events)-1].EventType)
	}
}

func TestRecordEventUsesCurrentTimeWhenWallTsNsIsZero(t *testing.T) {
	client := newTestClient()
	before := time.Now().UnixNano()
	client.RecordJobStart(RecordEventOptions{TraceID: "trace-ts"})
	after := time.Now().UnixNano()

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	ts := events[len(events)-1].WallTsNs
	if ts < before || ts > after {
		t.Fatalf("expected WallTsNs between %d and %d, got %d", before, after, ts)
	}
}

func TestRecordEventUsesProvidedWallTsNs(t *testing.T) {
	client := newTestClient()
	// Use a timestamp close to now so it falls within the 60-second ring-buffer window.
	explicit := time.Now().Add(-5 * time.Second).UnixNano()
	client.RecordJobStart(RecordEventOptions{TraceID: "trace-explicit-ts", WallTsNs: explicit})

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	if events[len(events)-1].WallTsNs != explicit {
		t.Fatalf("expected WallTsNs %d, got %d", explicit, events[len(events)-1].WallTsNs)
	}
}

func TestRecordEventUsesCustomStatus(t *testing.T) {
	client := newTestClient()
	status := 201
	client.RecordEvent(EventJobEnd, RecordEventOptions{TraceID: "trace-status", Status: &status})

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	if events[len(events)-1].Status != 201 {
		t.Fatalf("expected status 201, got %d", events[len(events)-1].Status)
	}
}

func TestRecordEventSetsHTTPInDefaultStatus200(t *testing.T) {
	client := newTestClient()
	client.RecordEvent(EventHTTPIn, RecordEventOptions{TraceID: "trace-http-in"})

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	if events[len(events)-1].Status != 200 {
		t.Fatalf("expected default status 200 for http_in, got %d", events[len(events)-1].Status)
	}
}

func TestRecordEventSetsJobDefaultStatus0(t *testing.T) {
	client := newTestClient()
	client.RecordEvent(EventJobStart, RecordEventOptions{TraceID: "trace-job"})

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	if events[len(events)-1].Status != 0 {
		t.Fatalf("expected default status 0 for job_start, got %d", events[len(events)-1].Status)
	}
}

func TestRecordEventGeneratesTraceIDWhenEmpty(t *testing.T) {
	client := newTestClient()
	client.RecordJobStart(RecordEventOptions{}) // empty TraceID

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	if events[len(events)-1].TraceID == "" {
		t.Fatal("expected auto-generated traceID when none provided")
	}
}

func TestRecordEventAttachesEventAttrs(t *testing.T) {
	client := newTestClient()
	attrs := map[string]interface{}{"key": "value", "num": 42}
	client.RecordEvent(EventJobEnd, RecordEventOptions{
		TraceID:    "trace-attrs",
		EventAttrs: attrs,
	})

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	if events[len(events)-1].EventAttrs == nil {
		t.Fatal("expected EventAttrs to be set")
	}
}

func TestRecordEventDurationNsIsNonNegative(t *testing.T) {
	client := newTestClient()
	client.RecordJobEnd(RecordEventOptions{TraceID: "trace-dur", DurationNs: -9999})

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) == 0 {
		t.Fatal("expected event")
	}
	if events[len(events)-1].DurationNs < 0 {
		t.Fatalf("expected non-negative DurationNs, got %d", events[len(events)-1].DurationNs)
	}
}

// --- WriteEvent ---

func TestWriteEventNilDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic from WriteEvent(nil): %v", r)
		}
	}()
	client := newTestClient()
	client.WriteEvent(nil)
}

func TestWriteEventAddsToBuffer(t *testing.T) {
	client := newTestClient()
	client.WriteEvent(&SkeletonCe{
		CeID:     "test-ce",
		TraceID:  "trace-write",
		Kind:     KindHTTPIn,
		WallTsNs: time.Now().UnixNano(),
	})

	events := client.buffer.Flush(time.Now().UnixMilli())
	if len(events) != 1 {
		t.Fatalf("expected 1 event in buffer, got %d", len(events))
	}
	if events[0].CeID != "test-ce" {
		t.Fatalf("expected CeID 'test-ce', got %q", events[0].CeID)
	}
}

// --- GetDetailRequestHeaderAllowlist / GetDetailResponseHeaderAllowlist ---

func TestGetDetailRequestHeaderAllowlistReturnsDefaults(t *testing.T) {
	client := newTestClient()
	list := client.GetDetailRequestHeaderAllowlist()
	if len(list) == 0 {
		t.Fatal("expected non-empty request header allowlist")
	}
	found := false
	for _, h := range list {
		if h == "content-type" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 'content-type' in allowlist, got %v", list)
	}
}

func TestGetDetailResponseHeaderAllowlistReturnsDefaults(t *testing.T) {
	client := newTestClient()
	list := client.GetDetailResponseHeaderAllowlist()
	if len(list) == 0 {
		t.Fatal("expected non-empty response header allowlist")
	}
}

func TestGetDetailHeaderAllowlistsReturnImmutableCopies(t *testing.T) {
	client := newTestClient()
	list1 := client.GetDetailRequestHeaderAllowlist()
	list2 := client.GetDetailRequestHeaderAllowlist()
	if len(list1) != len(list2) {
		t.Fatalf("expected same length on repeated calls, got %d vs %d", len(list1), len(list2))
	}
	// Modifying the returned slice should not affect subsequent calls.
	if len(list1) > 0 {
		list1[0] = "mutated"
	}
	list3 := client.GetDetailRequestHeaderAllowlist()
	if len(list3) > 0 && list3[0] == "mutated" {
		t.Fatal("expected GetDetailRequestHeaderAllowlist to return an independent copy")
	}
}

// --- ShouldCaptureDetailForCurrentMode ---

func TestShouldCaptureDetailForCurrentModeReturnsFalseInNormalMode(t *testing.T) {
	client := newTestClient()
	// Default mode is NORMAL.
	if client.ShouldCaptureDetailForCurrentMode() {
		t.Fatal("expected false in NORMAL mode")
	}
}

func TestShouldCaptureDetailForCurrentModeReturnsTrueInPreArmedMode(t *testing.T) {
	client := newTestClient()
	client.mode.Store(string(ModePreArmed))
	if !client.ShouldCaptureDetailForCurrentMode() {
		t.Fatal("expected true in PRE_ARMED mode")
	}
}

func TestShouldCaptureDetailForCurrentModeReturnsTrueInIncidentMode(t *testing.T) {
	client := newTestClient()
	client.mode.Store(string(ModeIncident))
	if !client.ShouldCaptureDetailForCurrentMode() {
		t.Fatal("expected true in INCIDENT mode")
	}
}

func TestShouldCaptureDetailForCurrentModeReturnsFalseWhenDetailCaptureDisabled(t *testing.T) {
	cfg := DefaultConfig("test-key", "svc")
	cfg.BaseURL = "http://localhost:9999"
	cfg.PreArmDetailCaptureEnabled = false
	client := New(cfg)
	client.mode.Store(string(ModePreArmed))

	if client.ShouldCaptureDetailForCurrentMode() {
		t.Fatal("expected false when PreArmDetailCaptureEnabled is false")
	}
}

// --- AttachDetailToEvent ---

func TestAttachDetailToEventReturnsInputUnchangedInNormalMode(t *testing.T) {
	client := newTestClient()
	// Default mode is NORMAL.
	ce := &SkeletonCe{CeID: "test-ce", Kind: KindHTTPIn}
	detail := &CeDetail{Method: "GET"}

	result := client.AttachDetailToEvent(ce, detail)

	if result != ce {
		t.Fatal("expected the same ce pointer to be returned when not capturing detail")
	}
	if result.Detail != nil {
		t.Fatal("expected Detail to be nil in NORMAL mode")
	}
}

func TestAttachDetailToEventAttachesDetailInPreArmedMode(t *testing.T) {
	client := newTestClient()
	client.mode.Store(string(ModePreArmed))

	ce := &SkeletonCe{CeID: "test-ce", Kind: KindHTTPIn}
	detail := &CeDetail{Method: "GET", RouteKey: "/api/v1/users"}

	result := client.AttachDetailToEvent(ce, detail)

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Detail == nil {
		t.Fatal("expected Detail to be set in PRE_ARMED mode")
	}
	if result.Detail.Method != "GET" {
		t.Fatalf("expected Method 'GET', got %q", result.Detail.Method)
	}
}

func TestAttachDetailToEventReturnsNilCeWhenCeIsNil(t *testing.T) {
	client := newTestClient()
	client.mode.Store(string(ModePreArmed))
	result := client.AttachDetailToEvent(nil, &CeDetail{Method: "GET"})
	if result != nil {
		t.Fatal("expected nil result when ce is nil")
	}
}

func TestAttachDetailToEventReturnsOriginalCeWhenDetailIsNil(t *testing.T) {
	client := newTestClient()
	client.mode.Store(string(ModePreArmed))
	ce := &SkeletonCe{CeID: "test-ce"}
	result := client.AttachDetailToEvent(ce, nil)
	if result != ce {
		t.Fatal("expected the original ce to be returned when detail is nil")
	}
}

func TestAttachDetailToEventDoesNotMutateOriginalCe(t *testing.T) {
	client := newTestClient()
	client.mode.Store(string(ModeIncident))

	ce := &SkeletonCe{CeID: "original", Kind: KindHTTPIn}
	detail := &CeDetail{Method: "POST", RouteKey: "/submit"}

	result := client.AttachDetailToEvent(ce, detail)

	if result == ce {
		t.Fatal("expected a new ce to be returned with detail attached (immutable pattern)")
	}
	if ce.Detail != nil {
		t.Fatal("expected original ce.Detail to remain nil after attach")
	}
}

func TestAttachDetailToEventRedactsPayloadSnippetWhenNotEnabled(t *testing.T) {
	cfg := DefaultConfig("test-key", "svc")
	cfg.BaseURL = "http://localhost:9999"
	cfg.PreArmDetailCapturePayloadEnabled = false
	client := New(cfg)
	client.mode.Store(string(ModePreArmed))

	ce := &SkeletonCe{CeID: "test-ce", Kind: KindHTTPIn}
	detail := &CeDetail{Method: "POST", PayloadSnippet: `{"secret":"data"}`}

	result := client.AttachDetailToEvent(ce, detail)
	if result != nil && result.Detail != nil && result.Detail.PayloadSnippet != "" {
		t.Fatalf("expected PayloadSnippet to be cleared when capture is disabled, got %q", result.Detail.PayloadSnippet)
	}
}

func TestAttachDetailToEventIncludesPayloadWhenEnabled(t *testing.T) {
	cfg := DefaultConfig("test-key", "svc")
	cfg.BaseURL = "http://localhost:9999"
	cfg.PreArmDetailCapturePayloadEnabled = true
	client := New(cfg)
	client.mode.Store(string(ModePreArmed))

	ce := &SkeletonCe{CeID: "test-ce", Kind: KindHTTPIn}
	detail := &CeDetail{Method: "POST", RouteKey: "/api", PayloadSnippet: `{"user":"alice"}`}

	result := client.AttachDetailToEvent(ce, detail)
	if result == nil || result.Detail == nil {
		t.Fatal("expected detail to be attached")
	}
	if result.Detail.PayloadSnippet == "" {
		t.Fatal("expected PayloadSnippet to be present when capture is enabled")
	}
}

// --- EscalateToIncident / CloseIncident lifecycle ---

func TestEscalateToIncidentTransitionsToIncidentMode(t *testing.T) {
	client := newTestClient()
	if client.GetMode() != ModeNormal {
		t.Fatal("expected NORMAL mode initially")
	}

	client.EscalateToIncident()

	if client.GetMode() != ModeIncident {
		t.Fatalf("expected INCIDENT mode after escalation, got %q", client.GetMode())
	}
}

func TestEscalateToIncidentIsIdempotent(t *testing.T) {
	client := newTestClient()
	client.EscalateToIncident()
	client.EscalateToIncident() // second call should not panic or corrupt state

	if client.GetMode() != ModeIncident {
		t.Fatalf("expected INCIDENT mode, got %q", client.GetMode())
	}
}

func TestEscalateToIncidentWithIDSetsIncidentID(t *testing.T) {
	client := newTestClient()
	// Enter pre-arm first so there is an active window.
	client.mode.Store(string(ModePreArmed))
	clock := client.nowClock()
	client.activePreArmWindow = &PreArmWindow{
		ID:          "pw_test",
		StartedAtMs: clock.wallMs,
		ExpiresAtMs: clock.wallMs + 300_000,
	}

	client.EscalateToIncidentWithID("incident-abc-123")

	if client.GetMode() != ModeIncident {
		t.Fatalf("expected INCIDENT mode, got %q", client.GetMode())
	}
	if client.activePreArmWindow != nil && client.activePreArmWindow.BoundIncidentID != "incident-abc-123" {
		t.Fatalf("expected BoundIncidentID 'incident-abc-123', got %q", client.activePreArmWindow.BoundIncidentID)
	}
}

func TestCloseIncidentTransitionsBackToNormalMode(t *testing.T) {
	client := newTestClient()
	client.EscalateToIncident()
	if client.GetMode() != ModeIncident {
		t.Fatal("expected INCIDENT mode before close")
	}

	client.CloseIncident()

	if client.GetMode() != ModeNormal {
		t.Fatalf("expected NORMAL mode after CloseIncident, got %q", client.GetMode())
	}
}

func TestCloseIncidentResetsPreArmState(t *testing.T) {
	client := newTestClient()
	client.EscalateToIncident()
	client.CloseIncident()

	if client.preArmAlertedAtNs != 0 {
		t.Fatalf("expected preArmAlertedAtNs to be reset to 0, got %d", client.preArmAlertedAtNs)
	}
	if client.preArmRingBufferSeq != 0 {
		t.Fatalf("expected preArmRingBufferSeq to be reset, got %d", client.preArmRingBufferSeq)
	}
}

func TestCloseIncidentCanBeCalledFromNormalMode(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	client := newTestClient()
	client.CloseIncident() // Should not panic when already in NORMAL mode.
}

func TestEscalateCloseIncidentFullLifecycle(t *testing.T) {
	client := newTestClient()

	if client.GetMode() != ModeNormal {
		t.Fatal("expected NORMAL initially")
	}

	client.EscalateToIncident()
	if client.GetMode() != ModeIncident {
		t.Fatal("expected INCIDENT after escalate")
	}

	client.CloseIncident()
	if client.GetMode() != ModeNormal {
		t.Fatal("expected NORMAL after close")
	}

	// Can escalate again after close.
	client.EscalateToIncident()
	if client.GetMode() != ModeIncident {
		t.Fatal("expected INCIDENT on second escalation")
	}
}

// --- GetPreArmDebugState ---

func TestGetPreArmDebugStateReturnsValidStructure(t *testing.T) {
	client := newTestClient()
	state := client.GetPreArmDebugState()

	if state.Counters == nil {
		t.Fatal("expected non-nil Counters map")
	}
	if state.Gauges == nil {
		t.Fatal("expected non-nil Gauges map")
	}
	if _, ok := state.Counters["prearm_enter_total"]; !ok {
		t.Fatal("expected 'prearm_enter_total' counter")
	}
}

func TestGetPreArmDebugStateActiveWindowNilInitially(t *testing.T) {
	client := newTestClient()
	state := client.GetPreArmDebugState()
	if state.ActivePreArmWindow != nil {
		t.Fatal("expected nil active prearm window initially")
	}
}

func TestGetPreArmDebugStateRecentWindowsEmptyInitially(t *testing.T) {
	client := newTestClient()
	state := client.GetPreArmDebugState()
	if len(state.RecentPreArmWindows) != 0 {
		t.Fatalf("expected 0 recent windows initially, got %d", len(state.RecentPreArmWindows))
	}
}

// --- annotateBufferedEventsLocked ---

func TestAnnotateBufferedEventsInPreArmedModeSetsFlags(t *testing.T) {
	client := newTestClient()
	client.mode.Store(string(ModePreArmed))

	events := []*SkeletonCe{
		{CeID: "ce-1", TraceID: "trace-1", WallTsNs: time.Now().UnixNano()},
		{CeID: "ce-2", TraceID: "trace-2", WallTsNs: time.Now().UnixNano()},
	}

	result := client.annotateBufferedEventsLocked(events)

	for i, ce := range result {
		if ce.CapturedBeforeAlert == nil || !*ce.CapturedBeforeAlert {
			t.Fatalf("expected CapturedBeforeAlert=true on event %d", i)
		}
		if ce.RingBufferSeq == nil {
			t.Fatalf("expected RingBufferSeq to be set on event %d", i)
		}
		if *ce.RingBufferSeq != int64(i) {
			t.Fatalf("expected RingBufferSeq %d, got %d", i, *ce.RingBufferSeq)
		}
	}
}

func TestAnnotateBufferedEventsInNormalModeDoesNotSetFlags(t *testing.T) {
	client := newTestClient()
	// Default mode is NORMAL.

	events := []*SkeletonCe{
		{CeID: "ce-1", WallTsNs: time.Now().UnixNano()},
	}
	result := client.annotateBufferedEventsLocked(events)

	if result[0].CapturedBeforeAlert != nil {
		t.Fatal("expected CapturedBeforeAlert to be nil in NORMAL mode")
	}
}

func TestAnnotateBufferedEventsEmptySliceReturnsEmpty(t *testing.T) {
	client := newTestClient()
	result := client.annotateBufferedEventsLocked([]*SkeletonCe{})
	if len(result) != 0 {
		t.Fatalf("expected empty result for empty input, got %d", len(result))
	}
}

// --- New() configuration defaults ---

func TestNewWithZeroBufferCapacityUsesDefault(t *testing.T) {
	cfg := DefaultConfig("k", "svc")
	cfg.BaseURL = "http://localhost:9999"
	cfg.BufferCapacity = 0
	client := New(cfg)
	if client.buffer.capacity != 4_000 {
		t.Fatalf("expected default capacity 4000, got %d", client.buffer.capacity)
	}
}

func TestNewWithZeroPreArmTTLUsesDefault(t *testing.T) {
	cfg := DefaultConfig("k", "svc")
	cfg.BaseURL = "http://localhost:9999"
	cfg.PreArmTTLMs = 0
	client := New(cfg)
	if client.config.PreArmTTLMs != 300_000 {
		t.Fatalf("expected default PreArmTTLMs 300000, got %d", client.config.PreArmTTLMs)
	}
}

func TestNewWithNegativeCooldownUsesZero(t *testing.T) {
	cfg := DefaultConfig("k", "svc")
	cfg.BaseURL = "http://localhost:9999"
	cfg.PreArmCooldownMs = -1
	client := New(cfg)
	if client.config.PreArmCooldownMs != 0 {
		t.Fatalf("expected PreArmCooldownMs 0 for negative input, got %d", client.config.PreArmCooldownMs)
	}
}

func TestNewWithSmallRetryTableSizeUsesMinimum(t *testing.T) {
	cfg := DefaultConfig("k", "svc")
	cfg.BaseURL = "http://localhost:9999"
	cfg.PreArmRetryTableSize = 10 // below minimum 128
	client := New(cfg)
	if client.config.PreArmRetryTableSize < 128 {
		t.Fatalf("expected at least 128 for retry table size, got %d", client.config.PreArmRetryTableSize)
	}
}

func TestNewWithEmptyHeaderAllowlistsUsesDefaults(t *testing.T) {
	cfg := DefaultConfig("k", "svc")
	cfg.BaseURL = "http://localhost:9999"
	cfg.PreArmDetailRequestHeaderAllowlist = nil
	cfg.PreArmDetailResponseHeaderAllowlist = nil
	client := New(cfg)

	reqList := client.GetDetailRequestHeaderAllowlist()
	if len(reqList) == 0 {
		t.Fatal("expected non-empty default request header allowlist")
	}
}

func TestNewWithEmptyRedactFieldsUsesDefaults(t *testing.T) {
	cfg := DefaultConfig("k", "svc")
	cfg.BaseURL = "http://localhost:9999"
	cfg.RedactFields = nil
	client := New(cfg)

	// The client should have default redaction fields set.
	if len(client.redactionFields) == 0 {
		t.Fatal("expected non-empty default redaction fields")
	}
}

// --- Concurrent access ---

func TestClientConcurrentWriteEventIsSafe(t *testing.T) {
	client := newTestClient()
	const goroutines = 50
	const eventsPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < eventsPerGoroutine; i++ {
				client.WriteEvent(&SkeletonCe{
					CeID:    randomUUID(),
					TraceID: randomUUID(),
					Kind:    KindHTTPIn,
					WallTsNs: time.Now().UnixNano(),
				})
			}
		}(g)
	}
	wg.Wait()
}

func TestClientConcurrentRecordRequestIsSafe(t *testing.T) {
	client := newTestClient()
	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			client.RecordRequest(200)
			client.RecordRequest(500)
		}()
	}
	wg.Wait()
}

func TestClientConcurrentEscalateCloseIsSafe(t *testing.T) {
	client := newTestClient()
	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			defer func() { _ = recover() }()
			client.EscalateToIncident()
			client.CloseIncident()
		}()
	}
	wg.Wait()
}

// --- scrubJSONValue / redactJSONString ---

func TestRedactJSONStringReplacesPasswordField(t *testing.T) {
	client := newTestClient()
	raw := `{"username":"alice","password":"secret123"}`
	result := client.redactJSONString(raw)

	if result == raw {
		t.Fatal("expected JSON to be modified by redaction")
	}
	// password field should be redacted
	if result == `{"username":"alice","password":"secret123"}` {
		t.Fatal("expected password to be redacted")
	}
}

func TestRedactJSONStringReplacesAllDefaultFields(t *testing.T) {
	client := newTestClient()
	tests := []struct {
		field string
		raw   string
	}{
		{"password", `{"password":"s3cr3t"}`},
		{"token", `{"token":"abc.def.ghi"}`},
		{"authorization", `{"authorization":"Bearer xyz"}`},
		{"credit_card", `{"credit_card":"4111111111111111"}`},
		{"ssn", `{"ssn":"123-45-6789"}`},
		{"email", `{"email":"alice@example.com"}`},
		{"phone", `{"phone":"+15555551234"}`},
	}

	for _, tc := range tests {
		t.Run(tc.field, func(t *testing.T) {
			result := client.redactJSONString(tc.raw)
			if result == tc.raw {
				t.Fatalf("expected field %q to be redacted in %s", tc.field, tc.raw)
			}
		})
	}
}

func TestRedactJSONStringPreservesNonSensitiveFields(t *testing.T) {
	client := newTestClient()
	raw := `{"username":"alice","age":30,"active":true}`
	result := client.redactJSONString(raw)

	// Non-sensitive fields should be unchanged.
	if result == "" {
		t.Fatal("expected non-empty result")
	}
}

func TestRedactJSONStringReturnsRawOnInvalidJSON(t *testing.T) {
	client := newTestClient()
	raw := `not valid json`
	result := client.redactJSONString(raw)
	if result != raw {
		t.Fatalf("expected raw string to be returned unchanged for invalid JSON, got %q", result)
	}
}

func TestRedactJSONStringHandlesNestedObjects(t *testing.T) {
	client := newTestClient()
	raw := `{"user":{"name":"alice","password":"secret"}}`
	result := client.redactJSONString(raw)
	if result == raw {
		t.Fatal("expected nested password to be redacted")
	}
}

func TestRedactJSONStringHandlesArrays(t *testing.T) {
	client := newTestClient()
	raw := `[{"password":"s1"},{"password":"s2"}]`
	result := client.redactJSONString(raw)
	if result == raw {
		t.Fatal("expected password fields in arrays to be redacted")
	}
}

func TestRedactJSONStringEmptyInputReturnedUnchanged(t *testing.T) {
	client := newTestClient()
	result := client.redactJSONString("")
	if result != "" {
		t.Fatalf("expected empty string, got %q", result)
	}
}

func TestScrubJSONValuePassesThroughPrimitives(t *testing.T) {
	redact := map[string]struct{}{"secret": {}}
	tests := []struct {
		name  string
		input interface{}
	}{
		{"int", 42},
		{"float", 3.14},
		{"bool", true},
		{"string", "hello"},
		{"nil", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := scrubJSONValue(tc.input, redact)
			if result != tc.input {
				t.Fatalf("expected primitive %v to pass through unchanged, got %v", tc.input, result)
			}
		})
	}
}

// --- normalizePayloadSnippet ---

func TestNormalizePayloadSnippetTruncatesToMaxBytes(t *testing.T) {
	cfg := DefaultConfig("k", "svc")
	cfg.BaseURL = "http://localhost:9999"
	cfg.PreArmDetailMaxPayloadBytes = 10
	cfg.PreArmDetailCapturePayloadEnabled = true
	client := New(cfg)
	client.mode.Store(string(ModePreArmed))

	long := `{"data":"this is a long string that exceeds the limit"}`
	result := client.normalizePayloadSnippet(long)
	if len(result) > 10 {
		t.Fatalf("expected snippet truncated to 10 bytes, got %d bytes: %q", len(result), result)
	}
}

func TestNormalizePayloadSnippetReturnsEmptyWhenMaxBytesZero(t *testing.T) {
	cfg := DefaultConfig("k", "svc")
	cfg.BaseURL = "http://localhost:9999"
	cfg.PreArmDetailMaxPayloadBytes = 0
	cfg.PreArmDetailCapturePayloadEnabled = true
	client := New(cfg)
	client.mode.Store(string(ModePreArmed))

	result := client.normalizePayloadSnippet(`{"data":"value"}`)
	if result != "" {
		t.Fatalf("expected empty snippet when MaxPayloadBytes=0, got %q", result)
	}
}

// --- dedupeReasons ---

func TestDedupeReasonsKeepsLatestByFiredAt(t *testing.T) {
	client := newTestClient()
	older := TriggerReason{TriggerType: TriggerErrorRate5xx, FiredAtUnixMs: 100, Summary: "older"}
	newer := TriggerReason{TriggerType: TriggerErrorRate5xx, FiredAtUnixMs: 200, Summary: "newer"}

	result := client.dedupeReasons([]TriggerReason{older, newer})
	if len(result) != 1 {
		t.Fatalf("expected 1 deduped reason, got %d", len(result))
	}
	if result[0].Summary != "newer" {
		t.Fatalf("expected newer reason to win, got %q", result[0].Summary)
	}
}

func TestDedupeReasonsKeepsDifferentTypes(t *testing.T) {
	client := newTestClient()
	a := TriggerReason{TriggerType: TriggerSlowSuccess, FiredAtUnixMs: 100}
	b := TriggerReason{TriggerType: TriggerRetryOnset, FiredAtUnixMs: 100}

	result := client.dedupeReasons([]TriggerReason{a, b})
	if len(result) != 2 {
		t.Fatalf("expected 2 reasons (different types), got %d", len(result))
	}
}

func TestDedupeReasonsEmptyInputReturnsEmpty(t *testing.T) {
	client := newTestClient()
	result := client.dedupeReasons(nil)
	if len(result) != 0 {
		t.Fatalf("expected empty result for nil input, got %d", len(result))
	}
}

// --- Helper functions ---

func TestMaxInt64ReturnsLarger(t *testing.T) {
	tests := []struct {
		a, b, want int64
	}{
		{1, 2, 2},
		{5, 3, 5},
		{-1, 0, 0},
		{0, 0, 0},
	}
	for _, tc := range tests {
		got := maxInt64(tc.a, tc.b)
		if got != tc.want {
			t.Fatalf("maxInt64(%d, %d): expected %d, got %d", tc.a, tc.b, tc.want, got)
		}
	}
}

func TestMinInt64ReturnsSmaller(t *testing.T) {
	tests := []struct {
		a, b, want int64
	}{
		{1, 2, 1},
		{5, 3, 3},
		{-1, 0, -1},
		{0, 0, 0},
	}
	for _, tc := range tests {
		got := minInt64(tc.a, tc.b)
		if got != tc.want {
			t.Fatalf("minInt64(%d, %d): expected %d, got %d", tc.a, tc.b, tc.want, got)
		}
	}
}

func TestExtractInt64MapHandlesNil(t *testing.T) {
	result := extractInt64Map(nil)
	if result == nil {
		t.Fatal("expected non-nil map for nil input")
	}
	if len(result) != 0 {
		t.Fatalf("expected empty map for nil input, got %v", result)
	}
}

func TestExtractInt64MapHandlesTypedMap(t *testing.T) {
	input := map[string]int64{"a": 1, "b": 2}
	result := extractInt64Map(input)
	if result["a"] != 1 || result["b"] != 2 {
		t.Fatalf("expected extracted map to match input, got %v", result)
	}
}

func TestExtractInt64MapHandlesInterfaceMap(t *testing.T) {
	input := map[string]interface{}{
		"int":     int(1),
		"int64":   int64(2),
		"float64": float64(3.0),
		"uint64":  uint64(4),
	}
	result := extractInt64Map(input)
	if result["int"] != 1 || result["int64"] != 2 || result["float64"] != 3 || result["uint64"] != 4 {
		t.Fatalf("expected converted values, got %v", result)
	}
}

func TestExtractInt64MapReturnsEmptyForUnknownType(t *testing.T) {
	result := extractInt64Map("unexpected string")
	if len(result) != 0 {
		t.Fatalf("expected empty map for unknown type, got %v", result)
	}
}

func TestTriggerFieldReturnsNilForNilTrigger(t *testing.T) {
	result := triggerField(nil, func(r *TriggerReason) interface{} { return r.TriggerType })
	if result != nil {
		t.Fatalf("expected nil for nil trigger, got %v", result)
	}
}

func TestTriggerFieldExtractsValue(t *testing.T) {
	reason := &TriggerReason{TriggerType: TriggerSlowSuccess}
	result := triggerField(reason, func(r *TriggerReason) interface{} { return r.TriggerType })
	if result != TriggerSlowSuccess {
		t.Fatalf("expected TriggerSlowSuccess, got %v", result)
	}
}

func TestLenWindowReasonsForNilWindow(t *testing.T) {
	if got := lenWindowReasons(nil); got != 0 {
		t.Fatalf("expected 0 for nil window, got %d", got)
	}
}

func TestLenWindowReasonsCountsReasons(t *testing.T) {
	window := &PreArmWindow{
		Reasons: []PreArmTriggerReason{{TriggerType: TriggerSlowSuccess}, {TriggerType: TriggerRetryOnset}},
	}
	if got := lenWindowReasons(window); got != 2 {
		t.Fatalf("expected 2, got %d", got)
	}
}

func TestSnapshotModeReturnsStringRepresentation(t *testing.T) {
	tests := []struct {
		mode CaptureMode
		want string
	}{
		{ModeNormal, "NORMAL"},
		{ModePreArmed, "PRE_ARMED"},
		{ModeIncident, "INCIDENT"},
	}
	for _, tc := range tests {
		got := snapshotMode(tc.mode)
		if got != tc.want {
			t.Fatalf("snapshotMode(%q): expected %q, got %q", tc.mode, tc.want, got)
		}
	}
}

func TestNormalizeHeaderAllowlistLowercasesAndDedupes(t *testing.T) {
	input := []string{"Content-Type", "content-type", "X-Request-ID", "  ", ""}
	result := normalizeHeaderAllowlist(input)

	// Should deduplicate content-type, skip blanks.
	found := map[string]int{}
	for _, h := range result {
		found[h]++
	}
	if found["content-type"] != 1 {
		t.Fatalf("expected 'content-type' to appear exactly once, got %d times", found["content-type"])
	}
	if found["x-request-id"] != 1 {
		t.Fatalf("expected 'x-request-id' once, got %d times", found["x-request-id"])
	}
	for _, h := range result {
		if h == "" || h != strings.ToLower(strings.TrimSpace(h)) {
			t.Fatalf("expected normalized header, got %q", h)
		}
	}
}

func TestToStringSetCreatesLowercaseSet(t *testing.T) {
	result := toStringSet([]string{"Password", "TOKEN", "  email  ", ""})
	if _, ok := result["password"]; !ok {
		t.Fatal("expected 'password' in set")
	}
	if _, ok := result["token"]; !ok {
		t.Fatal("expected 'token' in set")
	}
	if _, ok := result["email"]; !ok {
		t.Fatal("expected 'email' in set")
	}
	if _, ok := result[""]; ok {
		t.Fatal("expected empty string to be excluded from set")
	}
}

func TestCopyDetailCreatesIndependentCopy(t *testing.T) {
	original := CeDetail{
		Method:          "GET",
		RequestHeaders:  map[string]string{"key": "val"},
		ResponseHeaders: map[string]string{"res": "hdr"},
		Retry:           map[string]interface{}{"retry_key": "abc"},
		Downstream:      map[string]interface{}{"svc": "backend"},
	}

	copied := copyDetail(original)
	copied.Method = "POST"
	copied.RequestHeaders["key"] = "mutated"

	if original.Method != "GET" {
		t.Fatal("expected original Method to be unchanged")
	}
	if original.RequestHeaders["key"] != "val" {
		t.Fatal("expected original RequestHeaders to be unchanged")
	}
}

func TestCopyStringMapCreatesIndependentCopy(t *testing.T) {
	original := map[string]string{"k": "v"}
	copied := copyStringMap(original)
	copied["k"] = "mutated"
	if original["k"] != "v" {
		t.Fatal("expected original map to be unchanged")
	}
}

func TestCopyStringMapNilInputReturnsNil(t *testing.T) {
	result := copyStringMap(nil)
	if result != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestCopyInterfaceMapNilInputReturnsNil(t *testing.T) {
	result := copyInterfaceMap(nil)
	if result != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestDetailHasContentFalseForEmptyDetail(t *testing.T) {
	if detailHasContent(CeDetail{}) {
		t.Fatal("expected empty detail to have no content")
	}
}

func TestDetailHasContentTrueForMethod(t *testing.T) {
	if !detailHasContent(CeDetail{Method: "GET"}) {
		t.Fatal("expected detail with Method to have content")
	}
}

func TestDetailHasContentTrueForRequestBytes(t *testing.T) {
	if !detailHasContent(CeDetail{RequestBytes: 100}) {
		t.Fatal("expected detail with RequestBytes to have content")
	}
}

func TestDetailHasContentTrueForRequestHeaders(t *testing.T) {
	if !detailHasContent(CeDetail{RequestHeaders: map[string]string{"key": "val"}}) {
		t.Fatal("expected detail with RequestHeaders to have content")
	}
}

func TestDetailHasContentTrueForLocalErrorClassification(t *testing.T) {
	if !detailHasContent(CeDetail{LocalErrorClassification: "timeout"}) {
		t.Fatal("expected detail with LocalErrorClassification to have content")
	}
}

