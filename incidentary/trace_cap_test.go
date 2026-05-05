// L1 — SDK-side trace cap (cross-SDK spec at docs/specs/l1-trace-cap.md).
// Mirror of the Node and Python SDK acceptance suites; same names, same
// semantics.

package incidentary

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

const (
	tidA = "00000000-0000-4000-8000-0000000000a1"
	tidB = "00000000-0000-4000-8000-0000000000b2"
)

func makeCap(opts ...TraceCapOptions) (*TraceCap, *[]TraceCapEvent) {
	o := TraceCapOptions{ServiceID: "test-svc", Enabled: true}
	if len(opts) > 0 {
		// Allow caller to override fields.
		merged := opts[0]
		if merged.ServiceID == "" {
			merged.ServiceID = "test-svc"
		}
		// Default Enabled=true unless explicitly false.
		if !merged.Enabled && opts[0].Enabled == false {
			// Test caller must use explicit field.
			merged.Enabled = opts[0].Enabled
		}
		o = merged
	}
	var mu sync.Mutex
	events := []TraceCapEvent{}
	o.Hook = func(e TraceCapEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}
	cap := NewTraceCap(o)
	return cap, &events
}

func emitN(cap *TraceCap, traceID string, n int) (accepted, dropped int) {
	for i := 0; i < n; i++ {
		if cap.Observe(traceID).ShouldDrop {
			dropped++
		} else {
			accepted++
		}
	}
	return
}

// ---------------------------------------------------------------------------

func TestTraceCap_Constants(t *testing.T) {
	if SpansPerTraceWarn != 5_000 {
		t.Errorf("SpansPerTraceWarn = %d, want 5_000", SpansPerTraceWarn)
	}
	if SpansPerTraceTruncate != 50_000 {
		t.Errorf("SpansPerTraceTruncate = %d, want 50_000", SpansPerTraceTruncate)
	}
	if SpansPerTraceBreaker != 500_000 {
		t.Errorf("SpansPerTraceBreaker = %d, want 500_000", SpansPerTraceBreaker)
	}
}

func TestTraceCap_UnderWarnThresholdPassesAllSpans(t *testing.T) {
	cap, events := makeCap(TraceCapOptions{Enabled: true})
	a, d := emitN(cap, tidA, int(SpansPerTraceWarn-1))
	if a != int(SpansPerTraceWarn-1) || d != 0 {
		t.Errorf("a=%d d=%d", a, d)
	}
	if len(*events) != 0 {
		t.Errorf("events=%v", *events)
	}
}

func TestTraceCap_AtWarnThresholdFiresOnce(t *testing.T) {
	cap, events := makeCap(TraceCapOptions{Enabled: true})
	a, d := emitN(cap, tidA, int(SpansPerTraceWarn))
	if a != int(SpansPerTraceWarn) || d != 0 {
		t.Errorf("a=%d d=%d", a, d)
	}
	if len(*events) != 1 {
		t.Fatalf("events=%v", *events)
	}
	e := (*events)[0]
	if e.Tier != TraceCapTierWarn || e.TraceID != tidA ||
		e.CumulativeSpanCount != SpansPerTraceWarn || e.ServiceID != "test-svc" {
		t.Errorf("payload mismatch: %+v", e)
	}
	if e.TimestampMs == 0 {
		t.Errorf("timestamp_ms not set")
	}
}

func TestTraceCap_CrossingWarnInOneSpanFiresOnceOnly(t *testing.T) {
	cap, events := makeCap(TraceCapOptions{Enabled: true})
	emitN(cap, tidA, int(SpansPerTraceWarn))
	emitN(cap, tidA, 1_000)
	count := 0
	for _, e := range *events {
		if e.Tier == TraceCapTierWarn {
			count++
		}
	}
	if count != 1 {
		t.Errorf("warn fires=%d, want 1", count)
	}
}

func TestTraceCap_AtTruncateThresholdDropsSubsequent(t *testing.T) {
	cap, events := makeCap(TraceCapOptions{Enabled: true})
	a1, d1 := emitN(cap, tidA, int(SpansPerTraceTruncate))
	if a1 != int(SpansPerTraceTruncate) || d1 != 0 {
		t.Fatalf("phase1: a=%d d=%d", a1, d1)
	}
	a2, d2 := emitN(cap, tidA, 1_000)
	if a2 != 0 || d2 != 1_000 {
		t.Errorf("phase2: a=%d d=%d", a2, d2)
	}
	truncates := 0
	for _, e := range *events {
		if e.Tier == TraceCapTierTruncate {
			truncates++
			if e.CumulativeSpanCount != SpansPerTraceTruncate {
				t.Errorf("truncate count = %d", e.CumulativeSpanCount)
			}
		}
	}
	if truncates != 1 {
		t.Errorf("truncate fires=%d, want 1", truncates)
	}
}

func TestTraceCap_AtBreakerThresholdDropsSubsequent(t *testing.T) {
	cap, events := makeCap(TraceCapOptions{Enabled: true})
	emitN(cap, tidA, int(SpansPerTraceBreaker))
	tiers := map[TraceCapTier]int{}
	for _, e := range *events {
		tiers[e.Tier]++
	}
	if tiers[TraceCapTierWarn] != 1 || tiers[TraceCapTierTruncate] != 1 || tiers[TraceCapTierBreaker] != 1 {
		t.Errorf("tiers=%v", tiers)
	}
	_, d := emitN(cap, tidA, 1)
	if d != 1 {
		t.Errorf("post-breaker drop=%d", d)
	}
	if len(*events) != 3 {
		t.Errorf("events grew unexpectedly: %d", len(*events))
	}
}

func TestTraceCap_DistinctTraceIdsIsolated(t *testing.T) {
	cap, events := makeCap(TraceCapOptions{Enabled: true})
	emitN(cap, tidA, int(SpansPerTraceWarn-1))
	emitN(cap, tidB, int(SpansPerTraceWarn-1))
	if len(*events) != 0 {
		t.Errorf("events=%v", *events)
	}
}

func TestTraceCap_LRUEvictsOldestUnderPressure(t *testing.T) {
	cap, events := makeCap(TraceCapOptions{Enabled: true, MaxTrackedTraces: 8})
	cap.Observe(tidA)
	for i := 0; i < 16; i++ {
		cap.Observe(fmt.Sprintf("evict-%d-%s", i, tidB))
	}
	emitN(cap, tidA, int(SpansPerTraceWarn))
	warns := 0
	for _, e := range *events {
		if e.Tier == TraceCapTierWarn {
			warns++
		}
	}
	if warns != 1 {
		t.Errorf("warns=%d", warns)
	}
}

func TestTraceCap_BreakerBlacklistPersistsAcrossEvictions(t *testing.T) {
	cap, _ := makeCap(TraceCapOptions{Enabled: true, MaxTrackedTraces: 8})
	emitN(cap, tidA, int(SpansPerTraceBreaker))
	for i := 0; i < 16; i++ {
		cap.Observe(fmt.Sprintf("flood-%d", i))
	}
	v := cap.Observe(tidA)
	if !v.ShouldDrop || v.Reason != VerdictReasonBreaker {
		t.Errorf("verdict=%+v", v)
	}
}

func TestTraceCap_OptOutDisablesAllCaps(t *testing.T) {
	cap, events := makeCap(TraceCapOptions{Enabled: false})
	a, d := emitN(cap, tidA, 600_000)
	if a != 600_000 || d != 0 {
		t.Errorf("a=%d d=%d", a, d)
	}
	if len(*events) != 0 {
		t.Errorf("events=%v", *events)
	}
}

func TestTraceCap_HookReceivesCorrectPayload(t *testing.T) {
	var received []TraceCapEvent
	var mu sync.Mutex
	cap := NewTraceCap(TraceCapOptions{
		ServiceID: "svc-payments",
		Enabled:   true,
		Hook: func(e TraceCapEvent) {
			mu.Lock()
			received = append(received, e)
			mu.Unlock()
		},
	})
	emitN(cap, tidA, int(SpansPerTraceWarn))
	if len(received) != 1 {
		t.Fatalf("received=%v", received)
	}
	e := received[0]
	if e.Tier != TraceCapTierWarn || e.TraceID != tidA ||
		e.CumulativeSpanCount != SpansPerTraceWarn || e.ServiceID != "svc-payments" {
		t.Errorf("payload=%+v", e)
	}
}

// Defensive paths

func TestTraceCap_EmptyTraceIDAccepted(t *testing.T) {
	cap, _ := makeCap(TraceCapOptions{Enabled: true})
	if cap.Observe("").ShouldDrop {
		t.Errorf("empty trace_id should not drop")
	}
}

func TestTraceCap_HookErrorsSwallowed(t *testing.T) {
	cap := NewTraceCap(TraceCapOptions{
		ServiceID: "svc",
		Enabled:   true,
		Hook: func(e TraceCapEvent) {
			panic(errors.New("boom"))
		},
	})
	// Crossing warn must not panic out of Observe.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("hook panic propagated: %v", r)
		}
	}()
	emitN(cap, tidA, int(SpansPerTraceWarn))
}

func TestTraceCap_BlacklistItselfIsBounded(t *testing.T) {
	cap, _ := makeCap(TraceCapOptions{Enabled: true, MaxBlacklistedTraces: 4})
	for i := 0; i < 6; i++ {
		emitN(cap, fmt.Sprintf("breaker-%d", i), int(SpansPerTraceBreaker))
	}
	v := cap.Observe("breaker-0")
	if v.ShouldDrop {
		t.Errorf("oldest breaker entry should have been evicted; got drop=%v", v)
	}
}
