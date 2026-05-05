// L1 wiring acceptance — TraceCap integrated with Client.
//
// Mirrors the Node SDK wiring suite. Verifies observe -> drop,
// truncated marker, hook re-binding, dropped_total.

package incidentary

import (
	"testing"
)

const wireTID = "00000000-0000-4000-8000-000000000c11"

func newWireTestClient(t *testing.T, traceCapEnabled bool) *Client {
	t.Helper()
	cfg := DefaultConfig("test-key", "test-svc")
	cfg.APIURL = "http://127.0.0.1:0"
	cfg.Integrations = []Integration{}
	cfg.TraceCapEnabled = traceCapEnabled
	return New(cfg)
}

func newWireTestSkeletonCe(traceID string) *SkeletonCe {
	return &SkeletonCe{
		CeID:       "ce_t",
		TraceID:    traceID,
		ServiceID:  "test-svc",
		WallTsNs:   1,
		Kind:       KindInternal,
		StatusCode: 0,
		DurationNs: 0,
	}
}

func TestL1Wiring_DefaultEnabled_UnderWarn_NoDrops(t *testing.T) {
	c := newWireTestClient(t, true)
	for i := int64(0); i < SpansPerTraceWarn-1; i++ {
		c.WriteEvent(newWireTestSkeletonCe(wireTID))
	}
	if got := c.TraceCapDroppedTotal(); got != 0 {
		t.Fatalf("expected 0 drops under warn, got %d", got)
	}
}

func TestL1Wiring_AboveTruncate_DropsSubsequent(t *testing.T) {
	c := newWireTestClient(t, true)
	for i := int64(0); i < SpansPerTraceTruncate+5; i++ {
		c.WriteEvent(newWireTestSkeletonCe(wireTID))
	}
	if got := c.TraceCapDroppedTotal(); got != 5 {
		t.Fatalf("expected 5 drops past truncate, got %d", got)
	}
}

func TestL1Wiring_TruncatingBoundary_MarksAttribute(t *testing.T) {
	c := newWireTestClient(t, true)
	for i := int64(0); i < SpansPerTraceTruncate; i++ {
		c.WriteEvent(newWireTestSkeletonCe(wireTID))
	}
	// The boundary span (#50_000) is in the buffer with the marker.
	// Iterate the ring buffer and find it.
	c.mu.Lock()
	c.buffer.mu.Lock()
	defer func() {
		c.buffer.mu.Unlock()
		c.mu.Unlock()
	}()

	found := false
	for _, slot := range c.buffer.slots {
		if slot == nil {
			continue
		}
		if attrs, ok := slot.Attributes.(map[string]any); ok {
			if v, ok := attrs["incidentary.trace.truncated_in_sdk"]; ok && v == true {
				found = true
				break
			}
		}
	}
	if !found {
		t.Fatalf("expected boundary span with incidentary.trace.truncated_in_sdk=true marker")
	}
}

func TestL1Wiring_DisabledViaConfig_PassesEverything(t *testing.T) {
	c := newWireTestClient(t, false)
	for i := int64(0); i < SpansPerTraceTruncate+100; i++ {
		c.WriteEvent(newWireTestSkeletonCe(wireTID))
	}
	if got := c.TraceCapDroppedTotal(); got != 0 {
		t.Fatalf("expected 0 drops when cap disabled, got %d", got)
	}
}

func TestL1Wiring_RegisterHook_FiresOnceOnWarn(t *testing.T) {
	c := newWireTestClient(t, true)
	var events []TraceCapEvent
	c.RegisterTraceCapHook(func(e TraceCapEvent) {
		events = append(events, e)
	})

	for i := int64(0); i < SpansPerTraceWarn; i++ {
		c.WriteEvent(newWireTestSkeletonCe(wireTID))
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 hook event at warn, got %d", len(events))
	}
	if events[0].Tier != TraceCapTierWarn {
		t.Fatalf("expected warn tier, got %s", events[0].Tier)
	}
}
