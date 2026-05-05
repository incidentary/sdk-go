// L1 — SDK-side per-trace span cap.
//
// Cross-SDK spec: docs/specs/l1-trace-cap.md (in the main incidentary repo).
// Threshold parity is mandatory:
//   - apps/api/src/billing/trace_meter.rs (Rust API)
//   - processor/incidentaryprocessor/trace_breaker.go (Bridge)
//   - SDKs: Node, Python, .NET share these same constants.
//
// Catches single-process runaway traces at the source. Memory bounded
// by an LRU on counters (default 1024) and breaker blacklist (256).

package incidentary

import (
	"container/list"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Threshold constants — DO NOT change in isolation. Must stay in sync
// with apps/api/src/billing/trace_meter.rs::SPANS_PER_TRACE_*.
const (
	SpansPerTraceWarn     int64 = 5_000
	SpansPerTraceTruncate int64 = 50_000
	SpansPerTraceBreaker  int64 = 500_000

	defaultMaxTrackedTraces     = 1_024
	defaultMaxBlacklistedTraces = 256
)

// TraceCapTier identifies which threshold a trace has crossed.
type TraceCapTier string

const (
	TraceCapTierWarn     TraceCapTier = "warn"
	TraceCapTierTruncate TraceCapTier = "truncate"
	TraceCapTierBreaker  TraceCapTier = "breaker"
)

// TraceCapEvent is the structured payload emitted on every tier transition.
type TraceCapEvent struct {
	Tier                TraceCapTier `json:"tier"`
	TraceID             string       `json:"trace_id"`
	CumulativeSpanCount int64        `json:"cumulative_span_count"`
	ServiceID           string       `json:"service_id"`
	TimestampMs         int64        `json:"timestamp_ms"`
}

// TraceCapHook receives tier transition events.
type TraceCapHook func(TraceCapEvent)

// VerdictReason is set when ShouldDrop is true; it identifies why.
type VerdictReason string

const (
	VerdictReasonNone     VerdictReason = ""
	VerdictReasonTruncate VerdictReason = "truncate"
	VerdictReasonBreaker  VerdictReason = "breaker"
)

// VerdictTier is set when ShouldDrop is false; identifies the trace's
// status for that span (none/warn/truncating).
type VerdictTier string

const (
	VerdictTierNone       VerdictTier = "none"
	VerdictTierWarn       VerdictTier = "warn"
	VerdictTierTruncating VerdictTier = "truncating"
)

// Verdict is the result of TraceCap.Observe(): drop or accept.
type Verdict struct {
	ShouldDrop bool
	Tier       VerdictTier
	Reason     VerdictReason
}

var (
	acceptNone       = Verdict{ShouldDrop: false, Tier: VerdictTierNone}
	acceptWarn       = Verdict{ShouldDrop: false, Tier: VerdictTierWarn}
	acceptTruncating = Verdict{ShouldDrop: false, Tier: VerdictTierTruncating}
	dropTruncate     = Verdict{ShouldDrop: true, Reason: VerdictReasonTruncate}
	dropBreaker      = Verdict{ShouldDrop: true, Reason: VerdictReasonBreaker}
)

// TraceCapOptions configures a TraceCap instance.
type TraceCapOptions struct {
	ServiceID            string
	Hook                 TraceCapHook
	Enabled              bool
	MaxTrackedTraces     int
	MaxBlacklistedTraces int
}

// boundedLRU is a goroutine-safe LRU using container/list as the order
// queue and a map for O(1) lookup.
type boundedLRU struct {
	mu     sync.Mutex
	max    int
	order  *list.List
	lookup map[string]*list.Element
}

type lruEntry struct {
	key   string
	value int64
}

func newBoundedLRU(max int) *boundedLRU {
	return &boundedLRU{
		max:    max,
		order:  list.New(),
		lookup: make(map[string]*list.Element, max),
	}
}

func (l *boundedLRU) has(key string) bool {
	l.mu.Lock()
	_, ok := l.lookup[key]
	l.mu.Unlock()
	return ok
}

func (l *boundedLRU) get(key string) (int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.lookup[key]; ok {
		l.order.MoveToBack(e)
		return e.Value.(*lruEntry).value, true
	}
	return 0, false
}

func (l *boundedLRU) set(key string, value int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.lookup[key]; ok {
		e.Value.(*lruEntry).value = value
		l.order.MoveToBack(e)
		return
	}
	entry := &lruEntry{key: key, value: value}
	e := l.order.PushBack(entry)
	l.lookup[key] = e
	if l.order.Len() > l.max {
		oldest := l.order.Front()
		if oldest != nil {
			l.order.Remove(oldest)
			delete(l.lookup, oldest.Value.(*lruEntry).key)
		}
	}
}

func (l *boundedLRU) delete(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.lookup[key]; ok {
		l.order.Remove(e)
		delete(l.lookup, key)
		return true
	}
	return false
}

func (l *boundedLRU) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.order.Len()
}

// boundedSet is the same shape as boundedLRU but for marker-only
// (membership) entries — used for blacklist + transitions emitted.
type boundedSet struct {
	*boundedLRU
}

func newBoundedSet(max int) *boundedSet {
	return &boundedSet{newBoundedLRU(max)}
}

func (s *boundedSet) add(key string) { s.set(key, 1) }

// defaultHook writes the structured event as a single JSON line on stderr.
func defaultHook(event TraceCapEvent) {
	b, err := json.Marshal(struct {
		Event string `json:"event"`
		TraceCapEvent
	}{
		Event:         "incidentary_trace_cap_tier",
		TraceCapEvent: event,
	})
	if err != nil {
		return
	}
	fmt.Fprintln(os.Stderr, string(b))
}

// TraceCap enforces the per-trace span cap for a single SDK instance.
// Methods are goroutine-safe.
type TraceCap struct {
	serviceID   string
	hook        TraceCapHook
	hookMu      sync.RWMutex
	enabled     bool
	counters    *boundedLRU
	blacklist   *boundedSet
	transitions *boundedSet
}

// NewTraceCap constructs a TraceCap with the given options.
func NewTraceCap(opts TraceCapOptions) *TraceCap {
	hook := opts.Hook
	if hook == nil {
		hook = defaultHook
	}
	maxTraces := opts.MaxTrackedTraces
	if maxTraces <= 0 {
		maxTraces = defaultMaxTrackedTraces
	}
	maxBlacklist := opts.MaxBlacklistedTraces
	if maxBlacklist <= 0 {
		maxBlacklist = defaultMaxBlacklistedTraces
	}
	return &TraceCap{
		serviceID:   opts.ServiceID,
		hook:        hook,
		enabled:     opts.Enabled,
		counters:    newBoundedLRU(maxTraces),
		blacklist:   newBoundedSet(maxBlacklist),
		transitions: newBoundedSet(maxTraces * 3),
	}
}

// Observe applies the cap to a single span attempt. Side effects:
//   - increments the per-trace counter (even on drop, so we eventually
//     trip the breaker)
//   - emits the tier-transition hook exactly once per (trace_id, tier)
func (t *TraceCap) Observe(traceID string) Verdict {
	if t == nil || !t.enabled || traceID == "" {
		return acceptNone
	}

	if t.blacklist.has(traceID) {
		return dropBreaker
	}

	prior, _ := t.counters.get(traceID)
	next := prior + 1
	t.counters.set(traceID, next)

	if next >= SpansPerTraceBreaker {
		if next == SpansPerTraceBreaker {
			t.blacklist.add(traceID)
			t.counters.delete(traceID)
			t.emitOnce(traceID, TraceCapTierBreaker, next)
		}
		return dropBreaker
	}
	if next > SpansPerTraceTruncate {
		return dropTruncate
	}
	if next == SpansPerTraceTruncate {
		t.emitOnce(traceID, TraceCapTierTruncate, next)
		return acceptTruncating
	}
	if next == SpansPerTraceWarn {
		t.emitOnce(traceID, TraceCapTierWarn, next)
		return acceptWarn
	}
	return acceptNone
}

// SetHook replaces the tier-transition hook. Safe to call after the
// cap has begun observing spans.
func (t *TraceCap) SetHook(hook TraceCapHook) {
	if hook == nil {
		hook = defaultHook
	}
	t.hookMu.Lock()
	t.hook = hook
	t.hookMu.Unlock()
}

// TrackedTraceCount returns the active counter map size (test seam).
func (t *TraceCap) TrackedTraceCount() int { return t.counters.size() }

// BlacklistedTraceCount returns the breaker blacklist size (test seam).
func (t *TraceCap) BlacklistedTraceCount() int { return t.blacklist.size() }

func (t *TraceCap) emitOnce(traceID string, tier TraceCapTier, count int64) {
	key := traceID + "|" + string(tier)
	if t.transitions.has(key) {
		return
	}
	t.transitions.add(key)

	t.hookMu.RLock()
	hook := t.hook
	t.hookMu.RUnlock()

	defer func() {
		// Hook is customer-controllable; never propagate.
		_ = recover()
	}()
	hook(TraceCapEvent{
		Tier:                tier,
		TraceID:             traceID,
		CumulativeSpanCount: count,
		ServiceID:           t.serviceID,
		TimestampMs:         time.Now().UnixMilli(),
	})
}
