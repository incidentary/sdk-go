package incidentary

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	defaultRequestHeaderAllowlist  = []string{"content-type", "content-length", "user-agent", "x-request-id"}
	defaultResponseHeaderAllowlist = []string{"content-type", "content-length", "x-request-id"}
	defaultRedactFields            = []string{"password", "token", "authorization", "credit_card", "ssn", "email", "phone"}
)

// Client manages capture mode, trigger engine and ring buffer.
type Client struct {
	ServiceName string

	// Registry holds the integration registry created during New().
	// Integrations wired here are set up once and can be torn down via
	// Teardown().
	Registry *IntegrationRegistry

	mu        sync.Mutex
	mode      atomic.Value // CaptureMode string
	window    *rollingWindow
	buffer    *RingBuffer
	config    Config
	transport *Transport

	triggerEngine *TriggerEngine
	monoOrigin    time.Time

	preArmStartedAt   int64
	preArmTimer       *time.Timer
	lastPreArmEndedAt int64
	preArmWindowSeq   uint64
	preArmAlertedAtNs int64
	preArmRingBufferSeq int64

	activePreArmWindow     *PreArmWindow
	recentPreArmWindows    [8]*PreArmWindow
	recentPreArmWriteIndex int

	preArmEnterTotal  uint64
	preArmBindTotal   uint64
	preArmExpireTotal uint64

	detailRequestHeaderAllowlist  []string
	detailResponseHeaderAllowlist []string
	redactionFields               map[string]struct{}
}

// Config holds client configuration.
type Config struct {
	APIKey      string
	ServiceName string
	APIURL      string
	BaseURL     string
	Environment string
	WorkspaceID string
	TimeoutMs   int64
	OnError     func(error)

	PreArmThresholdHigh float64
	PreArmThresholdLow  float64
	PreArmMinDurationMs int64
	PreArmTTLMs         int64
	PreArmCooldownMs    int64

	BufferCapacity int

	PreArmEnableSlowSuccess bool
	PreArmEnableInFlight    bool
	PreArmEnableRetry       bool

	PreArmSlowMinMs                   int64
	PreArmSlowMultiplier              float64
	PreArmSlowAlpha                   float64
	PreArmSlowSuccessRateHigh         float64
	PreArmSlowSuccessRateMild         float64
	PreArmSlowMinSamples              int
	PreArmSlowInclude4xxAsSuccessLike bool

	PreArmInflightMinAbs       int64
	PreArmInflightMultiplier   float64
	PreArmInflightNetGrowthMin int64
	PreArmInflightHoldSecs     int64
	PreArmInflightMildHoldSecs int64

	PreArmRetryWindowMs  int64
	PreArmRetryRateHigh  float64
	PreArmRetryRateMild  float64
	PreArmRetryMinTotal  int
	PreArmRetryTableSize int

	PreArmDetailCaptureEnabled          bool
	PreArmDetailCapturePayloadEnabled   bool
	PreArmDetailMaxPayloadBytes         int
	PreArmDetailRequestHeaderAllowlist  []string
	PreArmDetailResponseHeaderAllowlist []string
	RedactFields                        []string

	// Integrations lists the integrations to register and set up during New().
	// When nil, DefaultIntegrations() is used. Pass an explicit empty slice to
	// disable all default integrations.
	Integrations []Integration
}

func DefaultConfig(apiKey, serviceName string) Config {
	return Config{
		APIKey:      apiKey,
		ServiceName: serviceName,
		APIURL:      "",
		BaseURL:     "",
		Environment: "production",
		WorkspaceID: "",
		TimeoutMs:   5_000,

		PreArmThresholdHigh: 10.0,
		PreArmThresholdLow:  2.0,
		PreArmMinDurationMs: 60_000,
		PreArmTTLMs:         300_000,
		PreArmCooldownMs:    30_000,
		BufferCapacity:      4_000,

		PreArmEnableSlowSuccess: true,
		PreArmEnableInFlight:    true,
		PreArmEnableRetry:       true,

		PreArmSlowMinMs:                   250,
		PreArmSlowMultiplier:              2.0,
		PreArmSlowAlpha:                   0.1,
		PreArmSlowSuccessRateHigh:         0.20,
		PreArmSlowSuccessRateMild:         0.10,
		PreArmSlowMinSamples:              50,
		PreArmSlowInclude4xxAsSuccessLike: true,

		PreArmInflightMinAbs:       32,
		PreArmInflightMultiplier:   2.0,
		PreArmInflightNetGrowthMin: 16,
		PreArmInflightHoldSecs:     3,
		PreArmInflightMildHoldSecs: 2,

		PreArmRetryWindowMs:  5_000,
		PreArmRetryRateHigh:  0.10,
		PreArmRetryRateMild:  0.05,
		PreArmRetryMinTotal:  20,
		PreArmRetryTableSize: 4_096,

		PreArmDetailCaptureEnabled:          true,
		PreArmDetailCapturePayloadEnabled:   false,
		PreArmDetailMaxPayloadBytes:         4_096,
		PreArmDetailRequestHeaderAllowlist:  append([]string{}, defaultRequestHeaderAllowlist...),
		PreArmDetailResponseHeaderAllowlist: append([]string{}, defaultResponseHeaderAllowlist...),
		RedactFields:                        append([]string{}, defaultRedactFields...),
	}
}

func New(cfg Config) *Client {
	if cfg.BufferCapacity <= 0 {
		cfg.BufferCapacity = 4_000
	}
	if cfg.PreArmTTLMs <= 0 {
		cfg.PreArmTTLMs = 300_000
	}
	if cfg.PreArmMinDurationMs < 0 {
		cfg.PreArmMinDurationMs = 0
	}
	if cfg.PreArmCooldownMs < 0 {
		cfg.PreArmCooldownMs = 0
	}
	if cfg.PreArmRetryTableSize < 128 {
		cfg.PreArmRetryTableSize = 128
	}
	if len(cfg.PreArmDetailRequestHeaderAllowlist) == 0 {
		cfg.PreArmDetailRequestHeaderAllowlist = append([]string{}, defaultRequestHeaderAllowlist...)
	}
	if len(cfg.PreArmDetailResponseHeaderAllowlist) == 0 {
		cfg.PreArmDetailResponseHeaderAllowlist = append([]string{}, defaultResponseHeaderAllowlist...)
	}
	if len(cfg.RedactFields) == 0 {
		cfg.RedactFields = append([]string{}, defaultRedactFields...)
	}

	baseURL := stringsTrim(cfg.BaseURL)
	if baseURL == "" {
		baseURL = stringsTrim(cfg.APIURL)
	}
	timeoutMs := cfg.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 5_000
	}
	transport := NewTransport(baseURL, cfg.APIKey, cfg.ServiceName, cfg.Environment, timeoutMs, cfg.OnError)
	transport.workspaceID = cfg.WorkspaceID

	client := &Client{
		ServiceName: cfg.ServiceName,
		config:      cfg,
		window:      newRollingWindow(10_000, 10),
		buffer:      NewRingBuffer(cfg.BufferCapacity, 60_000),
		transport:   transport,
		monoOrigin:  time.Now(),
		triggerEngine: NewTriggerEngine(TriggerEngineConfig{
			EnableSlowSuccess:    cfg.PreArmEnableSlowSuccess,
			EnableInFlightPileup: cfg.PreArmEnableInFlight,
			EnableRetryOnset:     cfg.PreArmEnableRetry,
			SlowSuccess: SlowSuccessConfig{
				MinSlowDurationNs:       cfg.PreArmSlowMinMs * 1_000_000,
				SlowMultiplier:          cfg.PreArmSlowMultiplier,
				EWMAAlpha:               cfg.PreArmSlowAlpha,
				HighRate:                cfg.PreArmSlowSuccessRateHigh,
				MildRate:                cfg.PreArmSlowSuccessRateMild,
				MinSamples:              cfg.PreArmSlowMinSamples,
				Include4xxAsSuccessLike: cfg.PreArmSlowInclude4xxAsSuccessLike,
				MinBaselineNs:           1_000_000,
				MaxBaselineNs:           60_000_000_000,
			},
			InFlight: InFlightConfig{
				MinAbsoluteInFlight: cfg.PreArmInflightMinAbs,
				BaselineMultiplier:  cfg.PreArmInflightMultiplier,
				NetGrowthMin:        cfg.PreArmInflightNetGrowthMin,
				SevereHoldSecs:      cfg.PreArmInflightHoldSecs,
				MildHoldSecs:        cfg.PreArmInflightMildHoldSecs,
				BaselineAlpha:       0.05,
			},
			Retry: RetryConfig{
				RetryWindowMs:    cfg.PreArmRetryWindowMs,
				HighRate:         cfg.PreArmRetryRateHigh,
				MildRate:         cfg.PreArmRetryRateMild,
				MinTotalAttempts: cfg.PreArmRetryMinTotal,
				TableSize:        cfg.PreArmRetryTableSize,
			},
		}),
		detailRequestHeaderAllowlist:  normalizeHeaderAllowlist(cfg.PreArmDetailRequestHeaderAllowlist),
		detailResponseHeaderAllowlist: normalizeHeaderAllowlist(cfg.PreArmDetailResponseHeaderAllowlist),
		redactionFields:               toStringSet(cfg.RedactFields),
	}
	client.mode.Store(string(ModeNormal))

	integrations := cfg.Integrations
	if integrations == nil {
		integrations = DefaultIntegrations()
	}
	registry := NewIntegrationRegistry(client)
	registry.Register(integrations...)
	_ = registry.SetupAll()
	client.Registry = registry

	return client
}

func (c *Client) GetMode() CaptureMode {
	return CaptureMode(c.mode.Load().(string))
}

func (c *Client) GetCaptureMode() CaptureMode {
	return c.GetMode()
}

func (c *Client) GetPreArmDebugState() PreArmDebugState {
	c.mu.Lock()
	defer c.mu.Unlock()

	clock := c.nowClock()
	snapshot := c.triggerEngine.Snapshot(clock.wallSec, clock.monoMs)

	recent := make([]PreArmWindow, 0, len(c.recentPreArmWindows))
	for _, window := range c.recentPreArmWindows {
		if window == nil {
			continue
		}
		recent = append(recent, copyWindow(*window))
	}

	var active *PreArmWindow
	if c.activePreArmWindow != nil {
		copy := copyWindow(*c.activePreArmWindow)
		active = &copy
	}

	return PreArmDebugState{
		Counters: map[string]uint64{
			"prearm_trigger_slow_success_total":    snapshot.Totals["prearm_trigger_slow_success_total"],
			"prearm_trigger_inflight_pileup_total": snapshot.Totals["prearm_trigger_inflight_pileup_total"],
			"prearm_trigger_retry_onset_total":     snapshot.Totals["prearm_trigger_retry_onset_total"],
			"prearm_enter_total":                   c.preArmEnterTotal,
			"prearm_bind_total":                    c.preArmBindTotal,
			"prearm_expire_total":                  c.preArmExpireTotal,
		},
		Gauges: map[string]interface{}{
			"current_prearm_state":                   snapshotMode(c.GetMode()),
			"current_in_flight":                      snapshot.InFlightPileup["current_in_flight"],
			"slow_success_rate_10s":                  snapshot.SlowSuccess["slow_success_rate_pct"],
			"retry_rate_10s":                         snapshot.RetryOnset["retry_rate_pct"],
			"retry_normalized_url_fallback_rate_10s": snapshot.RetryOnset["normalized_url_fallback_rate_10s"],
			"current_trigger_reasons_count":          lenWindowReasons(c.activePreArmWindow),
		},
		RetryKeyQuality10s:   extractInt64Map(snapshot.RetryOnset["retry_key_quality_10s"]),
		RetryKeyQualityTotal: extractInt64Map(snapshot.RetryOnset["retry_key_quality_total"]),
		LastTrigger: map[string]interface{}{
			"last_trigger_type":           triggerField(snapshot.LastTrigger, func(r *TriggerReason) interface{} { return r.TriggerType }),
			"last_trigger_severity":       triggerField(snapshot.LastTrigger, func(r *TriggerReason) interface{} { return r.Severity }),
			"last_trigger_observed_value": triggerField(snapshot.LastTrigger, func(r *TriggerReason) interface{} { return r.ObservedValue }),
			"last_trigger_threshold":      triggerField(snapshot.LastTrigger, func(r *TriggerReason) interface{} { return r.ThresholdValue }),
			"last_trigger_timestamp":      triggerField(snapshot.LastTrigger, func(r *TriggerReason) interface{} { return r.FiredAtUnixMs }),
		},
		ActivePreArmWindow:    active,
		RecentPreArmWindows:   recent,
		TriggerEngineDisabled: snapshot.Disabled,
	}
}

func (c *Client) RecordRequestStart(kind CeKind) {
	defer func() { _ = recover() }()
	if kind == "" {
		kind = KindHTTPIn
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	clock := c.nowClock()
	c.triggerEngine.OnRequestStart(clock.wallSec)
	c.evaluatePreArmLocked(clock, kind)
}

// RecordRequest records an HTTP call result and evaluates pre-arm. Never panics.
func (c *Client) RecordRequest(statusCode int) {
	c.RecordRequestWithOptions(statusCode, RecordRequestOptions{Kind: KindHTTPIn})
}

func (c *Client) RecordRequestWithOptions(statusCode int, opts RecordRequestOptions) {
	defer func() { _ = recover() }()
	if opts.Kind == "" {
		opts.Kind = KindHTTPIn
	}
	if opts.OutboundRetryQuality == "" {
		opts.OutboundRetryQuality = RetryKeyQualityUnknown
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	clock := c.nowClock()
	c.window.record(statusCode >= 500, clock.wallMs)
	c.triggerEngine.OnRequestComplete(RequestCompleteSignal{
		Kind:                  opts.Kind,
		StatusCode:            statusCode,
		DurationNs:            maxInt64(0, opts.DurationNs),
		Cancelled:             opts.Cancelled,
		TimedOut:              opts.TimedOut,
		OutboundRetryKeyHash:  opts.OutboundRetryKeyHash,
		OutboundRetryQuality:  opts.OutboundRetryQuality,
		ExplicitRetryObserved: opts.ExplicitRetryObserved,
	}, clock.wallSec, clock.monoMs)
	c.evaluatePreArmLocked(clock, opts.Kind)
}

func (c *Client) RecordEvent(eventType IncidentaryEventType, opts RecordEventOptions) {
	defer func() { _ = recover() }()

	status := c.eventTypeDefaultStatus(eventType)
	if opts.Status != nil {
		status = *opts.Status
	}

	traceID := opts.TraceID
	if traceID == "" {
		traceID = randomUUID()
	}

	wallTsNs := opts.WallTsNs
	if wallTsNs <= 0 {
		wallTsNs = time.Now().UnixNano()
	}

	ce := &SkeletonCe{
		CeID:       randomUUID(),
		TraceID:    traceID,
		ParentCeID: opts.ParentCeID,
		ServiceID:  c.ServiceName,
		WallTsNs:   wallTsNs,
		Kind:       c.eventTypeToKind(eventType),
		EventType:  string(eventType),
		EventClass: "causal",
		EventAttrs: opts.EventAttrs,
		Status:     status,
		DurationNs: maxInt64(0, opts.DurationNs),
		SdkVersion: "0.2.0",
	}
	c.WriteEvent(ce)
}

func (c *Client) RecordQueuePublish(opts RecordEventOptions) {
	c.RecordEvent(EventQueuePublish, opts)
}

func (c *Client) RecordQueueConsume(opts RecordEventOptions) {
	c.RecordEvent(EventQueueConsume, opts)
}

func (c *Client) RecordJobStart(opts RecordEventOptions) {
	c.RecordEvent(EventJobStart, opts)
}

func (c *Client) RecordJobEnd(opts RecordEventOptions) {
	c.RecordEvent(EventJobEnd, opts)
}

func (c *Client) RecordWebhookIn(opts RecordEventOptions) {
	c.RecordEvent(EventWebhookIn, opts)
}

func (c *Client) RecordWebhookOut(opts RecordEventOptions) {
	c.RecordEvent(EventWebhookOut, opts)
}

// WriteEvent adds a CE to the ring buffer. Never panics.
func (c *Client) WriteEvent(ce *SkeletonCe) {
	defer func() { _ = recover() }()
	if ce == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buffer.Write(ce)
}

// FlushToBackend flushes ring-buffer content through a transport.
func (c *Client) FlushToBackend(transport *Transport) {
	defer func() { _ = recover() }()
	if transport == nil {
		transport = c.transport
	}
	if transport == nil {
		return
	}

	c.mu.Lock()
	batch := c.annotateBufferedEventsLocked(c.buffer.Flush(time.Now().UnixMilli()))
	mode := c.GetMode()
	c.mu.Unlock()

	transport.UploadBatchWithMode(batch, mode, "")
}

func (c *Client) annotateBufferedEventsLocked(events []*SkeletonCe) []*SkeletonCe {
	if len(events) == 0 {
		return events
	}

	mode := c.GetMode()
	if mode == ModePreArmed {
		for _, event := range events {
			captured := true
			seq := c.preArmRingBufferSeq
			event.CapturedBeforeAlert = &captured
			event.RingBufferSeq = &seq
			c.preArmRingBufferSeq++
		}
		return events
	}

	if mode != ModeIncident || c.preArmAlertedAtNs == 0 {
		return events
	}

	for _, event := range events {
		if event.WallTsNs > c.preArmAlertedAtNs {
			continue
		}
		captured := true
		seq := c.preArmRingBufferSeq
		event.CapturedBeforeAlert = &captured
		event.RingBufferSeq = &seq
		c.preArmRingBufferSeq++
	}

	return events
}

func (c *Client) ShouldCaptureDetailForCurrentMode() bool {
	if !c.config.PreArmDetailCaptureEnabled {
		return false
	}
	return c.GetMode() != ModeNormal
}

func (c *Client) GetDetailRequestHeaderAllowlist() []string {
	return append([]string{}, c.detailRequestHeaderAllowlist...)
}

func (c *Client) GetDetailResponseHeaderAllowlist() []string {
	return append([]string{}, c.detailResponseHeaderAllowlist...)
}

func (c *Client) AttachDetailToEvent(ce *SkeletonCe, detail *CeDetail) *SkeletonCe {
	if ce == nil || detail == nil {
		return ce
	}
	if !c.ShouldCaptureDetailForCurrentMode() {
		return ce
	}

	detailCopy := copyDetail(*detail)
	if detailCopy.PayloadSnippet != "" {
		if !c.config.PreArmDetailCapturePayloadEnabled {
			detailCopy.PayloadSnippet = ""
		} else {
			detailCopy.PayloadSnippet = c.normalizePayloadSnippet(detailCopy.PayloadSnippet)
		}
	}

	if !detailHasContent(detailCopy) {
		return ce
	}

	ceCopy := *ce
	ceCopy.Detail = &detailCopy
	return &ceCopy
}

// EscalateToIncident transitions to INCIDENT mode.
func (c *Client) EscalateToIncident() {
	c.EscalateToIncidentWithID("")
}

func (c *Client) EscalateToIncidentWithID(incidentID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.GetMode() != ModeIncident {
		c.mode.Store(string(ModeIncident))
		c.preArmAlertedAtNs = time.Now().UnixNano()
		c.clearPreArmTimerLocked()
	}

	if c.activePreArmWindow != nil {
		if incidentID != "" {
			c.activePreArmWindow.BoundIncidentID = incidentID
		}
		c.preArmBindTotal++
	}
}

// Teardown calls TeardownAll on the integration registry, invoking any
// cleanup functions registered by integrations. It is safe to call multiple
// times. Call this when the application shuts down gracefully.
func (c *Client) Teardown() {
	if c.Registry != nil {
		c.Registry.TeardownAll()
	}
}

// CloseIncident transitions back to NORMAL mode.
func (c *Client) CloseIncident() {
	c.mu.Lock()
	defer c.mu.Unlock()

	clock := c.nowClock()
	c.mode.Store(string(ModeNormal))
	c.preArmAlertedAtNs = 0
	c.preArmRingBufferSeq = 0
	c.preArmStartedAt = 0
	c.lastPreArmEndedAt = clock.wallMs
	c.clearPreArmTimerLocked()

	if c.activePreArmWindow != nil {
		c.activePreArmWindow.ClosedAtMs = clock.wallMs
		c.activePreArmWindow.CloseReason = "incident_close"
		c.pushRecentWindowLocked(*c.activePreArmWindow)
		c.activePreArmWindow = nil
	}
}

func (c *Client) evaluatePreArmLocked(clock preArmClock, boundaryKind CeKind) {
	rate := c.window.errorRatePct(clock.wallMs)

	mode := c.GetMode()
	if mode == ModeNormal {
		triggerDecision := c.triggerEngine.Evaluate(mode, clock.wallMs, clock.wallSec, clock.monoMs)
		legacyReason := c.buildLegacy5xxReason(rate, clock.wallMs)
		if rate < c.config.PreArmThresholdHigh {
			legacyReason = nil
		}

		if c.isCooldownActiveLocked(clock.wallMs) {
			return
		}

		if legacyReason != nil {
			c.enterPreArmLocked([]TriggerReason{*legacyReason}, clock, "legacy_5xx_"+strings.ToLower(string(boundaryKind)))
			return
		}

		if triggerDecision != nil && triggerDecision.ShouldEnterPreArm {
			c.enterPreArmLocked(triggerDecision.Reasons, clock, "local_trigger_"+strings.ToLower(string(boundaryKind)))
		}
		return
	}

	// Continue evaluating in PRE_ARMED/INCIDENT for telemetry visibility.
	c.triggerEngine.Evaluate(mode, clock.wallMs, clock.wallSec, clock.monoMs)

	if mode == ModePreArmed {
		elapsed := clock.wallMs - c.preArmStartedAt
		minDurationSatisfied := elapsed >= c.config.PreArmMinDurationMs
		ttlExpired := elapsed >= c.config.PreArmTTLMs
		rateRecovered := rate < c.config.PreArmThresholdLow

		if minDurationSatisfied && (ttlExpired || rateRecovered) {
			reason := "error_rate_recovered"
			if ttlExpired {
				reason = "ttl"
			}
			c.exitPreArmLocked(clock, reason)
		}
	}
}

func (c *Client) enterPreArmLocked(reasons []TriggerReason, clock preArmClock, source string) {
	c.mode.Store(string(ModePreArmed))
	c.preArmAlertedAtNs = 0
	c.preArmRingBufferSeq = 0
	c.preArmStartedAt = clock.wallMs
	c.preArmEnterTotal++

	deduped := c.dedupeReasons(reasons)
	window := PreArmWindow{
		ID:              fmt.Sprintf("pw_%d_%d", clock.wallMs, c.preArmWindowSeq),
		StartedAtMs:     clock.wallMs,
		ExpiresAtMs:     clock.wallMs + c.config.PreArmTTLMs,
		Reasons:         deduped,
		BoundIncidentID: "",
	}
	c.preArmWindowSeq++
	c.activePreArmWindow = &window

	delay := time.Duration(minInt64(1_000, maxInt64(1, c.config.PreArmTTLMs))) * time.Millisecond
	c.schedulePreArmRecheckLocked(delay)

	if c.transport != nil {
		reasonsPayload := make([]map[string]interface{}, 0, len(deduped))
		for _, reason := range deduped {
			reasonsPayload = append(reasonsPayload, map[string]interface{}{
				"trigger_type":    reason.TriggerType,
				"severity":        reason.Severity,
				"summary":         reason.Summary,
				"observed_label":  reason.ObservedLabel,
				"threshold_label": reason.ThresholdLabel,
			})
		}
		c.transport.NotifyBackend("pre_arm_start", c.ServiceName, map[string]interface{}{
			"window_id":     window.ID,
			"started_at_ms": window.StartedAtMs,
			"expires_at_ms": window.ExpiresAtMs,
			"source":        source,
			"reasons":       reasonsPayload,
		})
	}
}

func (c *Client) exitPreArmLocked(clock preArmClock, reason string) {
	c.mode.Store(string(ModeNormal))
	c.preArmAlertedAtNs = 0
	c.preArmRingBufferSeq = 0
	c.preArmStartedAt = 0
	c.lastPreArmEndedAt = clock.wallMs
	c.preArmExpireTotal++
	c.clearPreArmTimerLocked()

	if c.activePreArmWindow != nil {
		c.activePreArmWindow.ClosedAtMs = clock.wallMs
		c.activePreArmWindow.CloseReason = reason
		ended := *c.activePreArmWindow
		c.pushRecentWindowLocked(ended)
		c.activePreArmWindow = nil

		if c.transport != nil {
			c.transport.NotifyBackend("pre_arm_end", c.ServiceName, map[string]interface{}{
				"window_id":         ended.ID,
				"started_at_ms":     ended.StartedAtMs,
				"ended_at_ms":       ended.ClosedAtMs,
				"close_reason":      ended.CloseReason,
				"bound_incident_id": ended.BoundIncidentID,
			})
		}
		return
	}

	if c.transport != nil {
		c.transport.NotifyBackend("pre_arm_end", c.ServiceName, map[string]interface{}{"close_reason": reason})
	}
}

func (c *Client) schedulePreArmRecheckLocked(delay time.Duration) {
	c.clearPreArmTimerLocked()
	c.preArmTimer = time.AfterFunc(delay, c.onPreArmTimer)
}

func (c *Client) onPreArmTimer() {
	defer func() { _ = recover() }()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.GetMode() != ModePreArmed {
		return
	}

	clock := c.nowClock()
	c.evaluatePreArmLocked(clock, KindHTTPIn)
	if c.GetMode() == ModePreArmed {
		c.schedulePreArmRecheckLocked(1 * time.Second)
	}
}

func (c *Client) clearPreArmTimerLocked() {
	if c.preArmTimer != nil {
		c.preArmTimer.Stop()
		c.preArmTimer = nil
	}
}

func (c *Client) pushRecentWindowLocked(window PreArmWindow) {
	copy := copyWindow(window)
	c.recentPreArmWindows[c.recentPreArmWriteIndex] = &copy
	c.recentPreArmWriteIndex = (c.recentPreArmWriteIndex + 1) % len(c.recentPreArmWindows)
}

func (c *Client) dedupeReasons(reasons []TriggerReason) []PreArmTriggerReason {
	latest := map[TriggerType]TriggerReason{}
	for _, reason := range reasons {
		previous, ok := latest[reason.TriggerType]
		if !ok || reason.FiredAtUnixMs >= previous.FiredAtUnixMs {
			latest[reason.TriggerType] = reason
		}
	}

	out := make([]PreArmTriggerReason, 0, len(latest))
	for _, reason := range latest {
		out = append(out, PreArmTriggerReason{
			TriggerType:    reason.TriggerType,
			Severity:       reason.Severity,
			ObservedValue:  reason.ObservedValue,
			ThresholdValue: reason.ThresholdValue,
			ObservedLabel:  reason.ObservedLabel,
			ThresholdLabel: reason.ThresholdLabel,
			FiredAtUnixMs:  reason.FiredAtUnixMs,
			Summary:        reason.Summary,
			Details:        copyInterfaceMap(reason.Details),
		})
	}
	return out
}

func (c *Client) buildLegacy5xxReason(ratePct float64, nowMs int64) *TriggerReason {
	reason := TriggerReason{
		TriggerType:    TriggerErrorRate5xx,
		Severity:       SeveritySevere,
		ObservedValue:  ratePct,
		ThresholdValue: c.config.PreArmThresholdHigh,
		ObservedLabel:  fmt.Sprintf("%.2f%% 5xx over 10s", ratePct),
		ThresholdLabel: fmt.Sprintf("%.2f%%", c.config.PreArmThresholdHigh),
		FiredAtUnixMs:  nowMs,
		Summary: fmt.Sprintf(
			"pre-armed due to 5xx spike: %.2f%% errors over last 10s, threshold %.2f%%",
			ratePct,
			c.config.PreArmThresholdHigh,
		),
		Details: map[string]interface{}{
			"error_rate_pct": ratePct,
			"threshold_pct":  c.config.PreArmThresholdHigh,
		},
	}
	return &reason
}

func (c *Client) eventTypeToKind(eventType IncidentaryEventType) CeKind {
	switch eventType {
	case EventHTTPIn, EventWebhookIn, EventGRPCIn:
		return KindHTTPIn
	case EventHTTPOut, EventWebhookOut, EventGRPCOut:
		return KindHTTPOut
	case EventQueuePublish:
		return KindQueuePublish
	case EventQueueConsume:
		return KindQueueConsume
	case EventDBQuery, EventJobStart, EventJobEnd, EventInternalTask:
		return KindInternal
	default:
		return KindInternal
	}
}

func (c *Client) eventTypeDefaultStatus(eventType IncidentaryEventType) int {
	switch eventType {
	case EventHTTPIn, EventHTTPOut, EventWebhookIn, EventWebhookOut:
		return 200
	default:
		return 0
	}
}

func (c *Client) isCooldownActiveLocked(nowMs int64) bool {
	if c.lastPreArmEndedAt <= 0 {
		return false
	}
	return nowMs-c.lastPreArmEndedAt < c.config.PreArmCooldownMs
}

func (c *Client) normalizePayloadSnippet(raw string) string {
	if c.config.PreArmDetailMaxPayloadBytes <= 0 {
		return ""
	}

	redacted := c.redactJSONString(raw)
	if len(redacted) <= c.config.PreArmDetailMaxPayloadBytes {
		return redacted
	}
	return redacted[:c.config.PreArmDetailMaxPayloadBytes]
}

func (c *Client) redactJSONString(raw string) string {
	if len(c.redactionFields) == 0 {
		return raw
	}

	var parsed interface{}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return raw
	}

	scrubbed := scrubJSONValue(parsed, c.redactionFields)
	bytes, err := json.Marshal(scrubbed)
	if err != nil {
		return raw
	}
	return string(bytes)
}

func scrubJSONValue(value interface{}, redact map[string]struct{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, item := range typed {
			if _, ok := redact[strings.ToLower(key)]; ok {
				out[key] = "<redacted>"
			} else {
				out[key] = scrubJSONValue(item, redact)
			}
		}
		return out
	case []interface{}:
		out := make([]interface{}, 0, len(typed))
		for _, item := range typed {
			out = append(out, scrubJSONValue(item, redact))
		}
		return out
	default:
		return typed
	}
}

type preArmClock struct {
	wallMs  int64
	wallSec int64
	monoMs  int64
}

func (c *Client) nowClock() preArmClock {
	now := time.Now()
	wallMs := now.UnixMilli()
	monoMs := time.Since(c.monoOrigin).Milliseconds()
	return preArmClock{wallMs: wallMs, wallSec: wallMs / 1000, monoMs: monoMs}
}

// rollingWindow implements a 10-bucket rolling error rate counter.
type rollingWindow struct {
	buckets   []struct{ total, errors int64 }
	head      int
	bucketMs  int64
	lastAdvMs int64
}

func newRollingWindow(windowMs int64, buckets int) *rollingWindow {
	if buckets <= 0 {
		buckets = 1
	}
	return &rollingWindow{
		buckets:   make([]struct{ total, errors int64 }, buckets),
		bucketMs:  maxInt64(1, windowMs/int64(buckets)),
		lastAdvMs: time.Now().UnixMilli(),
	}
}

func (w *rollingWindow) record(isError bool, nowMs int64) {
	w.advance(nowMs)
	w.buckets[w.head].total++
	if isError {
		w.buckets[w.head].errors++
	}
}

func (w *rollingWindow) errorRatePct(nowMs int64) float64 {
	w.advance(nowMs)
	var total, errors int64
	for _, bucket := range w.buckets {
		total += bucket.total
		errors += bucket.errors
	}
	if total == 0 {
		return 0
	}
	return float64(errors) / float64(total) * 100
}

func (w *rollingWindow) advance(nowMs int64) {
	steps := (nowMs - w.lastAdvMs) / w.bucketMs
	if steps <= 0 {
		return
	}

	n := int(steps)
	if n > len(w.buckets) {
		n = len(w.buckets)
	}

	for i := 0; i < n; i++ {
		w.head = (w.head + 1) % len(w.buckets)
		w.buckets[w.head] = struct{ total, errors int64 }{}
	}
	w.lastAdvMs = nowMs
}

func copyWindow(window PreArmWindow) PreArmWindow {
	reasons := make([]PreArmTriggerReason, 0, len(window.Reasons))
	for _, reason := range window.Reasons {
		reasons = append(reasons, PreArmTriggerReason{
			TriggerType:    reason.TriggerType,
			Severity:       reason.Severity,
			ObservedValue:  reason.ObservedValue,
			ThresholdValue: reason.ThresholdValue,
			ObservedLabel:  reason.ObservedLabel,
			ThresholdLabel: reason.ThresholdLabel,
			FiredAtUnixMs:  reason.FiredAtUnixMs,
			Summary:        reason.Summary,
			Details:        copyInterfaceMap(reason.Details),
		})
	}
	window.Reasons = reasons
	return window
}

func copyDetail(detail CeDetail) CeDetail {
	detail.RequestHeaders = copyStringMap(detail.RequestHeaders)
	detail.ResponseHeaders = copyStringMap(detail.ResponseHeaders)
	detail.Retry = copyInterfaceMap(detail.Retry)
	detail.Downstream = copyInterfaceMap(detail.Downstream)
	return detail
}

func copyStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func copyInterfaceMap(input map[string]interface{}) map[string]interface{} {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func detailHasContent(detail CeDetail) bool {
	if detail.Method != "" || detail.RouteKey != "" || detail.RouteTemplate != "" {
		return true
	}
	if detail.RequestBytes > 0 || detail.ResponseBytes > 0 {
		return true
	}
	if len(detail.RequestHeaders) > 0 || len(detail.ResponseHeaders) > 0 {
		return true
	}
	if len(detail.Retry) > 0 || len(detail.Downstream) > 0 {
		return true
	}
	if detail.LocalErrorClassification != "" || detail.PayloadSnippet != "" {
		return true
	}
	return false
}

func normalizeHeaderAllowlist(input []string) []string {
	out := make([]string, 0, len(input))
	seen := map[string]struct{}{}
	for _, item := range input {
		lower := strings.ToLower(strings.TrimSpace(item))
		if lower == "" {
			continue
		}
		if _, exists := seen[lower]; exists {
			continue
		}
		seen[lower] = struct{}{}
		out = append(out, lower)
	}
	return out
}

func toStringSet(input []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, item := range input {
		normalized := strings.ToLower(strings.TrimSpace(item))
		if normalized != "" {
			out[normalized] = struct{}{}
		}
	}
	return out
}

func extractInt64Map(value interface{}) map[string]int64 {
	if value == nil {
		return map[string]int64{}
	}
	if typed, ok := value.(map[string]int64); ok {
		return typed
	}
	if typed, ok := value.(map[string]interface{}); ok {
		out := make(map[string]int64, len(typed))
		for key, item := range typed {
			switch v := item.(type) {
			case int:
				out[key] = int64(v)
			case int64:
				out[key] = v
			case float64:
				out[key] = int64(v)
			case uint64:
				out[key] = int64(v)
			}
		}
		return out
	}
	return map[string]int64{}
}

func triggerField(trigger *TriggerReason, getter func(*TriggerReason) interface{}) interface{} {
	if trigger == nil {
		return nil
	}
	return getter(trigger)
}

func lenWindowReasons(window *PreArmWindow) int {
	if window == nil {
		return 0
	}
	return len(window.Reasons)
}

func snapshotMode(mode CaptureMode) string {
	return string(mode)
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
