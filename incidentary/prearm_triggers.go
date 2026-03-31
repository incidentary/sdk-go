package incidentary

import (
	"fmt"
	"math"
)

const (
	windowBuckets   = 10
	windowSeconds   = 10
	rateScale       = 10_000
	mildRingSize    = 32
	mildWindowMs    = 10_000
	retryProbeLimit = 8
	maxUint16       = 65_535
)

type TriggerType string

type TriggerSeverity string

const (
	TriggerSlowSuccess    TriggerType = "slow_success"
	TriggerInFlightPileup TriggerType = "in_flight_pileup"
	TriggerRetryOnset     TriggerType = "retry_onset"
	TriggerErrorRate5xx   TriggerType = "error_rate_5xx"

	SeverityMild   TriggerSeverity = "mild"
	SeveritySevere TriggerSeverity = "severe"
)

type TriggerReason struct {
	TriggerType    TriggerType            `json:"trigger_type"`
	Severity       TriggerSeverity        `json:"severity"`
	ObservedValue  float64                `json:"observed_value"`
	ThresholdValue float64                `json:"threshold_value"`
	ObservedLabel  string                 `json:"observed_label"`
	ThresholdLabel string                 `json:"threshold_label"`
	FiredAtUnixMs  int64                  `json:"fired_at_unix_ms"`
	Summary        string                 `json:"summary"`
	Details        map[string]interface{} `json:"details"`
}

type RequestCompleteSignal struct {
	Kind                  CeKind
	StatusCode            int
	DurationNs            int64
	Cancelled             bool
	TimedOut              bool
	OutboundRetryKeyHash  uint64
	OutboundRetryQuality  DownstreamEdgeKeyQuality
	ExplicitRetryObserved *bool
}

type TriggerDecision struct {
	ShouldEnterPreArm bool
	Reasons           []TriggerReason
}

type TriggerEngineSnapshot struct {
	Disabled               map[string]bool
	Totals                 map[string]uint64
	SlowSuccess            map[string]interface{}
	InFlightPileup         map[string]interface{}
	RetryOnset             map[string]interface{}
	LastTrigger            *TriggerReason
	RecentMildTriggerCount int
}

type SlowSuccessConfig struct {
	MinSlowDurationNs       int64
	SlowMultiplier          float64
	EWMAAlpha               float64
	HighRate                float64
	MildRate                float64
	MinSamples              int
	Include4xxAsSuccessLike bool
	MinBaselineNs           int64
	MaxBaselineNs           int64
}

type InFlightConfig struct {
	MinAbsoluteInFlight int64
	BaselineMultiplier  float64
	NetGrowthMin        int64
	SevereHoldSecs      int64
	MildHoldSecs        int64
	BaselineAlpha       float64
}

type RetryConfig struct {
	RetryWindowMs    int64
	HighRate         float64
	MildRate         float64
	MinTotalAttempts int
	TableSize        int
}

type TriggerEngineConfig struct {
	EnableSlowSuccess    bool
	EnableInFlightPileup bool
	EnableRetryOnset     bool
	SlowSuccess          SlowSuccessConfig
	InFlight             InFlightConfig
	Retry                RetryConfig
}

func clampFloat(value, minimum, maximum float64) float64 {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

func toRateBps(rate float64) int64 {
	return int64(clampFloat(math.Round(rate*rateScale), 0, rateScale))
}

func computeRateBps(part, total int64) int64 {
	if total <= 0 {
		return 0
	}
	return (part * rateScale) / total
}

func bpsToPercent(bps int64) float64 {
	return float64(bps) / 100.0
}

func shouldEmitTransition(previous, current TriggerSeverity) bool {
	if current == "" {
		return false
	}
	if previous == "" {
		return true
	}
	return previous == SeverityMild && current == SeveritySevere
}

type slowSuccessTrigger struct {
	cfg SlowSuccessConfig

	total     [windowBuckets]int64
	slow      [windowBuckets]int64
	bucketSec [windowBuckets]int64

	ewmaNs       float64
	lastSeverity TriggerSeverity
}

func newSlowSuccessTrigger(cfg SlowSuccessConfig) *slowSuccessTrigger {
	trigger := &slowSuccessTrigger{cfg: cfg}
	for i := 0; i < windowBuckets; i++ {
		trigger.bucketSec[i] = -1
	}
	return trigger
}

func (t *slowSuccessTrigger) OnRequestComplete(signal RequestCompleteSignal, nowSec int64) {
	if !t.isSuccessLike(signal.StatusCode, signal.Cancelled, signal.TimedOut) {
		return
	}

	duration := int64(clampFloat(float64(signal.DurationNs), float64(t.cfg.MinBaselineNs), float64(t.cfg.MaxBaselineNs)))
	baseline := duration
	if t.ewmaNs > 0 {
		baseline = int64(t.ewmaNs)
	}
	slowThreshold := t.slowThreshold(baseline)
	t.updateEWMA(duration)

	idx := int(nowSec % windowBuckets)
	t.ensureCurrentBucket(idx, nowSec)
	t.total[idx]++
	if signal.DurationNs > slowThreshold {
		t.slow[idx]++
	}
}

func (t *slowSuccessTrigger) Evaluate(nowMs, nowSec int64) *TriggerReason {
	total, slow, rateBps := t.collectWindow(nowSec)
	current := t.classify(total, slow)
	previous := t.lastSeverity
	t.lastSeverity = current

	if !shouldEmitTransition(previous, current) {
		return nil
	}

	thresholdBps := toRateBps(t.cfg.MildRate)
	if current == SeveritySevere {
		thresholdBps = toRateBps(t.cfg.HighRate)
	}

	reason := TriggerReason{
		TriggerType:    TriggerSlowSuccess,
		Severity:       current,
		ObservedValue:  float64(rateBps),
		ThresholdValue: float64(thresholdBps),
		ObservedLabel:  fmt.Sprintf("%.2f%% slow successes over 10s", bpsToPercent(rateBps)),
		ThresholdLabel: fmt.Sprintf("%.2f%%", bpsToPercent(thresholdBps)),
		FiredAtUnixMs:  nowMs,
		Summary: fmt.Sprintf(
			"pre-armed due to slow-success spike: %.2f%% slow successes over last 10s, threshold %.2f%%",
			bpsToPercent(rateBps),
			bpsToPercent(thresholdBps),
		),
		Details: map[string]interface{}{
			"total_success_like_10s":   total,
			"slow_success_like_10s":    slow,
			"slow_success_rate_pct":    bpsToPercent(rateBps),
			"ewma_success_duration_ns": int64(t.ewmaNs),
			"slow_threshold_ns":        t.slowThreshold(int64(t.ewmaNs)),
		},
	}
	return &reason
}

func (t *slowSuccessTrigger) Snapshot(nowSec int64) map[string]interface{} {
	total, slow, rateBps := t.collectWindow(nowSec)
	return map[string]interface{}{
		"severity":                 t.classify(total, slow),
		"total_success_like_10s":   total,
		"slow_success_like_10s":    slow,
		"slow_success_rate_pct":    bpsToPercent(rateBps),
		"ewma_success_duration_ns": int64(t.ewmaNs),
		"slow_threshold_ns":        t.slowThreshold(int64(t.ewmaNs)),
	}
}

func (t *slowSuccessTrigger) Reset() {
	for i := 0; i < windowBuckets; i++ {
		t.total[i] = 0
		t.slow[i] = 0
		t.bucketSec[i] = -1
	}
	t.ewmaNs = 0
	t.lastSeverity = ""
}

func (t *slowSuccessTrigger) collectWindow(nowSec int64) (int64, int64, int64) {
	var total int64
	var slow int64
	minSec := nowSec - (windowSeconds - 1)
	for i := 0; i < windowBuckets; i++ {
		sec := t.bucketSec[i]
		if sec >= minSec && sec <= nowSec {
			total += t.total[i]
			slow += t.slow[i]
		}
	}
	return total, slow, computeRateBps(slow, total)
}

func (t *slowSuccessTrigger) classify(total, slow int64) TriggerSeverity {
	if total < int64(t.cfg.MinSamples) {
		return ""
	}
	rateBps := computeRateBps(slow, total)
	if rateBps >= toRateBps(t.cfg.HighRate) {
		return SeveritySevere
	}
	if rateBps >= toRateBps(t.cfg.MildRate) {
		return SeverityMild
	}
	return ""
}

func (t *slowSuccessTrigger) isSuccessLike(statusCode int, cancelled, timedOut bool) bool {
	if cancelled || timedOut {
		return false
	}
	if statusCode >= 200 && statusCode < 400 {
		return true
	}
	if t.cfg.Include4xxAsSuccessLike && statusCode >= 400 && statusCode < 500 {
		return true
	}
	return false
}

func (t *slowSuccessTrigger) updateEWMA(durationNs int64) {
	if t.ewmaNs <= 0 {
		t.ewmaNs = float64(durationNs)
		return
	}
	next := t.ewmaNs + t.cfg.EWMAAlpha*(float64(durationNs)-t.ewmaNs)
	t.ewmaNs = clampFloat(next, float64(t.cfg.MinBaselineNs), float64(t.cfg.MaxBaselineNs))
}

func (t *slowSuccessTrigger) slowThreshold(baselineNs int64) int64 {
	if baselineNs <= 0 {
		baselineNs = t.cfg.MinBaselineNs
	}
	scaled := int64(math.Round(float64(baselineNs) * t.cfg.SlowMultiplier))
	if scaled < t.cfg.MinSlowDurationNs {
		return t.cfg.MinSlowDurationNs
	}
	return scaled
}

func (t *slowSuccessTrigger) ensureCurrentBucket(index int, nowSec int64) {
	if t.bucketSec[index] == nowSec {
		return
	}
	t.bucketSec[index] = nowSec
	t.total[index] = 0
	t.slow[index] = 0
}

type inFlightPileupTrigger struct {
	cfg InFlightConfig

	started   [windowBuckets]int64
	completed [windowBuckets]int64
	peak      [windowBuckets]int64
	bucketSec [windowBuckets]int64

	currentInFlight    int64
	ewmaPeak           float64
	conditionStartMono int64
	conditionSet       bool
	persistenceMs      int64
	lastSeverity       TriggerSeverity
}

func newInFlightPileupTrigger(cfg InFlightConfig) *inFlightPileupTrigger {
	trigger := &inFlightPileupTrigger{cfg: cfg}
	for i := 0; i < windowBuckets; i++ {
		trigger.bucketSec[i] = -1
	}
	return trigger
}

func (t *inFlightPileupTrigger) OnRequestStart(nowSec int64) {
	idx := int(nowSec % windowBuckets)
	t.ensureCurrentBucket(idx, nowSec)
	t.currentInFlight++
	t.started[idx]++
	if t.currentInFlight > t.peak[idx] {
		t.peak[idx] = t.currentInFlight
	}
}

func (t *inFlightPileupTrigger) OnRequestComplete(nowSec int64) {
	idx := int(nowSec % windowBuckets)
	t.ensureCurrentBucket(idx, nowSec)
	if t.currentInFlight > 0 {
		t.currentInFlight--
	} else {
		t.currentInFlight = 0
	}
	t.completed[idx]++
}

func (t *inFlightPileupTrigger) Evaluate(mode CaptureMode, nowMs, nowSec, monoMs int64) *TriggerReason {
	_, _, netGrowth, peak := t.collectWindow(nowSec)
	threshold := t.threshold()
	conditionMet := t.currentInFlight >= threshold && netGrowth >= t.cfg.NetGrowthMin

	if conditionMet {
		if !t.conditionSet {
			t.conditionSet = true
			t.conditionStartMono = monoMs
		}
		t.persistenceMs = monoMs - t.conditionStartMono
		if t.persistenceMs < 0 {
			t.persistenceMs = 0
		}
	} else {
		t.conditionSet = false
		t.persistenceMs = 0
	}

	current := t.classify(conditionMet, t.persistenceMs)
	previous := t.lastSeverity
	t.lastSeverity = current

	if mode == ModeNormal {
		t.updateBaseline(peak)
	}

	if !shouldEmitTransition(previous, current) {
		return nil
	}

	reason := TriggerReason{
		TriggerType:    TriggerInFlightPileup,
		Severity:       current,
		ObservedValue:  float64(t.currentInFlight),
		ThresholdValue: float64(threshold),
		ObservedLabel:  fmt.Sprintf("%d in-flight", t.currentInFlight),
		ThresholdLabel: fmt.Sprintf("%d in-flight threshold", threshold),
		FiredAtUnixMs:  nowMs,
		Summary: fmt.Sprintf(
			"pre-armed due to in-flight pileup: current=%d, baseline=%d, net growth=%d over last 10s",
			t.currentInFlight,
			int64(math.Round(t.ewmaPeak)),
			netGrowth,
		),
		Details: map[string]interface{}{
			"current_in_flight":   t.currentInFlight,
			"peak_in_flight_10s":  peak,
			"net_growth_10s":      netGrowth,
			"ewma_peak_in_flight": int64(math.Round(t.ewmaPeak)),
			"inflight_threshold":  threshold,
			"persistence_ms":      t.persistenceMs,
		},
	}
	return &reason
}

func (t *inFlightPileupTrigger) Snapshot(nowSec int64) map[string]interface{} {
	_, _, netGrowth, peak := t.collectWindow(nowSec)
	threshold := t.threshold()
	conditionMet := t.currentInFlight >= threshold && netGrowth >= t.cfg.NetGrowthMin
	return map[string]interface{}{
		"severity":            t.classify(conditionMet, t.persistenceMs),
		"current_in_flight":   t.currentInFlight,
		"peak_in_flight_10s":  peak,
		"net_growth_10s":      netGrowth,
		"ewma_peak_in_flight": int64(math.Round(t.ewmaPeak)),
		"inflight_threshold":  threshold,
		"persistence_ms":      t.persistenceMs,
	}
}

func (t *inFlightPileupTrigger) Reset() {
	for i := 0; i < windowBuckets; i++ {
		t.started[i] = 0
		t.completed[i] = 0
		t.peak[i] = 0
		t.bucketSec[i] = -1
	}
	t.currentInFlight = 0
	t.ewmaPeak = 0
	t.conditionStartMono = 0
	t.conditionSet = false
	t.persistenceMs = 0
	t.lastSeverity = ""
}

func (t *inFlightPileupTrigger) collectWindow(nowSec int64) (int64, int64, int64, int64) {
	var started int64
	var completed int64
	var peak int64
	minSec := nowSec - (windowSeconds - 1)
	for i := 0; i < windowBuckets; i++ {
		sec := t.bucketSec[i]
		if sec >= minSec && sec <= nowSec {
			started += t.started[i]
			completed += t.completed[i]
			if t.peak[i] > peak {
				peak = t.peak[i]
			}
		}
	}
	return started, completed, started - completed, peak
}

func (t *inFlightPileupTrigger) ensureCurrentBucket(index int, nowSec int64) {
	if t.bucketSec[index] == nowSec {
		return
	}
	t.bucketSec[index] = nowSec
	t.started[index] = 0
	t.completed[index] = 0
	t.peak[index] = 0
}

func (t *inFlightPileupTrigger) threshold() int64 {
	dynamic := int64(math.Round(t.ewmaPeak * t.cfg.BaselineMultiplier))
	if dynamic < t.cfg.MinAbsoluteInFlight {
		return t.cfg.MinAbsoluteInFlight
	}
	return dynamic
}

func (t *inFlightPileupTrigger) classify(conditionMet bool, persistenceMs int64) TriggerSeverity {
	if !conditionMet {
		return ""
	}
	secs := persistenceMs / 1000
	if secs >= t.cfg.SevereHoldSecs {
		return SeveritySevere
	}
	if secs >= t.cfg.MildHoldSecs {
		return SeverityMild
	}
	return ""
}

func (t *inFlightPileupTrigger) updateBaseline(peak int64) {
	if peak <= 0 {
		return
	}
	if t.ewmaPeak <= 0 {
		t.ewmaPeak = float64(peak)
		return
	}
	t.ewmaPeak = t.ewmaPeak + t.cfg.BaselineAlpha*(float64(peak)-t.ewmaPeak)
}

type retryOnsetTrigger struct {
	cfg RetryConfig

	total     [windowBuckets]int64
	retries   [windowBuckets]int64
	bucketSec [windowBuckets]int64

	qualityBuckets [5][windowBuckets]int64
	qualityTotals  [5]int64

	tableSize int
	tableMask uint64
	keyHash   []uint64
	lastSeen  []int64
	attempts  []uint16
	occupied  []bool

	collisionCount   uint64
	replacementCount uint64
	occupancy        uint64
	lastSeverity     TriggerSeverity
}

func newRetryOnsetTrigger(cfg RetryConfig) *retryOnsetTrigger {
	tableSize := nextPowerOfTwo(maxInt(128, cfg.TableSize))
	trigger := &retryOnsetTrigger{
		cfg:       cfg,
		tableSize: tableSize,
		tableMask: uint64(tableSize - 1),
		keyHash:   make([]uint64, tableSize),
		lastSeen:  make([]int64, tableSize),
		attempts:  make([]uint16, tableSize),
		occupied:  make([]bool, tableSize),
	}
	for i := 0; i < windowBuckets; i++ {
		trigger.bucketSec[i] = -1
	}
	return trigger
}

func (t *retryOnsetTrigger) OnRequestComplete(signal RequestCompleteSignal, nowSec, monoMs int64) {
	if signal.Kind != KindHTTPOut {
		return
	}

	idx := int(nowSec % windowBuckets)
	t.ensureCurrentBucket(idx, nowSec)
	t.total[idx]++
	qualityIndex := qualityToIndex(signal.OutboundRetryQuality)
	t.qualityBuckets[qualityIndex][idx]++
	t.qualityTotals[qualityIndex]++

	retryObserved := false
	if signal.ExplicitRetryObserved != nil {
		retryObserved = *signal.ExplicitRetryObserved
	} else if signal.OutboundRetryKeyHash != 0 {
		retryObserved = t.observeHeuristicRetry(signal.OutboundRetryKeyHash, monoMs)
	}

	if retryObserved {
		t.retries[idx]++
	}
}

func (t *retryOnsetTrigger) Evaluate(nowMs, nowSec int64) *TriggerReason {
	total, retries, rateBps, fallbackRate, _ := t.collectWindow(nowSec)
	current := t.classify(total, retries)
	previous := t.lastSeverity
	t.lastSeverity = current

	if !shouldEmitTransition(previous, current) {
		return nil
	}

	threshold := toRateBps(t.cfg.MildRate)
	if current == SeveritySevere {
		threshold = toRateBps(t.cfg.HighRate)
	}

	reason := TriggerReason{
		TriggerType:    TriggerRetryOnset,
		Severity:       current,
		ObservedValue:  float64(rateBps),
		ThresholdValue: float64(threshold),
		ObservedLabel:  fmt.Sprintf("%.2f%% retries over 10s", bpsToPercent(rateBps)),
		ThresholdLabel: fmt.Sprintf("%.2f%%", bpsToPercent(threshold)),
		FiredAtUnixMs:  nowMs,
		Summary: fmt.Sprintf(
			"pre-armed due to retry onset: %.2f%% retries over last 10s, threshold %.2f%%",
			bpsToPercent(rateBps),
			bpsToPercent(threshold),
		),
		Details: map[string]interface{}{
			"total_outbound_attempts_10s":            total,
			"retry_observations_10s":                 retries,
			"retry_rate_pct":                         bpsToPercent(rateBps),
			"retry_normalized_url_fallback_rate_10s": fallbackRate,
			"retry_table_load_factor":                float64(t.occupancy) / float64(t.tableSize),
			"collision_count":                        t.collisionCount,
			"replacement_count":                      t.replacementCount,
		},
	}
	return &reason
}

func (t *retryOnsetTrigger) Snapshot(nowSec int64) map[string]interface{} {
	total, retries, rateBps, fallbackRate, quality := t.collectWindow(nowSec)
	return map[string]interface{}{
		"severity":                         t.classify(total, retries),
		"total_outbound_attempts_10s":      total,
		"retry_observations_10s":           retries,
		"retry_rate_pct":                   bpsToPercent(rateBps),
		"normalized_url_fallback_rate_10s": fallbackRate,
		"retry_key_quality_10s":            qualityArrayToMap(quality),
		"retry_key_quality_total":          qualityArrayToMap(t.qualityTotals),
		"retry_table_load_factor":          float64(t.occupancy) / float64(t.tableSize),
		"collision_count":                  t.collisionCount,
		"replacement_count":                t.replacementCount,
	}
}

func (t *retryOnsetTrigger) Reset() {
	for i := 0; i < windowBuckets; i++ {
		t.total[i] = 0
		t.retries[i] = 0
		t.bucketSec[i] = -1
		for q := 0; q < 5; q++ {
			t.qualityBuckets[q][i] = 0
		}
	}
	for q := 0; q < 5; q++ {
		t.qualityTotals[q] = 0
	}
	for i := 0; i < t.tableSize; i++ {
		t.keyHash[i] = 0
		t.lastSeen[i] = 0
		t.attempts[i] = 0
		t.occupied[i] = false
	}
	t.collisionCount = 0
	t.replacementCount = 0
	t.occupancy = 0
	t.lastSeverity = ""
}

func (t *retryOnsetTrigger) ensureCurrentBucket(index int, nowSec int64) {
	if t.bucketSec[index] == nowSec {
		return
	}
	t.bucketSec[index] = nowSec
	t.total[index] = 0
	t.retries[index] = 0
	for q := 0; q < 5; q++ {
		t.qualityBuckets[q][index] = 0
	}
}

func (t *retryOnsetTrigger) collectWindow(nowSec int64) (int64, int64, int64, float64, [5]int64) {
	var total int64
	var retries int64
	quality := [5]int64{}
	minSec := nowSec - (windowSeconds - 1)
	for i := 0; i < windowBuckets; i++ {
		sec := t.bucketSec[i]
		if sec >= minSec && sec <= nowSec {
			total += t.total[i]
			retries += t.retries[i]
			for q := 0; q < 5; q++ {
				quality[q] += t.qualityBuckets[q][i]
			}
		}
	}
	fallbackRate := 0.0
	if total > 0 {
		fallbackRate = float64(quality[indexNormalizedURL]) / float64(total)
	}
	return total, retries, computeRateBps(retries, total), fallbackRate, quality
}

func (t *retryOnsetTrigger) classify(total, retries int64) TriggerSeverity {
	if total < int64(t.cfg.MinTotalAttempts) {
		return ""
	}
	rate := computeRateBps(retries, total)
	if rate >= toRateBps(t.cfg.HighRate) {
		return SeveritySevere
	}
	if rate >= toRateBps(t.cfg.MildRate) {
		return SeverityMild
	}
	return ""
}

func (t *retryOnsetTrigger) observeHeuristicRetry(keyHash uint64, nowMs int64) bool {
	start := int(keyHash & t.tableMask)
	emptyIndex := -1
	staleIndex := -1
	stalestIndex := start
	stalestAge := int64(-1)

	for offset := 0; offset < retryProbeLimit; offset++ {
		index := (start + offset) & int(t.tableMask)
		if !t.occupied[index] {
			emptyIndex = index
			break
		}

		age := nowMs - t.lastSeen[index]
		if t.keyHash[index] == keyHash {
			if age <= t.cfg.RetryWindowMs {
				if t.attempts[index] < maxUint16 {
					t.attempts[index]++
				}
				t.lastSeen[index] = nowMs
				return t.attempts[index] >= 2
			}
			t.attempts[index] = 1
			t.lastSeen[index] = nowMs
			return false
		}

		t.collisionCount++
		if age > t.cfg.RetryWindowMs && staleIndex < 0 {
			staleIndex = index
		}
		if age > stalestAge {
			stalestAge = age
			stalestIndex = index
		}
	}

	target := emptyIndex
	if target < 0 {
		target = staleIndex
	}
	if target < 0 {
		target = stalestIndex
	}

	if !t.occupied[target] {
		t.occupancy++
	} else {
		t.replacementCount++
	}

	t.occupied[target] = true
	t.keyHash[target] = keyHash
	t.lastSeen[target] = nowMs
	t.attempts[target] = 1
	return false
}

const (
	indexExplicit = iota
	indexRouteTemplate
	indexLogicalEdge
	indexNormalizedURL
	indexUnknown
)

func qualityToIndex(quality DownstreamEdgeKeyQuality) int {
	switch quality {
	case RetryKeyQualityExplicit:
		return indexExplicit
	case RetryKeyQualityRouteTemplate:
		return indexRouteTemplate
	case RetryKeyQualityLogicalEdge:
		return indexLogicalEdge
	case RetryKeyQualityNormalizedURL:
		return indexNormalizedURL
	default:
		return indexUnknown
	}
}

func qualityArrayToMap[T ~int64 | ~uint64](values [5]T) map[string]T {
	return map[string]T{
		"explicit":       values[indexExplicit],
		"route_template": values[indexRouteTemplate],
		"logical_edge":   values[indexLogicalEdge],
		"normalized_url": values[indexNormalizedURL],
		"unknown":        values[indexUnknown],
	}
}

type TriggerEngine struct {
	cfg TriggerEngineConfig

	slow     *slowSuccessTrigger
	inFlight *inFlightPileupTrigger
	retry    *retryOnsetTrigger

	disabled       [3]bool
	disabledLogged [3]bool

	mildTypes   [mildRingSize]TriggerType
	mildAtMs    [mildRingSize]int64
	mildReasons [mildRingSize]TriggerReason
	mildSet     [mildRingSize]bool
	mildWrite   int

	severeScratch       [3]TriggerReason
	mildDistinctScratch [3]TriggerReason

	totals struct {
		slow     uint64
		inflight uint64
		retry    uint64
	}

	lastTrigger    TriggerReason
	hasLastTrigger bool
}

func NewTriggerEngine(cfg TriggerEngineConfig) *TriggerEngine {
	return &TriggerEngine{
		cfg:      cfg,
		slow:     newSlowSuccessTrigger(cfg.SlowSuccess),
		inFlight: newInFlightPileupTrigger(cfg.InFlight),
		retry:    newRetryOnsetTrigger(cfg.Retry),
	}
}

func (e *TriggerEngine) OnRequestStart(nowSec int64) {
	if e.cfg.EnableInFlightPileup && !e.disabled[1] {
		e.safeOnInFlightStart(nowSec)
	}
}

func (e *TriggerEngine) OnRequestComplete(signal RequestCompleteSignal, nowSec, monoMs int64) {
	if e.cfg.EnableSlowSuccess && !e.disabled[0] {
		e.safeOnSlowComplete(signal, nowSec)
	}
	if e.cfg.EnableInFlightPileup && !e.disabled[1] {
		e.safeOnInFlightComplete(nowSec)
	}
	if e.cfg.EnableRetryOnset && !e.disabled[2] {
		e.safeOnRetryComplete(signal, nowSec, monoMs)
	}
}

func (e *TriggerEngine) Evaluate(mode CaptureMode, nowMs, nowSec, monoMs int64) *TriggerDecision {
	severeCount := 0
	if e.cfg.EnableSlowSuccess && !e.disabled[0] {
		if reason := e.slow.Evaluate(nowMs, nowSec); reason != nil {
			e.onTriggerFired(*reason, monoMs, &severeCount)
		}
	}

	if e.cfg.EnableInFlightPileup && !e.disabled[1] {
		if reason := e.inFlight.Evaluate(mode, nowMs, nowSec, monoMs); reason != nil {
			e.onTriggerFired(*reason, monoMs, &severeCount)
		}
	}

	if e.cfg.EnableRetryOnset && !e.disabled[2] {
		if reason := e.retry.Evaluate(nowMs, nowSec); reason != nil {
			e.onTriggerFired(*reason, monoMs, &severeCount)
		}
	}

	if severeCount > 0 {
		return &TriggerDecision{
			ShouldEnterPreArm: true,
			Reasons:           cloneReasonSlice(e.severeScratch[:severeCount]),
		}
	}

	mildCount := e.collectDistinctMildCount(monoMs)
	if mildCount >= 2 {
		return &TriggerDecision{
			ShouldEnterPreArm: true,
			Reasons:           cloneReasonSlice(e.mildDistinctScratch[:mildCount]),
		}
	}

	return nil
}

func (e *TriggerEngine) Snapshot(nowSec, monoMs int64) TriggerEngineSnapshot {
	mildCount := e.collectDistinctMildCount(monoMs)
	var last *TriggerReason
	if e.hasLastTrigger {
		copied := copyReason(e.lastTrigger)
		last = &copied
	}

	return TriggerEngineSnapshot{
		Disabled: map[string]bool{
			"slow_success":     e.disabled[0],
			"in_flight_pileup": e.disabled[1],
			"retry_onset":      e.disabled[2],
		},
		Totals: map[string]uint64{
			"prearm_trigger_slow_success_total":    e.totals.slow,
			"prearm_trigger_inflight_pileup_total": e.totals.inflight,
			"prearm_trigger_retry_onset_total":     e.totals.retry,
		},
		SlowSuccess:            e.slow.Snapshot(nowSec),
		InFlightPileup:         e.inFlight.Snapshot(nowSec),
		RetryOnset:             e.retry.Snapshot(nowSec),
		LastTrigger:            last,
		RecentMildTriggerCount: mildCount,
	}
}

func (e *TriggerEngine) ResetForTests() {
	e.slow.Reset()
	e.inFlight.Reset()
	e.retry.Reset()
	e.disabled = [3]bool{}
	e.disabledLogged = [3]bool{}
	e.mildTypes = [mildRingSize]TriggerType{}
	e.mildAtMs = [mildRingSize]int64{}
	e.mildReasons = [mildRingSize]TriggerReason{}
	e.mildSet = [mildRingSize]bool{}
	e.mildWrite = 0
	e.severeScratch = [3]TriggerReason{}
	e.mildDistinctScratch = [3]TriggerReason{}
	e.totals = struct {
		slow     uint64
		inflight uint64
		retry    uint64
	}{}
	e.lastTrigger = TriggerReason{}
	e.hasLastTrigger = false
}

func (e *TriggerEngine) collectDistinctMildCount(monoMs int64) int {
	var (
		slowSet, inFlightSet, retrySet          bool
		slowAt, inFlightAt, retryAt             int64 = -1, -1, -1
		slowReason, inFlightReason, retryReason TriggerReason
	)

	for i := 0; i < mildRingSize; i++ {
		if !e.mildSet[i] {
			continue
		}
		at := e.mildAtMs[i]
		if monoMs-at > mildWindowMs {
			continue
		}
		reason := e.mildReasons[i]
		switch reason.TriggerType {
		case TriggerSlowSuccess:
			if !slowSet || at >= slowAt {
				slowSet = true
				slowAt = at
				slowReason = reason
			}
		case TriggerInFlightPileup:
			if !inFlightSet || at >= inFlightAt {
				inFlightSet = true
				inFlightAt = at
				inFlightReason = reason
			}
		case TriggerRetryOnset:
			if !retrySet || at >= retryAt {
				retrySet = true
				retryAt = at
				retryReason = reason
			}
		}
	}

	count := 0
	if slowSet {
		e.mildDistinctScratch[count] = slowReason
		count++
	}
	if inFlightSet {
		e.mildDistinctScratch[count] = inFlightReason
		count++
	}
	if retrySet {
		e.mildDistinctScratch[count] = retryReason
		count++
	}
	return count
}

func (e *TriggerEngine) onTriggerFired(reason TriggerReason, monoMs int64, severeCount *int) {
	e.lastTrigger = reason
	e.hasLastTrigger = true

	switch reason.TriggerType {
	case TriggerSlowSuccess:
		e.totals.slow++
	case TriggerInFlightPileup:
		e.totals.inflight++
	case TriggerRetryOnset:
		e.totals.retry++
	}

	if reason.Severity == SeveritySevere {
		index := *severeCount
		if index < len(e.severeScratch) {
			e.severeScratch[index] = reason
			*severeCount = index + 1
		}
		return
	}

	index := e.mildWrite
	e.mildTypes[index] = reason.TriggerType
	e.mildAtMs[index] = monoMs
	e.mildReasons[index] = reason
	e.mildSet[index] = true
	e.mildWrite = (e.mildWrite + 1) % mildRingSize
}

func (e *TriggerEngine) disableTrigger(index int, recoverErr interface{}) {
	e.disabled[index] = true
	if !e.disabledLogged[index] {
		e.disabledLogged[index] = true
		fmt.Printf("[incidentary] disabling trigger index=%d after internal failure: %v\n", index, recoverErr)
	}
}

func (e *TriggerEngine) safeOnInFlightStart(nowSec int64) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			e.disableTrigger(1, recoverErr)
		}
	}()
	e.inFlight.OnRequestStart(nowSec)
}

func (e *TriggerEngine) safeOnSlowComplete(signal RequestCompleteSignal, nowSec int64) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			e.disableTrigger(0, recoverErr)
		}
	}()
	e.slow.OnRequestComplete(signal, nowSec)
}

func (e *TriggerEngine) safeOnInFlightComplete(nowSec int64) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			e.disableTrigger(1, recoverErr)
		}
	}()
	e.inFlight.OnRequestComplete(nowSec)
}

func (e *TriggerEngine) safeOnRetryComplete(signal RequestCompleteSignal, nowSec, monoMs int64) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			e.disableTrigger(2, recoverErr)
		}
	}()
	e.retry.OnRequestComplete(signal, nowSec, monoMs)
}

func copyReason(reason TriggerReason) TriggerReason {
	copiedDetails := map[string]interface{}{}
	for key, value := range reason.Details {
		copiedDetails[key] = value
	}
	reason.Details = copiedDetails
	return reason
}

func cloneReasonSlice(reasons []TriggerReason) []TriggerReason {
	out := make([]TriggerReason, len(reasons))
	for i, reason := range reasons {
		out[i] = copyReason(reason)
	}
	return out
}

func nextPowerOfTwo(value int) int {
	if value <= 1 {
		return 1
	}
	size := 1
	for size < value {
		size <<= 1
	}
	return size
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
