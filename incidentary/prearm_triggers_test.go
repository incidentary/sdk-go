package incidentary

import "testing"

func testTriggerConfig() TriggerEngineConfig {
	return TriggerEngineConfig{
		EnableSlowSuccess:    true,
		EnableInFlightPileup: true,
		EnableRetryOnset:     true,
		SlowSuccess: SlowSuccessConfig{
			MinSlowDurationNs:       250_000_000,
			SlowMultiplier:          2.0,
			EWMAAlpha:               0.1,
			HighRate:                0.2,
			MildRate:                0.1,
			MinSamples:              50,
			Include4xxAsSuccessLike: true,
			MinBaselineNs:           1_000_000,
			MaxBaselineNs:           60_000_000_000,
		},
		InFlight: InFlightConfig{
			MinAbsoluteInFlight: 32,
			BaselineMultiplier:  2.0,
			NetGrowthMin:        16,
			SevereHoldSecs:      3,
			MildHoldSecs:        2,
			BaselineAlpha:       0.05,
		},
		Retry: RetryConfig{
			RetryWindowMs:    5_000,
			HighRate:         0.1,
			MildRate:         0.05,
			MinTotalAttempts: 20,
			TableSize:        4_096,
		},
	}
}

func signal(overrides func(*RequestCompleteSignal)) RequestCompleteSignal {
	s := RequestCompleteSignal{
		Kind:                  KindHTTPIn,
		StatusCode:            200,
		DurationNs:            100_000_000,
		Cancelled:             false,
		TimedOut:              false,
		OutboundRetryKeyHash:  0,
		OutboundRetryQuality:  RetryKeyQualityUnknown,
		ExplicitRetryObserved: nil,
	}
	if overrides != nil {
		overrides(&s)
	}
	return s
}

func TestTriggerEngineSlowSuccessNormalLatencyNoFire(t *testing.T) {
	cfg := testTriggerConfig()
	cfg.EnableInFlightPileup = false
	cfg.EnableRetryOnset = false
	cfg.SlowSuccess.SlowMultiplier = 1.1
	cfg.SlowSuccess.HighRate = 0.15
	engine := NewTriggerEngine(cfg)

	for sec := int64(0); sec < 12; sec++ {
		for i := 0; i < 10; i++ {
			engine.OnRequestComplete(signal(nil), sec, sec*1000)
		}
		decision := engine.Evaluate(ModeNormal, sec*1000, sec, sec*1000)
		if decision != nil {
			t.Fatalf("did not expect decision under healthy latency")
		}
	}

	snapshot := engine.Snapshot(12, 12_000)
	if snapshot.SlowSuccess["severity"] != TriggerSeverity("") {
		t.Fatalf("expected no slow_success severity")
	}
}

func TestTriggerEngineSlowSuccessSpikeSevere(t *testing.T) {
	cfg := testTriggerConfig()
	cfg.EnableInFlightPileup = false
	cfg.EnableRetryOnset = false
	cfg.SlowSuccess.SlowMultiplier = 1.1
	engine := NewTriggerEngine(cfg)

	for sec := int64(0); sec < 10; sec++ {
		for i := 0; i < 10; i++ {
			engine.OnRequestComplete(signal(nil), sec, sec*1000)
		}
		engine.Evaluate(ModeNormal, sec*1000, sec, sec*1000)
	}

	for i := 0; i < 10; i++ {
		engine.OnRequestComplete(signal(func(s *RequestCompleteSignal) {
			s.DurationNs = 2_000_000_000
		}), 10, 10_000)
	}
	engine.Evaluate(ModeNormal, 10_000, 10, 10_000)

	for i := 0; i < 10; i++ {
		engine.OnRequestComplete(signal(func(s *RequestCompleteSignal) {
			s.DurationNs = 2_000_000_000
		}), 11, 11_000)
	}

	decision := engine.Evaluate(ModeNormal, 11_000, 11, 11_000)
	if decision == nil || !decision.ShouldEnterPreArm {
		t.Fatalf("expected severe decision")
	}
	found := false
	for _, reason := range decision.Reasons {
		if reason.TriggerType == TriggerSlowSuccess && reason.Severity == SeveritySevere {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected slow_success severe reason")
	}
}

func TestTriggerEngineInFlightBalancedNoFire(t *testing.T) {
	cfg := testTriggerConfig()
	cfg.EnableSlowSuccess = false
	cfg.EnableRetryOnset = false
	engine := NewTriggerEngine(cfg)

	for sec := int64(0); sec < 12; sec++ {
		for i := 0; i < 20; i++ {
			engine.OnRequestStart(sec)
			engine.OnRequestComplete(signal(nil), sec, sec*1000)
		}
		if decision := engine.Evaluate(ModeNormal, sec*1000, sec, sec*1000); decision != nil {
			t.Fatalf("did not expect decision under balanced load")
		}
	}
}

func TestTriggerEngineInFlightNeverNegative(t *testing.T) {
	cfg := testTriggerConfig()
	cfg.EnableSlowSuccess = false
	cfg.EnableRetryOnset = false
	engine := NewTriggerEngine(cfg)

	for i := 0; i < 100; i++ {
		engine.OnRequestComplete(signal(nil), 1, 1_000)
	}
	snapshot := engine.Snapshot(1, 1_000)
	if snapshot.InFlightPileup["current_in_flight"].(int64) != 0 {
		t.Fatalf("expected current_in_flight=0")
	}
}

func TestTriggerEngineRetryExplicitPathFires(t *testing.T) {
	cfg := testTriggerConfig()
	cfg.EnableSlowSuccess = false
	cfg.EnableInFlightPileup = false
	engine := NewTriggerEngine(cfg)

	for i := 0; i < 25; i++ {
		isRetry := i%2 == 0
		engine.OnRequestComplete(signal(func(s *RequestCompleteSignal) {
			s.Kind = KindHTTPOut
			s.OutboundRetryQuality = RetryKeyQualityExplicit
			s.ExplicitRetryObserved = &isRetry
		}), 1, int64(1000+i))
	}

	decision := engine.Evaluate(ModeNormal, 1_000, 1, 1_000)
	if decision == nil {
		t.Fatalf("expected retry decision")
	}
	found := false
	for _, reason := range decision.Reasons {
		if reason.TriggerType == TriggerRetryOnset {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected retry_onset reason")
	}
}

func TestTriggerEngineArbiterTwoDistinctMild(t *testing.T) {
	cfg := testTriggerConfig()
	cfg.EnableInFlightPileup = false
	cfg.SlowSuccess.HighRate = 0.9
	cfg.SlowSuccess.MildRate = 0.1
	cfg.SlowSuccess.MinSamples = 10
	cfg.SlowSuccess.SlowMultiplier = 1.1
	cfg.Retry.HighRate = 0.9
	cfg.Retry.MildRate = 0.05
	cfg.Retry.MinTotalAttempts = 10
	engine := NewTriggerEngine(cfg)

	for i := 0; i < 20; i++ {
		engine.OnRequestComplete(signal(nil), 1, 1_000)
	}
	for i := 0; i < 3; i++ {
		engine.OnRequestComplete(signal(func(s *RequestCompleteSignal) {
			s.DurationNs = 2_000_000_000
		}), 1, 1_050)
	}
	engine.Evaluate(ModeNormal, 1_000, 1, 1_000)

	for i := 0; i < 20; i++ {
		truthy := true
		engine.OnRequestComplete(signal(func(s *RequestCompleteSignal) {
			s.Kind = KindHTTPOut
			s.OutboundRetryQuality = RetryKeyQualityExplicit
			s.ExplicitRetryObserved = &truthy
		}), 1, int64(1100+i))
	}

	decision := engine.Evaluate(ModeNormal, 1_100, 1, 1_120)
	if decision == nil || !decision.ShouldEnterPreArm {
		t.Fatalf("expected arbiter decision from two mild triggers")
	}
}
