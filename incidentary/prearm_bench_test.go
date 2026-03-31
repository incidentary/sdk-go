package incidentary

import "testing"

func benchmarkConfig() TriggerEngineConfig {
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
			BaselineMultiplier:  2,
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

func benchSignalIn() RequestCompleteSignal {
	return RequestCompleteSignal{
		Kind:       KindHTTPIn,
		StatusCode: 200,
		DurationNs: 120_000_000,
	}
}

func benchSignalOut(hash uint64) RequestCompleteSignal {
	return RequestCompleteSignal{
		Kind:                 KindHTTPOut,
		StatusCode:           200,
		DurationNs:           90_000_000,
		OutboundRetryKeyHash: hash,
		OutboundRetryQuality: RetryKeyQualityRouteTemplate,
	}
}

func BenchmarkTriggerRequestPath1k(b *testing.B) {
	engine := NewTriggerEngine(benchmarkConfig())
	signal := benchSignalIn()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sec := int64(i / 1_000)
		mono := int64(i)
		engine.OnRequestStart(sec)
		engine.OnRequestComplete(signal, sec, mono)
	}
}

func BenchmarkTriggerRequestPath10k(b *testing.B) {
	engine := NewTriggerEngine(benchmarkConfig())
	signal := benchSignalIn()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sec := int64(i / 10_000)
		mono := int64(i)
		engine.OnRequestStart(sec)
		engine.OnRequestComplete(signal, sec, mono)
	}
}

func BenchmarkTriggerRequestPath50k(b *testing.B) {
	engine := NewTriggerEngine(benchmarkConfig())
	signal := benchSignalIn()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sec := int64(i / 50_000)
		mono := int64(i)
		engine.OnRequestStart(sec)
		engine.OnRequestComplete(signal, sec, mono)
	}
}

func BenchmarkRetryTableModerateCollision(b *testing.B) {
	engine := NewTriggerEngine(benchmarkConfig())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sec := int64(i / 1_000)
		mono := int64(i)
		h := uint64(i*2654435761) & 0xFFFFFFFF
		engine.OnRequestComplete(benchSignalOut(h), sec, mono)
	}
}

func BenchmarkRetryTableHighCollision(b *testing.B) {
	engine := NewTriggerEngine(benchmarkConfig())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sec := int64(i / 1_000)
		mono := int64(i)
		engine.OnRequestComplete(benchSignalOut(42), sec, mono)
	}
}

func BenchmarkBucketRotation(b *testing.B) {
	engine := NewTriggerEngine(benchmarkConfig())
	signal := benchSignalIn()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sec := int64(i)
		engine.OnRequestStart(sec)
		engine.OnRequestComplete(signal, sec, int64(i))
	}
}

func BenchmarkTriggerEvaluate(b *testing.B) {
	engine := NewTriggerEngine(benchmarkConfig())
	signal := benchSignalIn()
	for i := 0; i < 10_000; i++ {
		sec := int64(i / 1000)
		engine.OnRequestStart(sec)
		engine.OnRequestComplete(signal, sec, int64(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sec := int64(i / 1000)
		engine.Evaluate(ModeNormal, int64(i), sec, int64(i))
	}
}
