package incidentary

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// --- Config defaults ---

func TestDefaultConfigMaxFlushOverheadMs(t *testing.T) {
	cfg := DefaultConfig("key", "svc")
	if cfg.MaxFlushOverheadMs != 100 {
		t.Fatalf("expected default MaxFlushOverheadMs=100, got %d", cfg.MaxFlushOverheadMs)
	}
}

// --- Initial batch size ---

func TestNewClientInitialBatchSize(t *testing.T) {
	client := newTestClient()
	if client.currentBatchSize != 100 {
		t.Fatalf("expected initial currentBatchSize=100, got %d", client.currentBatchSize)
	}
}

func TestNewClientMaxFlushOverheadMs(t *testing.T) {
	cfg := DefaultConfig("key", "svc")
	cfg.MaxFlushOverheadMs = 200
	cfg.BaseURL = "http://localhost:9999"
	client := New(cfg)
	if client.maxFlushOverheadMs != 200 {
		t.Fatalf("expected maxFlushOverheadMs=200, got %d", client.maxFlushOverheadMs)
	}
}

func TestNewClientDefaultMaxFlushOverheadMs(t *testing.T) {
	cfg := DefaultConfig("key", "svc")
	cfg.MaxFlushOverheadMs = 0 // not set
	cfg.BaseURL = "http://localhost:9999"
	client := New(cfg)
	if client.maxFlushOverheadMs != 100 {
		t.Fatalf("expected default maxFlushOverheadMs=100, got %d", client.maxFlushOverheadMs)
	}
}

// --- EMA computation ---

func TestUpdateFlushLatencyEMAFirstSample(t *testing.T) {
	client := newTestClient()
	client.updateFlushLatency(50.0)
	if client.flushLatencyEma != 50.0 {
		t.Fatalf("expected EMA=50.0 on first sample, got %f", client.flushLatencyEma)
	}
}

func TestUpdateFlushLatencyEMASubsequentSamples(t *testing.T) {
	client := newTestClient()
	// First sample initializes
	client.updateFlushLatency(100.0)
	// Second sample: EMA = 0.3*50 + 0.7*100 = 15 + 70 = 85
	client.updateFlushLatency(50.0)
	expected := 0.3*50.0 + 0.7*100.0
	if math.Abs(client.flushLatencyEma-expected) > 0.01 {
		t.Fatalf("expected EMA=%.2f, got %.2f", expected, client.flushLatencyEma)
	}
}

// --- Batch size adjustment: increase ---

func TestBatchSizeIncreasesWhenLatencyBelowHalfCeiling(t *testing.T) {
	client := newTestClient()
	client.maxFlushOverheadMs = 100
	client.currentBatchSize = 100
	client.flushLatencyEma = -1 // uninitialized
	// Latency is 40ms, ceiling is 100ms, 40 < 50 → increase by 20%
	client.updateFlushLatency(40.0)
	expected := 120 // 100 * 1.2
	if client.currentBatchSize != expected {
		t.Fatalf("expected batch size=%d after low latency, got %d", expected, client.currentBatchSize)
	}
}

// --- Batch size adjustment: decrease ---

func TestBatchSizeDecreasesWhenLatencyAbove90PctCeiling(t *testing.T) {
	client := newTestClient()
	client.maxFlushOverheadMs = 100
	client.currentBatchSize = 100
	client.flushLatencyEma = -1 // uninitialized
	// Latency is 95ms, ceiling is 100ms, 95 > 90 → decrease by 30%
	client.updateFlushLatency(95.0)
	expected := 70 // 100 * 0.7
	if client.currentBatchSize != expected {
		t.Fatalf("expected batch size=%d after high latency, got %d", expected, client.currentBatchSize)
	}
}

// --- Batch size stays same when in middle range ---

func TestBatchSizeUnchangedWhenLatencyInMiddleRange(t *testing.T) {
	client := newTestClient()
	client.maxFlushOverheadMs = 100
	client.currentBatchSize = 100
	client.flushLatencyEma = -1 // uninitialized
	// Latency is 70ms, ceiling is 100ms, 50 <= 70 <= 90 → no change
	client.updateFlushLatency(70.0)
	if client.currentBatchSize != 100 {
		t.Fatalf("expected batch size=100 (unchanged), got %d", client.currentBatchSize)
	}
}

// --- Batch size clamping ---

func TestBatchSizeClampedToMin(t *testing.T) {
	client := newTestClient()
	client.maxFlushOverheadMs = 100
	client.currentBatchSize = 10 // already at min
	client.flushLatencyEma = -1
	// Very high latency → should try to decrease but clamp at 10
	client.updateFlushLatency(99.0)
	if client.currentBatchSize != 10 {
		t.Fatalf("expected batch size clamped to min=10, got %d", client.currentBatchSize)
	}
}

func TestBatchSizeClampedToMax(t *testing.T) {
	client := newTestClient()
	client.maxFlushOverheadMs = 100
	client.currentBatchSize = 5000 // already at max
	client.flushLatencyEma = -1
	// Very low latency → should try to increase but clamp at 5000
	client.updateFlushLatency(10.0)
	if client.currentBatchSize != 5000 {
		t.Fatalf("expected batch size clamped to max=5000, got %d", client.currentBatchSize)
	}
}

// --- ShouldFlushNow ---

func TestShouldFlushNowReturnsTrueWhenBufferExceedsBatchSize(t *testing.T) {
	client := newTestClient()
	client.currentBatchSize = 5
	for i := 0; i < 6; i++ {
		client.buffer.Write(&SkeletonCe{
			CeID:     "ce",
			WallTsNs: time.Now().UnixNano(),
		})
	}
	if !client.ShouldFlushNow() {
		t.Fatal("expected ShouldFlushNow=true when buffer count exceeds batch size")
	}
}

func TestShouldFlushNowReturnsFalseWhenBufferBelowBatchSize(t *testing.T) {
	client := newTestClient()
	client.currentBatchSize = 100
	client.buffer.Write(&SkeletonCe{
		CeID:     "ce",
		WallTsNs: time.Now().UnixNano(),
	})
	if client.ShouldFlushNow() {
		t.Fatal("expected ShouldFlushNow=false when buffer count is below batch size")
	}
}

// --- Telemetry in batch payload ---

func TestFlushToBackendIncludesTelemetryInBatch(t *testing.T) {
	var receivedBody []byte
	var gotRequest int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.StoreInt32(&gotRequest, 1)
		body := make([]byte, 1024*64)
		n, _ := r.Body.Read(body)
		receivedBody = body[:n]
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"accepted":1,"dropped":0}`))
	}))
	defer server.Close()

	cfg := DefaultConfig("key", "svc")
	cfg.BaseURL = server.URL
	cfg.MaxFlushOverheadMs = 200
	client := New(cfg)
	client.flushLatencyEma = 42.5
	client.currentBatchSize = 150

	client.WriteEvent(&SkeletonCe{
		CeID:     "ce-telem",
		TraceID:  "trace-telem",
		WallTsNs: time.Now().UnixNano(),
	})
	client.FlushToBackend(nil)
	time.Sleep(200 * time.Millisecond)

	if atomic.LoadInt32(&gotRequest) == 0 {
		t.Fatal("expected server to receive a request")
	}

	var batch struct {
		Agent struct {
			Telemetry map[string]interface{} `json:"telemetry"`
		} `json:"agent"`
	}
	if err := json.Unmarshal(receivedBody, &batch); err != nil {
		t.Fatalf("failed to parse batch JSON: %v", err)
	}
	if batch.Agent.Telemetry == nil {
		t.Fatal("expected agent.telemetry to be present in batch")
	}

	emaVal, ok := batch.Agent.Telemetry["flush_latency_ema_ms"]
	if !ok {
		t.Fatal("expected flush_latency_ema_ms in telemetry")
	}
	if emaVal.(float64) != 42.5 {
		t.Fatalf("expected flush_latency_ema_ms=42.5, got %v", emaVal)
	}

	batchSizeVal, ok := batch.Agent.Telemetry["current_batch_size"]
	if !ok {
		t.Fatal("expected current_batch_size in telemetry")
	}
	// JSON numbers decode as float64
	if int(batchSizeVal.(float64)) != 150 {
		t.Fatalf("expected current_batch_size=150, got %v", batchSizeVal)
	}
}

// --- FlushToBackend updates EMA after successful flush ---

func TestFlushToBackendUpdatesEMAOnSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Slight delay to have a measurable latency
		time.Sleep(5 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"accepted":1,"dropped":0}`))
	}))
	defer server.Close()

	cfg := DefaultConfig("key", "svc")
	cfg.BaseURL = server.URL
	client := New(cfg)

	client.WriteEvent(&SkeletonCe{
		CeID:     "ce-ema",
		TraceID:  "trace-ema",
		WallTsNs: time.Now().UnixNano(),
	})

	client.FlushToBackend(nil)
	time.Sleep(300 * time.Millisecond)

	client.mu.Lock()
	ema := client.flushLatencyEma
	client.mu.Unlock()

	if ema <= 0 {
		t.Fatalf("expected positive flush latency EMA after successful flush, got %f", ema)
	}
}

// --- GetCurrentBatchSize ---

func TestGetCurrentBatchSize(t *testing.T) {
	client := newTestClient()
	client.currentBatchSize = 250
	if got := client.GetCurrentBatchSize(); got != 250 {
		t.Fatalf("expected GetCurrentBatchSize=250, got %d", got)
	}
}

// --- GetFlushLatencyEma ---

func TestGetFlushLatencyEma(t *testing.T) {
	client := newTestClient()
	client.flushLatencyEma = 33.3
	if got := client.GetFlushLatencyEma(); math.Abs(got-33.3) > 0.01 {
		t.Fatalf("expected GetFlushLatencyEma=33.3, got %f", got)
	}
}

// --- Batch size does not go below min after repeated decreases ---

func TestRepeatedDecreasesClampAtMin(t *testing.T) {
	client := newTestClient()
	client.maxFlushOverheadMs = 100
	client.currentBatchSize = 20
	client.flushLatencyEma = -1
	for i := 0; i < 50; i++ {
		client.updateFlushLatency(99.0)
	}
	if client.currentBatchSize < 10 {
		t.Fatalf("batch size went below minimum: %d", client.currentBatchSize)
	}
}

// --- Batch size does not exceed max after repeated increases ---

func TestRepeatedIncreasesClampAtMax(t *testing.T) {
	client := newTestClient()
	client.maxFlushOverheadMs = 100
	client.currentBatchSize = 4000
	client.flushLatencyEma = -1
	for i := 0; i < 50; i++ {
		client.updateFlushLatency(10.0)
	}
	if client.currentBatchSize > 5000 {
		t.Fatalf("batch size exceeded maximum: %d", client.currentBatchSize)
	}
}
