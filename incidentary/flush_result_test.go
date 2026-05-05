package incidentary

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// --- FlushResult struct ---

func TestFlushResultZeroValueHasEmptyCaptureMode(t *testing.T) {
	var result FlushResult
	if result.RequestedCaptureMode != "" {
		t.Fatalf("expected empty RequestedCaptureMode on zero value, got %q", result.RequestedCaptureMode)
	}
}

func TestFlushResultCarriesLatencyMs(t *testing.T) {
	result := FlushResult{LatencyMs: 42.5}
	if result.LatencyMs != 42.5 {
		t.Fatalf("expected LatencyMs=42.5, got %f", result.LatencyMs)
	}
}

// --- Transport propagates X-Capture-Mode-Requested via onFlush callback ---

func TestUploadBatchCallsOnFlushWithCaptureModeWhenHeaderPresent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Capture-Mode-Requested", "FULL")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"accepted":1,"dropped":0}`))
	}))
	defer server.Close()

	tr := NewTransport(server.URL, "key", "svc", "prod", 5_000, nil)

	var mu sync.Mutex
	var gotResult *FlushResult

	events := []*SkeletonCe{{CeID: "ce-cap", WallTsNs: time.Now().UnixNano()}}
	tr.UploadBatchWithModeAndTelemetry(events, ModeNormal, "", nil, func(result FlushResult) {
		mu.Lock()
		defer mu.Unlock()
		gotResult = &result
	})

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		done := gotResult != nil
		mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for onFlush callback")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if gotResult.RequestedCaptureMode != "FULL" {
		t.Fatalf("expected RequestedCaptureMode='FULL', got %q", gotResult.RequestedCaptureMode)
	}
	if gotResult.LatencyMs <= 0 {
		t.Fatalf("expected positive LatencyMs, got %f", gotResult.LatencyMs)
	}
}

func TestUploadBatchCallsOnFlushWithEmptyCaptureModeWhenHeaderAbsent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No X-Capture-Mode-Requested header.
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"accepted":1,"dropped":0}`))
	}))
	defer server.Close()

	tr := NewTransport(server.URL, "key", "svc", "prod", 5_000, nil)

	var mu sync.Mutex
	var gotResult *FlushResult

	events := []*SkeletonCe{{CeID: "ce-noheader", WallTsNs: time.Now().UnixNano()}}
	tr.UploadBatchWithModeAndTelemetry(events, ModeNormal, "", nil, func(result FlushResult) {
		mu.Lock()
		defer mu.Unlock()
		gotResult = &result
	})

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		done := gotResult != nil
		mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for onFlush callback")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if gotResult.RequestedCaptureMode != "" {
		t.Fatalf("expected empty RequestedCaptureMode when header absent, got %q", gotResult.RequestedCaptureMode)
	}
}

func TestUploadBatchDoesNotPanicWithNilOnFlush(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Capture-Mode-Requested", "FULL")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"accepted":1,"dropped":0}`))
	}))
	defer server.Close()

	tr := NewTransport(server.URL, "key", "svc", "prod", 5_000, nil)

	events := []*SkeletonCe{{CeID: "ce-nil-cb", WallTsNs: time.Now().UnixNano()}}
	// nil callback should not panic.
	tr.UploadBatchWithModeAndTelemetry(events, ModeNormal, "", nil, nil)
	time.Sleep(200 * time.Millisecond)
}

// --- Client FlushToBackend propagates capture mode ---

func TestFlushToBackendReceivesCaptureMode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Capture-Mode-Requested", "FULL")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"accepted":1,"dropped":0}`))
	}))
	defer server.Close()

	cfg := DefaultConfig("key", "svc")
	cfg.BaseURL = server.URL
	client := New(cfg)

	client.WriteEvent(&SkeletonCe{
		CeID:     "ce-client-cap",
		TraceID:  "trace-cap",
		WallTsNs: time.Now().UnixNano(),
	})

	client.FlushToBackend(nil)
	// The flush is async; wait for the EMA to be updated as a signal that
	// the onFlush callback has run.
	deadline := time.After(2 * time.Second)
	for {
		client.mu.Lock()
		ema := client.flushLatencyEma
		client.mu.Unlock()
		if ema > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for flush to complete")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// The capture mode logging happens inside the callback. We verify
	// the transport does NOT mutate the client's capture mode — it stays
	// NORMAL since the transport only returns the signal.
	if client.GetMode() != ModeNormal {
		t.Fatalf("expected mode to remain NORMAL (transport must not mutate), got %q", client.GetMode())
	}
}
