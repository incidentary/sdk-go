package incidentary

import (
	"sync"
	"testing"
	"time"
)

// --- NewRingBuffer ---

func TestNewRingBufferWithPositiveCapacityAndWindow(t *testing.T) {
	rb := NewRingBuffer(100, 30_000)
	if rb.capacity != 100 {
		t.Fatalf("expected capacity 100, got %d", rb.capacity)
	}
	if rb.windowMs != 30_000 {
		t.Fatalf("expected windowMs 30000, got %d", rb.windowMs)
	}
}

func TestNewRingBufferWithZeroCapacityDefaultsToOne(t *testing.T) {
	rb := NewRingBuffer(0, 60_000)
	if rb.capacity < 1 {
		t.Fatalf("expected capacity >= 1 for zero input, got %d", rb.capacity)
	}
}

func TestNewRingBufferWithNegativeCapacityDefaultsToOne(t *testing.T) {
	rb := NewRingBuffer(-5, 60_000)
	if rb.capacity < 1 {
		t.Fatalf("expected capacity >= 1 for negative input, got %d", rb.capacity)
	}
}

func TestNewRingBufferWithZeroWindowDefaultsTo60s(t *testing.T) {
	rb := NewRingBuffer(10, 0)
	if rb.windowMs != 60_000 {
		t.Fatalf("expected windowMs 60000 for zero input, got %d", rb.windowMs)
	}
}

func TestNewRingBufferWithNegativeWindowDefaultsTo60s(t *testing.T) {
	rb := NewRingBuffer(10, -1)
	if rb.windowMs != 60_000 {
		t.Fatalf("expected windowMs 60000 for negative input, got %d", rb.windowMs)
	}
}

// --- Write / Flush ---

func TestRingBufferWriteAndFlushReturnsSingleEvent(t *testing.T) {
	rb := NewRingBuffer(10, 60_000)
	ce := &SkeletonCe{CeID: "ce-1", WallTsNs: time.Now().UnixNano()}
	rb.Write(ce)

	result := rb.Flush(time.Now().UnixMilli())
	if len(result) != 1 {
		t.Fatalf("expected 1 event, got %d", len(result))
	}
	if result[0].CeID != "ce-1" {
		t.Fatalf("expected CeID 'ce-1', got %q", result[0].CeID)
	}
}

func TestRingBufferFlushClearsBuffer(t *testing.T) {
	rb := NewRingBuffer(10, 60_000)
	rb.Write(&SkeletonCe{CeID: "ce-1", WallTsNs: time.Now().UnixNano()})

	rb.Flush(time.Now().UnixMilli())
	result := rb.Flush(time.Now().UnixMilli())
	if len(result) != 0 {
		t.Fatalf("expected empty buffer after flush, got %d events", len(result))
	}
}

func TestRingBufferFlushReturnsSortedByWallTsNs(t *testing.T) {
	rb := NewRingBuffer(10, 60_000)
	now := time.Now()

	// Write out of order.
	rb.Write(&SkeletonCe{CeID: "ce-3", WallTsNs: now.Add(200 * time.Millisecond).UnixNano()})
	rb.Write(&SkeletonCe{CeID: "ce-1", WallTsNs: now.Add(0).UnixNano()})
	rb.Write(&SkeletonCe{CeID: "ce-2", WallTsNs: now.Add(100 * time.Millisecond).UnixNano()})

	result := rb.Flush(time.Now().UnixMilli())
	if len(result) != 3 {
		t.Fatalf("expected 3 events, got %d", len(result))
	}
	for i := 1; i < len(result); i++ {
		if result[i].WallTsNs < result[i-1].WallTsNs {
			t.Fatalf("expected events sorted by WallTsNs, but event %d (%d) < event %d (%d)",
				i, result[i].WallTsNs, i-1, result[i-1].WallTsNs)
		}
	}
}

func TestRingBufferFlushFiltersOutOldEvents(t *testing.T) {
	rb := NewRingBuffer(10, 1_000) // 1 second window
	// Write an event with a timestamp in the past (before the window).
	oldTs := time.Now().Add(-2 * time.Second).UnixNano()
	rb.Write(&SkeletonCe{CeID: "old-ce", WallTsNs: oldTs})
	// Write a recent event.
	rb.Write(&SkeletonCe{CeID: "new-ce", WallTsNs: time.Now().UnixNano()})

	result := rb.Flush(time.Now().UnixMilli())
	if len(result) != 1 {
		t.Fatalf("expected 1 recent event, got %d (old events should be filtered)", len(result))
	}
	if result[0].CeID != "new-ce" {
		t.Fatalf("expected 'new-ce', got %q", result[0].CeID)
	}
}

func TestRingBufferOverwritesOldestWhenFull(t *testing.T) {
	capacity := 3
	rb := NewRingBuffer(capacity, 60_000)

	now := time.Now()
	rb.Write(&SkeletonCe{CeID: "ce-1", WallTsNs: now.Add(0).UnixNano()})
	rb.Write(&SkeletonCe{CeID: "ce-2", WallTsNs: now.Add(10 * time.Millisecond).UnixNano()})
	rb.Write(&SkeletonCe{CeID: "ce-3", WallTsNs: now.Add(20 * time.Millisecond).UnixNano()})
	// This write should overwrite ce-1 (oldest).
	rb.Write(&SkeletonCe{CeID: "ce-4", WallTsNs: now.Add(30 * time.Millisecond).UnixNano()})

	result := rb.Flush(time.Now().UnixMilli())
	if len(result) != capacity {
		t.Fatalf("expected %d events (capacity), got %d", capacity, len(result))
	}

	// ce-1 should have been overwritten.
	for _, ce := range result {
		if ce.CeID == "ce-1" {
			t.Fatal("expected 'ce-1' to have been overwritten by ring wrap-around")
		}
	}
}

func TestRingBufferWraparoundMultipleTimes(t *testing.T) {
	capacity := 5
	rb := NewRingBuffer(capacity, 60_000)
	now := time.Now()

	// Write 3x capacity to ensure wrap-around.
	total := 15
	for i := 0; i < total; i++ {
		rb.Write(&SkeletonCe{
			CeID:     randomUUID(),
			WallTsNs: now.Add(time.Duration(i) * time.Millisecond).UnixNano(),
		})
	}

	result := rb.Flush(time.Now().UnixMilli())
	if len(result) != capacity {
		t.Fatalf("expected exactly %d events after wrap-around, got %d", capacity, len(result))
	}
}

func TestRingBufferFlushEmptyBufferReturnsEmpty(t *testing.T) {
	rb := NewRingBuffer(10, 60_000)
	result := rb.Flush(time.Now().UnixMilli())
	if len(result) != 0 {
		t.Fatalf("expected empty result for empty buffer, got %d", len(result))
	}
}

func TestRingBufferCapacityOneCanStoreOneEvent(t *testing.T) {
	rb := NewRingBuffer(1, 60_000)
	rb.Write(&SkeletonCe{CeID: "single", WallTsNs: time.Now().UnixNano()})

	result := rb.Flush(time.Now().UnixMilli())
	if len(result) != 1 {
		t.Fatalf("expected 1 event for capacity-1 buffer, got %d", len(result))
	}
	if result[0].CeID != "single" {
		t.Fatalf("expected CeID 'single', got %q", result[0].CeID)
	}
}

func TestRingBufferCapacityOneOverwritesOnSecondWrite(t *testing.T) {
	rb := NewRingBuffer(1, 60_000)
	now := time.Now()
	rb.Write(&SkeletonCe{CeID: "first", WallTsNs: now.UnixNano()})
	rb.Write(&SkeletonCe{CeID: "second", WallTsNs: now.Add(time.Millisecond).UnixNano()})

	result := rb.Flush(time.Now().UnixMilli())
	if len(result) != 1 {
		t.Fatalf("expected 1 event, got %d", len(result))
	}
	if result[0].CeID != "second" {
		t.Fatalf("expected 'second' to survive overwrite, got %q", result[0].CeID)
	}
}

func TestRingBufferCountNeverExceedsCapacity(t *testing.T) {
	capacity := 4
	rb := NewRingBuffer(capacity, 60_000)

	for i := 0; i < 20; i++ {
		rb.Write(&SkeletonCe{CeID: randomUUID(), WallTsNs: time.Now().UnixNano()})
	}

	rb.mu.Lock()
	if rb.count > rb.capacity {
		t.Fatalf("count %d exceeded capacity %d", rb.count, rb.capacity)
	}
	rb.mu.Unlock()
}

// --- Window boundary conditions ---

func TestRingBufferEventAtExactWindowBoundaryIsIncluded(t *testing.T) {
	windowMs := int64(1_000)
	rb := NewRingBuffer(10, windowMs)
	nowMs := time.Now().UnixMilli()

	// Event exactly at the cutoff boundary: wallTsNs == (nowMs - windowMs) * 1_000_000
	cutoffNs := (nowMs - windowMs) * 1_000_000
	rb.Write(&SkeletonCe{CeID: "boundary", WallTsNs: cutoffNs})

	result := rb.Flush(nowMs)
	if len(result) == 0 {
		t.Fatal("expected event at exact window boundary to be included")
	}
}

func TestRingBufferEventJustBeforeWindowIsFiltered(t *testing.T) {
	windowMs := int64(1_000)
	rb := NewRingBuffer(10, windowMs)
	nowMs := time.Now().UnixMilli()

	// One nanosecond before the cutoff — should be excluded.
	cutoffNs := (nowMs-windowMs)*1_000_000 - 1
	rb.Write(&SkeletonCe{CeID: "just-before", WallTsNs: cutoffNs})

	result := rb.Flush(nowMs)
	if len(result) != 0 {
		t.Fatalf("expected event just before window boundary to be filtered, got %d", len(result))
	}
}

// --- Concurrent access ---

func TestRingBufferConcurrentWriteFlushIsSafe(t *testing.T) {
	rb := NewRingBuffer(100, 60_000)
	const goroutines = 20
	const writesPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				rb.Write(&SkeletonCe{
					CeID:     randomUUID(),
					WallTsNs: time.Now().UnixNano(),
				})
			}
		}()
	}
	wg.Wait()
	// Final flush should not panic.
	_ = rb.Flush(time.Now().UnixMilli())
}

func TestRingBufferConcurrentWriteAndFlushNoPanic(t *testing.T) {
	rb := NewRingBuffer(10, 60_000)
	const writers = 10
	const flushers = 3

	var wg sync.WaitGroup
	wg.Add(writers + flushers)

	for g := 0; g < writers; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				rb.Write(&SkeletonCe{CeID: randomUUID(), WallTsNs: time.Now().UnixNano()})
			}
		}()
	}
	for g := 0; g < flushers; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				_ = rb.Flush(time.Now().UnixMilli())
			}
		}()
	}
	wg.Wait()
}

// --- Large-scale performance ---

func TestRingBufferLargeCapacityWriteAndFlush(t *testing.T) {
	capacity := 10_000
	rb := NewRingBuffer(capacity, 60_000)
	now := time.Now()

	for i := 0; i < capacity; i++ {
		rb.Write(&SkeletonCe{
			CeID:     randomUUID(),
			WallTsNs: now.Add(time.Duration(i) * time.Microsecond).UnixNano(),
		})
	}

	result := rb.Flush(time.Now().UnixMilli())
	if len(result) != capacity {
		t.Fatalf("expected %d events, got %d", capacity, len(result))
	}
	// Verify sorted order.
	for i := 1; i < len(result); i++ {
		if result[i].WallTsNs < result[i-1].WallTsNs {
			t.Fatalf("expected sorted order at index %d", i)
		}
	}
}
