package incidentary

import "sync"

// RingBuffer is a fixed-allocation circular buffer for SkeletonCe events.
// All operations are goroutine-safe.
type RingBuffer struct {
	mu       sync.Mutex
	slots    []*SkeletonCe
	head     int
	count    int
	capacity int
	windowMs int64
}

func NewRingBuffer(capacity int, windowMs int64) *RingBuffer {
	if capacity <= 0 {
		capacity = 1
	}
	if windowMs <= 0 {
		windowMs = 60_000
	}

	return &RingBuffer{
		slots:    make([]*SkeletonCe, capacity),
		capacity: capacity,
		windowMs: windowMs,
	}
}

// Write adds a CE, overwriting oldest if full. O(1).
func (r *RingBuffer) Write(ce *SkeletonCe) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.slots[r.head] = ce
	r.head = (r.head + 1) % r.capacity
	if r.count < r.capacity {
		r.count++
	}
}

// Flush returns CEs within the window, sorted by WallTsNs, and clears buffer.
func (r *RingBuffer) Flush(nowMs int64) []*SkeletonCe {
	r.mu.Lock()
	defer r.mu.Unlock()

	cutoffNs := (nowMs - r.windowMs) * 1_000_000
	n := r.count
	if n > r.capacity {
		n = r.capacity
	}

	result := make([]*SkeletonCe, 0, n)
	for i := 0; i < n; i++ {
		idx := (r.head - n + i + r.capacity) % r.capacity
		if ce := r.slots[idx]; ce != nil && ce.WallTsNs >= cutoffNs {
			result = append(result, ce)
		}
	}

	for i := range r.slots {
		r.slots[i] = nil
	}
	r.head, r.count = 0, 0

	for i := 1; i < len(result); i++ {
		for j := i; j > 0 && result[j].WallTsNs < result[j-1].WallTsNs; j-- {
			result[j], result[j-1] = result[j-1], result[j]
		}
	}

	return result
}
