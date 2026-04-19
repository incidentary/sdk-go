package incidentary

// Canonical UUIDv7 helper for the Incidentary Go SDK.
//
// UUIDv7 (RFC 9562 §5.7) encodes a Unix-millis timestamp in the most
// significant 48 bits. IDs generated minutes apart sort
// lexicographically in the order they were created — a material
// speedup for B-tree locality on hot ingest paths. Binary-compatible
// with v4 on the wire, so call sites can switch transparently.

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"
)

// NewID returns a canonical UUIDv7 string.
//
// Format:
//
//	48 bits: Unix-epoch milliseconds (big-endian)
//	 4 bits: version = 7
//	12 bits: rand_a
//	 2 bits: variant = 10 (RFC 4122)
//	62 bits: rand_b
//
// Falls back silently to zero-random on an rand.Read failure rather
// than panicking — unique-but-predictable IDs are preferable to a
// crashed SDK in a degenerate crypto state.
func NewID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])

	ms := time.Now().UnixMilli()
	// 48 bits of timestamp, big-endian, into buf[0..6].
	buf[0] = byte(ms >> 40)
	buf[1] = byte(ms >> 32)
	buf[2] = byte(ms >> 24)
	buf[3] = byte(ms >> 16)
	buf[4] = byte(ms >> 8)
	buf[5] = byte(ms)

	// Version = 7 in the top 4 bits of byte 6.
	buf[6] = (buf[6] & 0x0F) | 0x70

	// Variant = 10 in the top 2 bits of byte 8.
	buf[8] = (buf[8] & 0x3F) | 0x80

	hexv := hex.EncodeToString(buf[:])
	parts := []string{hexv[0:8], hexv[8:12], hexv[12:16], hexv[16:20], hexv[20:32]}
	return strings.Join(parts, "-")
}
