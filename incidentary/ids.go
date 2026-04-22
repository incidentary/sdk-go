package incidentary

// Identifier generators for the Incidentary Go SDK.
//
// Exposes two functions with a deliberate split:
//
//	NewID           — UUIDv7 (RFC 9562 §5.7) for DB-backed identifiers
//	                  (trace IDs, CE IDs, anywhere sort-key locality
//	                  matters). The 48-bit millisecond prefix improves
//	                  B-tree locality on hot ingest paths.
//	NewRandomToken  — UUIDv4 for externally visible, privacy-sensitive
//	                  tokens where the timestamp embedded in v7 would
//	                  leak creation time across a trust boundary.
//
// Both share the 128-bit UUID layout, so either form slots into a
// `uuid` column transparently.

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

// NewRandomToken returns a canonical UUIDv4 string — 122 bits of
// CSPRNG output with no embedded timestamp. Use this for externally
// visible tokens (deploy dedup keys, share-URL slugs, CSRF nonces)
// where the millisecond prefix that v7 embeds would leak the token's
// creation time across a trust boundary.
//
// Falls back silently to zero-random on an rand.Read failure rather
// than panicking — unique-but-predictable tokens are preferable to a
// crashed SDK in a degenerate crypto state. Callers that need hard
// failure on a CSPRNG outage should bring their own generator.
func NewRandomToken() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])

	// Version = 4 in the top 4 bits of byte 6.
	buf[6] = (buf[6] & 0x0F) | 0x40
	// Variant = 10 in the top 2 bits of byte 8.
	buf[8] = (buf[8] & 0x3F) | 0x80

	hexv := hex.EncodeToString(buf[:])
	parts := []string{hexv[0:8], hexv[8:12], hexv[12:16], hexv[16:20], hexv[20:32]}
	return strings.Join(parts, "-")
}
