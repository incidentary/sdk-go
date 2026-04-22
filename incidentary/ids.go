package incidentary

// Canonical UUIDv4 id generator for the Incidentary Go SDK.
//
// UUIDv4 (RFC 9562 §5.4) is 122 bits of CSPRNG random with no
// embedded timestamp. The server accepts v1/v4/v7 transparently —
// the binary representation is identical across versions — but all
// first-party SDKs emit v4.
//
// Earlier drafts of this helper emitted UUIDv7 on the grounds that
// the 48-bit millisecond prefix would improve server-side storage
// locality. That reasoning was wrong for the Incidentary server
// schema:
//
//   - ClickHouse compares UUIDs second-half-first for historical
//     reasons, so v7's timestamp prefix contributes nothing to
//     sparse-index ordering or pruning.
//   - Every UUID-bearing ClickHouse table already carries time
//     locality in an explicit i64 nanosecond column (wall_ts_ns /
//     occurred_at) that sits *before* the UUID in the sort key.
//
// With the storage-locality case empty, the remaining consideration
// is v7's 48-bit timestamp prefix — a recoverable creation-time side
// channel for any value that might cross a trust boundary. v4 has no
// such leak.

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

// NewID returns a canonical UUIDv4 string (RFC 9562 §5.4): 36 chars,
// four hyphens, version nibble '4', RFC 4122 variant bits.
//
// Randomness is sourced from crypto/rand (OS CSPRNG). Falls back
// silently to zero-random on an rand.Read failure rather than
// panicking — a degenerate-but-unique ID is preferable to a crashed
// SDK when the caller is trying to capture an incident.
func NewID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])

	// Version = 4 in the top 4 bits of byte 6.
	buf[6] = (buf[6] & 0x0F) | 0x40

	// Variant = 10 (RFC 4122) in the top 2 bits of byte 8.
	buf[8] = (buf[8] & 0x3F) | 0x80

	hexv := hex.EncodeToString(buf[:])
	parts := []string{hexv[0:8], hexv[8:12], hexv[12:16], hexv[16:20], hexv[20:32]}
	return strings.Join(parts, "-")
}
