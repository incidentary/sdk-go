package incidentary

import (
	"regexp"
	"testing"
	"time"
)

var uuidv7Pattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
)

func TestNewIDMatchesV7Pattern(t *testing.T) {
	id := NewID()
	if !uuidv7Pattern.MatchString(id) {
		t.Fatalf("NewID() = %q, does not match v7 pattern", id)
	}
}

func TestNewIDVersionNibbleIsSeven(t *testing.T) {
	id := NewID()
	// Canonical layout: chars at index 14 is the version nibble.
	if id[14] != '7' {
		t.Errorf("version nibble = %q, want '7'", string(id[14]))
	}
}

func TestNewIDVariantBitsAreRFC4122(t *testing.T) {
	id := NewID()
	// Index 19 is the variant nibble; RFC 4122 layouts are 8/9/a/b.
	c := id[19]
	if c != '8' && c != '9' && c != 'a' && c != 'b' {
		t.Errorf("variant nibble = %q, want 8/9/a/b", string(c))
	}
}

func TestNewIDIsTimeOrdered(t *testing.T) {
	a := NewID()
	time.Sleep(2 * time.Millisecond)
	b := NewID()
	if a >= b {
		t.Errorf("expected a < b lexicographically: a=%q b=%q", a, b)
	}
}

func TestNewIDIsUniqueWithinMillisecond(t *testing.T) {
	const n = 256
	seen := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		id := NewID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id at iteration %d: %q", i, id)
		}
		seen[id] = struct{}{}
	}
}

func TestNewIDEncodesRecentTimestamp(t *testing.T) {
	before := time.Now().UnixMilli()
	id := NewID()
	after := time.Now().UnixMilli()

	// Strip dashes and parse the first 12 hex chars as uint64.
	tsHex := id[0:8] + id[9:13]
	var ts int64
	for _, c := range tsHex {
		ts <<= 4
		switch {
		case c >= '0' && c <= '9':
			ts |= int64(c - '0')
		case c >= 'a' && c <= 'f':
			ts |= int64(c-'a') + 10
		default:
			t.Fatalf("unexpected hex char %q", string(c))
		}
	}
	if ts < before-5_000 || ts > after+5_000 {
		t.Errorf("timestamp %d out of envelope [%d, %d]", ts, before, after)
	}
}
