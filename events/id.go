package events

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"time"
)

// NewID returns a UUID version 7: time-ordered, so inbox and outbox indexes
// stay append-friendly, and globally unique, so it is safe as a dedup key.
func NewID() string {
	return newIDAt(time.Now())
}

func newIDAt(now time.Time) string {
	var b [16]byte
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(now.UnixMilli())) //nolint:gosec // milliseconds since 1970 fit in 48 bits until year 10889
	copy(b[0:6], ts[2:8])
	if _, err := rand.Read(b[6:]); err != nil {
		panic("events: crypto/rand unavailable: " + err.Error()) // the process cannot make identifiers; nothing sane continues
	}
	b[6] = (b[6] & 0x0f) | 0x70 // version 7
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 4122

	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}
