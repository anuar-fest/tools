// Package clock provides a fake clock for tests inside this module.
package clock

import (
	"sync"
	"time"
)

// Fake is a manually advanced clock. Safe for concurrent use.
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// NewFake starts at a fixed, deterministic instant.
func NewFake() *Fake {
	return &Fake{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves the clock forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}
