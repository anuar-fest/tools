package resilience

import "time"

// Clock is the time source policies read. Tests substitute a fake so breaker
// windows and backoff can be asserted without sleeping.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

func (f ClockFunc) Now() time.Time { return f() }

// SystemClock reads the wall clock.
func SystemClock() Clock { return ClockFunc(time.Now) }

// OrSystem returns c, or the system clock when c is nil.
func OrSystem(c Clock) Clock {
	if c == nil {
		return SystemClock()
	}
	return c
}
