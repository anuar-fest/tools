package breaker

import "time"

// TripPolicy decides when a closed breaker opens. The method set is unexported
// so the policies stay a closed, reviewed set; all methods run under the
// breaker's mutex.
type TripPolicy interface {
	record(success bool, now time.Time)
	shouldTrip(now time.Time) bool
	reset()
}

// ConsecutiveFailures trips after n failures in a row. Simple and right for
// dependencies whose failures are all-or-nothing (connection refused, DNS).
func ConsecutiveFailures(n int) TripPolicy {
	if n < 1 {
		n = 1
	}
	return &consecutive{limit: n}
}

type consecutive struct {
	limit, run int
}

func (c *consecutive) record(success bool, _ time.Time) {
	if success {
		c.run = 0
	} else {
		c.run++
	}
}
func (c *consecutive) shouldTrip(time.Time) bool { return c.run >= c.limit }
func (c *consecutive) reset()                    { c.run = 0 }

// ErrorRate trips when, over the last window, at least minRequests were made
// and the failure ratio reached ratio. Right for high-volume dependencies
// where an absolute count would trip on noise.
func ErrorRate(window time.Duration, minRequests int, ratio float64) TripPolicy {
	n := int(window / time.Second)
	if n < 1 {
		n = 1
	}
	if minRequests < 1 {
		minRequests = 1
	}
	if ratio <= 0 || ratio > 1 {
		ratio = 0.5
	}
	return &errorRate{slots: make([]rateSlot, n), minRequests: minRequests, ratio: ratio}
}

type rateSlot struct {
	sec               int64
	requests, failure int
}

type errorRate struct {
	slots       []rateSlot
	minRequests int
	ratio       float64
}

func (e *errorRate) slot(sec int64) *rateSlot {
	s := &e.slots[int(sec%int64(len(e.slots)))]
	if s.sec != sec {
		*s = rateSlot{sec: sec}
	}
	return s
}

func (e *errorRate) record(success bool, now time.Time) {
	s := e.slot(now.Unix())
	s.requests++
	if !success {
		s.failure++
	}
}

func (e *errorRate) shouldTrip(now time.Time) bool {
	cutoff := now.Unix() - int64(len(e.slots))
	var reqs, fails int
	for i := range e.slots {
		if e.slots[i].sec > cutoff {
			reqs += e.slots[i].requests
			fails += e.slots[i].failure
		}
	}
	return reqs >= e.minRequests && float64(fails)/float64(reqs) >= e.ratio
}

func (e *errorRate) reset() {
	for i := range e.slots {
		e.slots[i] = rateSlot{}
	}
}

// FailuresInWindow trips after n failures within window regardless of
// successes. It is the policy the old resilient_executor used; it trips on
// noise at high request rates, so prefer ErrorRate for anything busy.
func FailuresInWindow(window time.Duration, n int) TripPolicy {
	e := ErrorRate(window, 1, 1).(*errorRate)
	return &failuresInWindow{errorRate: e, limit: n}
}

type failuresInWindow struct {
	*errorRate
	limit int
}

func (f *failuresInWindow) shouldTrip(now time.Time) bool {
	cutoff := now.Unix() - int64(len(f.slots))
	fails := 0
	for i := range f.slots {
		if f.slots[i].sec > cutoff {
			fails += f.slots[i].failure
		}
	}
	return fails >= f.limit
}
