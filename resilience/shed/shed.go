// Package shed refuses work at the ingress when the process is already
// carrying as much as it can, instead of queueing it until every request
// times out. Requests carry a priority class; lower classes are refused first,
// so a traffic spike degrades background work before customer-facing calls.
package shed

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/anuar-fest/tools/resilience"
)

// Class is a request priority. Lower is more important.
type Class int

const (
	// Critical is admitted until MaxInFlight.
	Critical Class = iota
	// Normal is admitted while in-flight is below NormalAbove × MaxInFlight.
	Normal
	// Background is admitted while in-flight is below BackgroundAbove × MaxInFlight.
	Background
)

func (c Class) String() string {
	switch c {
	case Critical:
		return "critical"
	case Normal:
		return "normal"
	case Background:
		return "background"
	default:
		return "unknown"
	}
}

// Config configures a Shedder.
type Config struct {
	Name string
	// MaxInFlight is the hard concurrency cap. Required. Size it from a load
	// test, not a guess: the point where p99 latency starts climbing.
	MaxInFlight int
	// NormalAbove is the fraction of MaxInFlight above which Normal is shed (default 0.9).
	NormalAbove float64
	// BackgroundAbove is the fraction above which Background is shed (default 0.7).
	BackgroundAbove float64
	Observer        resilience.Observer
}

// Shedder admits requests by class. Safe for concurrent use; admission is a
// single CAS so it is cheap enough for every request.
type Shedder struct {
	cfg      Config
	limits   [3]int64
	inflight atomic.Int64
}

// New validates cfg.
func New(cfg Config) (*Shedder, error) {
	if cfg.MaxInFlight <= 0 {
		return nil, fmt.Errorf("shed %q: MaxInFlight must be > 0", cfg.Name)
	}
	if cfg.NormalAbove <= 0 || cfg.NormalAbove > 1 {
		cfg.NormalAbove = 0.9
	}
	if cfg.BackgroundAbove <= 0 || cfg.BackgroundAbove > 1 {
		cfg.BackgroundAbove = 0.7
	}
	if cfg.BackgroundAbove > cfg.NormalAbove {
		return nil, fmt.Errorf("shed %q: BackgroundAbove must be <= NormalAbove", cfg.Name)
	}
	cfg.Observer = resilience.OrNop(cfg.Observer)
	s := &Shedder{cfg: cfg}
	s.limits[Critical] = int64(cfg.MaxInFlight)
	s.limits[Normal] = atLeastOne(float64(cfg.MaxInFlight) * cfg.NormalAbove)
	s.limits[Background] = atLeastOne(float64(cfg.MaxInFlight) * cfg.BackgroundAbove)
	return s, nil
}

func atLeastOne(f float64) int64 {
	n := int64(f)
	if n < 1 {
		n = 1
	}
	return n
}

// Admit tries to take an in-flight slot for class. On success the returned
// release must be called when the work ends. On refusal ok is false and the
// caller should answer 503 with a Retry-After.
func (s *Shedder) Admit(ctx context.Context, class Class) (release func(), ok bool) {
	if class < Critical || class > Background {
		class = Background
	}
	limit := s.limits[class]
	for {
		cur := s.inflight.Load()
		if cur >= limit {
			s.cfg.Observer.ObserveDecision(ctx, resilience.Event{
				Dependency: s.cfg.Name,
				Policy:     resilience.PolicyShed,
				Decision:   resilience.DecisionShed,
				Err:        resilience.ErrShed,
			})
			return nil, false
		}
		if s.inflight.CompareAndSwap(cur, cur+1) {
			var released atomic.Bool
			return func() {
				if released.CompareAndSwap(false, true) {
					s.inflight.Add(-1)
				}
			}, true
		}
	}
}

// InFlight is the number of admitted requests still running.
func (s *Shedder) InFlight() int { return int(s.inflight.Load()) }

// Limit returns the admission limit for class.
func (s *Shedder) Limit(class Class) int { return int(s.limits[class]) }

// Name returns the shedder label.
func (s *Shedder) Name() string { return s.cfg.Name }
