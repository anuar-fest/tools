// Package breaker stops calling a dependency that is failing, so a slow or
// dead upstream does not keep every caller's goroutine, connection and
// deadline busy. One Breaker protects one dependency; never share a breaker
// across dependencies with different failure domains.
//
// States: closed (calls flow, failures are counted) → open (calls are
// rejected with ErrBreakerOpen until OpenFor passes) → half-open (at most
// MaxHalfOpenProbes calls are let through; SuccessesToClose successes close
// it, one failure re-opens it).
package breaker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/anuar-fest/tools/resilience"
)

// Config configures a Breaker. Zero fields take the documented defaults.
type Config struct {
	// Name is the dependency label on every metric and transition.
	Name string
	// Trip decides when the closed breaker opens (default ConsecutiveFailures(5)).
	Trip TripPolicy
	// OpenFor is how long the breaker rejects calls before probing (default 30s).
	OpenFor time.Duration
	// OpenForJitter spreads OpenFor by ±fraction so replicas do not probe in
	// lockstep (default 0.2).
	OpenForJitter float64
	// MaxHalfOpenProbes bounds concurrent probes while half-open (default 1).
	MaxHalfOpenProbes int
	// SuccessesToClose is how many probes must succeed to close (default 1).
	SuccessesToClose int
	// SlowCallThreshold, when > 0, counts a successful call slower than this
	// as a failure: a dependency that answers in 9s is down for most callers.
	SlowCallThreshold time.Duration
	// IsFailure classifies a recorded error (default DefaultIsFailure).
	IsFailure func(error) bool
	Observer  resilience.Observer
	Clock     resilience.Clock
	// Rand returns [0,1); tests inject a fixed value.
	Rand func() float64
}

func (c Config) withDefaults() Config {
	if c.Trip == nil {
		c.Trip = ConsecutiveFailures(5)
	}
	if c.OpenFor <= 0 {
		c.OpenFor = 30 * time.Second
	}
	if c.OpenForJitter < 0 {
		c.OpenForJitter = 0
	}
	if c.OpenForJitter == 0 {
		c.OpenForJitter = 0.2
	}
	if c.MaxHalfOpenProbes <= 0 {
		c.MaxHalfOpenProbes = 1
	}
	if c.SuccessesToClose <= 0 {
		c.SuccessesToClose = 1
	}
	if c.IsFailure == nil {
		c.IsFailure = DefaultIsFailure
	}
	c.Observer = resilience.OrNop(c.Observer)
	c.Clock = resilience.OrSystem(c.Clock)
	if c.Rand == nil {
		c.Rand = rand.Float64
	}
	return c
}

// Breaker is a circuit breaker. Safe for concurrent use.
type Breaker struct {
	cfg Config

	mu        sync.Mutex
	state     resilience.BreakerState
	gen       uint64 // bumped on every transition; stale permits are ignored
	openUntil time.Time
	probes    int
	successes int
}

// New validates cfg and returns a closed breaker.
func New(cfg Config) (*Breaker, error) {
	if cfg.OpenForJitter > 1 {
		return nil, fmt.Errorf("breaker %q: OpenForJitter must be in [0,1], got %v", cfg.Name, cfg.OpenForJitter)
	}
	cfg = cfg.withDefaults()
	return &Breaker{cfg: cfg}, nil
}

// Permit is the right to make one call. Record must be called exactly once
// with the call's outcome; a second call is ignored.
type Permit struct {
	b        *Breaker
	ctx      context.Context
	gen      uint64
	start    time.Time
	halfOpen bool
	done     bool
}

// Allow asks whether a call may proceed. It returns ErrBreakerOpen when the
// breaker is open or already has enough half-open probes in flight.
func (b *Breaker) Allow(ctx context.Context) (*Permit, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.cfg.Clock.Now()
	if b.state == resilience.BreakerOpen {
		if now.Before(b.openUntil) {
			b.reject(ctx)
			return nil, resilience.ErrBreakerOpen
		}
		b.transition(ctx, resilience.BreakerHalfOpen)
	}
	if b.state == resilience.BreakerHalfOpen {
		if b.probes >= b.cfg.MaxHalfOpenProbes {
			b.reject(ctx)
			return nil, resilience.ErrBreakerOpen
		}
		b.probes++
		return &Permit{b: b, ctx: ctx, gen: b.gen, start: now, halfOpen: true}, nil
	}
	return &Permit{b: b, ctx: ctx, gen: b.gen, start: now}, nil
}

// Record reports the outcome of the permitted call.
func (p *Permit) Record(err error) {
	b := p.b
	b.mu.Lock()
	defer b.mu.Unlock()

	if p.done {
		return
	}
	p.done = true
	if p.gen != b.gen {
		return // the breaker moved on while this call was in flight
	}

	now := b.cfg.Clock.Now()
	failure := b.cfg.IsFailure(err)
	if !failure && err == nil && b.cfg.SlowCallThreshold > 0 && now.Sub(p.start) > b.cfg.SlowCallThreshold {
		failure = true
	}

	switch b.state {
	case resilience.BreakerClosed:
		b.cfg.Trip.record(!failure, now)
		if failure && b.cfg.Trip.shouldTrip(now) {
			b.open(p.ctx, now)
		}
	case resilience.BreakerHalfOpen:
		b.probes--
		if failure {
			b.open(p.ctx, now)
			return
		}
		b.successes++
		if b.successes >= b.cfg.SuccessesToClose {
			b.transition(p.ctx, resilience.BreakerClosed)
		}
	case resilience.BreakerOpen:
		// unreachable: opening bumps gen, so the permit is stale
	}
}

// State returns the current state without side effects: an open breaker whose
// OpenFor has passed still reports open until the next Allow.
func (b *Breaker) State() resilience.BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Name returns the dependency label.
func (b *Breaker) Name() string { return b.cfg.Name }

// Reset forces the breaker closed and clears counters. For operators and tests.
func (b *Breaker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != resilience.BreakerClosed {
		b.transition(context.Background(), resilience.BreakerClosed)
	} else {
		b.cfg.Trip.reset()
	}
}

// open moves to Open for a jittered OpenFor. Caller holds b.mu.
func (b *Breaker) open(ctx context.Context, now time.Time) {
	spread := 1 + b.cfg.OpenForJitter*(2*b.cfg.Rand()-1)
	b.openUntil = now.Add(time.Duration(float64(b.cfg.OpenFor) * spread))
	b.transition(ctx, resilience.BreakerOpen)
}

// transition changes state, bumps the generation and resets counters. Caller holds b.mu.
func (b *Breaker) transition(ctx context.Context, to resilience.BreakerState) {
	from := b.state
	b.state = to
	b.gen++
	b.probes = 0
	b.successes = 0
	b.cfg.Trip.reset()
	b.cfg.Observer.ObserveBreakerState(ctx, resilience.StateChange{Dependency: b.cfg.Name, From: from, To: to})
}

func (b *Breaker) reject(ctx context.Context) {
	b.cfg.Observer.ObserveDecision(ctx, resilience.Event{
		Dependency: b.cfg.Name,
		Policy:     resilience.PolicyBreaker,
		Decision:   resilience.DecisionBreakerOpen,
		Err:        resilience.ErrBreakerOpen,
	})
}

// Do runs op through the breaker.
func Do(ctx context.Context, b *Breaker, op func(context.Context) error) error {
	_, err := DoValue(ctx, b, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, op(ctx)
	})
	return err
}

// DoValue runs op through the breaker and records its outcome.
func DoValue[T any](ctx context.Context, b *Breaker, op func(context.Context) (T, error)) (T, error) {
	var zero T
	permit, err := b.Allow(ctx)
	if err != nil {
		return zero, err
	}
	v, err := op(ctx)
	permit.Record(err)
	return v, err
}

type ignoredError struct{ err error }

func (e *ignoredError) Error() string { return e.err.Error() }
func (e *ignoredError) Unwrap() error { return e.err }

// Ignore marks err as not a dependency failure: a 4xx the caller caused, a
// business rejection. The breaker records it as a success.
func Ignore(err error) error {
	if err == nil {
		return nil
	}
	return &ignoredError{err: err}
}

// IsIgnored reports whether err was marked with Ignore.
func IsIgnored(err error) bool {
	var ie *ignoredError
	return errors.As(err, &ie)
}

// DefaultIsFailure counts every error as a dependency failure except: nil,
// errors marked Ignore, context.Canceled (the caller gave up), the caller's
// own deadline (ambiguous: the caller may have had no budget), and rejections
// produced by other policies in this module. A per-attempt timeout
// (ErrAttemptTimeout) is a failure: a hung dependency must trip the breaker.
func DefaultIsFailure(err error) bool {
	if err == nil || IsIgnored(err) {
		return false
	}
	if errors.Is(err, context.Canceled) || resilience.IsPolicyError(err) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, resilience.ErrAttemptTimeout) {
		return false
	}
	return true
}
