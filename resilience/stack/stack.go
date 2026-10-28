// Package stack composes the policies of this module in one fixed order for
// one dependency:
//
//	deadline → bulkhead → limit → retry( breaker( attempt-timeout( op ) ) )
//
// Why this order: the deadline bounds everything inside it; the bulkhead
// caps goroutines before any waiting happens; the limiter shapes traffic
// before a slot is spent on a call that would be refused; the breaker sits
// inside retry so a retry never goes into an open circuit (ErrBreakerOpen is
// not retryable) and every attempt, including a timed-out one, is recorded.
//
// One Stack per dependency. Do not nest stacks and do not wrap pure CPU work,
// validation or in-memory lookups: a policy only makes sense around I/O.
package stack

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/breaker"
	"github.com/anuar-fest/tools/resilience/bulkhead"
	"github.com/anuar-fest/tools/resilience/deadline"
	"github.com/anuar-fest/tools/resilience/ratelimit"
	"github.com/anuar-fest/tools/resilience/retry"
)

// Config configures a Stack. Every policy is optional except the name; a
// zero Retry policy means the retry defaults (3 attempts, conservative
// classifier). Set Retry.MaxAttempts to 1 to disable retries.
type Config struct {
	// Name is the dependency label on every metric.
	Name string
	// Timeout bounds the whole call, retries included. 0 means only the
	// caller's deadline bounds it.
	Timeout time.Duration
	// Reserve is time kept back from the caller's deadline for the caller's
	// own work after this call (writing the response, recording the failure).
	Reserve  time.Duration
	Bulkhead *bulkhead.Bulkhead
	Limiter  ratelimit.Limiter
	Breaker  *breaker.Breaker
	Retry    retry.Policy
	Observer resilience.Observer
}

// Stack is a composed policy set for one dependency. Safe for concurrent use.
type Stack struct {
	cfg Config
}

// New validates cfg.
func New(cfg Config) (*Stack, error) {
	if cfg.Name == "" {
		return nil, errors.New("stack: Name is required")
	}
	if cfg.Timeout < 0 || cfg.Reserve < 0 {
		return nil, fmt.Errorf("stack %q: Timeout and Reserve must be >= 0", cfg.Name)
	}
	cfg.Observer = resilience.OrNop(cfg.Observer)
	if cfg.Retry.Dependency == "" {
		cfg.Retry.Dependency = cfg.Name
	}
	if cfg.Retry.Observer == nil {
		cfg.Retry.Observer = cfg.Observer
	}
	return &Stack{cfg: cfg}, nil
}

// Name returns the dependency label.
func (s *Stack) Name() string { return s.cfg.Name }

// Do runs op through the stack.
func (s *Stack) Do(ctx context.Context, op func(context.Context) error) error {
	_, err := DoValue(ctx, s, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, op(ctx)
	})
	return err
}

// DoValue runs op through the stack and returns its value.
func DoValue[T any](ctx context.Context, s *Stack, op func(context.Context) (T, error)) (T, error) {
	var zero T
	start := time.Now()

	ctx, cancel := deadline.Child(ctx, s.cfg.Timeout, s.cfg.Reserve)
	defer cancel()

	if s.cfg.Bulkhead != nil {
		release, err := s.cfg.Bulkhead.Acquire(ctx)
		if err != nil {
			s.observe(ctx, err, start)
			return zero, err
		}
		defer release()
	}

	if s.cfg.Limiter != nil && !s.cfg.Limiter.Allow() {
		s.observe(ctx, resilience.ErrRateLimited, start)
		return zero, resilience.ErrRateLimited
	}

	v, err := retry.DoValue(ctx, s.cfg.Retry, func(ctx context.Context) (T, error) {
		if s.cfg.Breaker == nil {
			return op(ctx)
		}
		permit, err := s.cfg.Breaker.Allow(ctx)
		if err != nil {
			return zero, err
		}
		v, err := op(ctx)
		// The retry layer marks attempt timeouts only after we return, so mark
		// here too: a hung dependency must count against the breaker.
		err = retry.MarkAttemptTimeout(ctx, err)
		permit.Record(err)
		return v, err
	})
	s.observe(ctx, err, start)
	if err != nil {
		return zero, err
	}
	return v, nil
}

func (s *Stack) observe(ctx context.Context, err error, start time.Time) {
	s.cfg.Observer.ObserveDecision(ctx, resilience.Event{
		Dependency: s.cfg.Name,
		Policy:     resilience.PolicyStack,
		Decision:   Outcome(err),
		Elapsed:    time.Since(start),
		Err:        err,
	})
}

// Outcome maps a final error to the Decision a call ends with.
func Outcome(err error) resilience.Decision {
	switch {
	case err == nil:
		return resilience.DecisionSuccess
	case errors.Is(err, resilience.ErrBulkheadFull):
		return resilience.DecisionBulkheadFull
	case errors.Is(err, resilience.ErrRateLimited):
		return resilience.DecisionLimitExceeded
	case errors.Is(err, resilience.ErrBreakerOpen):
		return resilience.DecisionBreakerOpen
	case errors.Is(err, resilience.ErrRetryBudget):
		return resilience.DecisionRetryBudget
	case errors.Is(err, resilience.ErrShed):
		return resilience.DecisionShed
	case errors.Is(err, context.DeadlineExceeded):
		return resilience.DecisionTimeout
	case errors.Is(err, context.Canceled):
		return resilience.DecisionCanceled
	default:
		return resilience.DecisionFailure
	}
}
