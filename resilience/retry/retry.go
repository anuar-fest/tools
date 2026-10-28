// Package retry re-runs an operation that failed for a transient reason, with
// a per-attempt timeout, exponential backoff with jitter, an explicit
// classifier that decides what is transient, and an optional fleet-wide budget
// that stops retries from amplifying an outage.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/anuar-fest/tools/resilience"
)

// Jitter selects how the computed backoff is randomised. Without jitter every
// client that failed together retries together.
type Jitter int

const (
	// JitterFull picks uniformly in [0, backoff]. The default; best at
	// de-synchronising a fleet.
	JitterFull Jitter = iota
	// JitterNone uses the computed backoff as is. Only for tests.
	JitterNone
	// JitterEqual picks in [backoff/2, backoff]: bounded minimum wait.
	JitterEqual
	// JitterDecorrelated picks in [initial, 3*previous], capped at MaxBackoff.
	JitterDecorrelated
)

// Classifier reports whether an error is worth another attempt.
type Classifier func(error) bool

// Policy configures retries. The zero value is usable: see withDefaults.
type Policy struct {
	// Dependency names what is being retried; it is the metric label.
	Dependency string
	// MaxAttempts counts the first attempt too. 1 disables retries. <= 0 means the default (3).
	MaxAttempts int
	// AttemptTimeout bounds each attempt. A timed-out attempt is retryable; the
	// caller's own deadline is not. 0 disables the per-attempt timeout.
	AttemptTimeout time.Duration
	// InitialBackoff is the base wait before the second attempt (default 100ms).
	InitialBackoff time.Duration
	// MaxBackoff caps the computed wait (default 2s).
	MaxBackoff time.Duration
	// Multiplier grows the wait per attempt (default 2).
	Multiplier float64
	Jitter     Jitter
	// MaxElapsed stops retrying once the next wait would end after this much
	// time since the first attempt. 0 means only the context bounds it.
	MaxElapsed time.Duration
	// MaxRetryAfter caps Retry-After hints carried by errors (default 10s).
	// A hint above the cap is ignored and the backoff is used instead.
	MaxRetryAfter time.Duration
	// RetryIf decides what is transient. Default: DefaultClassifier.
	RetryIf Classifier
	// Budget, when set, is consulted before every retry.
	Budget   *Budget
	Observer resilience.Observer
	Clock    resilience.Clock
	// Sleep waits for d or until ctx is done. Tests inject a fake.
	Sleep func(ctx context.Context, d time.Duration) error
	// Rand returns a value in [0,1). Tests inject a fixed one.
	Rand func() float64
}

func (p Policy) withDefaults() Policy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 3
	}
	if p.InitialBackoff <= 0 {
		p.InitialBackoff = 100 * time.Millisecond
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = 2 * time.Second
	}
	if p.Multiplier < 1 {
		p.Multiplier = 2
	}
	if p.MaxRetryAfter <= 0 {
		p.MaxRetryAfter = 10 * time.Second
	}
	if p.RetryIf == nil {
		p.RetryIf = DefaultClassifier
	}
	p.Observer = resilience.OrNop(p.Observer)
	p.Clock = resilience.OrSystem(p.Clock)
	if p.Sleep == nil {
		p.Sleep = sleep
	}
	if p.Rand == nil {
		p.Rand = rand.Float64
	}
	return p
}

// Do runs op until it succeeds, is classified as not retryable, the attempts
// or budget run out, or ctx ends. The returned error is the last attempt's
// error, wrapped with the attempt count when more than one was made.
func Do(ctx context.Context, p Policy, op func(context.Context) error) error {
	_, err := DoValue(ctx, p, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, op(ctx)
	})
	return err
}

// DoValue is Do for operations that return a value.
func DoValue[T any](ctx context.Context, p Policy, op func(context.Context) (T, error)) (T, error) {
	p = p.withDefaults()
	var zero T

	if p.Budget != nil {
		p.Budget.Deposit()
	}
	start := p.Clock.Now()
	var prev time.Duration

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			p.observe(ctx, decisionForCtx(ctx), attempt, 0, err)
			return zero, err
		}

		v, elapsed, err := attemptValue(ctx, p, op)
		if err == nil {
			p.observe(ctx, resilience.DecisionSuccess, attempt, elapsed, nil)
			return v, nil
		}

		// The caller's context ending is never retried, whatever the error says.
		if ctxErr := ctx.Err(); ctxErr != nil {
			p.observe(ctx, decisionForCtx(ctx), attempt, elapsed, err)
			return zero, wrapAttempts(attempt, err)
		}
		if !p.RetryIf(err) {
			// A rejection by an inner policy (breaker open, bulkhead full) was
			// already observed by that policy; only a real failure is ours.
			if !resilience.IsPolicyError(err) {
				p.observe(ctx, resilience.DecisionFailure, attempt, elapsed, err)
			}
			return zero, wrapAttempts(attempt, err)
		}
		if attempt >= p.MaxAttempts {
			p.observe(ctx, resilience.DecisionRetriesExhausted, attempt, elapsed, err)
			return zero, wrapAttempts(attempt, err)
		}
		if p.Budget != nil && !p.Budget.Withdraw() {
			p.observe(ctx, resilience.DecisionRetryBudget, attempt, elapsed, err)
			return zero, fmt.Errorf("%w: %w", resilience.ErrRetryBudget, wrapAttempts(attempt, err))
		}

		delay := p.delay(attempt, prev, err)
		if p.MaxElapsed > 0 && p.Clock.Now().Sub(start)+delay > p.MaxElapsed {
			p.observe(ctx, resilience.DecisionRetriesExhausted, attempt, elapsed, err)
			return zero, wrapAttempts(attempt, err)
		}

		p.observe(ctx, resilience.DecisionRetry, attempt, elapsed, err)
		if sleepErr := p.Sleep(ctx, delay); sleepErr != nil {
			p.observe(ctx, decisionForCtx(ctx), attempt, 0, sleepErr)
			return zero, fmt.Errorf("%w (while waiting to retry after: %w)", sleepErr, err)
		}
		prev = delay
	}
}

// attemptValue runs op once under the per-attempt timeout and marks a timeout that
// belongs to this attempt (not to the caller) as ErrAttemptTimeout.
func attemptValue[T any](ctx context.Context, p Policy, op func(context.Context) (T, error)) (T, time.Duration, error) {
	actx := ctx
	if p.AttemptTimeout > 0 {
		var cancel context.CancelFunc
		actx, cancel = context.WithTimeoutCause(ctx, p.AttemptTimeout, resilience.ErrAttemptTimeout)
		defer cancel()
	}

	t0 := p.Clock.Now()
	v, err := op(actx)
	elapsed := p.Clock.Now().Sub(t0)
	return v, elapsed, MarkAttemptTimeout(actx, err)
}

// MarkAttemptTimeout wraps err with ErrAttemptTimeout when err is a deadline
// error and attemptCtx's own deadline (not its parent's) caused it. Code that
// records an outcome inside an attempt (a breaker) must call it so a hung
// dependency counts as a failure rather than as the caller giving up.
func MarkAttemptTimeout(attemptCtx context.Context, err error) error {
	if err == nil || errors.Is(err, resilience.ErrAttemptTimeout) {
		return err
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return err
	}
	if errors.Is(context.Cause(attemptCtx), resilience.ErrAttemptTimeout) {
		return fmt.Errorf("%w: %w", resilience.ErrAttemptTimeout, err)
	}
	return err
}

func (p Policy) delay(attempt int, prev time.Duration, err error) time.Duration {
	var ra RetryAfterer
	if errors.As(err, &ra) {
		if d := ra.RetryAfter(); d > 0 && d <= p.MaxRetryAfter {
			return d
		}
	}
	return Backoff(p, attempt, prev, p.Rand())
}

// Backoff computes the wait after the given 1-based attempt, with r in [0,1)
// as the random source. It is pure so tests can table it.
func Backoff(p Policy, attempt int, prev time.Duration, r float64) time.Duration {
	p = p.withDefaults()
	if attempt < 1 {
		attempt = 1
	}

	base := float64(p.InitialBackoff)
	maxf := float64(p.MaxBackoff)
	for i := 1; i < attempt && base < maxf; i++ {
		base *= p.Multiplier
	}
	if base > maxf {
		base = maxf
	}

	var d float64
	switch p.Jitter {
	case JitterNone:
		d = base
	case JitterEqual:
		d = base/2 + r*(base/2)
	case JitterDecorrelated:
		lo := float64(p.InitialBackoff)
		hi := float64(prev) * 3
		if prev == 0 {
			hi = lo * 3
		}
		if hi > maxf {
			hi = maxf
		}
		if hi < lo {
			hi = lo
		}
		d = lo + r*(hi-lo)
	default: // JitterFull
		d = r * base
	}
	return time.Duration(d)
}

func (p Policy) observe(ctx context.Context, d resilience.Decision, attempt int, elapsed time.Duration, err error) {
	p.Observer.ObserveDecision(ctx, resilience.Event{
		Dependency: p.Dependency,
		Policy:     resilience.PolicyRetry,
		Decision:   d,
		Attempt:    attempt,
		Elapsed:    elapsed,
		Err:        err,
	})
}

func decisionForCtx(ctx context.Context) resilience.Decision {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return resilience.DecisionTimeout
	}
	return resilience.DecisionCanceled
}

func wrapAttempts(attempt int, err error) error {
	if attempt <= 1 {
		return err
	}
	return fmt.Errorf("after %d attempts: %w", attempt, err)
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
