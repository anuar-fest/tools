// Package deadline derives per-hop budgets from a caller's deadline. A hop
// that is given the caller's whole remaining time leaves nothing for the
// caller to record the failure; a hop that ignores the caller's deadline keeps
// working for a client that has already gone.
package deadline

import (
	"context"
	"time"

	"github.com/anuar-fest/tools/resilience"
)

// Child returns a context for one outbound hop. Its deadline is the earlier of
// now+timeout and the parent's deadline minus reserve, where reserve is the
// time the caller keeps for itself (persisting an error, writing a response).
// timeout <= 0 means "only clamp to the parent". If the parent has less than
// reserve left, the child is already expired with cause ErrNoBudget, so the
// hop fails fast instead of starting work it cannot finish.
func Child(ctx context.Context, timeout, reserve time.Duration) (context.Context, context.CancelFunc) {
	now := time.Now()
	var (
		deadline time.Time
		cause    = resilience.ErrCallTimeout
	)
	if timeout > 0 {
		deadline = now.Add(timeout)
	}
	if pd, ok := ctx.Deadline(); ok {
		limit := pd.Add(-reserve)
		if deadline.IsZero() || limit.Before(deadline) {
			deadline = limit
			cause = resilience.ErrNoBudget
		}
	}
	if deadline.IsZero() {
		return context.WithCancel(ctx)
	}
	if !deadline.After(now) {
		return context.WithDeadlineCause(ctx, now, resilience.ErrNoBudget)
	}
	return context.WithDeadlineCause(ctx, deadline, cause)
}

// Attempt bounds one attempt and marks a timeout as ErrAttemptTimeout, which
// the retry classifier treats as transient.
func Attempt(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeoutCause(ctx, d, resilience.ErrAttemptTimeout)
}

// Detach returns a context that keeps ctx's values (trace, logger, tenant)
// but not its cancellation, bounded by budget. Use it after the commit point:
// the client may leave, the receipt must still be sent.
func Detach(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	base := context.WithoutCancel(ctx)
	if budget <= 0 {
		return context.WithCancel(base)
	}
	return context.WithTimeoutCause(base, budget, resilience.ErrCallTimeout)
}

// Remaining reports how much time is left before ctx's deadline. ok is false
// when ctx has no deadline. A passed deadline reports 0.
func Remaining(ctx context.Context) (left time.Duration, ok bool) {
	d, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	left = time.Until(d)
	if left < 0 {
		left = 0
	}
	return left, true
}

// Enough reports whether ctx has at least need left (or no deadline at all).
// Use it to refuse work that cannot finish rather than start it and time out.
func Enough(ctx context.Context, need time.Duration) bool {
	left, ok := Remaining(ctx)
	return !ok || left >= need
}
