package resilience

import "errors"

// Sentinel errors returned by policies. Callers match them with errors.Is.
// None of them is retryable by the default classifier in package retry:
// they are this process protecting itself, not the dependency failing.
var (
	ErrBulkheadFull = errors.New("resilience: bulkhead full")
	ErrBreakerOpen  = errors.New("resilience: circuit breaker open")
	ErrRateLimited  = errors.New("resilience: rate limited")
	ErrRetryBudget  = errors.New("resilience: retry budget exhausted")
	ErrPoolFull     = errors.New("resilience: worker pool queue full")
	ErrPoolClosed   = errors.New("resilience: worker pool closed")
	ErrShed         = errors.New("resilience: load shed")
	ErrNoBudget     = errors.New("resilience: no deadline budget left")

	// ErrAttemptTimeout is the context cause set on a per-attempt deadline, so a
	// classifier can tell "this attempt was slow" (retryable) from "the caller's
	// deadline passed" (not retryable).
	ErrAttemptTimeout = errors.New("resilience: attempt timeout")
	// ErrCallTimeout is the context cause set on the whole-call deadline.
	ErrCallTimeout = errors.New("resilience: call timeout")
)

// IsPolicyError reports whether err was produced by a policy in this module
// rather than by the protected operation.
func IsPolicyError(err error) bool {
	return errors.Is(err, ErrBulkheadFull) ||
		errors.Is(err, ErrBreakerOpen) ||
		errors.Is(err, ErrRateLimited) ||
		errors.Is(err, ErrRetryBudget) ||
		errors.Is(err, ErrPoolFull) ||
		errors.Is(err, ErrPoolClosed) ||
		errors.Is(err, ErrShed) ||
		errors.Is(err, ErrNoBudget)
}
