package retry

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"time"

	"github.com/anuar-fest/tools/resilience"
)

// RetryAfterer is implemented by errors that carry a server-supplied wait
// (HTTP Retry-After, gRPC RetryInfo). The policy honours it up to MaxRetryAfter.
type RetryAfterer interface {
	RetryAfter() time.Duration
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

type transientError struct{ err error }

func (e *transientError) Error() string { return e.err.Error() }
func (e *transientError) Unwrap() error { return e.err }

type retryAfterError struct {
	err   error
	after time.Duration
}

func (e *retryAfterError) Error() string             { return e.err.Error() }
func (e *retryAfterError) Unwrap() error             { return e.err }
func (e *retryAfterError) RetryAfter() time.Duration { return e.after }

// Permanent marks err as not worth retrying, whatever the classifier says.
// Use it for validation failures, 4xx responses, and non-idempotent writes
// whose outcome is unknown.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// Transient marks err as retryable. Use it where the adapter knows better than
// the default classifier (a 503 body, a driver-specific serialization failure).
func Transient(err error) error {
	if err == nil {
		return nil
	}
	return &transientError{err: err}
}

// WithRetryAfter marks err as retryable and attaches the wait the server asked for.
func WithRetryAfter(err error, after time.Duration) error {
	if err == nil {
		return nil
	}
	return &retryAfterError{err: &transientError{err: err}, after: after}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var pe *permanentError
	return errors.As(err, &pe)
}

// IsTransient reports whether err was marked with Transient or WithRetryAfter.
func IsTransient(err error) bool {
	var te *transientError
	return errors.As(err, &te)
}

// DefaultClassifier is deliberately conservative: it retries only errors that
// are known to be transient. Unknown errors are not retried; mark them with
// Transient or supply a RetryIf when the operation is idempotent.
//
// Retried: errors marked Transient; a per-attempt timeout (ErrAttemptTimeout);
// net.Error timeouts; connection refused/reset/broken pipe; unexpected EOF.
// Never retried: errors marked Permanent; context.Canceled; the caller's own
// deadline; errors produced by other policies in this module.
func DefaultClassifier(err error) bool {
	if err == nil {
		return false
	}
	if IsPermanent(err) {
		return false
	}
	if IsTransient(err) {
		return true
	}
	if errors.Is(err, context.Canceled) || resilience.IsPolicyError(err) {
		return false
	}
	if errors.Is(err, resilience.ErrAttemptTimeout) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	return false
}

// Always retries every error except Permanent ones, context.Canceled, the
// caller's deadline, and policy errors. Only for operations that are
// idempotent by construction.
func Always(err error) bool {
	if err == nil || IsPermanent(err) {
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
