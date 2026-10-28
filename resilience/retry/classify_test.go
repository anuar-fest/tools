package retry_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/retry"
)

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

var _ net.Error = timeoutError{}

func TestDefaultClassifier(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unknown", errors.New("x"), false},
		{"transient", retry.Transient(errors.New("x")), true},
		{"permanent wins over transient", retry.Permanent(retry.Transient(errors.New("x"))), false},
		{"wrapped permanent", fmt.Errorf("ctx: %w", retry.Permanent(errBoom)), false},
		{"canceled", context.Canceled, false},
		{"caller deadline", context.DeadlineExceeded, false},
		{"attempt timeout", fmt.Errorf("%w: %w", resilience.ErrAttemptTimeout, context.DeadlineExceeded), true},
		{"net timeout", &net.OpError{Err: timeoutError{}}, true},
		{"conn refused", &net.OpError{Err: syscall.ECONNREFUSED}, true},
		{"conn reset", syscall.ECONNRESET, true},
		{"unexpected eof", io.ErrUnexpectedEOF, true},
		{"breaker open", resilience.ErrBreakerOpen, false},
		{"bulkhead full", fmt.Errorf("x: %w", resilience.ErrBulkheadFull), false},
		{"rate limited", resilience.ErrRateLimited, false},
		{"retry-after", retry.WithRetryAfter(errors.New("429"), time.Second), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := retry.DefaultClassifier(tc.err); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestAlways(t *testing.T) {
	if !retry.Always(errors.New("x")) {
		t.Fatal("unknown errors should be retried by Always")
	}
	if retry.Always(retry.Permanent(errors.New("x"))) || retry.Always(context.Canceled) || retry.Always(resilience.ErrShed) {
		t.Fatal("permanent, canceled and policy errors must not be retried")
	}
	if retry.Always(context.DeadlineExceeded) {
		t.Fatal("caller deadline must not be retried")
	}
	if !retry.Always(fmt.Errorf("%w: %w", resilience.ErrAttemptTimeout, context.DeadlineExceeded)) {
		t.Fatal("attempt timeout must be retried")
	}
}

func TestMarkersPreserveUnwrapAndNil(t *testing.T) {
	if retry.Permanent(nil) != nil || retry.Transient(nil) != nil || retry.WithRetryAfter(nil, time.Second) != nil {
		t.Fatal("nil in must be nil out")
	}
	err := retry.WithRetryAfter(errBoom, 2*time.Second)
	if !errors.Is(err, errBoom) || !retry.IsTransient(err) || retry.IsPermanent(err) {
		t.Fatalf("unexpected classification for %v", err)
	}
	var ra retry.RetryAfterer
	if !errors.As(err, &ra) || ra.RetryAfter() != 2*time.Second {
		t.Fatal("RetryAfter not carried")
	}
	if err.Error() != "boom" {
		t.Fatalf("markers must not change the message: %q", err.Error())
	}
}
