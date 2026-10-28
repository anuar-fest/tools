package stack_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/breaker"
	"github.com/anuar-fest/tools/resilience/bulkhead"
	"github.com/anuar-fest/tools/resilience/internal/obstest"
	"github.com/anuar-fest/tools/resilience/retry"
	"github.com/anuar-fest/tools/resilience/stack"
)

var errBoom = errors.New("boom")

type denyAll struct{}

func (denyAll) Allow() bool { return false }

func noSleep(ctx context.Context, _ time.Duration) error { return ctx.Err() }

func TestNew_Validates(t *testing.T) {
	if _, err := stack.New(stack.Config{}); err == nil {
		t.Fatal("Name required")
	}
	if _, err := stack.New(stack.Config{Name: "x", Timeout: -1}); err == nil {
		t.Fatal("negative timeout rejected")
	}
}

func TestDoValue_RetriesThroughBreakerAndRecordsEveryAttempt(t *testing.T) {
	rec := &obstest.Recorder{}
	br, _ := breaker.New(breaker.Config{Name: "dep", Trip: breaker.ConsecutiveFailures(3), OpenFor: time.Hour, Observer: rec})
	s, err := stack.New(stack.Config{
		Name:     "dep",
		Breaker:  br,
		Retry:    retry.Policy{MaxAttempts: 3, Sleep: noSleep, Jitter: retry.JitterNone},
		Observer: rec,
	})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	_, err = stack.DoValue(context.Background(), s, func(context.Context) (int, error) {
		calls++
		return 0, retry.Transient(errBoom)
	})
	if !errors.Is(err, errBoom) || calls != 3 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	// Three failed attempts were each recorded: the breaker is open.
	if br.State() != resilience.BreakerOpen {
		t.Fatalf("breaker = %v, every attempt must be recorded", br.State())
	}
	// Next call fails fast without running op and without retrying.
	calls = 0
	_, err = stack.DoValue(context.Background(), s, func(context.Context) (int, error) { calls++; return 1, nil })
	if !errors.Is(err, resilience.ErrBreakerOpen) || calls != 0 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	// breaker_open is reported twice on purpose: once by the breaker (policy
	// decision) and once by the stack (call outcome). The retry layer stays quiet.
	if rec.Count(resilience.DecisionRetry) != 2 || rec.Count(resilience.DecisionBreakerOpen) != 2 || rec.Count(resilience.DecisionFailure) != 1 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
	last := rec.Events()[len(rec.Events())-1]
	if last.Policy != resilience.PolicyStack || last.Decision != resilience.DecisionBreakerOpen {
		t.Fatalf("final stack event = %+v", last)
	}
}

func TestDoValue_AttemptTimeoutTripsBreaker(t *testing.T) {
	br, _ := breaker.New(breaker.Config{Name: "dep", Trip: breaker.ConsecutiveFailures(2), OpenFor: time.Hour})
	s, _ := stack.New(stack.Config{
		Name:    "dep",
		Breaker: br,
		Retry:   retry.Policy{MaxAttempts: 2, AttemptTimeout: 5 * time.Millisecond, Sleep: noSleep},
	})
	err := s.Do(context.Background(), func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, resilience.ErrAttemptTimeout) {
		t.Fatalf("err = %v", err)
	}
	if br.State() != resilience.BreakerOpen {
		t.Fatalf("a hung dependency must trip the breaker, state = %v", br.State())
	}
}

func TestDoValue_BulkheadAndLimiterRejectBeforeRetry(t *testing.T) {
	rec := &obstest.Recorder{}
	bh, _ := bulkhead.New(bulkhead.Config{Name: "dep", MaxConcurrent: 1})
	release, _ := bh.Acquire(context.Background())
	defer release()
	s, _ := stack.New(stack.Config{Name: "dep", Bulkhead: bh, Observer: rec, Retry: retry.Policy{Sleep: noSleep}})
	calls := 0
	err := s.Do(context.Background(), func(context.Context) error { calls++; return nil })
	if !errors.Is(err, resilience.ErrBulkheadFull) || calls != 0 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}

	s2, _ := stack.New(stack.Config{Name: "dep", Limiter: denyAll{}, Observer: rec})
	err = s2.Do(context.Background(), func(context.Context) error { calls++; return nil })
	if !errors.Is(err, resilience.ErrRateLimited) || calls != 0 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if rec.Count(resilience.DecisionBulkheadFull) != 1 || rec.Count(resilience.DecisionLimitExceeded) != 1 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
}

func TestDoValue_CallTimeoutBoundsRetries(t *testing.T) {
	s, _ := stack.New(stack.Config{
		Name:    "dep",
		Timeout: 30 * time.Millisecond,
		Retry:   retry.Policy{MaxAttempts: 100, RetryIf: retry.Always, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, Jitter: retry.JitterNone},
	})
	start := time.Now()
	err := s.Do(context.Background(), func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
			return errBoom
		}
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("call timeout not honoured")
	}
	if stack.Outcome(err) != resilience.DecisionTimeout {
		t.Fatalf("outcome = %v", stack.Outcome(err))
	}
}

func TestOutcome(t *testing.T) {
	cases := map[error]resilience.Decision{
		nil:                        resilience.DecisionSuccess,
		resilience.ErrBulkheadFull: resilience.DecisionBulkheadFull,
		resilience.ErrRateLimited:  resilience.DecisionLimitExceeded,
		resilience.ErrBreakerOpen:  resilience.DecisionBreakerOpen,
		resilience.ErrRetryBudget:  resilience.DecisionRetryBudget,
		context.DeadlineExceeded:   resilience.DecisionTimeout,
		context.Canceled:           resilience.DecisionCanceled,
		errBoom:                    resilience.DecisionFailure,
		retry.Transient(errBoom):   resilience.DecisionFailure,
	}
	for err, want := range cases {
		if got := stack.Outcome(err); got != want {
			t.Fatalf("Outcome(%v) = %v want %v", err, got, want)
		}
	}
}

func TestPreset_BuildsRunnableStack(t *testing.T) {
	for _, class := range []stack.DepClass{stack.ReadFast, stack.ReadSlow, stack.WriteIdempotent, stack.WriteOnce} {
		cfg, err := stack.Preset("dep", class, stack.PresetOptions{MaxConcurrent: 4})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Bulkhead == nil || cfg.Breaker == nil || cfg.Timeout == 0 {
			t.Fatalf("%s: incomplete preset %+v", class, cfg)
		}
		s, err := stack.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		v, err := stack.DoValue(context.Background(), s, func(context.Context) (string, error) { return "ok", nil })
		if err != nil || v != "ok" {
			t.Fatalf("%s: v=%q err=%v", class, v, err)
		}
	}
	cfg, _ := stack.Preset("dep", stack.WriteOnce, stack.PresetOptions{})
	if cfg.Retry.MaxAttempts != 1 {
		t.Fatal("WriteOnce must not retry")
	}
	if _, err := stack.Preset("dep", stack.DepClass(99), stack.PresetOptions{}); err == nil {
		t.Fatal("unknown class rejected")
	}
}
