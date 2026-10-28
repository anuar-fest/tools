package retry_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/internal/clock"
	"github.com/anuar-fest/tools/resilience/internal/obstest"
	"github.com/anuar-fest/tools/resilience/retry"
)

var errBoom = errors.New("boom")

// fakeSleep records requested delays and never sleeps.
type fakeSleep struct{ delays []time.Duration }

func (f *fakeSleep) sleep(ctx context.Context, d time.Duration) error {
	f.delays = append(f.delays, d)
	return ctx.Err()
}

func policy(rec *obstest.Recorder, fs *fakeSleep) retry.Policy {
	return retry.Policy{
		Dependency:     "dep",
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     time.Second,
		Jitter:         retry.JitterNone,
		Observer:       rec,
		Sleep:          fs.sleep,
		Rand:           func() float64 { return 0.5 },
	}
}

func TestDoValue_SucceedsFirstAttempt(t *testing.T) {
	rec := &obstest.Recorder{}
	fs := &fakeSleep{}
	calls := 0
	v, err := retry.DoValue(context.Background(), policy(rec, fs), func(context.Context) (int, error) {
		calls++
		return 42, nil
	})
	if err != nil || v != 42 || calls != 1 {
		t.Fatalf("got v=%d err=%v calls=%d", v, err, calls)
	}
	if got := rec.Decisions(); !slices.Equal(got, []resilience.Decision{resilience.DecisionSuccess}) {
		t.Fatalf("decisions = %v", got)
	}
	if len(fs.delays) != 0 {
		t.Fatalf("slept %v", fs.delays)
	}
}

func TestDoValue_RetriesTransientThenSucceeds(t *testing.T) {
	rec := &obstest.Recorder{}
	fs := &fakeSleep{}
	calls := 0
	v, err := retry.DoValue(context.Background(), policy(rec, fs), func(context.Context) (string, error) {
		calls++
		if calls < 3 {
			return "", retry.Transient(errBoom)
		}
		return "ok", nil
	})
	if err != nil || v != "ok" || calls != 3 {
		t.Fatalf("got v=%q err=%v calls=%d", v, err, calls)
	}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}
	if !slices.Equal(fs.delays, want) {
		t.Fatalf("delays = %v, want %v", fs.delays, want)
	}
	wantD := []resilience.Decision{resilience.DecisionRetry, resilience.DecisionRetry, resilience.DecisionSuccess}
	if got := rec.Decisions(); !slices.Equal(got, wantD) {
		t.Fatalf("decisions = %v", got)
	}
	if evs := rec.Events(); evs[2].Attempt != 3 || evs[2].Dependency != "dep" || evs[2].Policy != resilience.PolicyRetry {
		t.Fatalf("success event = %+v", evs[2])
	}
}

func TestDo_PermanentErrorIsNotRetried(t *testing.T) {
	rec := &obstest.Recorder{}
	fs := &fakeSleep{}
	calls := 0
	err := retry.Do(context.Background(), policy(rec, fs), func(context.Context) error {
		calls++
		return retry.Permanent(errBoom)
	})
	if !errors.Is(err, errBoom) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if rec.Count(resilience.DecisionFailure) != 1 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
}

func TestDo_UnknownErrorIsNotRetriedByDefault(t *testing.T) {
	calls := 0
	err := retry.Do(context.Background(), policy(&obstest.Recorder{}, &fakeSleep{}), func(context.Context) error {
		calls++
		return errBoom
	})
	if !errors.Is(err, errBoom) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestDo_ExhaustsAttemptsAndWrapsCount(t *testing.T) {
	rec := &obstest.Recorder{}
	fs := &fakeSleep{}
	calls := 0
	err := retry.Do(context.Background(), policy(rec, fs), func(context.Context) error {
		calls++
		return retry.Transient(errBoom)
	})
	if !errors.Is(err, errBoom) || calls != 3 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if err.Error() != "after 3 attempts: boom" {
		t.Fatalf("message = %q", err.Error())
	}
	if rec.Count(resilience.DecisionRetriesExhausted) != 1 || rec.Count(resilience.DecisionRetry) != 2 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
}

func TestDo_MaxAttemptsOneNeverRetries(t *testing.T) {
	p := policy(&obstest.Recorder{}, &fakeSleep{})
	p.MaxAttempts = 1
	calls := 0
	err := retry.Do(context.Background(), p, func(context.Context) error {
		calls++
		return retry.Transient(errBoom)
	})
	if !errors.Is(err, errBoom) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestDo_CallerCancelDuringSleepStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rec := &obstest.Recorder{}
	p := policy(rec, &fakeSleep{})
	p.Sleep = func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	}
	calls := 0
	err := retry.Do(ctx, p, func(context.Context) error {
		calls++
		return retry.Transient(errBoom)
	})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, errBoom) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if rec.Count(resilience.DecisionCanceled) != 1 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
}

func TestDo_AttemptTimeoutIsRetriedButCallerDeadlineIsNot(t *testing.T) {
	// An attempt that blocks until its own deadline is transient.
	rec := &obstest.Recorder{}
	p := policy(rec, &fakeSleep{})
	p.AttemptTimeout = 5 * time.Millisecond
	calls := 0
	err := retry.Do(context.Background(), p, func(ctx context.Context) error {
		calls++
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, resilience.ErrAttemptTimeout) || !errors.Is(err, context.DeadlineExceeded) || calls != 3 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}

	// The caller's deadline passing is final.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	rec = &obstest.Recorder{}
	p = policy(rec, &fakeSleep{})
	p.AttemptTimeout = time.Second
	calls = 0
	err = retry.Do(ctx, p, func(ctx context.Context) error {
		calls++
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, resilience.ErrAttemptTimeout) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if rec.Count(resilience.DecisionTimeout) != 1 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
}

func TestDo_RetryAfterHintHonouredAndCapped(t *testing.T) {
	fs := &fakeSleep{}
	p := policy(&obstest.Recorder{}, fs)
	p.MaxRetryAfter = time.Second
	calls := 0
	_ = retry.Do(context.Background(), p, func(context.Context) error {
		calls++
		if calls == 1 {
			return retry.WithRetryAfter(errBoom, 700*time.Millisecond)
		}
		return retry.WithRetryAfter(errBoom, time.Hour) // above the cap: ignored
	})
	want := []time.Duration{700 * time.Millisecond, 200 * time.Millisecond}
	if !slices.Equal(fs.delays, want) {
		t.Fatalf("delays = %v, want %v", fs.delays, want)
	}
}

func TestDo_MaxElapsedStopsBeforeSleeping(t *testing.T) {
	fc := clock.NewFake()
	fs := &fakeSleep{}
	rec := &obstest.Recorder{}
	p := policy(rec, fs)
	p.Clock = fc
	p.MaxElapsed = 150 * time.Millisecond
	p.Sleep = func(ctx context.Context, d time.Duration) error {
		fc.Advance(d)
		return fs.sleep(ctx, d)
	}
	calls := 0
	err := retry.Do(context.Background(), p, func(context.Context) error {
		calls++
		return retry.Transient(errBoom)
	})
	// attempt 1 → sleep 100ms (elapsed 100ms) → attempt 2 → next sleep 200ms would exceed 150ms.
	if !errors.Is(err, errBoom) || calls != 2 || len(fs.delays) != 1 {
		t.Fatalf("err=%v calls=%d delays=%v", err, calls, fs.delays)
	}
	if rec.Count(resilience.DecisionRetriesExhausted) != 1 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
}

func TestDo_BudgetRefusesRetry(t *testing.T) {
	rec := &obstest.Recorder{}
	p := policy(rec, &fakeSleep{})
	p.Budget = retry.NewBudget(retry.BudgetConfig{Ratio: 0.1, MinRetriesPerWindow: 1})
	calls := 0
	op := func(context.Context) error {
		calls++
		return retry.Transient(errBoom)
	}
	// First call: budget allows one retry in the window, then refuses.
	err := retry.Do(context.Background(), p, op)
	if !errors.Is(err, resilience.ErrRetryBudget) || !errors.Is(err, errBoom) || calls != 2 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if rec.Count(resilience.DecisionRetryBudget) != 1 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
	// Second call: no budget at all now.
	calls = 0
	err = retry.Do(context.Background(), p, op)
	if !errors.Is(err, resilience.ErrRetryBudget) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestDo_AlreadyCancelledContextDoesNotRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := retry.Do(ctx, policy(&obstest.Recorder{}, &fakeSleep{}), func(context.Context) error {
		calls++
		return nil
	})
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestBackoff_Table(t *testing.T) {
	base := retry.Policy{InitialBackoff: 100 * time.Millisecond, MaxBackoff: 1 * time.Second, Multiplier: 2}
	cases := []struct {
		name    string
		jitter  retry.Jitter
		attempt int
		prev    time.Duration
		r       float64
		want    time.Duration
	}{
		{"none a1", retry.JitterNone, 1, 0, 0.9, 100 * time.Millisecond},
		{"none a3", retry.JitterNone, 3, 0, 0.9, 400 * time.Millisecond},
		{"none capped", retry.JitterNone, 10, 0, 0.9, time.Second},
		{"full r0", retry.JitterFull, 2, 0, 0, 0},
		{"full r.5", retry.JitterFull, 2, 0, 0.5, 100 * time.Millisecond},
		{"equal r0", retry.JitterEqual, 2, 0, 0, 100 * time.Millisecond},
		{"equal r1", retry.JitterEqual, 2, 0, 0.999, 199900 * time.Microsecond},
		{"decorrelated first", retry.JitterDecorrelated, 1, 0, 0.5, 200 * time.Millisecond},
		{"decorrelated grows", retry.JitterDecorrelated, 2, 400 * time.Millisecond, 1, 1 * time.Second},
		{"attempt<1 treated as 1", retry.JitterNone, 0, 0, 0, 100 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := base
			p.Jitter = tc.jitter
			got := retry.Backoff(p, tc.attempt, tc.prev, tc.r)
			if got < tc.want-time.Microsecond || got > tc.want+time.Microsecond {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestBackoff_DecorrelatedNeverExceedsMax(t *testing.T) {
	p := retry.Policy{InitialBackoff: 100 * time.Millisecond, MaxBackoff: 500 * time.Millisecond, Jitter: retry.JitterDecorrelated}
	prev := time.Duration(0)
	for i := 1; i < 20; i++ {
		prev = retry.Backoff(p, i, prev, 0.999)
		if prev > 500*time.Millisecond || prev < 100*time.Millisecond {
			t.Fatalf("attempt %d: %v out of range", i, prev)
		}
	}
}
