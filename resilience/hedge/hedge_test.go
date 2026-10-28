package hedge_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/hedge"
	"github.com/anuar-fest/tools/resilience/internal/obstest"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var errBoom = errors.New("boom")

func TestDoValue_FastFirstAttemptNeverHedges(t *testing.T) {
	rec := &obstest.Recorder{}
	var calls atomic.Int32
	v, err := hedge.DoValue(context.Background(), hedge.Config{Name: "dep", Delay: 50 * time.Millisecond, Observer: rec},
		func(context.Context) (int, error) { calls.Add(1); return 7, nil })
	if err != nil || v != 7 || calls.Load() != 1 {
		t.Fatalf("v=%d err=%v calls=%d", v, err, calls.Load())
	}
	if rec.Count(resilience.DecisionHedgeStarted) != 0 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
}

func TestDoValue_SlowFirstAttemptHedgeWins(t *testing.T) {
	rec := &obstest.Recorder{}
	var calls atomic.Int32
	v, err := hedge.DoValue(context.Background(), hedge.Config{Name: "dep", Delay: 10 * time.Millisecond, Observer: rec},
		func(ctx context.Context) (int, error) {
			n := calls.Add(1)
			if n == 1 {
				select {
				case <-ctx.Done(): // the loser is cancelled
					return 0, ctx.Err()
				case <-time.After(2 * time.Second):
					return 1, nil
				}
			}
			return 2, nil
		})
	if err != nil || v != 2 {
		t.Fatalf("v=%d err=%v", v, err)
	}
	if rec.Count(resilience.DecisionHedgeStarted) != 1 || rec.Count(resilience.DecisionHedgeWon) != 1 || rec.Count(resilience.DecisionHedgeLost) != 1 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
	// goleak: the loser must have exited via ctx by the time TestMain checks.
	time.Sleep(10 * time.Millisecond)
}

func TestDoValue_AllFailReturnsLastError(t *testing.T) {
	// First attempt hangs, hedge fails fast: the call waits for the first
	// attempt, which only ends with the caller's deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var calls atomic.Int32
	_, err := hedge.DoValue(ctx, hedge.Config{Name: "dep", Delay: 5 * time.Millisecond},
		func(ctx context.Context) (int, error) {
			n := calls.Add(1)
			if n == 1 {
				<-ctx.Done()
				return 0, ctx.Err()
			}
			return 0, errBoom
		})
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 2 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}

	// Both fail promptly: the last error is returned.
	calls.Store(0)
	_, err = hedge.DoValue(context.Background(), hedge.Config{Name: "dep", Delay: 5 * time.Millisecond},
		func(ctx context.Context) (int, error) {
			n := calls.Add(1)
			if n == 1 {
				time.Sleep(20 * time.Millisecond)
				return 0, errors.New("first")
			}
			return 0, errBoom
		})
	if err == nil || calls.Load() != 2 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestDoValue_FailureIsNotHedged(t *testing.T) {
	rec := &obstest.Recorder{}
	var calls atomic.Int32
	_, err := hedge.DoValue(context.Background(), hedge.Config{Name: "dep", Delay: time.Hour, Observer: rec},
		func(context.Context) (int, error) { calls.Add(1); return 0, errBoom })
	if !errors.Is(err, errBoom) || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
	if rec.Count(resilience.DecisionFailure) != 1 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
}

type noBudget struct{}

func (noBudget) Allow() bool { return false }

func TestDoValue_BudgetRefusesHedge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var calls atomic.Int32
	_, err := hedge.DoValue(ctx, hedge.Config{Name: "dep", Delay: 5 * time.Millisecond, Budget: noBudget{}},
		func(ctx context.Context) (int, error) {
			calls.Add(1)
			<-ctx.Done()
			return 0, ctx.Err()
		})
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
	time.Sleep(5 * time.Millisecond)
}

func TestDoValue_MaxHedgesBoundsAttempts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var calls atomic.Int32
	_, _ = hedge.DoValue(ctx, hedge.Config{Name: "dep", Delay: 5 * time.Millisecond, MaxHedges: 2},
		func(ctx context.Context) (int, error) {
			calls.Add(1)
			<-ctx.Done()
			return 0, ctx.Err()
		})
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 1 + 2 hedges", calls.Load())
	}
	time.Sleep(5 * time.Millisecond)
}

func TestDoValue_ValidatesDelay(t *testing.T) {
	if _, err := hedge.DoValue(context.Background(), hedge.Config{}, func(context.Context) (int, error) { return 0, nil }); err == nil {
		t.Fatal("Delay 0 must be rejected")
	}
}
