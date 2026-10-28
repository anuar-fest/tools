package breaker_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/breaker"
	"github.com/anuar-fest/tools/resilience/internal/clock"
	"github.com/anuar-fest/tools/resilience/internal/obstest"
)

var errBoom = errors.New("boom")

func newBreaker(t *testing.T, fc *clock.Fake, rec *obstest.Recorder, cfg breaker.Config) *breaker.Breaker {
	t.Helper()
	cfg.Name = "dep"
	cfg.Clock = fc
	cfg.Observer = rec
	cfg.Rand = func() float64 { return 0.5 } // no jitter: OpenFor exactly
	b, err := breaker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func fail(t *testing.T, b *breaker.Breaker, n int) {
	t.Helper()
	for range n {
		p, err := b.Allow(context.Background())
		if err != nil {
			t.Fatalf("allow: %v", err)
		}
		p.Record(errBoom)
	}
}

func TestBreaker_ConsecutiveFailuresOpensThenRecovers(t *testing.T) {
	fc := clock.NewFake()
	rec := &obstest.Recorder{}
	b := newBreaker(t, fc, rec, breaker.Config{Trip: breaker.ConsecutiveFailures(3), OpenFor: 10 * time.Second})

	fail(t, b, 2)
	if b.State() != resilience.BreakerClosed {
		t.Fatal("2 failures must not trip a 3-failure policy")
	}
	fail(t, b, 1)
	if b.State() != resilience.BreakerOpen {
		t.Fatal("3rd failure should open")
	}

	if _, err := b.Allow(context.Background()); !errors.Is(err, resilience.ErrBreakerOpen) {
		t.Fatalf("open breaker must reject, got %v", err)
	}
	if rec.Count(resilience.DecisionBreakerOpen) != 1 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}

	fc.Advance(10 * time.Second)
	p, err := b.Allow(context.Background())
	if err != nil {
		t.Fatalf("after OpenFor a probe must pass: %v", err)
	}
	if b.State() != resilience.BreakerHalfOpen {
		t.Fatalf("state = %v", b.State())
	}
	// Only one probe at a time.
	if _, err := b.Allow(context.Background()); !errors.Is(err, resilience.ErrBreakerOpen) {
		t.Fatal("second concurrent probe must be rejected")
	}
	p.Record(nil)
	if b.State() != resilience.BreakerClosed {
		t.Fatalf("probe success should close, state = %v", b.State())
	}

	changes := rec.Changes()
	want := []resilience.StateChange{
		{Dependency: "dep", From: resilience.BreakerClosed, To: resilience.BreakerOpen},
		{Dependency: "dep", From: resilience.BreakerOpen, To: resilience.BreakerHalfOpen},
		{Dependency: "dep", From: resilience.BreakerHalfOpen, To: resilience.BreakerClosed},
	}
	if len(changes) != len(want) {
		t.Fatalf("changes = %+v", changes)
	}
	for i := range want {
		if changes[i] != want[i] {
			t.Fatalf("change %d = %+v want %+v", i, changes[i], want[i])
		}
	}
}

func TestBreaker_HalfOpenFailureReopens(t *testing.T) {
	fc := clock.NewFake()
	b := newBreaker(t, fc, &obstest.Recorder{}, breaker.Config{Trip: breaker.ConsecutiveFailures(1), OpenFor: time.Second})
	fail(t, b, 1)
	fc.Advance(time.Second)
	p, err := b.Allow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p.Record(errBoom)
	if b.State() != resilience.BreakerOpen {
		t.Fatalf("state = %v", b.State())
	}
	// A new OpenFor window started at the failure.
	fc.Advance(999 * time.Millisecond)
	if _, err := b.Allow(context.Background()); !errors.Is(err, resilience.ErrBreakerOpen) {
		t.Fatal("must stay open for a full OpenFor after a failed probe")
	}
}

func TestBreaker_SuccessesToCloseAndProbeLimit(t *testing.T) {
	fc := clock.NewFake()
	b := newBreaker(t, fc, &obstest.Recorder{}, breaker.Config{
		Trip: breaker.ConsecutiveFailures(1), OpenFor: time.Second, MaxHalfOpenProbes: 2, SuccessesToClose: 2,
	})
	fail(t, b, 1)
	fc.Advance(time.Second)
	p1, err := b.Allow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p2, err := b.Allow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Allow(context.Background()); !errors.Is(err, resilience.ErrBreakerOpen) {
		t.Fatal("third probe must be rejected")
	}
	p1.Record(nil)
	if b.State() != resilience.BreakerHalfOpen {
		t.Fatal("one success is not enough")
	}
	p2.Record(nil)
	if b.State() != resilience.BreakerClosed {
		t.Fatal("two successes should close")
	}
}

func TestBreaker_StalePermitIsIgnored(t *testing.T) {
	fc := clock.NewFake()
	b := newBreaker(t, fc, &obstest.Recorder{}, breaker.Config{Trip: breaker.ConsecutiveFailures(1), OpenFor: time.Second})
	old, err := b.Allow(context.Background()) // obtained while closed
	if err != nil {
		t.Fatal(err)
	}
	fail(t, b, 1) // opens
	fc.Advance(time.Second)
	probe, err := b.Allow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	old.Record(nil) // must not count as the probe's success
	if b.State() != resilience.BreakerHalfOpen {
		t.Fatalf("stale success closed the breaker: %v", b.State())
	}
	probe.Record(nil)
	probe.Record(errBoom) // second Record is ignored
	if b.State() != resilience.BreakerClosed {
		t.Fatalf("state = %v", b.State())
	}
}

func TestBreaker_ErrorRateTrip(t *testing.T) {
	fc := clock.NewFake()
	b := newBreaker(t, fc, &obstest.Recorder{}, breaker.Config{Trip: breaker.ErrorRate(10*time.Second, 10, 0.5), OpenFor: time.Second})
	// 4 failures out of 8: below minRequests, must not trip.
	for i := range 8 {
		p, _ := b.Allow(context.Background())
		if i%2 == 0 {
			p.Record(errBoom)
		} else {
			p.Record(nil)
		}
	}
	if b.State() != resilience.BreakerClosed {
		t.Fatal("below minRequests must not trip")
	}
	// 2 more failures → 6/10 = 60% ≥ 50% with 10 requests.
	fail(t, b, 2)
	if b.State() != resilience.BreakerOpen {
		t.Fatalf("state = %v", b.State())
	}
}

func TestBreaker_ErrorRateWindowSlides(t *testing.T) {
	fc := clock.NewFake()
	b := newBreaker(t, fc, &obstest.Recorder{}, breaker.Config{Trip: breaker.ErrorRate(2*time.Second, 2, 0.5), OpenFor: time.Second})
	fail(t, b, 1)
	fc.Advance(5 * time.Second) // the failure left the window
	fail(t, b, 1)
	if b.State() != resilience.BreakerClosed {
		t.Fatal("one failure in the window must not trip a 2-request minimum")
	}
}

func TestBreaker_SlowCallCountsAsFailure(t *testing.T) {
	fc := clock.NewFake()
	b := newBreaker(t, fc, &obstest.Recorder{}, breaker.Config{Trip: breaker.ConsecutiveFailures(1), SlowCallThreshold: 100 * time.Millisecond})
	p, _ := b.Allow(context.Background())
	fc.Advance(200 * time.Millisecond)
	p.Record(nil)
	if b.State() != resilience.BreakerOpen {
		t.Fatal("a slow success must trip")
	}
}

func TestBreaker_IgnoredAndCancelledDoNotCount(t *testing.T) {
	fc := clock.NewFake()
	b := newBreaker(t, fc, &obstest.Recorder{}, breaker.Config{Trip: breaker.ConsecutiveFailures(1)})
	for _, err := range []error{breaker.Ignore(errBoom), context.Canceled, context.DeadlineExceeded, resilience.ErrBulkheadFull} {
		p, _ := b.Allow(context.Background())
		p.Record(err)
		if b.State() != resilience.BreakerClosed {
			t.Fatalf("%v must not trip", err)
		}
	}
	p, _ := b.Allow(context.Background())
	p.Record(fmt.Errorf("%w: %w", resilience.ErrAttemptTimeout, context.DeadlineExceeded))
	if b.State() != resilience.BreakerOpen {
		t.Fatal("an attempt timeout must trip")
	}
}

func TestBreaker_DoValueRecordsOutcome(t *testing.T) {
	fc := clock.NewFake()
	b := newBreaker(t, fc, &obstest.Recorder{}, breaker.Config{Trip: breaker.ConsecutiveFailures(1), OpenFor: time.Second})
	_, err := breaker.DoValue(context.Background(), b, func(context.Context) (int, error) { return 0, errBoom })
	if !errors.Is(err, errBoom) || b.State() != resilience.BreakerOpen {
		t.Fatalf("err=%v state=%v", err, b.State())
	}
	calls := 0
	_, err = breaker.DoValue(context.Background(), b, func(context.Context) (int, error) { calls++; return 1, nil })
	if !errors.Is(err, resilience.ErrBreakerOpen) || calls != 0 {
		t.Fatalf("open breaker must not run op: err=%v calls=%d", err, calls)
	}
}

func TestBreaker_ResetCloses(t *testing.T) {
	b := newBreaker(t, clock.NewFake(), &obstest.Recorder{}, breaker.Config{Trip: breaker.ConsecutiveFailures(1)})
	fail(t, b, 1)
	b.Reset()
	if b.State() != resilience.BreakerClosed {
		t.Fatal("reset should close")
	}
}

func TestBreaker_FailuresInWindow(t *testing.T) {
	fc := clock.NewFake()
	b := newBreaker(t, fc, &obstest.Recorder{}, breaker.Config{Trip: breaker.FailuresInWindow(10*time.Second, 3)})
	for range 100 {
		p, _ := b.Allow(context.Background())
		p.Record(nil)
	}
	fail(t, b, 3)
	if b.State() != resilience.BreakerOpen {
		t.Fatal("3 failures in window must trip regardless of successes")
	}
}

func TestBreaker_ConcurrentUse(t *testing.T) {
	b := newBreaker(t, clock.NewFake(), &obstest.Recorder{}, breaker.Config{Trip: breaker.ErrorRate(10*time.Second, 50, 0.5), OpenFor: time.Hour})
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Every 4th call fails, so no scheduling prefix exceeds 25% failures.
			for j := range 100 {
				p, err := b.Allow(context.Background())
				if err != nil {
					return
				}
				if j%4 == 3 {
					p.Record(errBoom)
				} else {
					p.Record(nil)
				}
			}
		}()
	}
	wg.Wait()
	if b.State() != resilience.BreakerClosed {
		t.Fatalf("25%% failures must not trip a 50%% policy: %v", b.State())
	}
}

func TestNew_RejectsBadJitter(t *testing.T) {
	if _, err := breaker.New(breaker.Config{OpenForJitter: 2}); err == nil {
		t.Fatal("expected error")
	}
}
