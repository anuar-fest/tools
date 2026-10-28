package resilience_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/anuar-fest/tools/resilience"
)

type counting struct{ decisions, states int }

func (c *counting) ObserveDecision(context.Context, resilience.Event)           { c.decisions++ }
func (c *counting) ObserveBreakerState(context.Context, resilience.StateChange) { c.states++ }

func TestObservers_FanOutAndSkipNil(t *testing.T) {
	a, b := &counting{}, &counting{}
	o := resilience.Observers(a, nil, b)
	o.ObserveDecision(context.Background(), resilience.Event{})
	o.ObserveBreakerState(context.Background(), resilience.StateChange{})
	if a.decisions != 1 || b.decisions != 1 || a.states != 1 || b.states != 1 {
		t.Fatalf("a=%+v b=%+v", a, b)
	}
	if resilience.Observers(a) != a {
		t.Fatal("a single observer should be returned as is")
	}
	if _, ok := resilience.OrNop(nil).(resilience.NopObserver); !ok {
		t.Fatal("OrNop(nil) must be NopObserver")
	}
}

func TestIsPolicyError(t *testing.T) {
	for _, err := range []error{
		resilience.ErrBulkheadFull, resilience.ErrBreakerOpen, resilience.ErrRateLimited, resilience.ErrRetryBudget,
		resilience.ErrPoolFull, resilience.ErrPoolClosed, resilience.ErrShed, resilience.ErrNoBudget,
	} {
		if !resilience.IsPolicyError(fmt.Errorf("wrapped: %w", err)) {
			t.Fatalf("%v should be a policy error", err)
		}
	}
	if resilience.IsPolicyError(errors.New("x")) || resilience.IsPolicyError(resilience.ErrAttemptTimeout) || resilience.IsPolicyError(nil) {
		t.Fatal("not policy errors")
	}
}

func TestBreakerStateString(t *testing.T) {
	if resilience.BreakerClosed.String() != "closed" || resilience.BreakerOpen.String() != "open" ||
		resilience.BreakerHalfOpen.String() != "half_open" || resilience.BreakerState(9).String() != "unknown" {
		t.Fatal("bad strings")
	}
}
