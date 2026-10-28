// Package hedge sends a second copy of a slow request after a delay and uses
// whichever answer arrives first. It cuts tail latency at the price of extra
// load, so it is only for idempotent reads, behind a budget, with the delay
// set from the dependency's real p95. Hedging a failing dependency is a
// retry storm with a shorter fuse; the Budget exists to stop that.
package hedge

import (
	"context"
	"fmt"
	"time"

	"github.com/anuar-fest/tools/resilience"
)

// Budget bounds the fraction of calls that may be hedged; a *ratelimit.Bucket
// sized at a few percent of the request rate fits.
type Budget interface {
	Allow() bool
}

// Config configures hedging for one dependency.
type Config struct {
	Name string
	// Delay is how long to wait for the first attempt before sending another.
	// Set it to the dependency's p95; required.
	Delay time.Duration
	// MaxHedges is how many extra attempts may be started (default 1).
	MaxHedges int
	// Budget, when set, is asked before every hedge.
	Budget   Budget
	Observer resilience.Observer
}

func (c Config) validate() (Config, error) {
	if c.Delay <= 0 {
		return c, fmt.Errorf("hedge %q: Delay must be > 0", c.Name)
	}
	if c.MaxHedges <= 0 {
		c.MaxHedges = 1
	}
	c.Observer = resilience.OrNop(c.Observer)
	return c, nil
}

// Do is DoValue for operations without a result.
func Do(ctx context.Context, cfg Config, op func(context.Context) error) error {
	_, err := DoValue(ctx, cfg, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, op(ctx)
	})
	return err
}

// DoValue runs op, starting another copy every Delay while none has answered,
// up to MaxHedges extra copies. The first success wins and the others are
// cancelled through their context; op must honour cancellation or the loser
// keeps running until it finishes on its own. If every started copy fails,
// the last error is returned; failures are not hedged, only slowness.
func DoValue[T any](ctx context.Context, cfg Config, op func(context.Context) (T, error)) (T, error) {
	var zero T
	cfg, err := cfg.validate()
	if err != nil {
		return zero, err
	}

	type result struct {
		v       T
		err     error
		attempt int
	}
	results := make(chan result, cfg.MaxHedges+1)
	cancels := make([]context.CancelFunc, 0, cfg.MaxHedges+1)
	cancelAll := func() {
		for _, c := range cancels {
			c()
		}
	}
	defer cancelAll()

	launch := func(attempt int) {
		actx, cancel := context.WithCancel(ctx)
		cancels = append(cancels, cancel)
		go func() {
			v, err := op(actx)
			results <- result{v: v, err: err, attempt: attempt}
		}()
	}

	start := time.Now()
	launch(1)
	started, outstanding := 1, 1
	timer := time.NewTimer(cfg.Delay)
	defer timer.Stop()

	var lastErr error
	for {
		select {
		case r := <-results:
			outstanding--
			if r.err == nil {
				decision := resilience.DecisionSuccess
				if r.attempt > 1 {
					decision = resilience.DecisionHedgeWon
				}
				cfg.observe(ctx, decision, r.attempt, time.Since(start), nil)
				for range outstanding {
					cfg.observe(ctx, resilience.DecisionHedgeLost, 0, 0, nil)
				}
				return r.v, nil
			}
			lastErr = r.err
			if outstanding == 0 {
				cfg.observe(ctx, resilience.DecisionFailure, r.attempt, time.Since(start), lastErr)
				return zero, lastErr
			}
		case <-timer.C:
			if started > cfg.MaxHedges {
				continue
			}
			if cfg.Budget != nil && !cfg.Budget.Allow() {
				continue
			}
			started++
			outstanding++
			launch(started)
			cfg.observe(ctx, resilience.DecisionHedgeStarted, started, time.Since(start), nil)
			if started <= cfg.MaxHedges {
				timer.Reset(cfg.Delay)
			}
		case <-ctx.Done():
			cfg.observe(ctx, resilience.DecisionCanceled, started, time.Since(start), ctx.Err())
			return zero, ctx.Err()
		}
	}
}

func (c Config) observe(ctx context.Context, d resilience.Decision, attempt int, elapsed time.Duration, err error) {
	c.Observer.ObserveDecision(ctx, resilience.Event{
		Dependency: c.Name,
		Policy:     resilience.PolicyHedge,
		Decision:   d,
		Attempt:    attempt,
		Elapsed:    elapsed,
		Err:        err,
	})
}
