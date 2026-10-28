// Package coalesce folds concurrent calls for the same key into one
// execution, so a cache miss under load produces one database read instead of
// a stampede. The leader runs with a detached context: a leader whose caller
// gives up must not fail every waiter behind it.
package coalesce

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/deadline"
)

// Config configures a Group.
type Config struct {
	Name string
	// FillTimeout bounds the detached fill (default 10s). It must be longer
	// than the slowest legitimate fill and shorter than "stuck".
	FillTimeout time.Duration
	Observer    resilience.Observer
}

// Group deduplicates in-flight fills by key. Safe for concurrent use.
type Group[V any] struct {
	cfg   Config
	mu    sync.Mutex
	calls map[string]*call[V]
}

type call[V any] struct {
	done chan struct{}
	v    V
	err  error
}

// New returns an empty group.
func New[V any](cfg Config) *Group[V] {
	if cfg.FillTimeout <= 0 {
		cfg.FillTimeout = 10 * time.Second
	}
	cfg.Observer = resilience.OrNop(cfg.Observer)
	return &Group[V]{cfg: cfg, calls: make(map[string]*call[V])}
}

// Do returns the result of fill for key, running it at most once for all
// callers that arrive while it is in flight. Each caller still honours its
// own ctx: a waiter whose ctx ends gets ctx.Err() while the fill continues
// for the others. A panic in fill becomes an error for every caller.
func (g *Group[V]) Do(ctx context.Context, key string, fill func(context.Context) (V, error)) (V, error) {
	var zero V

	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		g.observe(ctx, resilience.DecisionCoalesced)
		return g.wait(ctx, c)
	}
	c := &call[V]{done: make(chan struct{})}
	g.calls[key] = c
	g.mu.Unlock()

	fctx, cancel := deadline.Detach(ctx, g.cfg.FillTimeout)
	go func() {
		defer cancel()
		defer func() {
			if r := recover(); r != nil {
				c.err = fmt.Errorf("coalesce %q: fill panicked: %v", g.cfg.Name, r)
			}
			g.mu.Lock()
			if g.calls[key] == c { // Forget may have replaced us
				delete(g.calls, key)
			}
			g.mu.Unlock()
			close(c.done)
		}()
		c.v, c.err = fill(fctx)
	}()

	v, err := g.wait(ctx, c)
	if err == nil {
		g.observe(ctx, resilience.DecisionSuccess)
	} else if ctx.Err() == nil {
		g.observe(ctx, resilience.DecisionFailure)
	}
	if err != nil {
		return zero, err
	}
	return v, nil
}

func (g *Group[V]) wait(ctx context.Context, c *call[V]) (V, error) {
	var zero V
	select {
	case <-c.done:
		return c.v, c.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

// Forget drops the in-flight call for key so the next Do starts a fresh fill.
// Callers already waiting still receive the running fill's result.
func (g *Group[V]) Forget(key string) {
	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
}

func (g *Group[V]) observe(ctx context.Context, d resilience.Decision) {
	g.cfg.Observer.ObserveDecision(ctx, resilience.Event{
		Dependency: g.cfg.Name,
		Policy:     resilience.PolicyCoalesce,
		Decision:   d,
	})
}
