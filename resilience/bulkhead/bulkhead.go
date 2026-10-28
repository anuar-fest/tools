// Package bulkhead caps how many calls to one dependency run at once, so a
// slow upstream can occupy at most MaxConcurrent goroutines and connections
// instead of every one the process has. Pair the cap with the HTTP
// Transport's MaxConnsPerHost for the same dependency.
package bulkhead

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anuar-fest/tools/resilience"
)

// Config configures a Bulkhead.
type Config struct {
	// Name is the dependency label.
	Name string
	// MaxConcurrent is the number of slots. Required.
	MaxConcurrent int
	// MaxWait is how long Acquire may wait for a slot. 0 means fail fast:
	// a full bulkhead is a signal to shed, not a queue to sit in.
	MaxWait  time.Duration
	Observer resilience.Observer
}

// Bulkhead is a counting semaphore with a bounded wait. Safe for concurrent use.
type Bulkhead struct {
	cfg      Config
	slots    chan struct{}
	inflight atomic.Int64
}

// New validates cfg and returns an empty bulkhead.
func New(cfg Config) (*Bulkhead, error) {
	if cfg.MaxConcurrent <= 0 {
		return nil, fmt.Errorf("bulkhead %q: MaxConcurrent must be > 0", cfg.Name)
	}
	if cfg.MaxWait < 0 {
		return nil, fmt.Errorf("bulkhead %q: MaxWait must be >= 0", cfg.Name)
	}
	cfg.Observer = resilience.OrNop(cfg.Observer)
	return &Bulkhead{cfg: cfg, slots: make(chan struct{}, cfg.MaxConcurrent)}, nil
}

// Acquire takes a slot, waiting at most MaxWait. It returns ErrBulkheadFull
// when no slot frees up in time, or ctx.Err() if ctx ends first. The returned
// release is idempotent.
func (b *Bulkhead) Acquire(ctx context.Context) (release func(), err error) {
	if r, ok := b.TryAcquire(); ok {
		return r, nil
	}
	if b.cfg.MaxWait <= 0 {
		b.observe(ctx, resilience.DecisionBulkheadFull, resilience.ErrBulkheadFull)
		return nil, resilience.ErrBulkheadFull
	}
	if err := ctx.Err(); err != nil {
		b.observe(ctx, resilience.DecisionCanceled, err)
		return nil, err
	}

	timer := time.NewTimer(b.cfg.MaxWait)
	defer timer.Stop()
	select {
	case b.slots <- struct{}{}:
		b.inflight.Add(1)
		return b.releaser(), nil
	case <-ctx.Done():
		b.observe(ctx, resilience.DecisionCanceled, ctx.Err())
		return nil, ctx.Err()
	case <-timer.C:
		b.observe(ctx, resilience.DecisionBulkheadFull, resilience.ErrBulkheadFull)
		return nil, resilience.ErrBulkheadFull
	}
}

// TryAcquire takes a slot without waiting.
func (b *Bulkhead) TryAcquire() (release func(), ok bool) {
	select {
	case b.slots <- struct{}{}:
		b.inflight.Add(1)
		return b.releaser(), true
	default:
		return nil, false
	}
}

func (b *Bulkhead) releaser() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			<-b.slots
			b.inflight.Add(-1)
		})
	}
}

// InFlight is the number of slots currently held.
func (b *Bulkhead) InFlight() int { return int(b.inflight.Load()) }

// Capacity is MaxConcurrent.
func (b *Bulkhead) Capacity() int { return b.cfg.MaxConcurrent }

// Name returns the dependency label.
func (b *Bulkhead) Name() string { return b.cfg.Name }

func (b *Bulkhead) observe(ctx context.Context, d resilience.Decision, err error) {
	b.cfg.Observer.ObserveDecision(ctx, resilience.Event{
		Dependency: b.cfg.Name,
		Policy:     resilience.PolicyBulkhead,
		Decision:   d,
		Err:        err,
	})
}

// Do runs op inside a slot.
func Do(ctx context.Context, b *Bulkhead, op func(context.Context) error) error {
	_, err := DoValue(ctx, b, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, op(ctx)
	})
	return err
}

// DoValue runs op inside a slot and releases it when op returns.
func DoValue[T any](ctx context.Context, b *Bulkhead, op func(context.Context) (T, error)) (T, error) {
	var zero T
	release, err := b.Acquire(ctx)
	if err != nil {
		return zero, err
	}
	defer release()
	return op(ctx)
}

// IsFull reports whether err is a bulkhead rejection.
func IsFull(err error) bool { return errors.Is(err, resilience.ErrBulkheadFull) }
