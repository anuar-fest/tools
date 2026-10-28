package stack

import (
	"fmt"
	"time"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/breaker"
	"github.com/anuar-fest/tools/resilience/bulkhead"
	"github.com/anuar-fest/tools/resilience/retry"
)

// DepClass is a typed preset for a kind of dependency. Presets exist so
// services share one reviewed set of numbers instead of forty guesses.
type DepClass int

const (
	// ReadFast: a lookup that normally answers in tens of milliseconds
	// (feature flags, a cache, a small internal API). Budget 500ms, 3 attempts.
	ReadFast DepClass = iota
	// ReadSlow: a query or search that legitimately takes seconds. Budget 5s,
	// 2 attempts.
	ReadSlow
	// WriteIdempotent: a write that may be repeated safely (PUT, an
	// idempotency-keyed POST). Budget 3s, 3 attempts.
	WriteIdempotent
	// WriteOnce: a write whose repeat would duplicate an effect (a payment
	// without a key). Budget 3s, no retries; the breaker and bulkhead still apply.
	WriteOnce
)

func (c DepClass) String() string {
	switch c {
	case ReadFast:
		return "read_fast"
	case ReadSlow:
		return "read_slow"
	case WriteIdempotent:
		return "write_idempotent"
	case WriteOnce:
		return "write_once"
	default:
		return "unknown"
	}
}

// PresetOptions tunes the parts of a preset that depend on the deployment.
type PresetOptions struct {
	// MaxConcurrent sizes the bulkhead (default 32). Match it to the HTTP
	// transport's MaxConnsPerHost for the same dependency.
	MaxConcurrent int
	Observer      resilience.Observer
}

// Preset returns a ready Config for class, including a bulkhead and a
// breaker. Override fields on the result before calling New when a
// dependency genuinely differs; keep the override next to the dependency.
func Preset(name string, class DepClass, opts PresetOptions) (Config, error) {
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = 32
	}
	bh, err := bulkhead.New(bulkhead.Config{Name: name, MaxConcurrent: opts.MaxConcurrent, Observer: opts.Observer})
	if err != nil {
		return Config{}, fmt.Errorf("preset %s for %q: %w", class, name, err)
	}
	br, err := breaker.New(breaker.Config{
		Name:     name,
		Trip:     breaker.ErrorRate(10*time.Second, 20, 0.5),
		OpenFor:  10 * time.Second,
		Observer: opts.Observer,
	})
	if err != nil {
		return Config{}, fmt.Errorf("preset %s for %q: %w", class, name, err)
	}

	cfg := Config{Name: name, Bulkhead: bh, Breaker: br, Observer: opts.Observer, Reserve: 50 * time.Millisecond}
	switch class {
	case ReadFast:
		cfg.Timeout = 500 * time.Millisecond
		cfg.Retry = retry.Policy{MaxAttempts: 3, AttemptTimeout: 200 * time.Millisecond, InitialBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond}
	case ReadSlow:
		cfg.Timeout = 5 * time.Second
		cfg.Retry = retry.Policy{MaxAttempts: 2, AttemptTimeout: 2500 * time.Millisecond, InitialBackoff: 100 * time.Millisecond, MaxBackoff: 500 * time.Millisecond}
	case WriteIdempotent:
		cfg.Timeout = 3 * time.Second
		cfg.Retry = retry.Policy{MaxAttempts: 3, AttemptTimeout: time.Second, InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second}
	case WriteOnce:
		cfg.Timeout = 3 * time.Second
		cfg.Retry = retry.Policy{MaxAttempts: 1, AttemptTimeout: 2500 * time.Millisecond}
	default:
		return Config{}, fmt.Errorf("preset for %q: unknown class %d", name, int(class))
	}
	return cfg, nil
}
