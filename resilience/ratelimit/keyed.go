package ratelimit

import (
	"fmt"
	"sync"
	"time"

	"github.com/anuar-fest/tools/resilience"
)

// KeyedConfig configures a Keyed limiter.
type KeyedConfig struct {
	// Name is the limiter label on metrics (an ingress route, "per-tenant").
	Name  string
	Rate  Rate
	Burst int
	// TTL evicts a key not seen for this long (default 10m).
	TTL time.Duration
	// MaxKeys bounds memory; beyond it, idle keys are swept and, if still full,
	// a random key is evicted (default 100 000).
	MaxKeys  int
	Clock    resilience.Clock
	Observer resilience.Observer
}

// Keyed holds one bucket per key with bounded memory. Safe for concurrent use.
type Keyed struct {
	cfg KeyedConfig

	mu      sync.Mutex
	buckets map[string]*keyedEntry
	ops     int
}

type keyedEntry struct {
	bucket *Bucket
	seen   time.Time
}

const sweepEvery = 1024

// NewKeyed validates cfg and returns an empty limiter.
func NewKeyed(cfg KeyedConfig) (*Keyed, error) {
	if cfg.Rate <= 0 || cfg.Burst <= 0 {
		return nil, fmt.Errorf("ratelimit %q: Rate and Burst must be > 0", cfg.Name)
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 10 * time.Minute
	}
	if cfg.MaxKeys <= 0 {
		cfg.MaxKeys = 100_000
	}
	cfg.Clock = resilience.OrSystem(cfg.Clock)
	cfg.Observer = resilience.OrNop(cfg.Observer)
	return &Keyed{cfg: cfg, buckets: make(map[string]*keyedEntry)}, nil
}

// Allow consumes a token for key. On refusal retryAfter says when one token
// will be available, for a Retry-After header.
func (k *Keyed) Allow(key string) (ok bool, retryAfter time.Duration) {
	now := k.cfg.Clock.Now()

	k.mu.Lock()
	k.ops++
	if k.ops%sweepEvery == 0 {
		k.sweepLocked(now)
	}
	e, found := k.buckets[key]
	if !found {
		if len(k.buckets) >= k.cfg.MaxKeys {
			k.sweepLocked(now)
			if len(k.buckets) >= k.cfg.MaxKeys {
				k.evictOneLocked()
			}
		}
		b, _ := NewBucket(BucketConfig{Rate: k.cfg.Rate, Burst: k.cfg.Burst, Clock: k.cfg.Clock})
		e = &keyedEntry{bucket: b}
		k.buckets[key] = e
	}
	e.seen = now
	k.mu.Unlock()

	if e.bucket.Allow() {
		return true, 0
	}
	return false, e.bucket.RetryAfter()
}

// Len is the number of keys currently tracked.
func (k *Keyed) Len() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.buckets)
}

// Name returns the limiter label.
func (k *Keyed) Name() string { return k.cfg.Name }

func (k *Keyed) sweepLocked(now time.Time) {
	for key, e := range k.buckets {
		if now.Sub(e.seen) > k.cfg.TTL {
			delete(k.buckets, key)
		}
	}
}

// evictOneLocked drops one arbitrary key. Map iteration order is random, which
// is the fairest cheap choice.
func (k *Keyed) evictOneLocked() {
	for key := range k.buckets {
		delete(k.buckets, key)
		return
	}
}
