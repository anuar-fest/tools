// Package ratelimit shapes how fast calls are made: a token bucket for one
// dependency or one ingress, and a keyed variant (per tenant, per IP) with
// bounded memory. Limiters only answer Allow; they never Wait on a request
// path, because waiting turns overload into latency and then into timeouts.
package ratelimit

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/anuar-fest/tools/resilience"
)

// Rate is tokens per second.
type Rate float64

// PerSecond is n tokens per second.
func PerSecond(n float64) Rate { return Rate(n) }

// Every is one token per interval.
func Every(interval time.Duration) Rate {
	if interval <= 0 {
		return Rate(math.Inf(1))
	}
	return Rate(float64(time.Second) / float64(interval))
}

// Limiter is the port the stack and middleware accept.
type Limiter interface {
	// Allow reports whether one call may proceed now, consuming a token if so.
	Allow() bool
}

// BucketConfig configures a Bucket.
type BucketConfig struct {
	Rate Rate
	// Burst is the bucket size: how many calls may pass back to back.
	// Keep it at most 2× the per-second rate; a large burst defeats shaping.
	Burst int
	Clock resilience.Clock
}

// Bucket is a token bucket. Safe for concurrent use.
type Bucket struct {
	rate  float64
	burst float64
	clock resilience.Clock

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// NewBucket returns a full bucket.
func NewBucket(cfg BucketConfig) (*Bucket, error) {
	if cfg.Rate <= 0 {
		return nil, fmt.Errorf("ratelimit: Rate must be > 0, got %v", cfg.Rate)
	}
	if cfg.Burst <= 0 {
		return nil, fmt.Errorf("ratelimit: Burst must be > 0, got %d", cfg.Burst)
	}
	cfg.Clock = resilience.OrSystem(cfg.Clock)
	return &Bucket{
		rate:   float64(cfg.Rate),
		burst:  float64(cfg.Burst),
		clock:  cfg.Clock,
		tokens: float64(cfg.Burst),
		last:   cfg.Clock.Now(),
	}, nil
}

// Allow consumes one token if available.
func (b *Bucket) Allow() bool { return b.AllowN(1) }

// AllowN consumes n tokens if all are available; otherwise it consumes none.
func (b *Bucket) AllowN(n int) bool {
	if n <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if b.tokens < float64(n) {
		return false
	}
	b.tokens -= float64(n)
	return true
}

// RetryAfter is how long until one token is available; 0 if one is now.
func (b *Bucket) RetryAfter() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if b.tokens >= 1 {
		return 0
	}
	return time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
}

// Tokens is the current token count, for tests and gauges.
func (b *Bucket) Tokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	return b.tokens
}

// refill adds tokens for the time elapsed. Caller holds b.mu.
func (b *Bucket) refill() {
	now := b.clock.Now()
	elapsed := now.Sub(b.last)
	if elapsed <= 0 {
		return
	}
	b.last = now
	b.tokens = math.Min(b.burst, b.tokens+elapsed.Seconds()*b.rate)
}
