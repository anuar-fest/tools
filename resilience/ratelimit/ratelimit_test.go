package ratelimit_test

import (
	"testing"
	"time"

	"github.com/anuar-fest/tools/resilience/internal/clock"
	"github.com/anuar-fest/tools/resilience/ratelimit"
)

func TestBucket_BurstThenRefill(t *testing.T) {
	fc := clock.NewFake()
	b, err := ratelimit.NewBucket(ratelimit.BucketConfig{Rate: ratelimit.PerSecond(10), Burst: 3, Clock: fc})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if !b.Allow() {
			t.Fatalf("burst call %d refused", i)
		}
	}
	if b.Allow() {
		t.Fatal("4th call must be refused")
	}
	if ra := b.RetryAfter(); ra != 100*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want 100ms", ra)
	}
	fc.Advance(100 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("one token should have refilled")
	}
	fc.Advance(time.Hour)
	if got := b.Tokens(); got != 3 {
		t.Fatalf("tokens must cap at burst, got %v", got)
	}
}

func TestBucket_AllowNIsAllOrNothing(t *testing.T) {
	b, _ := ratelimit.NewBucket(ratelimit.BucketConfig{Rate: ratelimit.PerSecond(1), Burst: 5, Clock: clock.NewFake()})
	if b.AllowN(6) {
		t.Fatal("6 > burst must be refused")
	}
	if got := b.Tokens(); got != 5 {
		t.Fatalf("refused AllowN must not consume, tokens=%v", got)
	}
	if !b.AllowN(5) || b.Allow() {
		t.Fatal("AllowN(5) then Allow should be true, false")
	}
}

func TestEvery(t *testing.T) {
	if r := ratelimit.Every(100 * time.Millisecond); r != 10 {
		t.Fatalf("Every(100ms) = %v, want 10/s", r)
	}
}

func TestNewBucket_Validates(t *testing.T) {
	if _, err := ratelimit.NewBucket(ratelimit.BucketConfig{Rate: 0, Burst: 1}); err == nil {
		t.Fatal("zero rate must be rejected")
	}
	if _, err := ratelimit.NewBucket(ratelimit.BucketConfig{Rate: 1, Burst: 0}); err == nil {
		t.Fatal("zero burst must be rejected")
	}
}

func TestKeyed_IsolatesKeysAndReportsRetryAfter(t *testing.T) {
	fc := clock.NewFake()
	k, err := ratelimit.NewKeyed(ratelimit.KeyedConfig{Name: "ingress", Rate: ratelimit.PerSecond(2), Burst: 1, Clock: fc})
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := k.Allow("a"); !ok {
		t.Fatal("first call for a must pass")
	}
	if ok, ra := k.Allow("a"); ok || ra != 500*time.Millisecond {
		t.Fatalf("second call for a: ok=%v retryAfter=%v", ok, ra)
	}
	if ok, _ := k.Allow("b"); !ok {
		t.Fatal("b has its own bucket")
	}
	if k.Len() != 2 {
		t.Fatalf("len = %d", k.Len())
	}
}

func TestKeyed_EvictsIdleKeysAndBoundsMemory(t *testing.T) {
	fc := clock.NewFake()
	k, _ := ratelimit.NewKeyed(ratelimit.KeyedConfig{Rate: 1, Burst: 1, TTL: time.Minute, MaxKeys: 3, Clock: fc})
	k.Allow("a")
	k.Allow("b")
	k.Allow("c")
	fc.Advance(2 * time.Minute) // a, b, c idle past TTL
	k.Allow("d")                // at MaxKeys: sweep makes room
	if k.Len() != 1 {
		t.Fatalf("expected idle keys swept, len = %d", k.Len())
	}
	k.Allow("e")
	k.Allow("f")
	k.Allow("g") // full and nothing idle: one key is evicted
	if k.Len() != 3 {
		t.Fatalf("len must stay at MaxKeys, got %d", k.Len())
	}
}
