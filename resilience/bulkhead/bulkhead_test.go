package bulkhead_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/bulkhead"
	"github.com/anuar-fest/tools/resilience/internal/obstest"
)

func TestNew_Validates(t *testing.T) {
	if _, err := bulkhead.New(bulkhead.Config{}); err == nil {
		t.Fatal("MaxConcurrent 0 must be rejected")
	}
	if _, err := bulkhead.New(bulkhead.Config{MaxConcurrent: 1, MaxWait: -1}); err == nil {
		t.Fatal("negative MaxWait must be rejected")
	}
}

func TestBulkhead_FailFastWhenFull(t *testing.T) {
	rec := &obstest.Recorder{}
	b, _ := bulkhead.New(bulkhead.Config{Name: "dep", MaxConcurrent: 2, Observer: rec})
	r1, err := b.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r2, err := b.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.InFlight() != 2 || b.Capacity() != 2 {
		t.Fatalf("inflight=%d cap=%d", b.InFlight(), b.Capacity())
	}
	if _, err := b.Acquire(context.Background()); !errors.Is(err, resilience.ErrBulkheadFull) || !bulkhead.IsFull(err) {
		t.Fatalf("got %v", err)
	}
	if rec.Count(resilience.DecisionBulkheadFull) != 1 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
	r1()
	r1() // idempotent
	if b.InFlight() != 1 {
		t.Fatalf("inflight after double release = %d", b.InFlight())
	}
	r2()
	if _, ok := b.TryAcquire(); !ok {
		t.Fatal("slot should be free")
	}
}

func TestBulkhead_BriefWaitGetsSlotWhenReleased(t *testing.T) {
	b, _ := bulkhead.New(bulkhead.Config{Name: "dep", MaxConcurrent: 1, MaxWait: time.Second})
	release, _ := b.Acquire(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		release()
	}()
	start := time.Now()
	r, err := b.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer r()
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("should have acquired as soon as the slot was released")
	}
}

func TestBulkhead_WaitTimesOut(t *testing.T) {
	b, _ := bulkhead.New(bulkhead.Config{Name: "dep", MaxConcurrent: 1, MaxWait: 5 * time.Millisecond})
	release, _ := b.Acquire(context.Background())
	defer release()
	if _, err := b.Acquire(context.Background()); !errors.Is(err, resilience.ErrBulkheadFull) {
		t.Fatalf("got %v", err)
	}
}

func TestBulkhead_WaitHonoursContext(t *testing.T) {
	rec := &obstest.Recorder{}
	b, _ := bulkhead.New(bulkhead.Config{Name: "dep", MaxConcurrent: 1, MaxWait: time.Minute, Observer: rec})
	release, _ := b.Acquire(context.Background())
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := b.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if rec.Count(resilience.DecisionCanceled) != 1 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
}

func TestBulkhead_CapsConcurrencyUnderLoad(t *testing.T) {
	b, _ := bulkhead.New(bulkhead.Config{Name: "dep", MaxConcurrent: 4, MaxWait: time.Second})
	var inflight, peak atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = bulkhead.Do(context.Background(), b, func(context.Context) error {
				n := inflight.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				inflight.Add(-1)
				return nil
			})
		}()
	}
	wg.Wait()
	if peak.Load() > 4 {
		t.Fatalf("peak concurrency %d exceeded cap", peak.Load())
	}
	if b.InFlight() != 0 {
		t.Fatalf("slots leaked: %d", b.InFlight())
	}
}
