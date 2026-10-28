package workpool_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/internal/obstest"
	"github.com/anuar-fest/tools/resilience/workpool"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestPool_RunsJobsAndDrainsOnClose(t *testing.T) {
	p, err := workpool.New(workpool.Config{Name: "p", Workers: 2, QueueSize: 8})
	if err != nil {
		t.Fatal(err)
	}
	var ran atomic.Int32
	for range 8 {
		if err := p.Submit(context.Background(), func(context.Context) { ran.Add(1) }); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 8 {
		t.Fatalf("ran %d of 8", ran.Load())
	}
	if err := p.Submit(context.Background(), func(context.Context) {}); !errors.Is(err, resilience.ErrPoolClosed) {
		t.Fatalf("submit after close = %v", err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("second close = %v", err)
	}
}

func TestPool_TrySubmitReportsFull(t *testing.T) {
	rec := &obstest.Recorder{}
	p, _ := workpool.New(workpool.Config{Name: "p", Workers: 1, QueueSize: 1, Observer: rec})
	block := make(chan struct{})
	// Occupy the worker, then fill the queue.
	_ = p.Submit(context.Background(), func(context.Context) { <-block })
	waitUntil(t, func() bool { return p.Queued() == 0 })
	if err := p.TrySubmit(func(context.Context) {}); err != nil {
		t.Fatalf("queue should have room: %v", err)
	}
	if err := p.TrySubmit(func(context.Context) {}); !errors.Is(err, resilience.ErrPoolFull) {
		t.Fatalf("got %v, want ErrPoolFull", err)
	}
	if rec.Count(resilience.DecisionPoolFull) != 1 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
	close(block)
	_ = p.Close(context.Background())
}

func TestPool_SubmitBlocksUntilContextOrClose(t *testing.T) {
	p, _ := workpool.New(workpool.Config{Name: "p", Workers: 1, QueueSize: 1})
	block := make(chan struct{})
	_ = p.Submit(context.Background(), func(context.Context) { <-block })
	waitUntil(t, func() bool { return p.Queued() == 0 })
	_ = p.Submit(context.Background(), func(context.Context) {}) // fills the queue

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Submit(ctx, func(context.Context) {}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked submit = %v", err)
	}

	// A blocked Submit is released by Close.
	done := make(chan error, 1)
	go func() { done <- p.Submit(context.Background(), func(context.Context) {}) }()
	time.Sleep(10 * time.Millisecond)
	closeDone := make(chan struct{})
	go func() {
		close(block)
		_ = p.Close(context.Background())
		close(closeDone)
	}()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, resilience.ErrPoolClosed) {
			t.Fatalf("submit during close = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Submit stayed blocked across Close")
	}
	<-closeDone
}

func TestPool_CloseDeadlineCancelsRunningJobs(t *testing.T) {
	p, _ := workpool.New(workpool.Config{Name: "p", Workers: 1, QueueSize: 1})
	var sawCancel atomic.Bool
	_ = p.Submit(context.Background(), func(ctx context.Context) {
		<-ctx.Done()
		sawCancel.Store(true)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close = %v", err)
	}
	if !sawCancel.Load() {
		t.Fatal("job must have been cancelled and joined")
	}
}

func TestPool_RecoversPanics(t *testing.T) {
	rec := &obstest.Recorder{}
	p, _ := workpool.New(workpool.Config{Name: "p", Workers: 1, QueueSize: 4, Observer: rec})
	_ = p.Submit(context.Background(), func(context.Context) { panic("boom") })
	var ran atomic.Bool
	_ = p.Submit(context.Background(), func(context.Context) { ran.Store(true) })
	_ = p.Close(context.Background())
	if !ran.Load() || p.Panics() != 1 {
		t.Fatalf("ran=%v panics=%d", ran.Load(), p.Panics())
	}
	if rec.Count(resilience.DecisionPanic) != 1 || rec.Count(resilience.DecisionSuccess) != 1 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
}

func TestPool_ConcurrentSubmitAndClose(t *testing.T) {
	p, _ := workpool.New(workpool.Config{Name: "p", Workers: 4, QueueSize: 4})
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				_ = p.TrySubmit(func(context.Context) {})
				_ = p.Submit(context.Background(), func(context.Context) {})
			}
		}()
	}
	time.Sleep(time.Millisecond)
	_ = p.Close(context.Background())
	wg.Wait()
}

func TestNew_Validates(t *testing.T) {
	if _, err := workpool.New(workpool.Config{Workers: -1}); err == nil {
		t.Fatal("negative workers must be rejected")
	}
	p, err := workpool.New(workpool.Config{})
	if err != nil || p.Workers() != 4 || p.Capacity() != 8 {
		t.Fatalf("defaults: err=%v workers=%d cap=%d", err, p.Workers(), p.Capacity())
	}
	_ = p.Close(context.Background())
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(time.Millisecond)
	}
}
