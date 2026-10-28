package coalesce_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/coalesce"
	"github.com/anuar-fest/tools/resilience/internal/obstest"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestGroup_ConcurrentCallersShareOneFill(t *testing.T) {
	rec := &obstest.Recorder{}
	g := coalesce.New[string](coalesce.Config{Name: "merchant", Observer: rec})
	var fills atomic.Int32
	release := make(chan struct{})
	fill := func(context.Context) (string, error) {
		fills.Add(1)
		<-release
		return "v", nil
	}

	var wg sync.WaitGroup
	results := make([]string, 16)
	for i := range 16 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := g.Do(context.Background(), "k", fill)
			if err != nil {
				t.Error(err)
			}
			results[i] = v
		}(i)
	}
	time.Sleep(10 * time.Millisecond)
	close(release)
	wg.Wait()
	if fills.Load() != 1 {
		t.Fatalf("fills = %d", fills.Load())
	}
	for _, r := range results {
		if r != "v" {
			t.Fatalf("result = %q", r)
		}
	}
	if rec.Count(resilience.DecisionCoalesced) != 15 || rec.Count(resilience.DecisionSuccess) != 1 {
		t.Fatalf("decisions: coalesced=%d success=%d", rec.Count(resilience.DecisionCoalesced), rec.Count(resilience.DecisionSuccess))
	}
	// Next call after completion fills again.
	if _, err := g.Do(context.Background(), "k", func(context.Context) (string, error) { fills.Add(1); return "w", nil }); err != nil {
		t.Fatal(err)
	}
	if fills.Load() != 2 {
		t.Fatalf("fills = %d", fills.Load())
	}
}

func TestGroup_LeaderCancelDoesNotFailWaiters(t *testing.T) {
	g := coalesce.New[int](coalesce.Config{Name: "k", FillTimeout: time.Second})
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	started := make(chan struct{})
	release := make(chan struct{})
	fill := func(ctx context.Context) (int, error) {
		close(started)
		select {
		case <-release:
			return 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	leaderDone := make(chan error, 1)
	go func() {
		_, err := g.Do(leaderCtx, "k", fill)
		leaderDone <- err
	}()
	<-started
	waiterDone := make(chan int, 1)
	go func() {
		v, _ := g.Do(context.Background(), "k", fill)
		waiterDone <- v
	}()
	time.Sleep(5 * time.Millisecond)
	cancelLeader()
	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader = %v", err)
	}
	close(release)
	if v := <-waiterDone; v != 1 {
		t.Fatalf("waiter got %d, the fill must survive the leader", v)
	}
}

func TestGroup_WaiterHonoursOwnContext(t *testing.T) {
	g := coalesce.New[int](coalesce.Config{Name: "k"})
	release := make(chan struct{})
	go func() {
		_, _ = g.Do(context.Background(), "k", func(context.Context) (int, error) { <-release; return 1, nil })
	}()
	time.Sleep(5 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := g.Do(ctx, "k", func(context.Context) (int, error) { return 0, nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter = %v", err)
	}
	close(release)
	time.Sleep(5 * time.Millisecond)
}

func TestGroup_FillTimeoutBoundsStuckFill(t *testing.T) {
	g := coalesce.New[int](coalesce.Config{Name: "k", FillTimeout: 10 * time.Millisecond})
	_, err := g.Do(context.Background(), "k", func(ctx context.Context) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestGroup_PanicBecomesError(t *testing.T) {
	g := coalesce.New[int](coalesce.Config{Name: "k"})
	_, err := g.Do(context.Background(), "k", func(context.Context) (int, error) { panic("boom") })
	if err == nil || err.Error() != `coalesce "k": fill panicked: boom` {
		t.Fatalf("err = %v", err)
	}
	// The key is released after a panic.
	v, err := g.Do(context.Background(), "k", func(context.Context) (int, error) { return 5, nil })
	if err != nil || v != 5 {
		t.Fatalf("v=%d err=%v", v, err)
	}
}

func TestGroup_Forget(t *testing.T) {
	g := coalesce.New[int](coalesce.Config{Name: "k"})
	release := make(chan struct{})
	var fills atomic.Int32
	go func() {
		_, _ = g.Do(context.Background(), "k", func(context.Context) (int, error) { fills.Add(1); <-release; return 1, nil })
	}()
	time.Sleep(5 * time.Millisecond)
	g.Forget("k")
	v, err := g.Do(context.Background(), "k", func(context.Context) (int, error) { fills.Add(1); return 2, nil })
	if err != nil || v != 2 || fills.Load() != 2 {
		t.Fatalf("v=%d err=%v fills=%d", v, err, fills.Load())
	}
	close(release)
	time.Sleep(5 * time.Millisecond)
}
