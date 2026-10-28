package deadline_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/deadline"
)

func within(got, want, tol time.Duration) bool { return got >= want-tol && got <= want+tol }

func TestChild_OwnTimeoutWhenParentHasNone(t *testing.T) {
	ctx, cancel := deadline.Child(context.Background(), 100*time.Millisecond, 10*time.Millisecond)
	defer cancel()
	left, ok := deadline.Remaining(ctx)
	if !ok || !within(left, 100*time.Millisecond, 20*time.Millisecond) {
		t.Fatalf("remaining = %v ok=%v", left, ok)
	}
}

func TestChild_ClampsToParentMinusReserve(t *testing.T) {
	parent, pcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer pcancel()
	ctx, cancel := deadline.Child(parent, time.Second, 30*time.Millisecond)
	defer cancel()
	left, _ := deadline.Remaining(ctx)
	if !within(left, 70*time.Millisecond, 20*time.Millisecond) {
		t.Fatalf("remaining = %v, want ~70ms", left)
	}
	<-ctx.Done()
	if !errors.Is(context.Cause(ctx), resilience.ErrNoBudget) {
		t.Fatalf("cause = %v", context.Cause(ctx))
	}
}

func TestChild_NoTimeoutOnlyClamps(t *testing.T) {
	parent, pcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer pcancel()
	ctx, cancel := deadline.Child(parent, 0, 0)
	defer cancel()
	left, ok := deadline.Remaining(ctx)
	if !ok || !within(left, 100*time.Millisecond, 20*time.Millisecond) {
		t.Fatalf("remaining = %v", left)
	}
	ctx2, cancel2 := deadline.Child(context.Background(), 0, 0)
	defer cancel2()
	if _, ok := ctx2.Deadline(); ok {
		t.Fatal("no parent deadline and no timeout must yield no deadline")
	}
}

func TestChild_ExpiredWhenParentHasLessThanReserve(t *testing.T) {
	parent, pcancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer pcancel()
	ctx, cancel := deadline.Child(parent, time.Second, 50*time.Millisecond)
	defer cancel()
	if ctx.Err() == nil {
		t.Fatal("child must already be expired")
	}
	if !errors.Is(context.Cause(ctx), resilience.ErrNoBudget) {
		t.Fatalf("cause = %v", context.Cause(ctx))
	}
}

func TestAttempt_CauseIsAttemptTimeout(t *testing.T) {
	ctx, cancel := deadline.Attempt(context.Background(), time.Millisecond)
	defer cancel()
	<-ctx.Done()
	if !errors.Is(context.Cause(ctx), resilience.ErrAttemptTimeout) {
		t.Fatalf("cause = %v", context.Cause(ctx))
	}
}

type key struct{}

func TestDetach_KeepsValuesDropsCancel(t *testing.T) {
	parent, pcancel := context.WithCancel(context.WithValue(context.Background(), key{}, "trace"))
	ctx, cancel := deadline.Detach(parent, 100*time.Millisecond)
	defer cancel()
	pcancel()
	if ctx.Err() != nil {
		t.Fatal("detached ctx must survive parent cancel")
	}
	if ctx.Value(key{}) != "trace" {
		t.Fatal("values must be kept")
	}
	left, ok := deadline.Remaining(ctx)
	if !ok || !within(left, 100*time.Millisecond, 20*time.Millisecond) {
		t.Fatalf("remaining = %v", left)
	}
}

func TestEnough(t *testing.T) {
	if !deadline.Enough(context.Background(), time.Hour) {
		t.Fatal("no deadline is always enough")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if deadline.Enough(ctx, time.Second) || !deadline.Enough(ctx, 10*time.Millisecond) {
		t.Fatal("Enough mis-reports")
	}
}
