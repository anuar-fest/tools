package events_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/anuar-fest/tools/events"
)

type recorder struct {
	mu    sync.Mutex
	stats []events.Stat
}

func (r *recorder) Observe(_ context.Context, s events.Stat) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stats = append(r.stats, s)
}

func (r *recorder) outcomes() []events.Outcome {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]events.Outcome, len(r.stats))
	for i, s := range r.stats {
		out[i] = s.Outcome
	}
	return out
}

var errBoom = errors.New("boom")

func env() events.Envelope { return events.New("a.b.c.v1", "svc", []byte("x")) }

func TestChain_OrderIsOutermostFirst(t *testing.T) {
	var trace []string
	mw := func(name string) events.Middleware {
		return func(next events.Handler) events.Handler {
			return func(ctx context.Context, e events.Envelope) error {
				trace = append(trace, name)
				return next(ctx, e)
			}
		}
	}
	h := events.Chain(func(context.Context, events.Envelope) error { trace = append(trace, "h"); return nil }, mw("a"), mw("b"))
	_ = h(context.Background(), env())
	if len(trace) != 3 || trace[0] != "a" || trace[1] != "b" || trace[2] != "h" {
		t.Fatalf("trace = %v", trace)
	}
}

func TestRecover_TurnsPanicIntoError(t *testing.T) {
	h := events.Chain(func(context.Context, events.Envelope) error { panic("bad") }, events.Recover(nil))
	err := h(context.Background(), env())
	if err == nil || events.IsDrop(err) {
		t.Fatalf("panic must become a retryable error, got %v", err)
	}
}

func TestTimeout(t *testing.T) {
	h := events.Chain(func(ctx context.Context, _ events.Envelope) error { <-ctx.Done(); return ctx.Err() }, events.Timeout(5*time.Millisecond))
	if err := h(context.Background(), env()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestObserve_Outcomes(t *testing.T) {
	rec := &recorder{}
	results := []error{nil, errBoom, events.Drop(errBoom)}
	i := 0
	h := events.Chain(func(context.Context, events.Envelope) error { err := results[i]; i++; return err }, events.Observe(rec, "c1"))
	ctx := events.WithDelivery(context.Background(), events.Delivery{Attempt: 2, Consumer: "c1"})
	for range results {
		_ = h(ctx, env())
	}
	// A timeout.
	th := events.Chain(func(ctx context.Context, _ events.Envelope) error { <-ctx.Done(); return ctx.Err() }, events.Observe(rec, "c1"), events.Timeout(time.Millisecond))
	_ = th(context.Background(), env())

	want := []events.Outcome{events.OutcomeAcked, events.OutcomeNacked, events.OutcomeDropped, events.OutcomeTimeout}
	got := rec.outcomes()
	if len(got) != len(want) {
		t.Fatalf("outcomes = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("outcome %d = %v want %v", i, got[i], want[i])
		}
	}
	if rec.stats[0].Attempt != 2 || rec.stats[0].Consumer != "c1" || rec.stats[0].Type != "a.b.c.v1" {
		t.Fatalf("stat = %+v", rec.stats[0])
	}
}

func TestInbox_DeduplicatesAndReleasesOnFailure(t *testing.T) {
	store := events.NewMemoryInbox()
	rec := &recorder{}
	calls := 0
	fail := true
	h := events.Chain(func(context.Context, events.Envelope) error {
		calls++
		if fail {
			return errBoom
		}
		return nil
	}, events.Inbox(store, "c1", rec))
	e := env()

	if err := h(context.Background(), e); !errors.Is(err, errBoom) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	// Failure released the claim: the redelivery runs the handler again.
	fail = false
	if err := h(context.Background(), e); err != nil || calls != 2 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	// Completed: a further redelivery is acked without running.
	if err := h(context.Background(), e); err != nil || calls != 2 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	if got := rec.outcomes(); len(got) != 1 || got[0] != events.OutcomeDuplicate {
		t.Fatalf("outcomes = %v", got)
	}
	// Another consumer has its own inbox.
	h2 := events.Chain(func(context.Context, events.Envelope) error { calls++; return nil }, events.Inbox(store, "c2", nil))
	if err := h2(context.Background(), e); err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestInbox_DropCompletes(t *testing.T) {
	store := events.NewMemoryInbox()
	calls := 0
	h := events.Chain(func(context.Context, events.Envelope) error { calls++; return events.Drop(errBoom) }, events.Inbox(store, "c1", nil))
	e := env()
	if err := h(context.Background(), e); !events.IsDrop(err) {
		t.Fatal("drop must propagate")
	}
	if err := h(context.Background(), e); err != nil || calls != 1 {
		t.Fatalf("a dropped event must not run again: err=%v calls=%d", err, calls)
	}
}

func TestMemoryInbox_LeaseExpiresAndRetentionSweeps(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := events.NewMemoryInbox()
	store.Lease = time.Minute
	store.Retention = time.Hour
	store.Now = func() time.Time { return now }

	ok, _ := store.Claim(context.Background(), "c", "e1")
	if !ok {
		t.Fatal("first claim")
	}
	if ok, _ := store.Claim(context.Background(), "c", "e1"); ok {
		t.Fatal("claimed within lease must be refused")
	}
	now = now.Add(2 * time.Minute)
	if ok, _ := store.Claim(context.Background(), "c", "e1"); !ok {
		t.Fatal("expired lease must be claimable (crashed worker)")
	}
	_ = store.Complete(context.Background(), "c", "e1")
	now = now.Add(2 * time.Hour)
	_, _ = store.Claim(context.Background(), "c", "e2") // triggers sweep
	if store.Len() != 1 {
		t.Fatalf("completed entry past retention must be swept, len=%d", store.Len())
	}
}
