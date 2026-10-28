package eventspg_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/anuar-fest/tools/events"
	eventspg "github.com/anuar-fest/tools/events/pg"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	// -short skips the container; every test then skips through requirePool.
	for _, a := range os.Args {
		if a == "-test.short=true" || a == "-test.short" {
			os.Exit(m.Run())
		}
	}
	ctx := context.Background()
	pgc, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("t"), postgres.WithUsername("t"), postgres.WithPassword("t"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90*time.Second)))
	if err != nil {
		slog.Error("postgres container", "err", err)
		os.Exit(1)
	}
	dsn, err := pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		slog.Error("dsn", "err", err)
		os.Exit(1)
	}
	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		slog.Error("pool", "err", err)
		os.Exit(1)
	}
	if _, err := pool.Exec(ctx, eventspg.Schema); err != nil {
		slog.Error("schema", "err", err)
		os.Exit(1)
	}
	code := m.Run()
	pool.Close()
	_ = testcontainers.TerminateContainer(pgc)
	os.Exit(code)
}

func requirePool(t *testing.T) context.Context {
	t.Helper()
	if testing.Short() || pool == nil {
		t.Skip("needs Docker; run without -short")
	}
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE event_outbox, event_inbox`); err != nil {
		t.Fatal(err)
	}
	return ctx
}

type fakePublisher struct {
	mu    sync.Mutex
	seen  map[string]int
	order []string
	fail  func(e events.Envelope) error
}

func newFakePublisher() *fakePublisher { return &fakePublisher{seen: map[string]int{}} }

func (f *fakePublisher) Publish(_ context.Context, e events.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		if err := f.fail(e); err != nil {
			return err
		}
	}
	f.seen[e.ID]++
	f.order = append(f.order, e.ID)
	return nil
}

func (f *fakePublisher) count(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seen[id]
}

func env(n int) events.Envelope {
	return events.New("orders.order.created.v1", "orders-api", []byte(`{"n":1}`), events.WithTenant("t1"), events.WithHeader("region", "eu"))
}

func newRelay(t *testing.T, pub events.Publisher, cfg eventspg.RelayConfig) *eventspg.Relay {
	t.Helper()
	if cfg.InstanceID == "" {
		cfg.InstanceID = "relay-1"
	}
	r, err := eventspg.NewRelay(pool, pub, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestOutbox_EnqueueIsTransactionalAndRelayPublishes(t *testing.T) {
	ctx := requirePool(t)

	// Rolled back: never published.
	tx, _ := pool.Begin(ctx)
	rolled := env(1)
	if err := eventspg.Enqueue(ctx, tx, rolled); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback(ctx)

	// Committed: published with every field intact.
	tx, _ = pool.Begin(ctx)
	e := env(2)
	e.TraceParent = "00-abc-def-01"
	if err := eventspg.Enqueue(ctx, tx, e); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	pub := newFakePublisher()
	r := newRelay(t, pub, eventspg.RelayConfig{})
	n, err := r.RelayBatch(ctx)
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if pub.count(rolled.ID) != 0 || pub.count(e.ID) != 1 {
		t.Fatalf("published rolled=%d committed=%d", pub.count(rolled.ID), pub.count(e.ID))
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	var publishedAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT published_at FROM event_outbox WHERE id = $1`, e.ID).Scan(&publishedAt); err != nil || publishedAt == nil {
		t.Fatalf("row not marked published: %v", err)
	}
	n, err = r.RelayBatch(ctx)
	if err != nil || n != 0 {
		t.Fatalf("second batch n=%d err=%v", n, err)
	}
}

func TestOutbox_RelayKeepsEnvelopeFields(t *testing.T) {
	ctx := requirePool(t)
	e := env(1)
	e.TraceParent = "00-abc-def-01"
	if err := eventspg.Enqueue(ctx, pool, e); err != nil {
		t.Fatal(err)
	}
	var got events.Envelope
	pub := events.PublisherFunc(func(_ context.Context, p events.Envelope) error { got = p; return nil })
	if _, err := newRelay(t, pub, eventspg.RelayConfig{}).RelayBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if got.ID != e.ID || got.Type != e.Type || got.Source != e.Source || got.TenantID != "t1" || got.CorrelationID != e.ID ||
		got.TraceParent != e.TraceParent || got.Headers["region"] != "eu" || string(got.Data) != `{"n":1}` || !got.Time.Equal(e.Time) {
		t.Fatalf("relayed envelope differs:\n%+v\n%+v", e, got)
	}
}

func TestOutbox_FailuresBackOffThenDie(t *testing.T) {
	ctx := requirePool(t)
	e := env(1)
	if err := eventspg.Enqueue(ctx, pool, e); err != nil {
		t.Fatal(err)
	}
	pub := newFakePublisher()
	pub.fail = func(events.Envelope) error { return errors.New("broker down") }
	r := newRelay(t, pub, eventspg.RelayConfig{MaxAttempts: 2, Backoff: []time.Duration{time.Hour}})

	if _, err := r.RelayBatch(ctx); err != nil {
		t.Fatal(err)
	}
	var attempts int
	var dead bool
	var next time.Time
	var lastErr string
	if err := pool.QueryRow(ctx, `SELECT attempts, dead, next_attempt_at, last_error FROM event_outbox WHERE id = $1`, e.ID).Scan(&attempts, &dead, &next, &lastErr); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || dead || time.Until(next) < 30*time.Minute || lastErr != "broker down" {
		t.Fatalf("attempts=%d dead=%v next=%v err=%q", attempts, dead, next, lastErr)
	}
	// Not due yet: nothing claimed.
	if n, _ := r.RelayBatch(ctx); n != 0 {
		t.Fatalf("row not due must not be claimed, n=%d", n)
	}
	// Make it due; the second failure kills it.
	if _, err := pool.Exec(ctx, `UPDATE event_outbox SET next_attempt_at = now() WHERE id = $1`, e.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RelayBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT attempts, dead FROM event_outbox WHERE id = $1`, e.ID).Scan(&attempts, &dead); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || !dead {
		t.Fatalf("attempts=%d dead=%v", attempts, dead)
	}
	deadRows, err := eventspg.Dead(ctx, pool, 10)
	if err != nil || len(deadRows) != 1 || deadRows[0].ID != e.ID || deadRows[0].Headers["last_error"] != "broker down" {
		t.Fatalf("dead=%+v err=%v", deadRows, err)
	}
	// Revived rows are relayed again once the publisher works.
	if err := eventspg.Revive(ctx, pool, e.ID); err != nil {
		t.Fatal(err)
	}
	pub.fail = nil
	if _, err := r.RelayBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if pub.count(e.ID) != 1 {
		t.Fatal("revived row must be published")
	}
	if err := eventspg.Revive(ctx, pool, e.ID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("reviving a live row = %v", err)
	}
}

func TestOutbox_ConcurrentRelaysPublishEachRowOnce(t *testing.T) {
	ctx := requirePool(t)
	const n = 300
	ids := make([]string, n)
	for i := range n {
		e := env(i)
		ids[i] = e.ID
		if err := eventspg.Enqueue(ctx, pool, e); err != nil {
			t.Fatal(err)
		}
	}
	pub := newFakePublisher()
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := newRelay(t, pub, eventspg.RelayConfig{InstanceID: "relay-" + string(rune('a'+i)), BatchSize: 20})
			for {
				m, err := r.RelayBatch(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if m == 0 {
					return
				}
			}
		}(i)
	}
	wg.Wait()
	for _, id := range ids {
		if c := pub.count(id); c != 1 {
			t.Fatalf("id %s published %d times", id, c)
		}
	}
	var pending int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM event_outbox WHERE published_at IS NULL`).Scan(&pending)
	if pending != 0 {
		t.Fatalf("%d rows still pending", pending)
	}
	pruned, err := eventspg.Prune(ctx, pool, 0)
	if err != nil || pruned != n {
		t.Fatalf("pruned=%d err=%v", pruned, err)
	}
}

func TestOutbox_RunStopsWithContext(t *testing.T) {
	ctx := requirePool(t)
	r := newRelay(t, newFakePublisher(), eventspg.RelayConfig{PollInterval: 10 * time.Millisecond})
	rctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := r.Run(rctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run = %v", err)
	}
}

func TestInbox_ClaimCompleteReleaseAndLease(t *testing.T) {
	ctx := requirePool(t)
	inbox, err := eventspg.NewInbox(pool, 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := inbox.Claim(ctx, "c", "e1")
	if err != nil || !ok {
		t.Fatalf("first claim ok=%v err=%v", ok, err)
	}
	if ok, _ := inbox.Claim(ctx, "c", "e1"); ok {
		t.Fatal("claim within lease must be refused")
	}
	if ok, _ := inbox.Claim(ctx, "other", "e1"); !ok {
		t.Fatal("another consumer has its own inbox")
	}
	if err := inbox.Release(ctx, "c", "e1"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := inbox.Claim(ctx, "c", "e1"); !ok {
		t.Fatal("released claim must be claimable")
	}
	if err := inbox.Complete(ctx, "c", "e1"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := inbox.Claim(ctx, "c", "e1"); ok {
		t.Fatal("completed event must never be claimed again")
	}
	if err := inbox.Release(ctx, "c", "e1"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := inbox.Claim(ctx, "c", "e1"); ok {
		t.Fatal("release must not forget a completed event")
	}

	// A crashed worker's claim expires with the lease.
	if ok, _ := inbox.Claim(ctx, "c", "e2"); !ok {
		t.Fatal("claim e2")
	}
	time.Sleep(300 * time.Millisecond)
	if ok, _ := inbox.Claim(ctx, "c", "e2"); !ok {
		t.Fatal("expired lease must be claimable")
	}

	// Middleware end to end.
	calls := 0
	h := events.Chain(func(context.Context, events.Envelope) error { calls++; return nil }, events.Inbox(inbox, "mw", nil))
	e := env(1)
	for range 3 {
		if err := h(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("handler ran %d times", calls)
	}
}

func TestInbox_ClaimInTxRollsBackWithTransaction(t *testing.T) {
	ctx := requirePool(t)
	tx, _ := pool.Begin(ctx)
	ok, err := eventspg.ClaimInTx(ctx, tx, "c", "e1")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	_ = tx.Rollback(ctx)

	tx, _ = pool.Begin(ctx)
	if ok, _ := eventspg.ClaimInTx(ctx, tx, "c", "e1"); !ok {
		t.Fatal("a rolled-back claim must not count")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	tx, _ = pool.Begin(ctx)
	if ok, _ := eventspg.ClaimInTx(ctx, tx, "c", "e1"); ok {
		t.Fatal("a committed claim must be final")
	}
	_ = tx.Rollback(ctx)

	pruned, err := eventspg.PruneInbox(ctx, pool, 0)
	if err != nil || pruned != 1 {
		t.Fatalf("pruned=%d err=%v", pruned, err)
	}
}
