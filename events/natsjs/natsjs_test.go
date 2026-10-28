package natsjs_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/anuar-fest/tools/events"
	"github.com/anuar-fest/tools/events/natsjs"
)

func startServer(t *testing.T) string {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server not ready")
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	return s.ClientURL()
}

func connect(t *testing.T, url string) *natsjs.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := natsjs.Connect(ctx, natsjs.Config{URL: url, Name: "test", Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.Close(ctx)
	})
	return c
}

func setup(t *testing.T) *natsjs.Client {
	t.Helper()
	c := connect(t, startServer(t))
	ctx := context.Background()
	if err := c.EnsureStream(ctx, natsjs.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}}); err != nil {
		t.Fatal(err)
	}
	if err := c.EnsureStream(ctx, natsjs.DeadLetterStream()); err != nil {
		t.Fatal(err)
	}
	return c
}

type recorder struct {
	mu    sync.Mutex
	stats []events.Stat
}

func (r *recorder) Observe(_ context.Context, s events.Stat) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stats = append(r.stats, s)
}

func (r *recorder) count(o events.Outcome) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, s := range r.stats {
		if s.Outcome == o {
			n++
		}
	}
	return n
}

func newEnvelope() events.Envelope {
	return events.New("orders.order.created.v1", "orders-api", []byte(`{"id":1}`), events.WithTenant("t1"), events.WithSubject("order/1"))
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func stop(t *testing.T, k events.Consumer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConnect_RetriesUntilContextEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_, err := natsjs.Connect(ctx, natsjs.Config{URL: "nats://127.0.0.1:1", ConnectTimeout: 50 * time.Millisecond})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestEnsureStream_MergesAndNeverShrinks(t *testing.T) {
	c := connect(t, startServer(t))
	ctx := context.Background()
	if err := c.EnsureStream(ctx, natsjs.StreamConfig{Name: "S", Subjects: []string{"s.a.>"}}); err != nil {
		t.Fatal(err)
	}
	// An older service with a shorter list must not remove s.a.>.
	if err := c.EnsureStream(ctx, natsjs.StreamConfig{Name: "S", Subjects: []string{"s.b.>"}}); err != nil {
		t.Fatal(err)
	}
	info, err := c.JetStream().Stream(ctx, "S")
	if err != nil {
		t.Fatal(err)
	}
	if subs := info.CachedInfo().Config.Subjects; len(subs) != 2 {
		t.Fatalf("subjects = %v", subs)
	}
	if info.CachedInfo().Config.MaxAge != 7*24*time.Hour || info.CachedInfo().Config.Duplicates != 2*time.Minute {
		t.Fatalf("defaults not applied: %+v", info.CachedInfo().Config)
	}
	// The owner may replace the list.
	if err := c.EnsureStream(ctx, natsjs.StreamConfig{Name: "S", Subjects: []string{"s.b.>"}, ReplaceSubjects: true}); err != nil {
		t.Fatal(err)
	}
	info, _ = c.JetStream().Stream(ctx, "S")
	if subs := info.CachedInfo().Config.Subjects; len(subs) != 1 || subs[0] != "s.b.>" {
		t.Fatalf("subjects = %v", subs)
	}
}

func TestPublishConsume_RoundTripWithDedup(t *testing.T) {
	c := setup(t)
	ctx := context.Background()
	pub := c.NewPublisher(natsjs.PublisherConfig{})

	var mu sync.Mutex
	var got []events.Envelope
	var attempts []int
	k, err := c.Consume(ctx, natsjs.ConsumerConfig{
		Stream: "ORDERS", Name: "orders-worker", Handler: func(ctx context.Context, e events.Envelope) error {
			d, _ := events.DeliveryFrom(ctx)
			mu.Lock()
			got = append(got, e)
			attempts = append(attempts, d.Attempt)
			mu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop(t, k)

	e := newEnvelope()
	if err := pub.Publish(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish(ctx, e); err != nil { // same id: deduplicated by the stream
		t.Fatal(err)
	}
	other := newEnvelope()
	if err := pub.Publish(ctx, other); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == 2 })
	mu.Lock()
	defer mu.Unlock()
	if got[0].ID != e.ID || got[0].Type != e.Type || got[0].TenantID != "t1" || got[0].Subject != "order/1" || got[0].CorrelationID != e.ID || string(got[0].Data) != `{"id":1}` {
		t.Fatalf("envelope = %+v", got[0])
	}
	if got[1].ID != other.ID || attempts[0] != 1 || attempts[1] != 1 {
		t.Fatalf("got=%+v attempts=%v", got, attempts)
	}
}

func TestConsume_NakThenAckWithRetryAfter(t *testing.T) {
	c := setup(t)
	ctx := context.Background()
	rec := &recorder{}
	var calls atomic.Int32
	var attempts []int
	var mu sync.Mutex
	k, err := c.Consume(ctx, natsjs.ConsumerConfig{
		Stream: "ORDERS", Name: "w", Observer: rec, Handler: func(ctx context.Context, e events.Envelope) error {
			d, _ := events.DeliveryFrom(ctx)
			mu.Lock()
			attempts = append(attempts, d.Attempt)
			mu.Unlock()
			if calls.Add(1) == 1 {
				return events.RetryAfter(errors.New("busy"), 50*time.Millisecond)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop(t, k)
	if err := c.NewPublisher(natsjs.PublisherConfig{}).Publish(ctx, newEnvelope()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return calls.Load() == 2 })
	waitFor(t, 2*time.Second, func() bool { return rec.count(events.OutcomeAcked) == 1 })
	mu.Lock()
	defer mu.Unlock()
	if attempts[0] != 1 || attempts[1] != 2 || rec.count(events.OutcomeNacked) != 1 {
		t.Fatalf("attempts=%v stats=%+v", attempts, rec.stats)
	}
}

func dlqSubscribe(t *testing.T, c *natsjs.Client) *nats.Subscription {
	t.Helper()
	sub, err := c.Conn().SubscribeSync("dlq.>")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return sub
}

func TestConsume_DropDeadLetters(t *testing.T) {
	c := setup(t)
	ctx := context.Background()
	dlq := dlqSubscribe(t, c)
	rec := &recorder{}
	var calls atomic.Int32
	k, err := c.Consume(ctx, natsjs.ConsumerConfig{
		Stream: "ORDERS", Name: "w", DeadLetterSubject: natsjs.DeadLetterSubject("w"), Observer: rec,
		Handler: func(context.Context, events.Envelope) error {
			calls.Add(1)
			return events.Drop(errors.New("unknown customer"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop(t, k)
	e := newEnvelope()
	if err := c.NewPublisher(natsjs.PublisherConfig{}).Publish(ctx, e); err != nil {
		t.Fatal(err)
	}
	m, err := dlq.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if m.Header.Get(natsjs.HeaderDLQReason) != "dropped" || m.Header.Get(natsjs.HeaderDLQConsumer) != "w" ||
		m.Header.Get(natsjs.HeaderDLQDeliveries) != "1" || m.Header.Get(natsjs.HeaderDLQError) != "unknown customer" ||
		m.Header.Get(events.HeaderID) != e.ID || string(m.Data) != string(e.Data) {
		t.Fatalf("dlq message = %v %s", m.Header, m.Data)
	}
	time.Sleep(200 * time.Millisecond)
	if calls.Load() != 1 || rec.count(events.OutcomeDeadLettered) != 1 || rec.count(events.OutcomeDropped) != 1 {
		t.Fatalf("calls=%d stats=%+v", calls.Load(), rec.stats)
	}
}

func TestConsume_MaxDeliverDeadLetters(t *testing.T) {
	c := setup(t)
	ctx := context.Background()
	dlq := dlqSubscribe(t, c)
	var calls atomic.Int32
	k, err := c.Consume(ctx, natsjs.ConsumerConfig{
		Stream: "ORDERS", Name: "w", MaxDeliver: 2, Backoff: []time.Duration{20 * time.Millisecond},
		DeadLetterSubject: natsjs.DeadLetterSubject("w"),
		Handler: func(context.Context, events.Envelope) error {
			calls.Add(1)
			return errors.New("still failing")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop(t, k)
	if err := c.NewPublisher(natsjs.PublisherConfig{}).Publish(ctx, newEnvelope()); err != nil {
		t.Fatal(err)
	}
	m, err := dlq.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if m.Header.Get(natsjs.HeaderDLQReason) != "max_deliver" || m.Header.Get(natsjs.HeaderDLQDeliveries) != "2" {
		t.Fatalf("dlq headers = %v", m.Header)
	}
	time.Sleep(200 * time.Millisecond)
	if calls.Load() != 2 {
		t.Fatalf("handler called %d times, want exactly MaxDeliver", calls.Load())
	}
}

func TestConsume_UndecodableDeadLetters(t *testing.T) {
	c := setup(t)
	ctx := context.Background()
	dlq := dlqSubscribe(t, c)
	var calls atomic.Int32
	k, err := c.Consume(ctx, natsjs.ConsumerConfig{
		Stream: "ORDERS", Name: "w", DeadLetterSubject: natsjs.DeadLetterSubject("w"),
		Handler: func(context.Context, events.Envelope) error { calls.Add(1); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop(t, k)
	if _, err := c.JetStream().Publish(ctx, "orders.legacy", []byte("raw bytes, no headers")); err != nil {
		t.Fatal(err)
	}
	m, err := dlq.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if m.Header.Get(natsjs.HeaderDLQReason) != "undecodable" || m.Header.Get(natsjs.HeaderDLQSubject) != "orders.legacy" || calls.Load() != 0 {
		t.Fatalf("headers=%v calls=%d", m.Header, calls.Load())
	}
}

func TestConsume_PanicIsRecoveredAndRedelivered(t *testing.T) {
	c := setup(t)
	ctx := context.Background()
	var calls atomic.Int32
	k, err := c.Consume(ctx, natsjs.ConsumerConfig{
		Stream: "ORDERS", Name: "w", Backoff: []time.Duration{20 * time.Millisecond},
		Handler: func(context.Context, events.Envelope) error {
			if calls.Add(1) == 1 {
				panic("boom")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop(t, k)
	if err := c.NewPublisher(natsjs.PublisherConfig{}).Publish(ctx, newEnvelope()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return calls.Load() == 2 })
}

func TestConsume_HeartbeatKeepsLongHandlerAlive(t *testing.T) {
	c := setup(t)
	ctx := context.Background()
	var calls atomic.Int32
	done := make(chan struct{})
	k, err := c.Consume(ctx, natsjs.ConsumerConfig{
		Stream: "ORDERS", Name: "w", AckWait: 600 * time.Millisecond, // heartbeat every 200ms
		Handler: func(context.Context, events.Envelope) error {
			calls.Add(1)
			time.Sleep(1500 * time.Millisecond) // longer than AckWait
			close(done)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop(t, k)
	if err := c.NewPublisher(natsjs.PublisherConfig{}).Publish(ctx, newEnvelope()); err != nil {
		t.Fatal(err)
	}
	<-done
	time.Sleep(800 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("a heartbeating handler must not be redelivered: calls=%d", calls.Load())
	}
}

func TestConsume_StopDrainsInFlightAndDeadlineCancels(t *testing.T) {
	c := setup(t)
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	var finished atomic.Bool
	k, err := c.Consume(ctx, natsjs.ConsumerConfig{
		Stream: "ORDERS", Name: "w", Workers: 1,
		Handler: func(context.Context, events.Envelope) error {
			close(started)
			<-release
			finished.Store(true)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.NewPublisher(natsjs.PublisherConfig{}).Publish(ctx, newEnvelope()); err != nil {
		t.Fatal(err)
	}
	<-started
	stopped := make(chan error, 1)
	go func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopped <- k.Stop(sctx)
	}()
	time.Sleep(100 * time.Millisecond)
	if finished.Load() {
		t.Fatal("handler should still be running")
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("stop = %v", err)
	}
	if !finished.Load() {
		t.Fatal("Stop must wait for the in-flight handler")
	}

	// A handler that ignores Stop's budget is cancelled and joined.
	var sawCancel atomic.Bool
	k2, err := c.Consume(ctx, natsjs.ConsumerConfig{
		Stream: "ORDERS", Name: "w2", Workers: 1,
		Handler: func(ctx context.Context, _ events.Envelope) error {
			<-ctx.Done()
			sawCancel.Store(true)
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.NewPublisher(natsjs.PublisherConfig{}).Publish(ctx, newEnvelope()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		info, err := c.JetStream().Consumer(ctx, "ORDERS", "w2")
		if err != nil {
			return false
		}
		ci, err := info.Info(ctx)
		return err == nil && ci.NumAckPending >= 1
	})
	sctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := k2.Stop(sctx); !errors.Is(err, context.DeadlineExceeded) || !sawCancel.Load() {
		t.Fatalf("stop = %v sawCancel=%v", err, sawCancel.Load())
	}
}

func TestPublish_NoStreamIsAnError(t *testing.T) {
	c := connect(t, startServer(t))
	pub := c.NewPublisher(natsjs.PublisherConfig{Timeout: time.Second})
	err := pub.Publish(context.Background(), newEnvelope())
	if !errors.Is(err, natsjs.ErrNoStream) {
		t.Fatalf("err = %v", err)
	}
}

func TestPublish_RejectsInvalidEnvelope(t *testing.T) {
	c := setup(t)
	if err := c.NewPublisher(natsjs.PublisherConfig{}).Publish(context.Background(), events.Envelope{Type: "bad"}); err == nil {
		t.Fatal("invalid envelope must be rejected before publishing")
	}
}

func TestHealthy(t *testing.T) {
	c := connect(t, startServer(t))
	if !c.Healthy() {
		t.Fatal("connected client must be healthy")
	}
}
