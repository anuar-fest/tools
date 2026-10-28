package metricsnats_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	metricsnats "github.com/anuar-fest/tools/metrics/nats"
)

func startServer(t *testing.T) *nats.Conn {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(s.Shutdown)
	nc, err := nats.Connect(s.ClientURL(), nats.Name("test-client"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// lockedBuffer is read by the test while a subscriber goroutine logs.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type rig struct {
	reader *sdkmetric.ManualReader
	meter  metric.Meter
	log    lockedBuffer
	n      *metricsnats.Instrumentation
}

func newRig(t *testing.T, cfg metricsnats.Config) *rig {
	t.Helper()
	r := &rig{reader: sdkmetric.NewManualReader()}
	r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(r.reader)).Meter("m")
	cfg.Meter = r.meter
	cfg.Logger = slog.New(slog.NewTextHandler(&r.log, nil))
	var err error
	r.n, err = metricsnats.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// series keys counters by destination/operation/error.type and histograms
// by destination/operation.
func (r *rig) series(t *testing.T, metricName string) map[string]uint64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]uint64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricName {
				continue
			}
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					dest, _ := dp.Attributes.Value("messaging.destination.name")
					op, _ := dp.Attributes.Value("messaging.operation.type")
					et, _ := dp.Attributes.Value("error.type")
					out[dest.AsString()+"/"+op.AsString()+"/"+et.AsString()] += uint64(dp.Value)
				}
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					dest, _ := dp.Attributes.Value("messaging.destination.name")
					op, _ := dp.Attributes.Value("messaging.operation.type")
					out[dest.AsString()+"/"+op.AsString()] += dp.Count
				}
			}
		}
	}
	return out
}

func (r *rig) consumerAttr(t *testing.T) string {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "messaging.client.consumed.messages" {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				c, _ := dp.Attributes.Value("messaging.consumer.group.name")
				return c.AsString()
			}
		}
	}
	return ""
}

func TestCore_PublishAndHandle(t *testing.T) {
	nc := startServer(t)
	r := newRig(t, metricsnats.Config{Consumer: "worker"})
	handled := make(chan struct{}, 2)
	sub, err := nc.Subscribe("orders.created", r.n.MsgHandler(func(_ context.Context, msg *nats.Msg) error {
		defer func() { handled <- struct{}{} }()
		if string(msg.Data) == "bad" {
			return errors.New("cannot handle")
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	ctx := context.Background()
	if err := r.n.PublishMsg(ctx, nc, &nats.Msg{Subject: "orders.created", Data: []byte("ok")}); err != nil {
		t.Fatal(err)
	}
	if err := r.n.PublishMsg(ctx, nc, &nats.Msg{Subject: "orders.created", Data: []byte("bad")}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-handled:
		case <-time.After(5 * time.Second):
			t.Fatal("message not handled")
		}
	}
	sent := r.series(t, "messaging.client.sent.messages")
	consumed := r.series(t, "messaging.client.consumed.messages")
	if sent["orders.created/send/"] != 2 || consumed["orders.created/process/"] != 1 || consumed["orders.created/process/errors.errorString"] != 1 {
		t.Fatalf("sent=%v consumed=%v", sent, consumed)
	}
	if d := r.series(t, "messaging.client.operation.duration"); d["orders.created/send"] != 2 || d["orders.created/process"] != 2 {
		t.Fatalf("durations: %v", d)
	}
	if c := r.consumerAttr(t); c != "worker" {
		t.Fatalf("consumer attribute = %q", c)
	}
}

func TestPublish_WrapsAnyPublishAndMapsDestination(t *testing.T) {
	r := newRig(t, metricsnats.Config{DestinationName: func(s string) string {
		parts := strings.Split(s, ".")
		if len(parts) == 3 {
			parts[1] = "*"
		}
		return strings.Join(parts, ".")
	}})
	ctx := context.Background()
	var inner int
	err := r.n.Publish(ctx, "orders.123.created", func(context.Context) error { inner++; return nil })
	if err != nil || inner != 1 {
		t.Fatalf("publish must run the function once: err=%v inner=%d", err, inner)
	}
	want := errors.New("publish failed")
	if err := r.n.Publish(ctx, "orders.456.created", func(context.Context) error { return want }); !errors.Is(err, want) {
		t.Fatalf("publish error must propagate: %v", err)
	}
	sent := r.series(t, "messaging.client.sent.messages")
	if sent["orders.*.created/send/"] != 1 || sent["orders.*.created/send/errors.errorString"] != 1 || len(sent) != 2 {
		t.Fatalf("ids must not become series: %v", sent)
	}
}

func TestHandler_RecordsPanicAndRePanics(t *testing.T) {
	r := newRig(t, metricsnats.Config{})
	h := r.n.Handler(func(context.Context, *nats.Msg) error { panic("handler bug") })
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = h(context.Background(), &nats.Msg{Subject: "orders.paid"})
	}()
	if recovered != "handler bug" {
		t.Fatalf("Handler must re-raise the panic for the outer guard, got %v", recovered)
	}
	if consumed := r.series(t, "messaging.client.consumed.messages"); consumed["orders.paid/process/panic"] != 1 {
		t.Fatalf("consumed: %v", consumed)
	}
	if r.log.String() != "" {
		t.Fatal("Handler must leave logging to the outer guard")
	}
}

func TestJetStream_PublishConsumePanicAndStats(t *testing.T) {
	nc := startServer(t)
	r := newRig(t, metricsnats.Config{Consumer: "durable"})
	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}}); err != nil {
		t.Fatal(err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{Durable: "durable", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{}, 1)
	cc, err := cons.Consume(r.n.JetStreamMsgHandler(func(_ context.Context, msg jetstream.Msg) error {
		defer func() { done <- struct{}{} }()
		_ = msg.Ack()
		if string(msg.Data()) == "panic" {
			panic("handler bug")
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Stop()

	ack, err := r.n.PublishJetStream(ctx, js, &nats.Msg{Subject: "orders.paid", Data: []byte("panic")})
	if err != nil || ack.Stream != "ORDERS" {
		t.Fatalf("publish: %v %v", ack, err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("message not handled")
	}
	// The sample and the log line land after the handler returned; wait for the log line, the last of the two.
	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(r.log.String(), "panic recovered") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if sent := r.series(t, "messaging.client.sent.messages"); sent["orders.paid/send/"] != 1 {
		t.Fatalf("sent: %v", sent)
	}
	if consumed := r.series(t, "messaging.client.consumed.messages"); consumed["orders.paid/process/panic"] != 1 {
		t.Fatalf("consumed: %v", consumed)
	}
	if !strings.Contains(r.log.String(), "panic recovered") || !strings.Contains(r.log.String(), "handler bug") {
		t.Fatalf("the metrics-only wrapper must log the recovered panic: %s", r.log.String())
	}

	reg, err := metricsnats.RegisterConnStats(r.meter, nc)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reg.Unregister() }()
	var rm metricdata.ResourceMetrics
	if err := r.reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	var connected, outMsgs int64 = -1, -1
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "messaging.nats.connected":
				connected = m.Data.(metricdata.Gauge[int64]).DataPoints[0].Value
			case "messaging.nats.messages":
				for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
					if d, _ := dp.Attributes.Value("direction"); d.AsString() == "out" {
						outMsgs = dp.Value
					}
				}
			}
		}
	}
	if connected != 1 || outMsgs < 1 {
		t.Fatalf("connected=%d out=%d", connected, outMsgs)
	}
	if _, err := metricsnats.RegisterConnStats(nil, nil); err == nil {
		t.Fatal("nil conn must fail")
	}
}
