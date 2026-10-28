package clientnats_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	clientnats "github.com/anuar-fest/tools/otelx/client/nats"
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

type rig struct {
	exporter *tracetest.InMemoryExporter
	tracer   trace.Tracer
	n        *clientnats.Instrumentation
}

func newRig(t *testing.T, consumer string) *rig {
	t.Helper()
	r := &rig{exporter: tracetest.NewInMemoryExporter()}
	r.tracer = sdktrace.NewTracerProvider(sdktrace.WithSyncer(r.exporter)).Tracer("t")
	var err error
	r.n, err = clientnats.New(clientnats.Config{Tracer: r.tracer, Propagator: propagation.TraceContext{}, Consumer: consumer})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *rig) spans(name string) []tracetest.SpanStub {
	var out []tracetest.SpanStub
	for _, s := range r.exporter.GetSpans() {
		if s.Name == name {
			out = append(out, s)
		}
	}
	return out
}

func attrValue(attrs []attribute.KeyValue, key string) string {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.String()
		}
	}
	return ""
}

func TestCore_PublishAndHandleJoinOneTrace(t *testing.T) {
	nc := startServer(t)
	r := newRig(t, "worker")
	handled := make(chan trace.SpanContext, 2)
	sub, err := nc.Subscribe("orders.created", r.n.MsgHandler(func(ctx context.Context, msg *nats.Msg) error {
		handled <- trace.SpanContextFromContext(ctx)
		if string(msg.Data) == "bad" {
			return errors.New("cannot handle")
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	ctx, root := r.tracer.Start(context.Background(), "request")
	if err := r.n.PublishMsg(ctx, nc, &nats.Msg{Subject: "orders.created", Data: []byte("ok")}); err != nil {
		t.Fatal(err)
	}
	if err := r.n.PublishMsg(ctx, nc, &nats.Msg{Subject: "orders.created", Data: []byte("bad")}); err != nil {
		t.Fatal(err)
	}
	root.End()
	for range 2 {
		select {
		case sc := <-handled:
			if sc.TraceID() != root.SpanContext().TraceID() {
				t.Fatalf("handler must run in the publisher's trace: %v vs %v", sc.TraceID(), root.SpanContext().TraceID())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("message not handled")
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(r.spans("orders.created process")) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	pubs, procs := r.spans("orders.created publish"), r.spans("orders.created process")
	if len(pubs) != 2 || len(procs) != 2 || pubs[0].SpanKind != trace.SpanKindProducer || procs[0].SpanKind != trace.SpanKindConsumer {
		t.Fatalf("spans: %d publish, %d process", len(pubs), len(procs))
	}
	var failed int
	for _, p := range procs {
		if p.Parent.SpanID() != pubs[0].SpanContext.SpanID() && p.Parent.SpanID() != pubs[1].SpanContext.SpanID() {
			t.Fatalf("process span must be a child of a publish span: %+v", p)
		}
		if p.Status.Code.String() == "Error" {
			failed++
			if attrValue(p.Attributes, "error.type") != "errors.errorString" {
				t.Fatalf("handler error must be classified: %v", p.Attributes)
			}
		}
		if attrValue(p.Attributes, "messaging.consumer.group.name") != "worker" {
			t.Fatalf("consumer attribute: %v", p.Attributes)
		}
	}
	if failed != 1 {
		t.Fatalf("one handler failure expected, got %d", failed)
	}
}

func TestJetStream_PublishConsumeAndPanic(t *testing.T) {
	nc := startServer(t)
	r := newRig(t, "durable")
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

	msg := &nats.Msg{Subject: "orders.paid", Data: []byte("panic"), Header: nats.Header{}}
	msg.Header.Set(nats.MsgIdHdr, "m-1")
	ack, err := r.n.PublishJetStream(ctx, js, msg)
	if err != nil || ack.Stream != "ORDERS" {
		t.Fatalf("publish: %v %v", ack, err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("message not handled")
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(r.spans("orders.paid process")) < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	pub := r.spans("orders.paid publish")[0]
	proc := r.spans("orders.paid process")[0]
	if attrValue(pub.Attributes, "messaging.nats.stream") != "ORDERS" || attrValue(pub.Attributes, "messaging.message.id") != "m-1" {
		t.Fatalf("publish attributes: %v", pub.Attributes)
	}
	if proc.Parent.SpanID() != pub.SpanContext.SpanID() || attrValue(proc.Attributes, "error.type") != "panic" || attrValue(proc.Attributes, "messaging.delivery.attempt") != "1" || attrValue(proc.Attributes, "messaging.nats.stream") != "ORDERS" {
		t.Fatalf("process span: %+v", proc)
	}
	if proc.Status.Code.String() != "Error" || len(proc.Events) == 0 {
		t.Fatalf("a recovered panic must be an error with the panic recorded: %+v", proc)
	}
}

func TestPublish_FailureMarksSpan(t *testing.T) {
	nc := startServer(t)
	r := newRig(t, "")
	nc.Close()
	err := r.n.PublishMsg(context.Background(), nc, &nats.Msg{Subject: "orders.created"})
	if err == nil {
		t.Fatal("publish on a closed connection must fail")
	}
	s := r.spans("orders.created publish")[0]
	if s.Status.Code.String() != "Error" || attrValue(s.Attributes, "error.type") == "" {
		t.Fatalf("publish failure must be recorded: %+v", s)
	}
}
