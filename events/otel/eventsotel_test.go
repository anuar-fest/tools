package eventsotel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/anuar-fest/tools/events"
	eventsotel "github.com/anuar-fest/tools/events/otel"
)

func init() {
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

func TestTrace_ProducerToConsumerAcrossEnvelope(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	tracer := tp.Tracer("t")

	var published events.Envelope
	pub := eventsotel.TracedPublisher(events.PublisherFunc(func(_ context.Context, e events.Envelope) error {
		published = e
		return nil
	}), tracer)

	ctx, root := tracer.Start(context.Background(), "request")
	e := events.New("orders.order.created.v1", "orders-api", nil, events.WithTenant("t1"))
	if err := pub.Publish(ctx, e); err != nil {
		t.Fatal(err)
	}
	root.End()
	if published.TraceParent == "" {
		t.Fatal("traceparent must be injected into the envelope")
	}

	// The consumer, in another process, continues the trace.
	var handlerSpan trace.SpanContext
	h := events.Chain(func(ctx context.Context, _ events.Envelope) error {
		handlerSpan = trace.SpanContextFromContext(ctx)
		return errors.New("boom")
	}, eventsotel.Traced(tracer, "orders-worker"))
	cctx := events.WithDelivery(context.Background(), events.Delivery{Attempt: 2})
	_ = h(cctx, published)

	spans := exporter.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("spans = %d", len(spans))
	}
	byName := map[string]tracetest.SpanStub{}
	for _, s := range spans {
		byName[s.Name] = s
	}
	rootID := byName["request"].SpanContext.TraceID()
	if byName["orders.order.created.v1 publish"].SpanContext.TraceID() != rootID || byName["orders.order.created.v1 process"].SpanContext.TraceID() != rootID {
		t.Fatal("publish and process spans must share the request's trace")
	}
	process := byName["orders.order.created.v1 process"]
	if process.Parent.SpanID() != byName["orders.order.created.v1 publish"].SpanContext.SpanID() {
		t.Fatal("process span must be a child of the publish span")
	}
	if process.SpanKind != trace.SpanKindConsumer || handlerSpan.SpanID() != process.SpanContext.SpanID() {
		t.Fatalf("handler must run inside the consumer span: %+v", process)
	}
	if process.Status.Code.String() != "Error" {
		t.Fatalf("status = %v", process.Status)
	}
	found := false
	for _, a := range process.Attributes {
		if a.Key == "messaging.delivery.attempt" && a.Value.AsInt64() == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("delivery attempt attribute missing: %v", process.Attributes)
	}
}

func TestObserver_ExportsMessagesAndDuration(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	o, err := eventsotel.NewObserver(eventsotel.Config{Meter: mp.Meter("test")})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	o.Observe(ctx, events.Stat{Consumer: "w", Type: "a.b.c.v1", Outcome: events.OutcomeAcked, Elapsed: 20 * time.Millisecond})
	o.Observe(ctx, events.Stat{Consumer: "w", Type: "a.b.c.v1", Outcome: events.OutcomeAcked, Elapsed: 30 * time.Millisecond})
	o.Observe(ctx, events.Stat{Consumer: "w", Type: "a.b.c.v1", Outcome: events.OutcomeNacked})

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	ms := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			ms[m.Name] = m
		}
	}
	sum := ms["events.messages"].Data.(metricdata.Sum[int64])
	want := attribute.NewSet(eventsotel.AttrConsumer.String("w"), eventsotel.AttrType.String("a.b.c.v1"), eventsotel.AttrOutcome.String("acked"))
	var acked int64
	for _, dp := range sum.DataPoints {
		if dp.Attributes.Equals(&want) {
			acked = dp.Value
		}
	}
	if acked != 2 {
		t.Fatalf("acked = %d", acked)
	}
	hist := ms["events.message.duration"].Data.(metricdata.Histogram[float64])
	if len(hist.DataPoints) != 1 || hist.DataPoints[0].Count != 2 {
		t.Fatalf("duration = %+v", hist.DataPoints)
	}
}
