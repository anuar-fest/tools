package otelx_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/anuar-fest/tools/otelx"
)

func attrValue(set attribute.Set, key string) string {
	v, ok := set.Value(attribute.Key(key))
	if !ok {
		return ""
	}
	return v.String()
}

func TestNew_TracesAndResource(t *testing.T) {
	ctx := context.Background()
	exporter := tracetest.NewInMemoryExporter()
	tel, err := otelx.New(ctx, otelx.Config{
		ServiceName: "svc", ServiceVersion: "1.2.3", Environment: "test", InstanceID: "pod-1",
		Traces:      otelx.TracesConfig{Processors: []sdktrace.SpanProcessor{sdktrace.NewSimpleSpanProcessor(exporter)}},
		SkipGlobals: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	res := *tel.Resource().Set()
	for k, want := range map[string]string{"service.name": "svc", "service.version": "1.2.3", "deployment.environment.name": "test", "service.instance.id": "pod-1", "telemetry.sdk.language": "go"} {
		if got := attrValue(res, k); got != want {
			t.Errorf("resource %s = %q, want %q", k, got, want)
		}
	}
	if !tel.TracingEnabled() {
		t.Fatal("a processor means tracing is on")
	}
	_, span := tel.Tracer("t").Start(ctx, "op")
	span.End()
	if spans := exporter.GetSpans(); len(spans) != 1 || attrValue(attribute.NewSet(spans[0].Resource.Attributes()...), "service.name") != "svc" {
		t.Fatalf("span must be exported with the resource: %+v", spans)
	}
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatal("Shutdown must be idempotent:", err)
	}
}

func TestNew_WithoutExporterNeverSamples(t *testing.T) {
	tel, err := otelx.New(context.Background(), otelx.Config{ServiceName: "svc", SkipGlobals: true})
	if err != nil {
		t.Fatal(err)
	}
	_, span := tel.Tracer("t").Start(context.Background(), "op")
	defer span.End()
	if span.IsRecording() || span.SpanContext().IsSampled() || tel.TracingEnabled() {
		t.Fatal("no exporter: spans must cost nothing")
	}
	if !span.SpanContext().HasTraceID() {
		t.Fatal("propagation still needs a trace id")
	}
}

func TestNew_Validation(t *testing.T) {
	if _, err := otelx.New(context.Background(), otelx.Config{}); err == nil {
		t.Fatal("ServiceName is required")
	}
	_, err := otelx.New(context.Background(), otelx.Config{ServiceName: "s", SkipGlobals: true,
		Traces: otelx.TracesConfig{Exporter: tracetest.NewInMemoryExporter(), SampleRatio: 2}})
	if err == nil {
		t.Fatal("SampleRatio > 1 must fail")
	}
}

func TestNew_SetsTheGlobals(t *testing.T) {
	tp, prop := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() { otel.SetTracerProvider(tp); otel.SetTextMapPropagator(prop) })
	tel, err := otelx.New(context.Background(), otelx.Config{ServiceName: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	if otel.GetTracerProvider() != tel.TracerProvider() {
		t.Fatal("the global tracer provider must be what New built")
	}
	if fields := otel.GetTextMapPropagator().Fields(); len(fields) != 3 || fields[0] != "traceparent" || fields[2] != "baggage" {
		t.Fatalf("the global propagator must be TraceContext plus Baggage: %v", fields)
	}
}

func TestLogAttrs(t *testing.T) {
	if otelx.LogAttrs(context.Background()) != nil {
		t.Fatal("no span, no attrs")
	}
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(context.Background(), "op")
	defer span.End()
	attrs := otelx.LogAttrs(ctx)
	if len(attrs) != 2 || attrs[0].Value.String() != span.SpanContext().TraceID().String() || attrs[1].Value.String() != span.SpanContext().SpanID().String() {
		t.Fatalf("%v", attrs)
	}
}
