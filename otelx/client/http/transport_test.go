package clienthttp_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	clienthttp "github.com/anuar-fest/tools/otelx/client/http"
)

func attrValue(attrs []attribute.KeyValue, key string) string {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.String()
		}
	}
	return ""
}

func TestTransport_PropagatesAndRecordsSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)).Tracer("t")
	var inbound string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inbound = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	client := &http.Client{Transport: clienthttp.Transport(srv.Client().Transport, clienthttp.Config{Tracer: tracer, Propagator: propagation.TraceContext{}, PeerService: "fraud"})}
	ctx, root := tracer.Start(context.Background(), "root")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/score?token=secret", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	root.End()
	if !strings.Contains(inbound, root.SpanContext().TraceID().String()) {
		t.Fatalf("traceparent must carry the caller's trace: %q", inbound)
	}
	if req.Header.Get("traceparent") != "" {
		t.Fatal("the caller's request must not be mutated")
	}
	var cs tracetest.SpanStub
	for _, s := range exporter.GetSpans() {
		if s.SpanKind == trace.SpanKindClient {
			cs = s
		}
	}
	if cs.Name != "GET fraud" || cs.Parent.SpanID() != root.SpanContext().SpanID() || cs.Status.Code.String() != "Error" {
		t.Fatalf("client span: %+v", cs)
	}
	if attrValue(cs.Attributes, "url.full") != srv.URL+"/score" || attrValue(cs.Attributes, "http.response.status_code") != "503" || attrValue(cs.Attributes, "peer.service") != "fraud" || attrValue(cs.Attributes, "error.type") != "503" {
		t.Fatalf("attributes: %v", cs.Attributes)
	}

	exporter.Reset()
	srv.Close()
	resp, err = client.Do(req.Clone(context.Background()))
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("closed server must fail")
	}
	if s := exporter.GetSpans()[0]; s.Status.Code.String() != "Error" || len(s.Events) == 0 || attrValue(s.Attributes, "error.type") == "" {
		t.Fatalf("transport error must be recorded: %+v", s)
	}
}

func TestTransport_NilNextUsesDefaultAndNamesAfterHost(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)).Tracer("t")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer srv.Close()
	client := &http.Client{Transport: clienthttp.Transport(nil, clienthttp.Config{Tracer: tracer})}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	s := exporter.GetSpans()[0]
	if !strings.HasPrefix(s.Name, "GET 127.0.0.1") || s.Status.Code.String() == "Error" || attrValue(s.Attributes, "error.type") != "" {
		t.Fatalf("span: %+v", s)
	}
}
