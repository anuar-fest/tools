package serverhttp_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/anuar-fest/tools/httpserver"
	serverhttp "github.com/anuar-fest/tools/otelx/server/http"
)

type rig struct {
	exporter *tracetest.InMemoryExporter
	tracer   trace.Tracer
}

func newRig() *rig {
	r := &rig{exporter: tracetest.NewInMemoryExporter()}
	r.tracer = sdktrace.NewTracerProvider(sdktrace.WithSyncer(r.exporter)).Tracer("t")
	return r
}

func (r *rig) chain(cfg serverhttp.TracingConfig, h http.Handler) http.Handler {
	cfg.Tracer = r.tracer
	cfg.Propagator = propagation.TraceContext{}
	return serverhttp.Tracing(cfg)(httpserver.Recover(nil)(httpserver.Routed(h)))
}

func attrValue(attrs []attribute.KeyValue, key string) string {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.String()
		}
	}
	return ""
}

func mux() *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("GET /items/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("ok"))
	})
	m.HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("x") })
	m.HandleFunc("GET /healthz", func(http.ResponseWriter, *http.Request) {})
	return m
}

func TestTracing_ContinuesTraceNamesRouteAndRecordsStatus(t *testing.T) {
	r := newRig()
	h := r.chain(serverhttp.TracingConfig{}, mux())
	remote := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled})
	req := httptest.NewRequest(http.MethodGet, "/items/5?x=1", nil)
	req.Header.Set("User-Agent", "ua/1")
	propagation.TraceContext{}.Inject(trace.ContextWithRemoteSpanContext(context.Background(), remote), propagation.HeaderCarrier(req.Header))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Header().Get(serverhttp.HeaderTraceID) != remote.TraceID().String() {
		t.Fatalf("trace id header = %q", rr.Header().Get(serverhttp.HeaderTraceID))
	}
	spans := r.exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d", len(spans))
	}
	s := spans[0]
	if s.Name != "GET /items/{id}" || s.SpanKind != trace.SpanKindServer || s.Parent.SpanID() != remote.SpanID() || s.SpanContext.TraceID() != remote.TraceID() {
		t.Fatalf("span: %+v", s)
	}
	for k, want := range map[string]string{"http.request.method": "GET", "http.route": "GET /items/{id}", "http.response.status_code": "201", "url.path": "/items/5", "user_agent.original": "ua/1", "url.scheme": "http", "network.protocol.version": "1.1"} {
		if got := attrValue(s.Attributes, k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if attrValue(s.Attributes, "url.query") != "" || s.Status.Code.String() != "Unset" {
		t.Fatalf("no query on spans, no error on 2xx: %+v", s)
	}

	r.exporter.Reset()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/boom", nil))
	s = r.exporter.GetSpans()[0]
	if s.Status.Code.String() != "Error" || attrValue(s.Attributes, "error.type") != "panic" || attrValue(s.Attributes, "http.response.status_code") != "500" {
		t.Fatalf("panic span: %+v", s)
	}
}

func TestTracing_PublicEndpointStartsNewTraceWithLink(t *testing.T) {
	r := newRig()
	h := r.chain(serverhttp.TracingConfig{PublicEndpoint: true}, mux())
	remote := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{7}, SpanID: trace.SpanID{8}, TraceFlags: trace.FlagsSampled})
	req := httptest.NewRequest(http.MethodGet, "/items/1", nil)
	propagation.TraceContext{}.Inject(trace.ContextWithRemoteSpanContext(context.Background(), remote), propagation.HeaderCarrier(req.Header))
	h.ServeHTTP(httptest.NewRecorder(), req)
	s := r.exporter.GetSpans()[0]
	if s.Parent.IsValid() || s.SpanContext.TraceID() == remote.TraceID() {
		t.Fatalf("public endpoint must start its own trace: %+v", s)
	}
	if len(s.Links) != 1 || s.Links[0].SpanContext.TraceID() != remote.TraceID() {
		t.Fatalf("and link the caller's: %+v", s.Links)
	}
}

func TestTracing_SkipAndNoHeader(t *testing.T) {
	r := newRig()
	h := r.chain(serverhttp.TracingConfig{Skip: func(req *http.Request) bool { return req.URL.Path == "/healthz" }, NoTraceIDHeader: true}, mux())
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if len(r.exporter.GetSpans()) != 0 {
		t.Fatal("skipped request must not be traced")
	}
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/items/1", nil))
	if len(r.exporter.GetSpans()) != 1 || rr.Header().Get(serverhttp.HeaderTraceID) != "" {
		t.Fatal("traced without the header")
	}
}
