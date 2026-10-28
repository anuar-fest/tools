package metricshttp_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/anuar-fest/tools/httpserver"
	metricshttp "github.com/anuar-fest/tools/metrics/http"
)

type rig struct {
	reader *sdkmetric.ManualReader
	mp     *sdkmetric.MeterProvider
}

func newRig() *rig {
	r := &rig{reader: sdkmetric.NewManualReader()}
	r.mp = sdkmetric.NewMeterProvider(sdkmetric.WithReader(r.reader))
	return r
}

func (r *rig) metrics(t *testing.T) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func mux() *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("GET /items/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("ok"))
	})
	m.HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("x") })
	return m
}

func TestObserver_ThroughTheMiddlewareChain(t *testing.T) {
	r := newRig()
	obs, err := metricshttp.NewObserver(metricshttp.Config{Meter: r.mp.Meter("m")})
	if err != nil {
		t.Fatal(err)
	}
	h := httpserver.Metrics(obs, nil)(httpserver.Recover(nil)(httpserver.Routed(mux())))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/items/5?x=1", nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/boom", nil))

	ms := r.metrics(t)
	hist := ms["http.server.request.duration"].Data.(metricdata.Histogram[float64])
	if len(hist.DataPoints) != 2 {
		t.Fatalf("one series per attribute set: %d", len(hist.DataPoints))
	}
	var sawPanic, sawCreated bool
	for _, dp := range hist.DataPoints {
		rt, _ := dp.Attributes.Value("http.route")
		if v, ok := dp.Attributes.Value("error.type"); ok && v.AsString() == "panic" {
			sawPanic = true
			if rt.AsString() != "GET /boom" {
				t.Fatalf("route on the panic series: %v", dp.Attributes)
			}
		}
		if st, _ := dp.Attributes.Value("http.response.status_code"); st.AsInt64() == 201 && rt.AsString() == "GET /items/{id}" {
			sawCreated = true
			if _, hasPath := dp.Attributes.Value("url.path"); hasPath {
				t.Fatal("the path must never be a metric attribute")
			}
		}
	}
	if !sawPanic || !sawCreated {
		t.Fatalf("panic=%v created=%v", sawPanic, sawCreated)
	}
	if active := ms["http.server.active_requests"].Data.(metricdata.Sum[int64]).DataPoints[0].Value; active != 0 {
		t.Fatalf("active requests must return to 0, got %d", active)
	}
	if resp := ms["http.server.response.body.size"].Data.(metricdata.Histogram[int64]); len(resp.DataPoints) != 2 {
		t.Fatalf("response size series: %d", len(resp.DataPoints))
	}
}

func TestObserver_Attributes(t *testing.T) {
	r := newRig()
	obs, err := metricshttp.NewObserver(metricshttp.Config{Meter: r.mp.Meter("m")})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	obs.ObserveRequest(ctx, httpserver.RequestStat{Method: "POST", Route: "POST /x", Status: 503, Elapsed: 20 * time.Millisecond, BytesIn: 10, BytesOut: 5})
	obs.ObserveRequest(ctx, httpserver.RequestStat{Method: "GET", Status: 200, Elapsed: time.Millisecond})
	ms := r.metrics(t)
	for _, dp := range ms["http.server.request.duration"].Data.(metricdata.Histogram[float64]).DataPoints {
		status, _ := dp.Attributes.Value("http.response.status_code")
		et, hasErr := dp.Attributes.Value("error.type")
		_, hasRoute := dp.Attributes.Value("http.route")
		switch status.AsInt64() {
		case 503:
			if !hasErr || et.AsString() != "503" || !hasRoute {
				t.Fatalf("503 series: %v", dp.Attributes)
			}
		case 200:
			if hasErr || hasRoute {
				t.Fatalf("200 without route must carry neither: %v", dp.Attributes)
			}
		}
	}
	if req := ms["http.server.request.body.size"].Data.(metricdata.Histogram[int64]); len(req.DataPoints) != 1 {
		t.Fatalf("request size only when declared: %d", len(req.DataPoints))
	}
}

func TestTransport_RecordsOneSamplePerCall(t *testing.T) {
	r := newRig()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer srv.Close()
	tr, err := metricshttp.Transport(srv.Client().Transport, metricshttp.TransportConfig{Meter: r.mp.Meter("m"), PeerService: "fraud"})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: tr}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/score?token=secret", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	srv.Close()
	resp, err = client.Do(req.Clone(context.Background()))
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("closed server must fail")
	}

	ms := r.metrics(t)
	hist := ms["http.client.request.duration"].Data.(metricdata.Histogram[float64])
	if len(hist.DataPoints) != 2 {
		t.Fatalf("one series for the 503, one for the transport error: %d", len(hist.DataPoints))
	}
	for _, dp := range hist.DataPoints {
		if _, hasURL := dp.Attributes.Value("url.full"); hasURL {
			t.Fatal("url.full must not be a metric attribute")
		}
		if peer, _ := dp.Attributes.Value("peer.service"); peer.AsString() != "fraud" || dp.Count != 1 {
			t.Fatalf("series: %v count=%d", dp.Attributes, dp.Count)
		}
		et, _ := dp.Attributes.Value("error.type")
		if st, ok := dp.Attributes.Value("http.response.status_code"); ok {
			if st.AsInt64() != 503 || et.AsString() != "503" {
				t.Fatalf("503 series: %v", dp.Attributes)
			}
		} else if et.AsString() == "" {
			t.Fatalf("transport error must be classified: %v", dp.Attributes)
		}
	}
}

func TestTransport_NilNextUsesDefault(t *testing.T) {
	r := newRig()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer srv.Close()
	tr, err := metricshttp.Transport(nil, metricshttp.TransportConfig{Meter: r.mp.Meter("m")})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	dp := r.metrics(t)["http.client.request.duration"].Data.(metricdata.Histogram[float64]).DataPoints[0]
	if _, hasErr := dp.Attributes.Value("error.type"); hasErr || dp.Count != 1 {
		t.Fatalf("2xx carries no error.type: %v", dp.Attributes)
	}
}
