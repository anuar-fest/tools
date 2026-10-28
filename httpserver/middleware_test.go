package httpserver_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anuar-fest/tools/httpserver"
)

type recObserver struct {
	mu       sync.Mutex
	inflight []int
	stats    []httpserver.RequestStat
}

func (o *recObserver) ObserveInFlight(_ context.Context, d int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inflight = append(o.inflight, d)
}

func (o *recObserver) ObserveRequest(_ context.Context, s httpserver.RequestStat) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stats = append(o.stats, s)
}

func okHandler(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }

func TestRequestID_GeneratedTrustedAndValidated(t *testing.T) {
	var seen string
	h := httpserver.RequestID(httpserver.RequestIDConfig{})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = httpserver.RequestIDFrom(r.Context())
	}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if seen == "" || rr.Header().Get(httpserver.HeaderRequestID) != seen {
		t.Fatalf("generated id must be in context and header: %q / %q", seen, rr.Header().Get(httpserver.HeaderRequestID))
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(httpserver.HeaderRequestID, "gw-123:abc/def")
	h.ServeHTTP(httptest.NewRecorder(), req)
	if seen != "gw-123:abc/def" {
		t.Fatalf("valid inbound id must be kept: %q", seen)
	}
	for _, bad := range []string{"has space", strings.Repeat("x", 129), "émoji", ""} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set(httpserver.HeaderRequestID, bad)
		h.ServeHTTP(httptest.NewRecorder(), req)
		if seen == bad {
			t.Fatalf("invalid inbound id %q must be replaced", bad)
		}
	}
	ignore := httpserver.RequestID(httpserver.RequestIDConfig{IgnoreInbound: true})(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = httpserver.RequestIDFrom(r.Context())
	}))
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(httpserver.HeaderRequestID, "gw-1")
	ignore.ServeHTTP(httptest.NewRecorder(), req)
	if seen == "gw-1" {
		t.Fatal("IgnoreInbound must generate a new id")
	}
}

func TestRecover_AnswersProblemAndMarksPanic(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	obs := &recObserver{}
	h := httpserver.RequestID(httpserver.RequestIDConfig{})(httpserver.Metrics(obs, nil)(httpserver.Recover(logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.Header().Set("ETag", "x")
		panic("boom")
	}))))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rr.Code != http.StatusInternalServerError || rr.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("code %d type %q", rr.Code, rr.Header().Get("Content-Type"))
	}
	if rr.Header().Get("ETag") != "" {
		t.Fatal("body headers of the aborted response must be dropped")
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil || body["status"] != float64(500) || body["request_id"] == "" {
		t.Fatalf("problem body: %s", rr.Body.String())
	}
	if !strings.Contains(buf.String(), `"panic":"boom"`) || !strings.Contains(buf.String(), "stack") {
		t.Fatalf("panic must be logged with stack: %s", buf.String())
	}
	if len(obs.stats) != 1 || !obs.stats[0].Panicked || obs.stats[0].Status != 500 {
		t.Fatalf("metrics outside Recover must see the panic: %+v", obs.stats)
	}
}

func TestRecover_AfterHeadersWrittenKeepsStatus(t *testing.T) {
	h := httpserver.Recover(nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("partial"))
		panic("late")
	}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusAccepted || rr.Body.String() != "partial" {
		t.Fatalf("a started response must not be rewritten: %d %q", rr.Code, rr.Body.String())
	}
}

func TestRecover_RepanicsErrAbortHandler(t *testing.T) {
	h := httpserver.Recover(nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic(http.ErrAbortHandler) }))
	defer func() {
		if r := recover(); r != http.ErrAbortHandler { //nolint:errorlint // identity, as net/http
			t.Fatalf("ErrAbortHandler must propagate, got %v", r)
		}
	}()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}

func TestAccessLog_FieldsLevelsAndFilters(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /items/{id}", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("item")) })
	mux.HandleFunc("GET /boom", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) })
	mux.HandleFunc("GET /health", okHandler)
	h := httpserver.AccessLog(httpserver.AccessLogConfig{Logger: logger, Skip: func(r *http.Request) bool { return r.URL.Path == "/health" }})(mux)

	req := httptest.NewRequest(http.MethodGet, "/items/42", strings.NewReader("abc"))
	req.RemoteAddr = "10.1.2.3:5555"
	h.ServeHTTP(httptest.NewRecorder(), req)
	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatal(err, buf.String())
	}
	for k, want := range map[string]any{"level": "INFO", "method": "GET", "route": "GET /items/{id}", "path": "/items/42", "status": float64(200), "bytes_in": float64(3), "bytes_out": float64(4), "client_ip": "10.1.2.3"} {
		if line[k] != want {
			t.Errorf("%s = %v, want %v (%s)", k, line[k], want, buf.String())
		}
	}
	if _, ok := line["duration_ms"]; !ok {
		t.Error("duration_ms missing")
	}
	buf.Reset()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/boom", nil))
	if !strings.Contains(buf.String(), `"level":"ERROR"`) || !strings.Contains(buf.String(), `"status":502`) {
		t.Fatalf("5xx must log at error: %s", buf.String())
	}
	buf.Reset()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil))
	if buf.Len() != 0 {
		t.Fatalf("skipped path must not log: %s", buf.String())
	}
	failures := httpserver.AccessLog(httpserver.AccessLogConfig{Logger: logger, FailuresOnly: true})(mux)
	failures.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/items/1", nil))
	if buf.Len() != 0 {
		t.Fatalf("FailuresOnly must skip 200: %s", buf.String())
	}
}

func TestMetrics_ObservesRouteStatusAndInFlight(t *testing.T) {
	obs := &recObserver{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /items/{id}", okHandler)
	h := httpserver.Metrics(obs, nil)(mux)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/items/7", nil))
	req := httptest.NewRequest("BREW", "/items/7", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)
	if got := obs.inflight; len(got) != 4 || got[0] != 1 || got[1] != -1 {
		t.Fatalf("inflight = %v", got)
	}
	s := obs.stats[0]
	if s.Method != "GET" || s.Route != "GET /items/{id}" || s.Status != 200 || s.BytesOut != 2 || s.Elapsed <= 0 {
		t.Fatalf("stat = %+v", s)
	}
	if obs.stats[1].Method != "_OTHER" || obs.stats[1].Status != http.StatusMethodNotAllowed {
		t.Fatalf("unknown method must be _OTHER: %+v", obs.stats[1])
	}
}

func TestRoute_SurvivesRequestCopiesThroughRouted(t *testing.T) {
	obs := &recObserver{}
	var buf bytes.Buffer
	mux := http.NewServeMux()
	mux.HandleFunc("GET /items/{id}", okHandler)
	// RequestID and Timeout copy the request; Routed publishes the pattern back out.
	h := httpserver.Metrics(obs, nil)(httpserver.AccessLog(httpserver.AccessLogConfig{Logger: slog.New(slog.NewJSONHandler(&buf, nil))})(
		httpserver.RequestID(httpserver.RequestIDConfig{})(httpserver.Timeout(time.Second)(httpserver.Routed(mux)))))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/items/3", nil))
	if obs.stats[0].Route != "GET /items/{id}" || !strings.Contains(buf.String(), `"route":"GET /items/{id}"`) {
		t.Fatalf("route lost across copies: %+v / %s", obs.stats[0], buf.String())
	}
	// Without Routed and with a copying middleware in between, the route is unknown, never wrong.
	obs = &recObserver{}
	httpserver.Metrics(obs, nil)(httpserver.RequestID(httpserver.RequestIDConfig{})(mux)).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/items/3", nil))
	if obs.stats[0].Route != "" {
		t.Fatalf("route = %q", obs.stats[0].Route)
	}
}

func TestMetrics_ClientGoneIs499(t *testing.T) {
	obs := &recObserver{}
	h := httpserver.Metrics(obs, nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx))
	if obs.stats[0].Status != httpserver.StatusClientClosedRequest {
		t.Fatalf("status = %d", obs.stats[0].Status)
	}
}

func TestRealIP_TrustsOnlyConfiguredProxies(t *testing.T) {
	var seen string
	capture := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { seen = r.RemoteAddr })
	h := httpserver.RealIP(httpserver.PrivateNetworks())(capture)
	cases := []struct{ peer, xff, xreal, want string }{
		{"203.0.113.9:1", "1.2.3.4", "", "203.0.113.9:1"},          // untrusted peer: header ignored
		{"10.0.0.2:1", "1.2.3.4, 10.0.0.3", "", "1.2.3.4"},         // rightmost untrusted hop
		{"10.0.0.2:1", "10.0.0.5", "198.51.100.7", "198.51.100.7"}, // all hops trusted: X-Real-IP
		{"10.0.0.2:1", "garbage, 1.2.3.4", "", "1.2.3.4"},          // the proxy appended 1.2.3.4; what the client sent before it is noise
		{"10.0.0.2:1", "1.2.3.4, garbage", "", "10.0.0.2:1"},       // the proxy itself wrote garbage: trust nothing
		{"10.0.0.2:1", "", "", "10.0.0.2:1"},
		{"[::1]:1", "2001:db8::1", "", "2001:db8::1"},
	}
	for _, c := range cases {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = c.peer
		if c.xff != "" {
			req.Header.Set("X-Forwarded-For", c.xff)
		}
		if c.xreal != "" {
			req.Header.Set("X-Real-IP", c.xreal)
		}
		h.ServeHTTP(httptest.NewRecorder(), req)
		if seen != c.want {
			t.Errorf("peer %s xff %q xreal %q: got %q want %q", c.peer, c.xff, c.xreal, seen, c.want)
		}
	}
	if _, err := httpserver.ParsePrefixes("10.0.0.0/8", "::1", "nope"); err == nil {
		t.Fatal("bad prefix must fail")
	}
	ps, _ := httpserver.ParsePrefixes("192.0.2.1")
	if ps[0] != netip.MustParsePrefix("192.0.2.1/32") {
		t.Fatalf("bare address must become /32: %v", ps)
	}
}

func TestCORS_PreflightAndActual(t *testing.T) {
	if _, err := httpserver.CORS(httpserver.CORSConfig{}); err == nil {
		t.Fatal("no origins must fail")
	}
	if _, err := httpserver.CORS(httpserver.CORSConfig{AllowedOrigins: []string{"*"}, AllowCredentials: true}); err == nil {
		t.Fatal("* with credentials must fail")
	}
	mw, err := httpserver.CORS(httpserver.CORSConfig{AllowedOrigins: []string{"https://app.example.com"}, AllowCredentials: true, ExposedHeaders: []string{"X-Request-Id"}})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { called = true; okHandler(w, nil) }))

	pre := httptest.NewRequest(http.MethodOptions, "/", nil)
	pre.Header.Set("Origin", "https://app.example.com")
	pre.Header.Set("Access-Control-Request-Method", "PUT")
	pre.Header.Set("Access-Control-Request-Headers", "content-type, authorization")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, pre)
	if rr.Code != http.StatusNoContent || called || rr.Header().Get("Access-Control-Allow-Origin") != "https://app.example.com" ||
		rr.Header().Get("Access-Control-Allow-Credentials") != "true" || rr.Header().Get("Access-Control-Max-Age") != "600" ||
		!strings.Contains(rr.Header().Get("Vary"), "Origin") {
		t.Fatalf("preflight: %d %v", rr.Code, rr.Header())
	}
	pre.Header.Set("Access-Control-Request-Headers", "x-evil")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, pre)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("disallowed header must be refused: %d", rr.Code)
	}
	pre.Header.Set("Origin", "https://evil.example.com")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, pre)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("disallowed origin preflight must be 403: %d", rr.Code)
	}
	actual := httptest.NewRequest(http.MethodGet, "/", nil)
	actual.Header.Set("Origin", "https://app.example.com")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, actual)
	if !called || rr.Header().Get("Access-Control-Allow-Origin") != "https://app.example.com" || rr.Header().Get("Access-Control-Expose-Headers") != "X-Request-Id" {
		t.Fatalf("actual request: %v", rr.Header())
	}
	called = false
	actual.Header.Set("Origin", "https://evil.example.com")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, actual)
	if !called || rr.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("disallowed origin passes through without CORS headers")
	}
	star, _ := httpserver.CORS(httpserver.CORSConfig{AllowedOrigins: []string{"*"}})
	rr = httptest.NewRecorder()
	star(http.HandlerFunc(okHandler)).ServeHTTP(rr, actual)
	if rr.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("* must be echoed as *: %v", rr.Header())
	}
}

func TestAPIKey_ConstantTimeMatchAndConsumer(t *testing.T) {
	if _, err := httpserver.APIKey(httpserver.APIKeyConfig{}); err == nil {
		t.Fatal("no keys must fail")
	}
	mw, err := httpserver.APIKey(httpserver.APIKeyConfig{Keys: map[string]string{"billing": "k-billing", "robot": "k-robot"}, SharedKey: "k-shared"})
	if err != nil {
		t.Fatal(err)
	}
	var consumer string
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		consumer = httpserver.ConsumerFrom(r.Context())
		okHandler(w, r)
	}))
	try := func(auth string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	if rr := try(""); rr.Code != http.StatusUnauthorized || rr.Header().Get("WWW-Authenticate") != "APIKEY" {
		t.Fatalf("missing key: %d %v", rr.Code, rr.Header())
	}
	if rr := try("APIKEY wrong"); rr.Code != http.StatusUnauthorized {
		t.Fatalf("wrong key: %d", rr.Code)
	}
	if rr := try("Bearer k-billing"); rr.Code != http.StatusUnauthorized {
		t.Fatalf("wrong scheme: %d", rr.Code)
	}
	if rr := try("apikey k-robot"); rr.Code != http.StatusOK || consumer != "robot" {
		t.Fatalf("case-insensitive scheme, consumer: %d %q", rr.Code, consumer)
	}
	if rr := try("APIKEY k-shared"); rr.Code != http.StatusOK || consumer != "" {
		t.Fatalf("shared key has no consumer: %d %q", rr.Code, consumer)
	}
	raw, _ := httpserver.APIKey(httpserver.APIKeyConfig{Header: "X-Api-Key", Keys: map[string]string{"a": "k"}})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Api-Key", "k")
	rr := httptest.NewRecorder()
	raw(http.HandlerFunc(okHandler)).ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("raw header mode: %d", rr.Code)
	}
}

func TestMaxBytes_DeclaredAndStreamed(t *testing.T) {
	h := httpserver.MaxBytes(8)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			httpserver.WriteError(w, r, err)
			return
		}
		okHandler(w, r)
	}))
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("0123456789"))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("declared length over cap: %d", rr.Code)
	}
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader("0123456789"))
	req.ContentLength = -1
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("streamed body over cap: %d %s", rr.Code, rr.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader("short"))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("body under cap: %d", rr.Code)
	}
}

func TestTimeout_DeadlineWithCauseMapsTo504(t *testing.T) {
	h := httpserver.Timeout(10 * time.Millisecond)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		httpserver.WriteError(w, r, context.Cause(r.Context()))
	}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusGatewayTimeout {
		t.Fatalf("code %d", rr.Code)
	}
}

func TestParsePage(t *testing.T) {
	cases := []struct {
		query   string
		want    httpserver.Page
		wantErr bool
	}{
		{"", httpserver.Page{Limit: 50}, false},
		{"limit=10&offset=20", httpserver.Page{Limit: 10, Offset: 20}, false},
		{"limit=200", httpserver.Page{Limit: 200}, false},
		{"limit=201", httpserver.Page{}, true},
		{"limit=0", httpserver.Page{}, true},
		{"limit=abc", httpserver.Page{}, true},
		{"offset=-1", httpserver.Page{}, true},
	}
	for _, c := range cases {
		got, err := httpserver.ParsePage(httptest.NewRequest(http.MethodGet, "/?"+c.query, nil), httpserver.PageConfig{})
		if (err != nil) != c.wantErr || got != c.want {
			t.Errorf("%q: got %+v, %v", c.query, got, err)
		}
		if c.wantErr {
			var p httpserver.Problem
			if !errors.As(err, &p) || p.Status != http.StatusBadRequest {
				t.Errorf("%q: error must be a 400 Problem: %v", c.query, err)
			}
		}
	}
}

func TestProblem_WriteAndWriteError(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/orders/1", nil).WithContext(httpserver.WithRequestID(context.Background(), "r-9"))
	rr := httptest.NewRecorder()
	httpserver.WriteProblem(rr, req, httpserver.NewProblem(http.StatusConflict, "Order already shipped.").With("order_id", "1"))
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if rr.Code != 409 || body["title"] != "Conflict" || body["detail"] != "Order already shipped." || body["instance"] != "/orders/1" || body["request_id"] != "r-9" || body["order_id"] != "1" {
		t.Fatalf("%d %s", rr.Code, rr.Body.String())
	}
	rr = httptest.NewRecorder()
	httpserver.WriteError(rr, req, errors.New("pq: relation users does not exist"))
	if rr.Code != 500 || strings.Contains(rr.Body.String(), "relation") {
		t.Fatalf("internal error text must not leak: %d %s", rr.Code, rr.Body.String())
	}
	rr = httptest.NewRecorder()
	httpserver.WriteError(rr, req, httpserver.NewProblem(http.StatusNotFound, "No such order."))
	if rr.Code != 404 {
		t.Fatalf("problem error must be written as is: %d", rr.Code)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rr = httptest.NewRecorder()
	httpserver.WriteError(rr, req.WithContext(ctx), errors.New("x"))
	if rr.Body.Len() != 0 {
		t.Fatal("nothing is written to a client that left")
	}
}

type hijackable struct {
	http.ResponseWriter
	hijacked bool
}

func (h *hijackable) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h.hijacked = true
	return nil, nil, nil
}

func TestRecorder_FlushHijackAndStatus(t *testing.T) {
	rr := httptest.NewRecorder()
	rec := httpserver.Record(rr)
	if httpserver.Record(rec) != rec {
		t.Fatal("Record must reuse the installed recorder")
	}
	rec.(http.Flusher).Flush()
	if !rr.Flushed || rec.Status() != http.StatusOK {
		t.Fatalf("flush must pass through and imply 200: %v %d", rr.Flushed, rec.Status())
	}
	hw := &hijackable{ResponseWriter: httptest.NewRecorder()}
	rec = httpserver.Record(hw)
	if _, _, err := rec.(http.Hijacker).Hijack(); err != nil || !hw.hijacked || !rec.Hijacked() {
		t.Fatalf("hijack must pass through: %v %v %v", err, hw.hijacked, rec.Hijacked())
	}
	obs := &recObserver{}
	httpserver.Metrics(obs, nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _, _ = w.(http.Hijacker).Hijack()
	})).ServeHTTP(&hijackable{ResponseWriter: httptest.NewRecorder()}, httptest.NewRequest(http.MethodGet, "/ws", nil))
	if obs.stats[0].Status != http.StatusSwitchingProtocols {
		t.Fatalf("hijacked request status = %d", obs.stats[0].Status)
	}
}

func TestLogAttrs(t *testing.T) {
	if httpserver.LogAttrs(context.Background()) != nil {
		t.Fatal("empty context, no attrs")
	}
	ctx := httpserver.WithRequestID(context.Background(), "r-1")
	attrs := httpserver.LogAttrs(ctx)
	if len(attrs) != 1 || attrs[0].Key != "request_id" || attrs[0].Value.String() != "r-1" {
		t.Fatalf("%v", attrs)
	}
}
