package httpx_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/breaker"
	"github.com/anuar-fest/tools/resilience/bulkhead"
	"github.com/anuar-fest/tools/resilience/httpx"
	"github.com/anuar-fest/tools/resilience/internal/obstest"
	"github.com/anuar-fest/tools/resilience/retry"
)

func noSleep(ctx context.Context, _ time.Duration) error { return ctx.Err() }

func newClient(t *testing.T, cfg httpx.TransportConfig) *http.Client {
	t.Helper()
	if cfg.Name == "" {
		cfg.Name = "dep"
	}
	if cfg.Retry.Sleep == nil {
		cfg.Retry.Sleep = noSleep
	}
	tr, err := httpx.NewTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Client{Transport: tr}
}

func TestTransport_Retries503ThenSucceeds(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) < 3 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "hello")
	}))
	defer srv.Close()

	rec := &obstest.Recorder{}
	c := newClient(t, httpx.TransportConfig{Observer: rec, Retry: retry.Policy{MaxAttempts: 3}})
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "hello" || hits.Load() != 3 {
		t.Fatalf("status=%d body=%q hits=%d", resp.StatusCode, body, hits.Load())
	}
	if rec.Count(resilience.DecisionRetry) != 2 || rec.Count(resilience.DecisionSuccess) != 2 {
		t.Fatalf("decisions = %v", rec.Decisions())
	}
}

func TestTransport_ExhaustedRetriesReturnLastResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", "yes")
		http.Error(w, "still down", http.StatusBadGateway)
	}))
	defer srv.Close()
	c := newClient(t, httpx.TransportConfig{Retry: retry.Policy{MaxAttempts: 2}})
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("a status is a response, not an error: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 502 || strings.TrimSpace(string(body)) != "still down" || resp.Header.Get("X-Upstream") != "yes" {
		t.Fatalf("status=%d body=%q header=%v", resp.StatusCode, body, resp.Header)
	}
}

func TestTransport_PostWithoutGetBodyIsNotRetried(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	c := newClient(t, httpx.TransportConfig{Retry: retry.Policy{MaxAttempts: 3}})

	// A streaming body cannot be replayed.
	req, _ := http.NewRequest(http.MethodPost, srv.URL, io.NopCloser(strings.NewReader("x")))
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 503 || hits.Load() != 1 {
		t.Fatalf("status=%d hits=%d", resp.StatusCode, hits.Load())
	}

	// A bytes body has GetBody, but POST is not idempotent without a key.
	hits.Store(0)
	resp, err = c.Post(srv.URL, "text/plain", bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if hits.Load() != 1 {
		t.Fatalf("POST without Idempotency-Key retried: hits=%d", hits.Load())
	}

	// With a key it is retried.
	hits.Store(0)
	req, _ = http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader([]byte("x")))
	req.Header.Set("Idempotency-Key", "k1")
	resp, err = c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if hits.Load() != 3 {
		t.Fatalf("keyed POST should retry: hits=%d", hits.Load())
	}
}

func TestTransport_500TripsBreakerButIsReturned(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bug", http.StatusInternalServerError)
	}))
	defer srv.Close()
	br, _ := breaker.New(breaker.Config{Name: "dep", Trip: breaker.ConsecutiveFailures(2), OpenFor: time.Hour})
	c := newClient(t, httpx.TransportConfig{Breaker: br, Retry: retry.Policy{MaxAttempts: 3}})

	for range 2 {
		resp, err := c.Get(srv.URL)
		if err != nil || resp.StatusCode != 500 {
			t.Fatalf("resp=%v err=%v", resp, err)
		}
		_ = resp.Body.Close()
	}
	if br.State() != resilience.BreakerOpen {
		t.Fatalf("two 500s must trip a 2-failure breaker: %v", br.State())
	}
	_, err := c.Get(srv.URL)
	if !errors.Is(err, resilience.ErrBreakerOpen) {
		t.Fatalf("open breaker must fail fast, got %v", err)
	}
}

func TestTransport_429DoesNotTripBreakerAndIsRetriedWithRetryAfter(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()
	br, _ := breaker.New(breaker.Config{Name: "dep", Trip: breaker.ConsecutiveFailures(1), OpenFor: time.Hour})
	var slept []time.Duration
	c := newClient(t, httpx.TransportConfig{
		Breaker: br,
		Retry: retry.Policy{MaxAttempts: 2, Sleep: func(ctx context.Context, d time.Duration) error {
			slept = append(slept, d)
			return ctx.Err()
		}},
	})
	resp, err := c.Get(srv.URL)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("resp=%v err=%v", resp, err)
	}
	_ = resp.Body.Close()
	if br.State() != resilience.BreakerClosed {
		t.Fatal("429 must not count as a dependency failure")
	}
	if len(slept) != 1 || slept[0] != time.Second {
		t.Fatalf("Retry-After not honoured: %v", slept)
	}
}

func TestTransport_BodyReadableAfterAttemptTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(60 * time.Millisecond) // body arrives after the attempt timeout would have fired
		_, _ = io.WriteString(w, "late body")
	}))
	defer srv.Close()
	c := newClient(t, httpx.TransportConfig{AttemptTimeout: 30 * time.Millisecond, CallTimeout: 2 * time.Second})
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || string(body) != "late body" {
		t.Fatalf("body=%q err=%v: the attempt timeout must not abort the body read", body, err)
	}
}

func TestTransport_AttemptTimeoutOnHeadersIsRetriedAndTripsBreaker(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer srv.Close()
	br, _ := breaker.New(breaker.Config{Name: "dep", Trip: breaker.ConsecutiveFailures(2), OpenFor: time.Hour})
	c := newClient(t, httpx.TransportConfig{Breaker: br, AttemptTimeout: 20 * time.Millisecond, Retry: retry.Policy{MaxAttempts: 2}})
	_, err := c.Get(srv.URL)
	if !errors.Is(err, resilience.ErrAttemptTimeout) || hits.Load() != 2 {
		t.Fatalf("err=%v hits=%d", err, hits.Load())
	}
	if br.State() != resilience.BreakerOpen {
		t.Fatalf("hung upstream must trip: %v", br.State())
	}
}

func TestTransport_CallTimeoutCoversBodyRead(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer srv.Close()
	c := newClient(t, httpx.TransportConfig{CallTimeout: 50 * time.Millisecond})
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err == nil {
		t.Fatal("body read must be cut by the call timeout")
	}
}

func TestTransport_BulkheadFullIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	bh, _ := bulkhead.New(bulkhead.Config{Name: "dep", MaxConcurrent: 1})
	release, _ := bh.Acquire(context.Background())
	defer release()
	c := newClient(t, httpx.TransportConfig{Bulkhead: bh})
	if _, err := c.Get(srv.URL); !errors.Is(err, resilience.ErrBulkheadFull) {
		t.Fatalf("err = %v", err)
	}
}

func TestNewTransport_Validates(t *testing.T) {
	if _, err := httpx.NewTransport(httpx.TransportConfig{}); err == nil {
		t.Fatal("Name required")
	}
	if _, err := httpx.NewTransport(httpx.TransportConfig{Name: "x", Retry: retry.Policy{AttemptTimeout: time.Second}}); err == nil {
		t.Fatal("Retry.AttemptTimeout must be rejected")
	}
}

func TestNewBaseTransport_Defaults(t *testing.T) {
	tr := httpx.NewBaseTransport(httpx.BaseTransportConfig{MaxConnsPerHost: 8})
	if tr.MaxConnsPerHost != 8 || tr.MaxIdleConnsPerHost != 8 || tr.TLSHandshakeTimeout != 3*time.Second || !tr.ForceAttemptHTTP2 {
		t.Fatalf("unexpected transport %+v", tr)
	}
}
