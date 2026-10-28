package httpx_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/anuar-fest/tools/resilience/httpx"
	"github.com/anuar-fest/tools/resilience/ratelimit"
	"github.com/anuar-fest/tools/resilience/shed"
)

func TestShed_Returns503WithProblemAndRetryAfter(t *testing.T) {
	s, _ := shed.New(shed.Config{Name: "api", MaxInFlight: 1})
	release, _ := s.Admit(t.Context(), shed.Critical)
	defer release()

	h := httpx.Shed(s, nil, 2*time.Second)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content-type = %q", ct)
	}
	ra, err := strconv.Atoi(rr.Header().Get("Retry-After"))
	if err != nil || ra < 2 || ra > 3 {
		t.Fatalf("Retry-After = %q", rr.Header().Get("Retry-After"))
	}
	var body map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil || body["status"] != float64(503) {
		t.Fatalf("body = %v err=%v", body, err)
	}

	release()
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != 200 || s.InFlight() != 0 {
		t.Fatalf("code=%d inflight=%d", rr.Code, s.InFlight())
	}
}

func TestRateLimit_Returns429PerKey(t *testing.T) {
	k, _ := ratelimit.NewKeyed(ratelimit.KeyedConfig{Name: "ingress", Rate: 1, Burst: 1})
	h := httpx.RateLimit(k, func(r *http.Request) string { return r.Header.Get("X-Tenant") })(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))

	do := func(tenant string) int {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Tenant", tenant)
		h.ServeHTTP(rr, req)
		return rr.Code
	}
	if do("a") != 200 || do("a") != 429 || do("b") != 200 {
		t.Fatal("per-key limiting broken")
	}
	for range 2 {
		if do("") != 200 {
			t.Fatal("empty key must not be limited")
		}
	}
}
