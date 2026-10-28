package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anuar-fest/tools/health"
)

func ok(context.Context) error { return nil }

func TestReady_ReportsEachCheck(t *testing.T) {
	g := health.New(health.Config{CacheTTL: -1, Info: map[string]string{"name": "svc"}})
	g.AddReadiness("db", ok)
	g.AddReadiness("nats", func(context.Context) error { return errors.New("disconnected") })
	rep := g.Ready(context.Background())
	if rep.OK() || rep.Checks["db"].Status != health.StatusOK || rep.Checks["nats"].Error != "disconnected" || rep.Info["name"] != "svc" {
		t.Fatalf("%+v", rep)
	}
	rr := httptest.NewRecorder()
	g.ReadyHandler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz/ready", nil))
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("%d %v", rr.Code, rr.Header())
	}
	var body health.Report
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil || body.Status != health.StatusFail {
		t.Fatalf("%v %s", err, rr.Body.String())
	}
	rr = httptest.NewRecorder()
	g.LiveHandler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz/live", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("liveness must not depend on readiness checks: %d", rr.Code)
	}
}

func TestReady_TimeoutAndPanicAreFailures(t *testing.T) {
	g := health.New(health.Config{Timeout: 30 * time.Millisecond, CacheTTL: -1})
	g.AddReadiness("stuck", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
	g.AddReadiness("panicky", func(context.Context) error { panic("boom") })
	g.AddReadiness("fine", ok)
	start := time.Now()
	rep := g.Ready(context.Background())
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("checks must run concurrently under one deadline, took %s", took)
	}
	if rep.Checks["stuck"].Status != health.StatusFail || rep.Checks["panicky"].Status != health.StatusFail || rep.Checks["fine"].Status != health.StatusOK {
		t.Fatalf("%+v", rep.Checks)
	}
}

func TestReady_DrainingAndStartupGateSkipChecks(t *testing.T) {
	var calls atomic.Int32
	g := health.New(health.Config{StartupGate: true, CacheTTL: -1})
	g.AddReadiness("db", func(context.Context) error { calls.Add(1); return nil })
	if rep := g.Ready(context.Background()); rep.Status != health.StatusStarting || calls.Load() != 0 {
		t.Fatalf("before MarkStarted: %+v", rep)
	}
	g.MarkStarted()
	if rep := g.Ready(context.Background()); !rep.OK() || calls.Load() != 1 {
		t.Fatalf("after MarkStarted: %+v", rep)
	}
	g.SetDraining(true)
	if rep := g.Ready(context.Background()); rep.Status != health.StatusDraining || calls.Load() != 1 || !g.Draining() {
		t.Fatalf("draining: %+v", rep)
	}
	g.SetDraining(false)
	if rep := g.Ready(context.Background()); !rep.OK() {
		t.Fatalf("undrained: %+v", rep)
	}
}

func TestReady_CacheIsSingleFlight(t *testing.T) {
	var calls atomic.Int32
	g := health.New(health.Config{CacheTTL: time.Hour})
	g.AddReadiness("db", func(context.Context) error {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		return nil
	})
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() { g.Ready(context.Background()) })
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("50 concurrent probes must cost one evaluation, got %d", calls.Load())
	}
	fresh := health.New(health.Config{CacheTTL: -1})
	fresh.AddReadiness("db", func(context.Context) error { calls.Add(1); return nil })
	fresh.Ready(context.Background())
	fresh.Ready(context.Background())
	if calls.Load() != 3 {
		t.Fatalf("negative TTL must disable the cache, got %d", calls.Load())
	}
}

func TestReady_OnReadinessChangeFiresOnTransitions(t *testing.T) {
	var flips []string
	up := true
	g := health.New(health.Config{CacheTTL: -1, OnReadinessChange: func(from, to health.Report) {
		flips = append(flips, string(from.Status)+"->"+string(to.Status))
	}})
	g.AddReadiness("dep", health.Bool(func() bool { return up }, "down"))
	g.Ready(context.Background())
	g.Ready(context.Background()) // same status: no call
	up = false
	g.Ready(context.Background())
	up = true
	g.Ready(context.Background())
	if strings.Join(flips, ",") != "ok->fail,fail->ok" {
		t.Fatalf("flips = %v", flips)
	}
}

func TestAdd_RejectsDuplicatesAndEmpty(t *testing.T) {
	g := health.New(health.Config{})
	g.AddLiveness("x", ok)
	for _, f := range []func(){
		func() { g.AddLiveness("x", ok) },
		func() { g.AddReadiness("", ok) },
		func() { g.AddReadiness("y", nil) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("wiring bug must panic")
				}
			}()
			f()
		}()
	}
}

func TestBool(t *testing.T) {
	up := false
	c := health.Bool(func() bool { return up }, "nats disconnected")
	if err := c(context.Background()); err == nil || err.Error() != "nats disconnected" {
		t.Fatalf("%v", err)
	}
	up = true
	if err := c(context.Background()); err != nil {
		t.Fatal(err)
	}
}
