package alarms_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/anuar-fest/tools/alarms"
	"github.com/anuar-fest/tools/events"
	"github.com/anuar-fest/tools/health"
	"github.com/anuar-fest/tools/resilience"
)

// series collects alarms (counter) and alarms.active (gauge) by type/name.
func series(t *testing.T, reader *sdkmetric.ManualReader) (counts, active map[string]int64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	counts, active = map[string]int64{}, map[string]int64{}
	key := func(set attribute.Set) string {
		typ, _ := set.Value("alarm.type")
		name, _ := set.Value("alarm.name")
		return typ.AsString() + "/" + name.AsString()
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					counts[key(dp.Attributes)] += dp.Value
				}
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					active[key(dp.Attributes)] = dp.Value
				}
			}
		}
	}
	return counts, active
}

func newAlarms(t *testing.T, buf *bytes.Buffer, defs ...alarms.Definition) (*alarms.Alarms, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	cfg := alarms.Config{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("m"), Service: "svc", Definitions: defs}
	if buf != nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(buf, nil))
	}
	a, err := alarms.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return a, reader
}

func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	return out
}

func TestRaise_CountsLogsAndSpanEvent(t *testing.T) {
	var buf bytes.Buffer
	a, reader := newAlarms(t, &buf, alarms.Definition{Type: "billing", Name: "quota_exceeded", Severity: alarms.Critical})
	exporter := tracetest.NewInMemoryExporter()
	ctx, span := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)).Tracer("t").Start(context.Background(), "op")
	a.Raise(ctx, alarms.Alarm{Type: "billing", Name: "quota_exceeded", Detail: "tenant t1 over quota", Attrs: []slog.Attr{slog.String("tenant", "t1")}})
	a.Raise(ctx, alarms.Alarm{Type: "billing", Name: "quota_exceeded", Detail: "again"})
	a.Raise(ctx, alarms.Alarm{Type: "Bad Type", Name: "x y"}) // invalid labels are sanitized
	a.RaiseError(ctx, "billing", "charge_failed", "weird", errors.New("card declined"))
	span.End()

	counts, _ := series(t, reader)
	if counts["billing/quota_exceeded"] != 2 || counts["_invalid/_invalid"] != 1 || counts["billing/charge_failed"] != 1 {
		t.Fatalf("series: %v", counts)
	}
	lines := logLines(t, &buf)
	if len(lines) != 4 {
		t.Fatalf("one line per alarm: %v", lines)
	}
	first := lines[0]
	if first["level"] != "ERROR" || first["alarm"] != true || first["alarm.name"] != "quota_exceeded" || first["alarm.severity"] != "critical" || first["service"] != "svc" || first["tenant"] != "t1" || first["msg"] != "tenant t1 over quota" {
		t.Fatalf("the severity comes from the definition: %v", first)
	}
	if last := lines[3]; last["level"] != "WARN" || last["msg"] != "card declined" || last["err"] != "card declined" {
		t.Fatalf("unknown severity falls back to warning: %v", last)
	}
	if ev := exporter.GetSpans()[0].Events; len(ev) != 4 || ev[0].Name != "alarm" {
		t.Fatalf("span events: %+v", ev)
	}
}

func TestSetClear_StateTransitions(t *testing.T) {
	var buf bytes.Buffer
	a, reader := newAlarms(t, &buf, alarms.Definition{Type: "cache", Name: "unreachable", Kind: alarms.State, Severity: alarms.Critical})
	ctx := context.Background()
	al := alarms.Alarm{Type: "cache", Name: "unreachable", Detail: "redis refused"}
	a.Clear(ctx, al) // not set: nothing
	a.Set(ctx, al)
	a.Set(ctx, al) // already active: nothing
	if !a.Active("cache", "unreachable") {
		t.Fatal("must be active")
	}
	counts, active := series(t, reader)
	if counts["cache/unreachable"] != 1 || active["cache/unreachable"] != 1 {
		t.Fatalf("set: counts=%v active=%v", counts, active)
	}
	a.Clear(ctx, alarms.Alarm{Type: "cache", Name: "unreachable"})
	a.Clear(ctx, al)
	if a.Active("cache", "unreachable") {
		t.Fatal("must be cleared")
	}
	counts, active = series(t, reader)
	if counts["cache/unreachable"] != 1 || active["cache/unreachable"] != 0 {
		t.Fatalf("clear: counts=%v active=%v", counts, active)
	}
	lines := logLines(t, &buf)
	if len(lines) != 2 || lines[0]["level"] != "ERROR" || lines[1]["level"] != "INFO" || lines[1]["msg"] != "alarm cleared" || lines[1]["alarm.severity"] != "info" {
		t.Fatalf("one line per transition: %v", lines)
	}
}

func TestNew_ValidatesDefinitionsAndCapsNames(t *testing.T) {
	for _, bad := range []alarms.Definition{
		{Type: "Billing", Name: "x"},
		{Type: "billing", Name: "x-y"},
		{Type: "billing", Name: "x", Severity: "fatal"},
		{Type: "billing", Name: "x", Kind: "pulse"},
		{Type: "health", Name: "readiness_lost"}, // built in already
	} {
		if _, err := alarms.New(alarms.Config{Definitions: []alarms.Definition{bad}}); err == nil {
			t.Errorf("%+v must be rejected", bad)
		}
	}
	reader := sdkmetric.NewManualReader()
	a, err := alarms.New(alarms.Config{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("m"), MaxNames: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"a", "b", "c", "d"} {
		a.Raise(context.Background(), alarms.Alarm{Type: "t", Name: n})
	}
	counts, _ := series(t, reader)
	if counts["t/a"] != 1 || counts["t/b"] != 1 || counts["t/_other"] != 2 {
		t.Fatalf("cap: %v", counts)
	}
	if defs := a.Definitions(); len(defs) != len(alarms.BuiltinDefinitions()) || defs[0].Type != "events" {
		t.Fatalf("built-in definitions, sorted: %+v", defs)
	}
}

func TestHooks(t *testing.T) {
	a, reader := newAlarms(t, nil)
	ctx := context.Background()
	ro := a.ResilienceObserver(nil)
	ro.ObserveDecision(ctx, resilience.Event{Decision: resilience.DecisionFailure})
	ro.ObserveBreakerState(ctx, resilience.StateChange{Dependency: "fraud", From: resilience.BreakerClosed, To: resilience.BreakerOpen})
	ro.ObserveBreakerState(ctx, resilience.StateChange{Dependency: "ledger", From: resilience.BreakerClosed, To: resilience.BreakerOpen})
	ro.ObserveBreakerState(ctx, resilience.StateChange{Dependency: "fraud", From: resilience.BreakerHalfOpen, To: resilience.BreakerClosed})
	if !a.Active(alarms.TypeResilience, "breaker_open") {
		t.Fatal("ledger is still open")
	}
	ro.ObserveBreakerState(ctx, resilience.StateChange{Dependency: "ledger", From: resilience.BreakerHalfOpen, To: resilience.BreakerClosed})
	if a.Active(alarms.TypeResilience, "breaker_open") {
		t.Fatal("none open")
	}
	eo := a.EventsObserver(nil)
	eo.Observe(ctx, events.Stat{Consumer: "w", Type: "x.v1", Outcome: events.OutcomeAcked})
	eo.Observe(ctx, events.Stat{Consumer: "w", Type: "x.v1", Outcome: events.OutcomeDeadLettered, Attempt: 5, Err: errors.New("boom")})
	eo.Observe(ctx, events.Stat{Consumer: "w", Type: "x.v1", Outcome: events.OutcomePanic})
	hc := health.New(health.Config{CacheTTL: -1, OnReadinessChange: a.ReadinessChanged})
	up := true
	hc.AddReadiness("db", health.Bool(func() bool { return up }, "down"))
	hc.Ready(ctx)
	up = false
	hc.Ready(ctx)
	if !a.Active(alarms.TypeHealth, "readiness_lost") {
		t.Fatal("readiness lost must be set")
	}
	up = true
	hc.Ready(ctx)
	counts, active := series(t, reader)
	want := map[string]int64{"resilience/breaker_opened": 2, "resilience/breaker_open": 1, "events/dead_letter": 1, "events/handler_panic": 1, "health/readiness_lost": 1}
	for k, v := range want {
		if counts[k] != v {
			t.Errorf("%s = %d, want %d (all: %v)", k, counts[k], v, counts)
		}
	}
	if len(counts) != len(want) {
		t.Fatalf("unexpected series: %v", counts)
	}
	if active["resilience/breaker_open"] != 0 || active["health/readiness_lost"] != 0 {
		t.Fatalf("states must be cleared: %v", active)
	}
}
