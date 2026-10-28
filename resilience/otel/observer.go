// Package resilienceotel exports the decisions of the resilience module
// through the OpenTelemetry API: a counter per decision, a duration
// histogram, a breaker-state gauge and transition counter, observable gauges
// for bulkhead and worker-pool saturation, and a span event on the active
// span when a policy rejects or retries.
//
// Metric names follow OTel conventions (dots); the Prometheus exporter turns
// them into resilience_decisions_total and friends.
//
// Labels are the closed sets from package resilience plus the dependency
// name. Never add a tenant, user or URL: that is what traces are for.
package resilienceotel

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/bulkhead"
	"github.com/anuar-fest/tools/resilience/workpool"
)

// InstrumentationName is the meter name used when Config.Meter is nil.
const InstrumentationName = "github.com/anuar-fest/tools/resilience"

// Attribute keys.
const (
	AttrDependency = attribute.Key("resilience.dependency")
	AttrPolicy     = attribute.Key("resilience.policy")
	AttrDecision   = attribute.Key("resilience.decision")
	AttrFrom       = attribute.Key("resilience.breaker.from")
	AttrTo         = attribute.Key("resilience.breaker.to")
	AttrState      = attribute.Key("resilience.breaker.state")
)

// Config configures the observer.
type Config struct {
	// Meter to create instruments on (default: the global MeterProvider).
	Meter metric.Meter
	// DisableSpanEvents stops adding "resilience.decision" events to the
	// active span. Successes are never added; only rejections and retries.
	DisableSpanEvents bool
}

// Observer implements resilience.Observer over OpenTelemetry.
type Observer struct {
	meter       metric.Meter
	decisions   metric.Int64Counter
	duration    metric.Float64Histogram
	state       metric.Int64Gauge
	transitions metric.Int64Counter
	spanEvents  bool
}

// New creates the instruments.
func New(cfg Config) (*Observer, error) {
	if cfg.Meter == nil {
		cfg.Meter = otel.GetMeterProvider().Meter(InstrumentationName)
	}
	o := &Observer{meter: cfg.Meter, spanEvents: !cfg.DisableSpanEvents}
	var err error
	o.decisions, err = cfg.Meter.Int64Counter("resilience.decisions",
		metric.WithDescription("Policy decisions by dependency, policy and decision."),
		metric.WithUnit("{decision}"))
	if err != nil {
		return nil, err
	}
	o.duration, err = cfg.Meter.Float64Histogram("resilience.call.duration",
		metric.WithDescription("Duration of calls and attempts that reported an elapsed time."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10))
	if err != nil {
		return nil, err
	}
	o.state, err = cfg.Meter.Int64Gauge("resilience.breaker.state",
		metric.WithDescription("Circuit breaker state: 0 closed, 1 open, 2 half-open."),
		metric.WithUnit("{state}"))
	if err != nil {
		return nil, err
	}
	o.transitions, err = cfg.Meter.Int64Counter("resilience.breaker.transitions",
		metric.WithDescription("Circuit breaker state transitions."),
		metric.WithUnit("{transition}"))
	if err != nil {
		return nil, err
	}
	return o, nil
}

// ObserveDecision implements resilience.Observer.
func (o *Observer) ObserveDecision(ctx context.Context, e resilience.Event) {
	attrs := metric.WithAttributes(
		AttrDependency.String(e.Dependency),
		AttrPolicy.String(e.Policy),
		AttrDecision.String(string(e.Decision)),
	)
	o.decisions.Add(ctx, 1, attrs)
	if e.Elapsed > 0 {
		o.duration.Record(ctx, e.Elapsed.Seconds(), attrs)
	}
	if o.spanEvents && e.Decision != resilience.DecisionSuccess {
		if span := trace.SpanFromContext(ctx); span.IsRecording() {
			evAttrs := []attribute.KeyValue{
				AttrDependency.String(e.Dependency),
				AttrPolicy.String(e.Policy),
				AttrDecision.String(string(e.Decision)),
			}
			if e.Attempt > 0 {
				evAttrs = append(evAttrs, attribute.Int("resilience.attempt", e.Attempt))
			}
			span.AddEvent("resilience.decision", trace.WithAttributes(evAttrs...))
		}
	}
}

// ObserveBreakerState implements resilience.Observer.
func (o *Observer) ObserveBreakerState(ctx context.Context, c resilience.StateChange) {
	dep := AttrDependency.String(c.Dependency)
	o.state.Record(ctx, int64(c.To), metric.WithAttributes(dep))
	o.transitions.Add(ctx, 1, metric.WithAttributes(dep, AttrFrom.String(c.From.String()), AttrTo.String(c.To.String())))
	if o.spanEvents {
		if span := trace.SpanFromContext(ctx); span.IsRecording() {
			span.AddEvent("resilience.breaker.transition", trace.WithAttributes(dep, AttrFrom.String(c.From.String()), AttrTo.String(c.To.String())))
		}
	}
}

// RegisterBulkhead exports resilience.bulkhead.inflight and .capacity for b.
// Unregister the returned registration on shutdown.
func (o *Observer) RegisterBulkhead(b *bulkhead.Bulkhead) (metric.Registration, error) {
	if b == nil {
		return nil, errors.New("resilienceotel: nil bulkhead")
	}
	inflight, err := o.meter.Int64ObservableGauge("resilience.bulkhead.inflight",
		metric.WithDescription("Slots currently held."), metric.WithUnit("{slot}"))
	if err != nil {
		return nil, err
	}
	capacity, err := o.meter.Int64ObservableGauge("resilience.bulkhead.capacity",
		metric.WithDescription("Total slots."), metric.WithUnit("{slot}"))
	if err != nil {
		return nil, err
	}
	dep := metric.WithAttributes(AttrDependency.String(b.Name()))
	return o.meter.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		obs.ObserveInt64(inflight, int64(b.InFlight()), dep)
		obs.ObserveInt64(capacity, int64(b.Capacity()), dep)
		return nil
	}, inflight, capacity)
}

// RegisterPool exports resilience.workpool.queued, .capacity and .panics for p.
func (o *Observer) RegisterPool(p *workpool.Pool) (metric.Registration, error) {
	if p == nil {
		return nil, errors.New("resilienceotel: nil pool")
	}
	queued, err := o.meter.Int64ObservableGauge("resilience.workpool.queued",
		metric.WithDescription("Jobs waiting in the queue."), metric.WithUnit("{job}"))
	if err != nil {
		return nil, err
	}
	capacity, err := o.meter.Int64ObservableGauge("resilience.workpool.capacity",
		metric.WithDescription("Queue size."), metric.WithUnit("{job}"))
	if err != nil {
		return nil, err
	}
	panics, err := o.meter.Int64ObservableCounter("resilience.workpool.panics",
		metric.WithDescription("Jobs that panicked since start."), metric.WithUnit("{job}"))
	if err != nil {
		return nil, err
	}
	dep := metric.WithAttributes(AttrDependency.String(p.Name()))
	return o.meter.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		obs.ObserveInt64(queued, int64(p.Queued()), dep)
		obs.ObserveInt64(capacity, int64(p.Capacity()), dep)
		obs.ObserveInt64(panics, int64(p.Panics()), dep)
		return nil
	}, queued, capacity, panics)
}

var _ resilience.Observer = (*Observer)(nil)
