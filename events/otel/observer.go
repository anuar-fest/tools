package eventsotel

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/anuar-fest/tools/events"
)

// Attribute keys on the metrics.
const (
	AttrConsumer = attribute.Key("events.consumer")
	AttrType     = attribute.Key("events.type")
	AttrOutcome  = attribute.Key("events.outcome")
)

// Config configures the metrics observer.
type Config struct {
	// Meter to create instruments on (default: the global MeterProvider).
	Meter metric.Meter
}

// Observer implements events.Observer over OpenTelemetry metrics:
// events.messages (counter) and events.message.duration (histogram, seconds)
// by consumer (or source, on publish), event type and outcome.
type Observer struct {
	messages metric.Int64Counter
	duration metric.Float64Histogram
}

// NewObserver creates the instruments.
func NewObserver(cfg Config) (*Observer, error) {
	if cfg.Meter == nil {
		cfg.Meter = otel.GetMeterProvider().Meter(InstrumentationName)
	}
	o := &Observer{}
	var err error
	o.messages, err = cfg.Meter.Int64Counter("events.messages",
		metric.WithDescription("Messages by consumer (or source), type and outcome."),
		metric.WithUnit("{message}"))
	if err != nil {
		return nil, err
	}
	o.duration, err = cfg.Meter.Float64Histogram("events.message.duration",
		metric.WithDescription("Handler or publish duration."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60))
	if err != nil {
		return nil, err
	}
	return o, nil
}

// Observe implements events.Observer.
func (o *Observer) Observe(ctx context.Context, s events.Stat) {
	set := metric.WithAttributes(AttrConsumer.String(s.Consumer), AttrType.String(s.Type), AttrOutcome.String(string(s.Outcome)))
	o.messages.Add(ctx, 1, set)
	if s.Elapsed > 0 {
		o.duration.Record(ctx, s.Elapsed.Seconds(), set)
	}
}

var _ events.Observer = (*Observer)(nil)
