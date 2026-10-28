// Package eventsotel connects package events to OpenTelemetry: W3C trace
// context travels in the envelope (producer span → consumer span across the
// broker), and every publish and handler outcome is exported as metrics.
package eventsotel

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/anuar-fest/tools/events"
)

// InstrumentationName is the tracer and meter name.
const InstrumentationName = "github.com/anuar-fest/tools/events"

// envelopeCarrier adapts an envelope's trace fields to the propagator.
type envelopeCarrier struct{ e *events.Envelope }

func (c envelopeCarrier) Get(key string) string {
	switch key {
	case events.HeaderTraceParent:
		return c.e.TraceParent
	case events.HeaderTraceState:
		return c.e.TraceState
	default:
		return c.e.Headers[key]
	}
}

func (c envelopeCarrier) Set(key, value string) {
	switch key {
	case events.HeaderTraceParent:
		c.e.TraceParent = value
	case events.HeaderTraceState:
		c.e.TraceState = value
	default:
		if c.e.Headers == nil {
			c.e.Headers = map[string]string{}
		}
		c.e.Headers[key] = value
	}
}

func (c envelopeCarrier) Keys() []string {
	keys := make([]string, 0, 2+len(c.e.Headers))
	keys = append(keys, events.HeaderTraceParent, events.HeaderTraceState)
	for k := range c.e.Headers {
		keys = append(keys, k)
	}
	return keys
}

var _ propagation.TextMapCarrier = envelopeCarrier{}

// Inject writes the trace context of ctx into e using the global propagator.
func Inject(ctx context.Context, e *events.Envelope) {
	otel.GetTextMapPropagator().Inject(ctx, envelopeCarrier{e: e})
}

// Extract returns ctx with the remote span context carried by e.
func Extract(ctx context.Context, e events.Envelope) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, envelopeCarrier{e: &e})
}

func attrs(e events.Envelope) []attribute.KeyValue {
	out := []attribute.KeyValue{
		attribute.String("messaging.system", "events"),
		attribute.String("messaging.destination.name", e.Type),
		attribute.String("messaging.message.id", e.ID),
		attribute.String("messaging.message.conversation_id", e.CorrelationID),
	}
	if e.TenantID != "" {
		out = append(out, attribute.String("tenant.id", e.TenantID))
	}
	return out
}

// TracedPublisher wraps pub: each Publish runs in a producer span and the
// envelope carries its context so the consumer's span joins the trace.
func TracedPublisher(pub events.Publisher, tracer trace.Tracer) events.Publisher {
	if tracer == nil {
		tracer = otel.Tracer(InstrumentationName)
	}
	return events.PublisherFunc(func(ctx context.Context, e events.Envelope) error {
		ctx, span := tracer.Start(ctx, e.Type+" publish", trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attrs(e)...))
		defer span.End()
		Inject(ctx, &e)
		err := pub.Publish(ctx, e)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "publish failed")
		}
		return err
	})
}

// Traced is handler middleware: it extracts the producer's context from the
// envelope and runs the handler in a consumer span that is a child of it.
func Traced(tracer trace.Tracer, consumer string) events.Middleware {
	if tracer == nil {
		tracer = otel.Tracer(InstrumentationName)
	}
	return func(next events.Handler) events.Handler {
		return func(ctx context.Context, e events.Envelope) error {
			ctx = Extract(ctx, e)
			spanAttrs := append(attrs(e), attribute.String("messaging.consumer.group.name", consumer))
			if d, ok := events.DeliveryFrom(ctx); ok {
				spanAttrs = append(spanAttrs, attribute.Int("messaging.delivery.attempt", d.Attempt))
			}
			ctx, span := tracer.Start(ctx, e.Type+" process", trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(spanAttrs...))
			defer span.End()
			err := next(ctx, e)
			if err != nil {
				span.RecordError(err)
				span.SetAttributes(attribute.Bool("messaging.drop", events.IsDrop(err)))
				span.SetStatus(codes.Error, "handler failed")
			}
			return err
		}
	}
}
