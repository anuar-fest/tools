// Package clientnats traces raw nats.go usage, core and JetStream: the
// trace context travels in the message header, every publish is a producer
// span and every handled message a consumer span joined to it, and a
// handler panic is recovered and recorded instead of killing the
// subscriber. Message counts and durations are metrics/nats; its wrappers
// nest inside the ones here.
//
// Services on the typed events path use events/otel instead; this package
// is for code that still publishes and subscribes with nats.Msg directly,
// such as the shaffra_events client.
package clientnats

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/anuar-fest/tools/metrics"
)

// InstrumentationName is the tracer name.
const InstrumentationName = "github.com/anuar-fest/tools/otelx/client/nats"

// Attribute keys (messaging semantic conventions).
const (
	AttrSystem        = attribute.Key("messaging.system")
	AttrDestination   = attribute.Key("messaging.destination.name")
	AttrOperation     = attribute.Key("messaging.operation.type")
	AttrConsumer      = attribute.Key("messaging.consumer.group.name")
	AttrMessageID     = attribute.Key("messaging.message.id")
	AttrBodySize      = attribute.Key("messaging.message.body.size")
	AttrStream        = attribute.Key("messaging.nats.stream")
	AttrDeliveryCount = attribute.Key("messaging.delivery.attempt")
	AttrErrorType     = attribute.Key("error.type")
)

// Config configures New.
type Config struct {
	// Tracer defaults to the global tracer named InstrumentationName.
	Tracer trace.Tracer
	// Propagator defaults to the global propagator.
	Propagator propagation.TextMapPropagator
	// Logger receives recovered handler panics. nil discards.
	Logger *slog.Logger
	// Consumer names this subscriber on consumer spans (the durable or
	// queue group name).
	Consumer string
}

// Instrumentation traces NATS operations.
type Instrumentation struct {
	cfg Config
}

// New builds the instrumentation.
func New(cfg Config) (*Instrumentation, error) {
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer(InstrumentationName)
	}
	if cfg.Propagator == nil {
		cfg.Propagator = otel.GetTextMapPropagator()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Instrumentation{cfg: cfg}, nil
}

// Inject writes the trace context of ctx into the message header.
func (n *Instrumentation) Inject(ctx context.Context, msg *nats.Msg) {
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	n.cfg.Propagator.Inject(ctx, propagation.HeaderCarrier(http.Header(msg.Header)))
}

// Extract returns ctx with the remote span context carried by the header.
func (n *Instrumentation) Extract(ctx context.Context, msg *nats.Msg) context.Context {
	if msg.Header == nil {
		return ctx
	}
	return n.cfg.Propagator.Extract(ctx, propagation.HeaderCarrier(http.Header(msg.Header)))
}

// PublishMsg publishes on a core connection in a producer span.
func (n *Instrumentation) PublishMsg(ctx context.Context, nc *nats.Conn, msg *nats.Msg) error {
	ctx, span := n.startPublish(ctx, msg)
	n.Inject(ctx, msg)
	err := nc.PublishMsg(msg)
	finishPublish(span, err)
	return err
}

// PublishJetStream publishes with an acknowledgement in a producer span.
func (n *Instrumentation) PublishJetStream(ctx context.Context, js jetstream.JetStream, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ctx, span := n.startPublish(ctx, msg)
	n.Inject(ctx, msg)
	ack, err := js.PublishMsg(ctx, msg, opts...)
	if ack != nil {
		span.SetAttributes(AttrStream.String(ack.Stream))
	}
	finishPublish(span, err)
	return ack, err
}

func (n *Instrumentation) startPublish(ctx context.Context, msg *nats.Msg) (context.Context, trace.Span) {
	attrs := []attribute.KeyValue{AttrSystem.String("nats"), AttrDestination.String(msg.Subject), AttrOperation.String("send"), AttrBodySize.Int(len(msg.Data))}
	if id := msg.Header.Get(nats.MsgIdHdr); id != "" {
		attrs = append(attrs, AttrMessageID.String(id))
	}
	return n.cfg.Tracer.Start(ctx, msg.Subject+" publish", trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attrs...))
}

func finishPublish(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetAttributes(AttrErrorType.String(metrics.ErrorType(err)))
		span.SetStatus(codes.Error, "publish failed")
	}
	span.End()
}

// Handler handles one core message; the error marks the span.
type Handler func(ctx context.Context, msg *nats.Msg) error

// MsgHandler wraps h for nc.Subscribe / QueueSubscribe: the producer's
// trace is continued, a consumer span covers the handler, a panic is
// recovered and recorded instead of killing the subscriber. Nest the
// metrics wrapper inside: n.MsgHandler(m.Handler(h)).
func (n *Instrumentation) MsgHandler(h Handler) nats.MsgHandler {
	return func(msg *nats.Msg) {
		ctx := n.Extract(context.Background(), msg)
		attrs := []attribute.KeyValue{AttrBodySize.Int(len(msg.Data))}
		n.process(ctx, msg.Subject, attrs, func(ctx context.Context) error { return h(ctx, msg) })
	}
}

// JetStreamHandler handles one JetStream message. The handler owns the
// acknowledgement; this wrapper only observes.
type JetStreamHandler func(ctx context.Context, msg jetstream.Msg) error

// JetStreamMsgHandler wraps h for consumer.Consume.
func (n *Instrumentation) JetStreamMsgHandler(h JetStreamHandler) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		ctx := context.Background()
		if hdr := msg.Headers(); hdr != nil {
			ctx = n.cfg.Propagator.Extract(ctx, propagation.HeaderCarrier(http.Header(hdr)))
		}
		attrs := []attribute.KeyValue{AttrBodySize.Int(len(msg.Data()))}
		if id := msg.Headers().Get(nats.MsgIdHdr); id != "" {
			attrs = append(attrs, AttrMessageID.String(id))
		}
		if md, err := msg.Metadata(); err == nil && md != nil {
			attrs = append(attrs, AttrStream.String(md.Stream), AttrDeliveryCount.Int64(clamp(md.NumDelivered)))
		}
		n.process(ctx, msg.Subject(), attrs, func(ctx context.Context) error { return h(ctx, msg) })
	}
}

func (n *Instrumentation) process(ctx context.Context, subject string, extra []attribute.KeyValue, run func(context.Context) error) {
	spanAttrs := append([]attribute.KeyValue{AttrSystem.String("nats"), AttrDestination.String(subject), AttrOperation.String("process")}, extra...)
	if n.cfg.Consumer != "" {
		spanAttrs = append(spanAttrs, AttrConsumer.String(n.cfg.Consumer))
	}
	ctx, span := n.cfg.Tracer.Start(ctx, subject+" process", trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(spanAttrs...))
	panicked, err := n.guard(ctx, subject, run)
	if err != nil {
		et := metrics.ErrorType(err)
		if panicked {
			et = "panic"
		}
		span.RecordError(err)
		span.SetAttributes(AttrErrorType.String(et))
		span.SetStatus(codes.Error, "handler failed")
	}
	span.End()
}

// guard runs the handler and turns a panic into an error with its stack
// logged, so one bad message does not end the subscription.
func (n *Instrumentation) guard(ctx context.Context, subject string, run func(context.Context) error) (panicked bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			n.cfg.Logger.ErrorContext(ctx, "panic recovered in nats handler", "subject", subject, "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
			panicked, err = true, fmt.Errorf("panic: %v", p)
		}
	}()
	return false, run(ctx)
}

func clamp(v uint64) int64 {
	if v > 1<<62 {
		return 1 << 62
	}
	return int64(v)
}
