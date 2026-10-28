// Package metricsnats measures raw nats.go usage, core and JetStream:
// messages published and handled, failures and durations per subject, and
// the connection's counters through RegisterConnStats. Spans are
// otelx/client/nats; the wrappers here nest inside its wrappers so one
// sample is recorded per message, inside the span.
//
// Services on the typed events path use events/otel instead; this package
// is for code that still publishes and subscribes with nats.Msg directly.
package metricsnats

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/anuar-fest/tools/metrics"
)

// InstrumentationName is the meter name.
const InstrumentationName = "github.com/anuar-fest/tools/metrics/nats"

// Attribute keys (messaging semantic conventions).
const (
	AttrSystem      = attribute.Key("messaging.system")
	AttrDestination = attribute.Key("messaging.destination.name")
	AttrOperation   = attribute.Key("messaging.operation.type")
	AttrConsumer    = attribute.Key("messaging.consumer.group.name")
	AttrErrorType   = attribute.Key("error.type")
)

// Config configures New.
type Config struct {
	// Meter defaults to the global meter named InstrumentationName.
	Meter metric.Meter
	// Consumer names this subscriber on consumer metrics (the durable or
	// queue group name).
	Consumer string
	// DestinationName maps a subject to the metric label. Default: the
	// subject itself. Set it when subjects embed ids ("orders.123.created"),
	// which would create a series per id.
	DestinationName func(subject string) string
	// Logger receives panics recovered by MsgHandler and
	// JetStreamMsgHandler. nil discards.
	Logger *slog.Logger
}

// Instrumentation measures NATS operations.
type Instrumentation struct {
	cfg      Config
	sent     metric.Int64Counter
	consumed metric.Int64Counter
	duration metric.Float64Histogram
}

// New creates the instruments.
func New(cfg Config) (*Instrumentation, error) {
	if cfg.Meter == nil {
		cfg.Meter = otel.GetMeterProvider().Meter(InstrumentationName)
	}
	if cfg.DestinationName == nil {
		cfg.DestinationName = func(s string) string { return s }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	n := &Instrumentation{cfg: cfg}
	var err error
	n.sent, err = cfg.Meter.Int64Counter("messaging.client.sent.messages", metric.WithDescription("Messages published."), metric.WithUnit("{message}"))
	if err != nil {
		return nil, err
	}
	n.consumed, err = cfg.Meter.Int64Counter("messaging.client.consumed.messages", metric.WithDescription("Messages handled."), metric.WithUnit("{message}"))
	if err != nil {
		return nil, err
	}
	n.duration, err = cfg.Meter.Float64Histogram("messaging.client.operation.duration",
		metric.WithDescription("Publish and handler durations."), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(metrics.LatencyBuckets...))
	if err != nil {
		return nil, err
	}
	return n, nil
}

// Publish records one sent message and its duration around publish,
// whatever publish does. Nest the tracing hook inside:
//
//	err := m.Publish(ctx, msg.Subject, func(ctx context.Context) error { return tr.PublishMsg(ctx, nc, msg) })
func (n *Instrumentation) Publish(ctx context.Context, subject string, publish func(context.Context) error) error {
	start := time.Now()
	err := publish(ctx)
	n.record(ctx, n.sent, "send", subject, start, metrics.ErrorType(err))
	return err
}

// PublishMsg publishes on a core connection and records it.
func (n *Instrumentation) PublishMsg(ctx context.Context, nc *nats.Conn, msg *nats.Msg) error {
	return n.Publish(ctx, msg.Subject, func(context.Context) error { return nc.PublishMsg(msg) })
}

// PublishJetStream publishes with an acknowledgement and records it.
func (n *Instrumentation) PublishJetStream(ctx context.Context, js jetstream.JetStream, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	var ack *jetstream.PubAck
	err := n.Publish(ctx, msg.Subject, func(ctx context.Context) error {
		var err error
		ack, err = js.PublishMsg(ctx, msg, opts...)
		return err
	})
	return ack, err
}

// Handler handles one core message; the error marks the sample.
type Handler func(ctx context.Context, msg *nats.Msg) error

// JetStreamHandler handles one JetStream message. The handler owns the
// acknowledgement; the wrappers only observe.
type JetStreamHandler func(ctx context.Context, msg jetstream.Msg) error

// Handler records one consumed message and the handler's duration. A panic
// is recorded as error.type=panic and re-raised, so the outer guard (the
// tracing hook's MsgHandler, or yours) still sees it with its stack.
func (n *Instrumentation) Handler(h Handler) Handler {
	return func(ctx context.Context, msg *nats.Msg) (err error) {
		start := time.Now()
		defer n.finishProcess(ctx, msg.Subject, start, &err, true)
		return h(ctx, msg)
	}
}

// JetStreamHandler is Handler for JetStream messages.
func (n *Instrumentation) JetStreamHandler(h JetStreamHandler) JetStreamHandler {
	return func(ctx context.Context, msg jetstream.Msg) (err error) {
		start := time.Now()
		defer n.finishProcess(ctx, msg.Subject(), start, &err, true)
		return h(ctx, msg)
	}
}

// MsgHandler wraps h for nc.Subscribe / QueueSubscribe when this is the
// only hook: a panic is recovered, logged with its stack and recorded, so
// the subscriber survives. With the tracing hook, use its MsgHandler around
// Handler instead.
func (n *Instrumentation) MsgHandler(h Handler) nats.MsgHandler {
	return func(msg *nats.Msg) {
		ctx := context.Background()
		start := time.Now()
		var err error
		defer n.finishProcess(ctx, msg.Subject, start, &err, false)
		err = h(ctx, msg)
	}
}

// JetStreamMsgHandler is MsgHandler for consumer.Consume.
func (n *Instrumentation) JetStreamMsgHandler(h JetStreamHandler) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		ctx := context.Background()
		start := time.Now()
		var err error
		defer n.finishProcess(ctx, msg.Subject(), start, &err, false)
		err = h(ctx, msg)
	}
}

// finishProcess runs deferred: it records the sample and decides what a
// panic does. repanic keeps the panic for an outer guard; otherwise it is
// logged here and swallowed.
func (n *Instrumentation) finishProcess(ctx context.Context, subject string, start time.Time, err *error, repanic bool) {
	p := recover()
	et := metrics.ErrorType(*err)
	if p != nil {
		et = "panic"
	}
	n.record(ctx, n.consumed, "process", subject, start, et)
	if p == nil {
		return
	}
	if repanic {
		panic(p)
	}
	n.cfg.Logger.ErrorContext(ctx, "panic recovered in nats handler", "subject", subject, "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
}

func (n *Instrumentation) record(ctx context.Context, counter metric.Int64Counter, op, subject string, start time.Time, errorType string) {
	attrs := []attribute.KeyValue{AttrSystem.String("nats"), AttrDestination.String(n.cfg.DestinationName(subject)), AttrOperation.String(op)}
	if op == "process" && n.cfg.Consumer != "" {
		attrs = append(attrs, AttrConsumer.String(n.cfg.Consumer))
	}
	if errorType != "" {
		attrs = append(attrs, AttrErrorType.String(errorType))
	}
	set := metric.WithAttributeSet(attribute.NewSet(attrs...))
	counter.Add(ctx, 1, set)
	n.duration.Record(ctx, time.Since(start).Seconds(), set)
}

// RegisterConnStats exports the connection's counters on meter (nil: the
// global meter): messaging.nats.messages{direction}, messaging.nats.bytes
// {direction}, messaging.nats.reconnects and messaging.nats.connected (0/1).
// Unregister on shutdown.
func RegisterConnStats(meter metric.Meter, nc *nats.Conn) (metric.Registration, error) {
	if nc == nil {
		return nil, fmt.Errorf("metricsnats: nil connection")
	}
	if meter == nil {
		meter = otel.Meter(InstrumentationName)
	}
	msgs, err := meter.Int64ObservableCounter("messaging.nats.messages", metric.WithDescription("Messages received (in) and sent (out) on the connection."), metric.WithUnit("{message}"))
	if err != nil {
		return nil, err
	}
	bytes, err := meter.Int64ObservableCounter("messaging.nats.bytes", metric.WithDescription("Payload bytes received (in) and sent (out)."), metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	reconnects, err := meter.Int64ObservableCounter("messaging.nats.reconnects", metric.WithDescription("Reconnects since start."), metric.WithUnit("{reconnect}"))
	if err != nil {
		return nil, err
	}
	connected, err := meter.Int64ObservableGauge("messaging.nats.connected", metric.WithDescription("1 while the connection is established."), metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	client := attribute.String("messaging.client.id", nc.Opts.Name)
	in := metric.WithAttributes(client, attribute.String("direction", "in"))
	out := metric.WithAttributes(client, attribute.String("direction", "out"))
	only := metric.WithAttributes(client)
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := nc.Stats()
		o.ObserveInt64(msgs, clamp(s.InMsgs), in)
		o.ObserveInt64(msgs, clamp(s.OutMsgs), out)
		o.ObserveInt64(bytes, clamp(s.InBytes), in)
		o.ObserveInt64(bytes, clamp(s.OutBytes), out)
		o.ObserveInt64(reconnects, clamp(s.Reconnects), only)
		var up int64
		if nc.Status() == nats.CONNECTED {
			up = 1
		}
		o.ObserveInt64(connected, up, only)
		return nil
	}, msgs, bytes, reconnects, connected)
}

func clamp(v uint64) int64 {
	if v > 1<<62 {
		return 1 << 62
	}
	return int64(v)
}
