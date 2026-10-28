package natsjs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/anuar-fest/tools/events"
)

// Dead-letter headers added to a message when it is moved to the DLQ.
const (
	HeaderDLQReason     = "x-dlq-reason"
	HeaderDLQError      = "x-dlq-error"
	HeaderDLQConsumer   = "x-dlq-consumer"
	HeaderDLQStream     = "x-dlq-stream"
	HeaderDLQSubject    = "x-dlq-subject"
	HeaderDLQDeliveries = "x-dlq-deliveries"
)

// ConsumerConfig configures a durable pull consumer with bounded workers.
type ConsumerConfig struct {
	// Stream to consume from. Required.
	Stream string
	// Name is the durable consumer name; one per logical subscriber. Required.
	Name string
	// FilterSubjects narrows the stream (default: everything in it).
	FilterSubjects []string
	// Handler processes each delivery. Required. Recover is always applied.
	Handler     events.Handler
	Middlewares []events.Middleware
	// Workers is the number of handlers running at once (default 4).
	Workers int
	// MaxAckPending caps unacknowledged deliveries (default 2 × Workers).
	MaxAckPending int
	// AckWait is how long a handler may run before the server redelivers;
	// heartbeats extend it (default 30s).
	AckWait time.Duration
	// HeartbeatInterval sends InProgress while the handler runs (default AckWait/3).
	HeartbeatInterval time.Duration
	// MaxDeliver is the total number of deliveries before dead-lettering (default 5).
	MaxDeliver int
	// Backoff is the redelivery delay per attempt, jittered ±20 %; the last
	// value repeats (default 1s, 5s, 15s, 30s). A RetryAfter error overrides it.
	Backoff []time.Duration
	// DeadLetterSubject receives messages that are dropped, undecodable or
	// past MaxDeliver, with x-dlq-* headers. Empty means terminate and log.
	DeadLetterSubject string
	// DeliverPolicy for a new consumer (default: all messages in the stream).
	DeliverPolicy jetstream.DeliverPolicy
	Observer      events.Observer
	Logger        *slog.Logger
}

func (c ConsumerConfig) withDefaults() (ConsumerConfig, error) {
	if c.Stream == "" || c.Name == "" {
		return c, errors.New("natsjs: consumer Stream and Name are required")
	}
	if c.Handler == nil {
		return c, fmt.Errorf("natsjs: consumer %q: Handler is required", c.Name)
	}
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.MaxAckPending <= 0 {
		c.MaxAckPending = 2 * c.Workers
	}
	if c.AckWait <= 0 {
		c.AckWait = 30 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = c.AckWait / 3
	}
	if c.MaxDeliver <= 0 {
		c.MaxDeliver = 5
	}
	if len(c.Backoff) == 0 {
		c.Backoff = []time.Duration{time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second}
	}
	c.Observer = events.OrNop(c.Observer)
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return c, nil
}

// Consumer is a running subscription.
type Consumer struct {
	cfg     ConsumerConfig
	js      jetstream.JetStream
	handler events.Handler
	cc      jetstream.ConsumeContext

	ctx    context.Context
	cancel context.CancelFunc
	sem    chan struct{}
	wg     sync.WaitGroup
}

// Consume creates or updates the durable consumer and starts delivering.
// Stop it with Consumer.Stop before closing the client.
func (c *Client) Consume(ctx context.Context, cfg ConsumerConfig) (*Consumer, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	jsCfg := jetstream.ConsumerConfig{
		Durable:        cfg.Name,
		Name:           cfg.Name,
		FilterSubjects: cfg.FilterSubjects,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        cfg.AckWait,
		MaxDeliver:     cfg.MaxDeliver,
		MaxAckPending:  cfg.MaxAckPending,
		DeliverPolicy:  cfg.DeliverPolicy,
	}
	cons, err := c.js.CreateOrUpdateConsumer(ctx, cfg.Stream, jsCfg)
	if err != nil {
		return nil, fmt.Errorf("natsjs: consumer %q on %q: %w", cfg.Name, cfg.Stream, err)
	}

	hctx, cancel := context.WithCancel(context.Background())
	k := &Consumer{
		cfg:     cfg,
		js:      c.js,
		handler: events.Recover(cfg.Logger)(events.Chain(cfg.Handler, cfg.Middlewares...)),
		ctx:     hctx,
		cancel:  cancel,
		sem:     make(chan struct{}, cfg.Workers),
	}
	cc, err := cons.Consume(k.dispatch, jetstream.PullMaxMessages(cfg.Workers))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("natsjs: consume %q: %w", cfg.Name, err)
	}
	k.cc = cc
	cfg.Logger.Info("consumer started", "consumer", cfg.Name, "stream", cfg.Stream, "subjects", cfg.FilterSubjects, "workers", cfg.Workers)
	return k, nil
}

// dispatch runs on the subscription's goroutine; blocking here is the
// backpressure that stops the client fetching more than Workers can handle.
func (k *Consumer) dispatch(msg jetstream.Msg) {
	select {
	case k.sem <- struct{}{}:
	case <-k.ctx.Done():
		return // stopping: leave the message unacked for redelivery
	}
	k.wg.Add(1)
	go func() {
		defer k.wg.Done()
		defer func() { <-k.sem }()
		k.handle(msg)
	}()
}

// Stop stops fetching, lets running handlers finish, and returns. If ctx
// ends first the handlers' context is cancelled and Stop still joins them.
func (k *Consumer) Stop(ctx context.Context) error {
	k.cc.Drain()
	done := make(chan struct{})
	go func() {
		k.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		k.cancel()
		return nil
	case <-ctx.Done():
		k.cancel()
		<-done
		return fmt.Errorf("natsjs: stop consumer %q: %w", k.cfg.Name, ctx.Err())
	}
}

func (k *Consumer) handle(msg jetstream.Msg) {
	attempt := 1
	stream := k.cfg.Stream
	if meta, err := msg.Metadata(); err == nil {
		attempt = int(min(meta.NumDelivered, 1<<31-1)) // NumDelivered is tiny; the clamp only satisfies gosec
		stream = meta.Stream
	}

	hdr := make(map[string]string, len(msg.Headers()))
	for key, vals := range msg.Headers() {
		if len(vals) > 0 {
			hdr[key] = vals[0]
		}
	}
	env, err := events.FromHeaders(hdr, msg.Data())
	if err != nil {
		k.observe(k.ctx, msg.Subject(), events.OutcomeUndecodable, attempt, 0, err)
		k.deadLetter(msg, env, attempt, stream, "undecodable", err)
		return
	}

	ctx := events.WithDelivery(k.ctx, events.Delivery{Attempt: attempt, Consumer: k.cfg.Name, Stream: stream})
	stopHeartbeat := k.heartbeat(msg)
	start := time.Now()
	err = k.handler(ctx, env)
	elapsed := time.Since(start)
	stopHeartbeat()

	switch {
	case err == nil:
		if ackErr := msg.Ack(); ackErr != nil {
			k.cfg.Logger.Warn("ack failed; message will be redelivered", "consumer", k.cfg.Name, "id", env.ID, "err", ackErr)
		}
		k.observe(ctx, env.Type, events.OutcomeAcked, attempt, elapsed, nil)
	case events.IsDrop(err):
		k.observe(ctx, env.Type, events.OutcomeDropped, attempt, elapsed, err)
		k.deadLetter(msg, env, attempt, stream, "dropped", err)
	case attempt >= k.cfg.MaxDeliver:
		k.observe(ctx, env.Type, events.OutcomeNacked, attempt, elapsed, err)
		k.deadLetter(msg, env, attempt, stream, "max_deliver", err)
	default:
		delay, ok := events.RetryDelay(err)
		if !ok {
			delay = k.backoff(attempt)
		}
		if nakErr := msg.NakWithDelay(delay); nakErr != nil {
			k.cfg.Logger.Warn("nak failed", "consumer", k.cfg.Name, "id", env.ID, "err", nakErr)
		}
		k.observe(ctx, env.Type, events.OutcomeNacked, attempt, elapsed, err)
	}
}

// heartbeat tells the server the handler is alive so AckWait does not expire
// under a long but healthy handler. Returns a stop function.
func (k *Consumer) heartbeat(msg jetstream.Msg) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(k.cfg.HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if err := msg.InProgress(); err != nil {
					k.cfg.Logger.Debug("in-progress heartbeat failed", "consumer", k.cfg.Name, "err", err)
				}
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func (k *Consumer) backoff(attempt int) time.Duration {
	i := min(max(attempt-1, 0), len(k.cfg.Backoff)-1)
	d := k.cfg.Backoff[i]
	jitter := time.Duration(float64(d) * 0.2 * (2*rand.Float64() - 1))
	return max(d+jitter, 0)
}

// deadLetter copies the message to the DLQ subject with reason headers and
// terminates the original. If the copy cannot be published, the original is
// nak'd instead so nothing is lost.
func (k *Consumer) deadLetter(msg jetstream.Msg, env events.Envelope, attempt int, stream, reason string, cause error) {
	log := k.cfg.Logger.With("consumer", k.cfg.Name, "id", env.ID, "type", env.Type, "reason", reason, "deliveries", attempt, "err", cause)
	if k.cfg.DeadLetterSubject == "" {
		log.Error("message terminated without dead-letter subject")
		if err := msg.Term(); err != nil {
			log.Warn("term failed", "term_err", err)
		}
		return
	}

	dl := nats.NewMsg(k.cfg.DeadLetterSubject)
	dl.Data = msg.Data()
	for key, vals := range msg.Headers() {
		if key == nats.MsgIdHdr {
			continue
		}
		dl.Header[key] = vals
	}
	dl.Header.Set(HeaderDLQReason, reason)
	if cause != nil {
		dl.Header.Set(HeaderDLQError, cause.Error())
	}
	dl.Header.Set(HeaderDLQConsumer, k.cfg.Name)
	dl.Header.Set(HeaderDLQStream, stream)
	dl.Header.Set(HeaderDLQSubject, msg.Subject())
	dl.Header.Set(HeaderDLQDeliveries, strconv.Itoa(attempt))
	if env.ID != "" {
		dl.Header.Set(nats.MsgIdHdr, env.ID+".dlq."+k.cfg.Name)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := k.js.PublishMsg(ctx, dl); err != nil {
		log.Error("dead-letter publish failed; message will be redelivered", "dlq_err", err)
		if nakErr := msg.NakWithDelay(k.backoff(attempt)); nakErr != nil {
			log.Warn("nak failed", "nak_err", nakErr)
		}
		return
	}
	if err := msg.Term(); err != nil {
		log.Warn("term after dead-letter failed", "term_err", err)
	}
	log.Warn("message dead-lettered")
	k.observe(ctx, env.Type, events.OutcomeDeadLettered, attempt, 0, cause)
}

func (k *Consumer) observe(ctx context.Context, typ string, outcome events.Outcome, attempt int, elapsed time.Duration, err error) {
	k.cfg.Observer.Observe(ctx, events.Stat{Consumer: k.cfg.Name, Type: typ, Outcome: outcome, Attempt: attempt, Elapsed: elapsed, Err: err})
}

var _ events.Consumer = (*Consumer)(nil)
