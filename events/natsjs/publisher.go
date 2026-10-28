package natsjs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/anuar-fest/tools/events"
)

// PublisherConfig configures a Publisher.
type PublisherConfig struct {
	// Timeout bounds one publish including the server's ack (default 5s).
	// Keep it short on request paths; for guaranteed delivery use the outbox.
	Timeout time.Duration
	// SubjectFor maps an envelope to a subject (default: the event Type).
	SubjectFor func(events.Envelope) string
	Observer   events.Observer
}

// Publisher publishes envelopes to JetStream with the event id as the
// broker-side dedup key.
type Publisher struct {
	js  jetstream.JetStream
	cfg PublisherConfig
}

// NewPublisher builds a Publisher.
func (c *Client) NewPublisher(cfg PublisherConfig) *Publisher {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.SubjectFor == nil {
		cfg.SubjectFor = func(e events.Envelope) string { return e.Type }
	}
	cfg.Observer = events.OrNop(cfg.Observer)
	return &Publisher{js: c.js, cfg: cfg}
}

// Publish implements events.Publisher. It returns once the stream has
// stored the message. ErrNoStream means no stream covers the subject.
func (p *Publisher) Publish(ctx context.Context, e events.Envelope) error {
	if err := e.Validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()

	msg := nats.NewMsg(p.cfg.SubjectFor(e))
	msg.Data = e.Data
	for k, v := range events.ToHeaders(e) {
		msg.Header.Set(k, v)
	}
	msg.Header.Set(nats.MsgIdHdr, e.ID)

	start := time.Now()
	_, err := p.js.PublishMsg(ctx, msg)
	stat := events.Stat{Consumer: e.Source, Type: e.Type, Outcome: events.OutcomePublished, Elapsed: time.Since(start)}
	if err != nil {
		stat.Outcome, stat.Err = events.OutcomePublishFailed, err
		p.cfg.Observer.Observe(ctx, stat)
		if errors.Is(err, jetstream.ErrNoStreamResponse) {
			return fmt.Errorf("natsjs: publish %s: %w", e.Type, ErrNoStream)
		}
		return fmt.Errorf("natsjs: publish %s: %w", e.Type, err)
	}
	p.cfg.Observer.Observe(ctx, stat)
	return nil
}

// ErrNoStream is returned when no stream captures the subject: the message
// would be lost, so it is an error, not a silent success.
var ErrNoStream = errors.New("natsjs: no stream for subject")

var _ events.Publisher = (*Publisher)(nil)
