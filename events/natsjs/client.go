// Package natsjs is the NATS JetStream adapter for package events: a
// connection with sane reconnect and drain behaviour, stream declaration
// that never shrinks subjects, a publisher that acknowledges durably and
// deduplicates by event id, and a consumer with bounded workers, explicit
// acks after the handler returns, heartbeats for long handlers, jittered
// redelivery backoff and dead-lettering.
package natsjs

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Config configures the connection. Credentials go in their own fields,
// never in the URL, so no log line ever contains them.
type Config struct {
	// URL is one or more comma-separated nats:// URLs.
	URL             string
	User            string
	Password        string
	Token           string
	CredentialsFile string
	TLS             *tls.Config
	// Name identifies this connection on the server (the service name).
	Name string
	// ConnectTimeout bounds one connect attempt (default 5s). Connect keeps
	// trying with backoff until ctx ends.
	ConnectTimeout time.Duration
	// ReconnectWait between reconnect attempts after a drop (default 2s).
	ReconnectWait time.Duration
	// MaxReconnects after a drop; 0 means unlimited.
	MaxReconnects int
	// DrainTimeout bounds Close (default 10s).
	DrainTimeout time.Duration
	Logger       *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 5 * time.Second
	}
	if c.ReconnectWait <= 0 {
		c.ReconnectWait = 2 * time.Second
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 10 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return c
}

// Client owns one NATS connection and its JetStream context.
type Client struct {
	cfg    Config
	nc     *nats.Conn
	js     jetstream.JetStream
	logger *slog.Logger

	closedOnce sync.Once
	closed     chan struct{}
}

// Connect dials with retries until ctx ends. A service that cannot reach
// NATS at boot keeps trying for as long as its startup budget allows, then
// fails with the last error.
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.URL == "" {
		return nil, errors.New("natsjs: URL is required")
	}
	cfg = cfg.withDefaults()
	c := &Client{cfg: cfg, logger: cfg.Logger, closed: make(chan struct{})}

	maxReconnects := cfg.MaxReconnects
	if maxReconnects <= 0 {
		maxReconnects = -1
	}
	opts := []nats.Option{
		nats.Name(cfg.Name),
		nats.Timeout(cfg.ConnectTimeout),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.MaxReconnects(maxReconnects),
		nats.DrainTimeout(cfg.DrainTimeout),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			c.logger.Warn("nats disconnected", "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.logger.Info("nats reconnected", "url", nc.ConnectedUrlRedacted())
		}),
		nats.ClosedHandler(func(*nats.Conn) {
			c.closedOnce.Do(func() { close(c.closed) })
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			subject := ""
			if sub != nil {
				subject = sub.Subject
			}
			c.logger.Error("nats async error", "err", err, "subject", subject)
		}),
	}
	switch {
	case cfg.CredentialsFile != "":
		opts = append(opts, nats.UserCredentials(cfg.CredentialsFile))
	case cfg.Token != "":
		opts = append(opts, nats.Token(cfg.Token))
	case cfg.User != "":
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	}
	if cfg.TLS != nil {
		opts = append(opts, nats.Secure(cfg.TLS))
	}

	wait := 250 * time.Millisecond
	for {
		nc, err := nats.Connect(cfg.URL, opts...)
		if err == nil {
			c.nc = nc
			break
		}
		c.logger.Warn("nats connect failed, retrying", "err", err, "in", wait)
		t := time.NewTimer(wait + time.Duration(rand.Int64N(int64(wait/2))))
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, fmt.Errorf("natsjs: connect: %w (last error: %w)", ctx.Err(), err)
		case <-t.C:
		}
		wait = min(wait*2, 5*time.Second)
	}

	js, err := jetstream.New(c.nc)
	if err != nil {
		c.nc.Close()
		return nil, fmt.Errorf("natsjs: jetstream: %w", err)
	}
	c.js = js
	c.logger.Info("nats connected", "url", c.nc.ConnectedUrlRedacted(), "name", cfg.Name)
	return c, nil
}

// Close drains the connection: subscriptions stop, in-flight messages are
// delivered, pending publishes flush, then the socket closes. ctx bounds the
// wait; when it ends the connection is closed hard.
func (c *Client) Close(ctx context.Context) error {
	if c.nc.IsClosed() {
		return nil
	}
	if err := c.nc.Drain(); err != nil {
		c.nc.Close()
		return fmt.Errorf("natsjs: drain: %w", err)
	}
	select {
	case <-c.closed:
		return nil
	case <-ctx.Done():
		c.nc.Close()
		return fmt.Errorf("natsjs: close: %w", ctx.Err())
	}
}

// Healthy reports whether the connection is currently established. Wire it
// into the readiness probe.
func (c *Client) Healthy() bool { return c.nc != nil && c.nc.Status() == nats.CONNECTED }

// JetStream exposes the underlying context for operations this package
// does not wrap (key-value, object store, management).
func (c *Client) JetStream() jetstream.JetStream { return c.js }

// Conn exposes the underlying connection.
func (c *Client) Conn() *nats.Conn { return c.nc }
