// Package httpx applies the module's policies to HTTP: an outbound
// RoundTripper that composes deadline → bulkhead → limit → retry(breaker(
// attempt)) with HTTP-aware classification, and inbound middleware that
// sheds and rate-limits with the 503/429 + Retry-After contract.
//
// The RoundTripper owns its timeouts rather than delegating them to
// package stack, because an HTTP response outlives the attempt: the body is
// read after RoundTrip returns, so cancelling the attempt context on return
// would abort every body read. Here the attempt and call contexts are
// cancelled when the body is closed.
package httpx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/anuar-fest/tools/resilience"
	"github.com/anuar-fest/tools/resilience/breaker"
	"github.com/anuar-fest/tools/resilience/bulkhead"
	"github.com/anuar-fest/tools/resilience/deadline"
	"github.com/anuar-fest/tools/resilience/ratelimit"
	"github.com/anuar-fest/tools/resilience/retry"
	"github.com/anuar-fest/tools/resilience/stack"
)

// maxSavedBody bounds how much of an error response is kept so the last
// response can be handed back after retries are exhausted.
const maxSavedBody = 64 << 10

// TransportConfig configures the outbound RoundTripper for one dependency.
type TransportConfig struct {
	// Name is the dependency label.
	Name string
	// Base performs the actual request (default: NewBaseTransport with the
	// defaults and MaxConnsPerHost matched to Bulkhead).
	Base http.RoundTripper
	// CallTimeout bounds the whole call including retries and reading the
	// body (default 30s; 0 disables).
	CallTimeout time.Duration
	// AttemptTimeout bounds one attempt up to the response headers (default
	// 10s; 0 disables). Reading the body is bounded by CallTimeout only.
	AttemptTimeout time.Duration
	// Retry configures the attempts. AttemptTimeout on it must be 0; use the
	// field above. Zero value means the retry defaults.
	Retry    retry.Policy
	Bulkhead *bulkhead.Bulkhead
	Limiter  ratelimit.Limiter
	Breaker  *breaker.Breaker
	// RetryStatuses are retried when the request is repeatable (default
	// 502, 503, 504, 429). Every 5xx is still a breaker failure; 429 is not.
	RetryStatuses []int
	// AllowNonIdempotentRetry retries POST/PATCH without an Idempotency-Key.
	// Only set it when the server deduplicates on its own.
	AllowNonIdempotentRetry bool
	Observer                resilience.Observer
}

// Transport is the composed RoundTripper.
type Transport struct {
	cfg           TransportConfig
	retryStatuses map[int]bool
}

// NewTransport validates cfg and returns the RoundTripper.
func NewTransport(cfg TransportConfig) (*Transport, error) {
	if cfg.Name == "" {
		return nil, errors.New("httpx: Name is required")
	}
	if cfg.Retry.AttemptTimeout != 0 {
		return nil, fmt.Errorf("httpx %q: set TransportConfig.AttemptTimeout, not Retry.AttemptTimeout", cfg.Name)
	}
	if cfg.CallTimeout == 0 {
		cfg.CallTimeout = 30 * time.Second
	}
	if cfg.AttemptTimeout == 0 {
		cfg.AttemptTimeout = 10 * time.Second
	}
	if cfg.CallTimeout < 0 || cfg.AttemptTimeout < 0 {
		return nil, fmt.Errorf("httpx %q: timeouts must be >= 0", cfg.Name)
	}
	if cfg.Base == nil {
		base := BaseTransportConfig{}
		if cfg.Bulkhead != nil {
			base.MaxConnsPerHost = cfg.Bulkhead.Capacity()
		}
		cfg.Base = NewBaseTransport(base)
	}
	if len(cfg.RetryStatuses) == 0 {
		cfg.RetryStatuses = []int{http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusTooManyRequests}
	}
	cfg.Observer = resilience.OrNop(cfg.Observer)
	if cfg.Retry.Dependency == "" {
		cfg.Retry.Dependency = cfg.Name
	}
	if cfg.Retry.Observer == nil {
		cfg.Retry.Observer = cfg.Observer
	}
	t := &Transport{cfg: cfg, retryStatuses: make(map[int]bool, len(cfg.RetryStatuses))}
	for _, s := range cfg.RetryStatuses {
		t.retryStatuses[s] = true
	}
	return t, nil
}

// StatusError is a 5xx or 429 response seen by the policies. It carries up
// to 64 KiB of the body so the final response can be reconstructed.
type StatusError struct {
	StatusCode int
	Header     http.Header
	Body       []byte
	RetryAfter time.Duration
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("httpx: upstream returned %d %s", e.StatusCode, http.StatusText(e.StatusCode))
}

// RoundTrip implements http.RoundTripper. A 5xx/429 that is not retried, or
// is still failing after the last attempt, is returned as a response, as the
// RoundTripper contract requires; policy rejections are returned as errors.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	ctx, callCancel := deadline.Child(req.Context(), t.cfg.CallTimeout, 0)

	fail := func(err error) (*http.Response, error) {
		callCancel()
		t.observe(ctx, err, start)
		return nil, err
	}

	if t.cfg.Bulkhead != nil {
		release, err := t.cfg.Bulkhead.Acquire(ctx)
		if err != nil {
			return fail(err)
		}
		defer release()
	}
	if t.cfg.Limiter != nil && !t.cfg.Limiter.Allow() {
		return fail(resilience.ErrRateLimited)
	}

	repeatable := t.repeatable(req)
	resp, err := retry.DoValue(ctx, t.cfg.Retry, func(ctx context.Context) (*http.Response, error) {
		return t.attempt(ctx, req, repeatable)
	})
	if err != nil {
		var se *StatusError
		if errors.As(err, &se) {
			callCancel()
			t.observe(ctx, err, start)
			return se.response(req), nil
		}
		return fail(err)
	}

	resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: callCancel}
	t.observe(ctx, nil, start)
	return resp, nil
}

func (t *Transport) attempt(ctx context.Context, req *http.Request, repeatable bool) (*http.Response, error) {
	var permit *breaker.Permit
	if t.cfg.Breaker != nil {
		var err error
		permit, err = t.cfg.Breaker.Allow(ctx)
		if err != nil {
			return nil, err
		}
	}
	record := func(err error) {
		if permit != nil {
			permit.Record(err)
		}
	}

	// The attempt timeout is a cancel, not a deadline: a deadline would keep
	// ticking and abort the body read after the headers arrived in time.
	actx, cancelCause := context.WithCancelCause(ctx)
	acancel := func() { cancelCause(nil) }
	if t.cfg.AttemptTimeout > 0 {
		timer := time.AfterFunc(t.cfg.AttemptTimeout, func() { cancelCause(resilience.ErrAttemptTimeout) })
		defer timer.Stop()
	}
	r, err := clone(actx, req)
	if err != nil {
		acancel()
		record(breaker.Ignore(err))
		return nil, retry.Permanent(err)
	}

	resp, err := t.cfg.Base.RoundTrip(r)
	if err != nil {
		acancel()
		err = retry.MarkAttemptTimeout(actx, err)
		record(err)
		if !repeatable {
			err = retry.Permanent(err)
		}
		return nil, err
	}

	sc := resp.StatusCode
	if sc >= 500 || sc == http.StatusTooManyRequests {
		se := &StatusError{StatusCode: sc, Header: resp.Header.Clone(), RetryAfter: parseRetryAfter(resp.Header, time.Now())}
		se.Body, _ = io.ReadAll(io.LimitReader(resp.Body, maxSavedBody)) // best effort: the response is an error either way
		_ = resp.Body.Close()
		acancel()

		var err error = se
		if sc == http.StatusTooManyRequests {
			err = breaker.Ignore(err) // our quota, not their health
		}
		record(err)
		if repeatable && t.retryStatuses[sc] {
			return nil, retry.WithRetryAfter(err, se.RetryAfter)
		}
		return nil, retry.Permanent(err)
	}

	record(nil)
	resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: acancel}
	return resp, nil
}

// repeatable reports whether the request may be sent more than once: the
// method is idempotent (or carries an Idempotency-Key, or the config allows
// it) and the body can be replayed.
func (t *Transport) repeatable(req *http.Request) bool {
	if req.Body != nil && req.Body != http.NoBody && req.GetBody == nil {
		return false
	}
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace, http.MethodPut, http.MethodDelete:
		return true
	}
	return t.cfg.AllowNonIdempotentRetry || req.Header.Get("Idempotency-Key") != ""
}

func clone(ctx context.Context, req *http.Request) (*http.Request, error) {
	r := req.Clone(ctx)
	if req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return nil, fmt.Errorf("httpx: replay body: %w", err)
		}
		r.Body = body
	}
	return r, nil
}

func (t *Transport) observe(ctx context.Context, err error, start time.Time) {
	var se *StatusError
	decision := stack.Outcome(err)
	if errors.As(err, &se) {
		decision = resilience.DecisionFailure
	}
	t.cfg.Observer.ObserveDecision(ctx, resilience.Event{
		Dependency: t.cfg.Name,
		Policy:     resilience.PolicyStack,
		Decision:   decision,
		Elapsed:    time.Since(start),
		Err:        err,
	})
}

func (e *StatusError) response(req *http.Request) *http.Response {
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", e.StatusCode, http.StatusText(e.StatusCode)),
		StatusCode:    e.StatusCode,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        e.Header,
		Body:          io.NopCloser(bytes.NewReader(e.Body)),
		ContentLength: int64(len(e.Body)),
		Request:       req,
	}
}

// parseRetryAfter reads a Retry-After header in seconds or HTTP-date form.
func parseRetryAfter(h http.Header, now time.Time) time.Duration {
	v := h.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if at, err := http.ParseTime(v); err == nil {
		if d := at.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

// cancelOnClose cancels a context when the body is closed or fully read.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
	once   sync.Once
}

func (c *cancelOnClose) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	if err != nil {
		c.once.Do(c.cancel)
	}
	return n, err
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.once.Do(c.cancel)
	return err
}

// BaseTransportConfig sizes a per-dependency http.Transport. Zero fields take
// the documented defaults, which are tighter than net/http's.
type BaseTransportConfig struct {
	// MaxConnsPerHost caps connections to one host (default 64). Match the
	// bulkhead for the same dependency.
	MaxConnsPerHost int
	// MaxIdleConnsPerHost keeps warm connections (default = MaxConnsPerHost).
	// net/http's default of 2 churns connections under any real load.
	MaxIdleConnsPerHost   int
	DialTimeout           time.Duration // default 3s
	TLSHandshakeTimeout   time.Duration // default 3s
	ResponseHeaderTimeout time.Duration // default 0: AttemptTimeout bounds it
	IdleConnTimeout       time.Duration // default 90s
	// ForceHTTP2 enables HTTP/2 where the server offers it (default true).
	DisableHTTP2 bool
}

// NewBaseTransport builds an http.Transport for one dependency. Never share
// one Transport between dependencies: a slow host must not exhaust the
// connection pool of a healthy one.
func NewBaseTransport(cfg BaseTransportConfig) *http.Transport {
	if cfg.MaxConnsPerHost <= 0 {
		cfg.MaxConnsPerHost = 64
	}
	if cfg.MaxIdleConnsPerHost <= 0 {
		cfg.MaxIdleConnsPerHost = cfg.MaxConnsPerHost
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 3 * time.Second
	}
	if cfg.TLSHandshakeTimeout <= 0 {
		cfg.TLSHandshakeTimeout = 3 * time.Second
	}
	if cfg.IdleConnTimeout <= 0 {
		cfg.IdleConnTimeout = 90 * time.Second
	}
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.MaxConnsPerHost = cfg.MaxConnsPerHost
	base.MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
	base.MaxIdleConns = 0
	base.TLSHandshakeTimeout = cfg.TLSHandshakeTimeout
	base.ResponseHeaderTimeout = cfg.ResponseHeaderTimeout
	base.IdleConnTimeout = cfg.IdleConnTimeout
	base.ForceAttemptHTTP2 = !cfg.DisableHTTP2
	dialer := &netDialer{timeout: cfg.DialTimeout}
	base.DialContext = dialer.DialContext
	return base
}
