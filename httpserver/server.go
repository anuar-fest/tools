package httpserver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// Config configures New. Only Addr and Handler are required.
type Config struct {
	// Addr to listen on, e.g. ":8080". Use ":0" in tests and read Addr().
	Addr string
	// Handler is the router.
	Handler http.Handler
	// ReadHeaderTimeout bounds a slow-header client (slowloris). Default 10s.
	ReadHeaderTimeout time.Duration
	// ReadTimeout bounds the whole request read. Default 0: uploads may
	// stream for minutes; MaxBytes and Timeout bound them per route.
	ReadTimeout time.Duration
	// WriteTimeout bounds the whole response write. Default 0 for the same
	// reason: downloads and SSE stream. Set it on services that never stream.
	WriteTimeout time.Duration
	// IdleTimeout closes keep-alive connections nobody uses. Default 120s.
	IdleTimeout time.Duration
	// MaxHeaderBytes caps request headers. Default 1 MiB.
	MaxHeaderBytes int
	// ShutdownTimeout bounds Shutdown when its context has no deadline, and
	// Run's shutdown. Default 15s; keep it below the pod's grace period.
	ShutdownTimeout time.Duration
	// TLS enables HTTPS with the given config (certificates included).
	TLS *tls.Config
	// Logger receives net/http's own messages (bad requests, TLS handshake
	// failures) at Warn. nil discards.
	Logger *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.ReadHeaderTimeout <= 0 {
		c.ReadHeaderTimeout = 10 * time.Second
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 120 * time.Second
	}
	if c.MaxHeaderBytes <= 0 {
		c.MaxHeaderBytes = 1 << 20
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 15 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return c
}

// ErrClosed is returned by Listen and Serve after Shutdown.
var ErrClosed = errors.New("httpserver: server closed")

// Server owns one http.Server and its listener. Listen binds early so a
// port conflict fails at boot, Serve blocks, Shutdown drains in-flight
// requests under a budget and then closes what is left.
type Server struct {
	cfg Config
	srv *http.Server

	mu      sync.Mutex
	ln      net.Listener
	serving bool
	closed  bool
}

// New validates cfg and prepares the server. It does not listen.
func New(cfg Config) (*Server, error) {
	if cfg.Addr == "" {
		return nil, errors.New("httpserver: Addr is required")
	}
	if cfg.Handler == nil {
		return nil, errors.New("httpserver: Handler is required")
	}
	cfg = cfg.withDefaults()
	return &Server{
		cfg: cfg,
		srv: &http.Server{
			Addr:              cfg.Addr,
			Handler:           cfg.Handler,
			ReadHeaderTimeout: cfg.ReadHeaderTimeout,
			ReadTimeout:       cfg.ReadTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
			MaxHeaderBytes:    cfg.MaxHeaderBytes,
			TLSConfig:         cfg.TLS,
			ErrorLog:          slog.NewLogLogger(cfg.Logger.Handler(), slog.LevelWarn),
		},
	}, nil
}

// Listen binds the address; ctx bounds the bind (host resolution). Call it
// before marking the service ready so a port already in use fails the boot,
// not the first probe. Idempotent.
func (s *Server) Listen(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.ln != nil {
		return nil
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("httpserver: listen %s: %w", s.cfg.Addr, err)
	}
	s.ln = ln
	return nil
}

// Addr is the bound address after Listen, nil before.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Serve listens if needed and blocks until Shutdown or a serve error. A
// clean shutdown returns nil.
func (s *Server) Serve() error {
	if err := s.Listen(context.Background()); err != nil { //nolint:contextcheck // Serve blocks for the server's life; a bind here has no caller deadline
		return err
	}
	s.mu.Lock()
	ln := s.ln
	s.serving = true
	s.mu.Unlock()

	var err error
	if s.cfg.TLS != nil {
		err = s.srv.ServeTLS(ln, "", "")
	} else {
		err = s.srv.Serve(ln)
	}
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("httpserver: serve %s: %w", s.cfg.Addr, err)
}

// Shutdown stops accepting, waits for in-flight requests until ctx ends
// (or ShutdownTimeout when ctx has no deadline), then closes whatever is
// still open so the process can exit. Hijacked connections are not
// tracked by net/http: close them through RegisterOnShutdown.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	ln, serving := s.ln, s.serving
	s.mu.Unlock()

	if _, has := ctx.Deadline(); !has {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.ShutdownTimeout)
		defer cancel()
	}
	err := s.srv.Shutdown(ctx)
	if !serving && ln != nil {
		// Listen without Serve: net/http never saw the listener.
		err = errors.Join(err, ln.Close())
	}
	if err != nil {
		// Shutdown gave up (deadline): close the remaining connections hard.
		return fmt.Errorf("httpserver: shutdown: %w", errors.Join(err, s.srv.Close()))
	}
	return nil
}

// RegisterOnShutdown runs f when Shutdown starts; use it to close hijacked
// (WebSocket) connections, which Shutdown cannot see.
func (s *Server) RegisterOnShutdown(f func()) { s.srv.RegisterOnShutdown(f) }

// Run serves until ctx is done, then shuts down with ShutdownTimeout and
// joins the serve goroutine. It returns a serve error, a shutdown error, or
// nil. It fits lifecycle.Component.Run.
func (s *Server) Run(ctx context.Context) error {
	if err := s.Listen(ctx); err != nil {
		return err
	}
	errc := make(chan error, 1)
	go func() { errc <- s.Serve() }() //nolint:contextcheck // Listen(ctx) above already bound the port; Serve only accepts connections
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.ShutdownTimeout)
	defer cancel()
	err := s.Shutdown(shutdownCtx)
	return errors.Join(err, <-errc)
}
