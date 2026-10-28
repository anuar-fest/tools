package servergrpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	"github.com/anuar-fest/tools/health"
)

// ServerConfig configures NewServer. Only Addr is required.
type ServerConfig struct {
	// Addr to listen on, e.g. ":9000". Use "127.0.0.1:0" in tests.
	Addr string
	// Interceptors instrument the server; nil means none.
	Interceptors *Interceptors
	// Health registers grpc.health.v1 backed by this registry.
	Health *health.Registry
	// Options are appended after the defaults, so they win.
	Options []grpc.ServerOption
	// MaxConnectionAge forces clients to reconnect, so a layer-4 balancer
	// can spread them after a scale-out. Default 5m, grace 30s.
	MaxConnectionAge      time.Duration
	MaxConnectionAgeGrace time.Duration
	// KeepaliveTime pings an idle client; default 2m. KeepaliveTimeout waits
	// for the ack; default 20s.
	KeepaliveTime    time.Duration
	KeepaliveTimeout time.Duration
	// MinClientPingInterval rejects clients that ping more often; default 1m.
	MinClientPingInterval time.Duration
	// MaxRecvMsgBytes caps one message; default 4 MiB.
	MaxRecvMsgBytes int
	// MaxConcurrentStreams per connection; default 1024.
	MaxConcurrentStreams uint32
	// ShutdownTimeout bounds GracefulStop before Stop; default 15s.
	ShutdownTimeout time.Duration
	// Logger receives serve errors. nil discards.
	Logger *slog.Logger
}

func (c ServerConfig) withDefaults() ServerConfig {
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&c.MaxConnectionAge, 5*time.Minute)
	def(&c.MaxConnectionAgeGrace, 30*time.Second)
	def(&c.KeepaliveTime, 2*time.Minute)
	def(&c.KeepaliveTimeout, 20*time.Second)
	def(&c.MinClientPingInterval, time.Minute)
	def(&c.ShutdownTimeout, 15*time.Second)
	if c.MaxRecvMsgBytes <= 0 {
		c.MaxRecvMsgBytes = 4 << 20
	}
	if c.MaxConcurrentStreams == 0 {
		c.MaxConcurrentStreams = 1024
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return c
}

// ErrClosed is returned by Listen and Serve after Shutdown.
var ErrClosed = errors.New("servergrpc: server closed")

// Server owns one grpc.Server and its listener, with the same Listen /
// Serve / Shutdown / Run shape as httpserver.Server so lifecycle treats
// them alike.
type Server struct {
	cfg ServerConfig
	srv *grpc.Server

	mu      sync.Mutex
	ln      net.Listener
	serving bool
	closed  bool
}

// NewServer builds the server. Register services on GRPC() before Serve.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Addr == "" {
		return nil, errors.New("servergrpc: Addr is required")
	}
	cfg = cfg.withDefaults()
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge: cfg.MaxConnectionAge, MaxConnectionAgeGrace: cfg.MaxConnectionAgeGrace,
			Time: cfg.KeepaliveTime, Timeout: cfg.KeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: cfg.MinClientPingInterval, PermitWithoutStream: true}),
		grpc.MaxRecvMsgSize(cfg.MaxRecvMsgBytes),
		grpc.MaxConcurrentStreams(cfg.MaxConcurrentStreams),
	}
	if cfg.Interceptors != nil {
		opts = append(opts, cfg.Interceptors.ServerOptions()...)
	}
	opts = append(opts, cfg.Options...)
	s := &Server{cfg: cfg, srv: grpc.NewServer(opts...)}
	if cfg.Health != nil {
		grpc_health_v1.RegisterHealthServer(s.srv, NewHealthServer(cfg.Health, 0))
	}
	return s, nil
}

// GRPC exposes the server for service registration.
func (s *Server) GRPC() *grpc.Server { return s.srv }

// Listen binds the address so a busy port fails the boot. Idempotent.
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
		return fmt.Errorf("servergrpc: listen %s: %w", s.cfg.Addr, err)
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

// Serve blocks until Shutdown or a serve error; a clean stop returns nil.
func (s *Server) Serve() error {
	if err := s.Listen(context.Background()); err != nil { //nolint:contextcheck // Serve blocks for the server's life; a bind here has no caller deadline
		return err
	}
	s.mu.Lock()
	ln := s.ln
	s.serving = true
	s.mu.Unlock()
	if err := s.srv.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("servergrpc: serve %s: %w", s.cfg.Addr, err)
	}
	return nil
}

// Shutdown stops accepting and waits for in-flight RPCs until ctx ends (or
// ShutdownTimeout when ctx has no deadline), then stops hard.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	ln, serving := s.ln, s.serving
	s.mu.Unlock()
	if !serving {
		s.srv.Stop()
		if ln != nil {
			return ln.Close()
		}
		return nil
	}
	if _, has := ctx.Deadline(); !has {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.ShutdownTimeout)
		defer cancel()
	}
	done := make(chan struct{})
	go func() {
		s.srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.srv.Stop()
		<-done
		return fmt.Errorf("servergrpc: shutdown: %w", context.Cause(ctx))
	}
}

// Run serves until ctx is done, then shuts down and joins. It fits
// lifecycle.Component.Run.
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
	return errors.Join(s.Shutdown(shutdownCtx), <-errc)
}
