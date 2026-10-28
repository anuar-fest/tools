// Package workpool runs jobs on a fixed number of goroutines fed by a bounded
// queue. A full queue is reported, never grown: the caller decides whether to
// block, drop or shed. Panics in jobs are recovered and counted. Close stops
// intake, drains or cancels, and joins every worker.
package workpool

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anuar-fest/tools/resilience"
)

// Job is a unit of work. It must return when ctx is done.
type Job func(ctx context.Context)

// Config configures a Pool.
type Config struct {
	// Name is the pool label on metrics.
	Name string
	// Workers is the number of goroutines (default 4).
	Workers int
	// QueueSize bounds pending jobs (default 2 × Workers). Size it for a burst,
	// not for sustained overload: a long queue only delays the failure.
	QueueSize int
	Observer  resilience.Observer
	// Logger receives recovered panics with their stack. nil discards.
	Logger *slog.Logger
}

// Pool is a bounded worker pool. Safe for concurrent use.
type Pool struct {
	cfg    Config
	queue  chan Job
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu        sync.RWMutex
	closed    bool
	closing   chan struct{}
	closeOnce sync.Once
	panics    atomic.Int64
}

// New starts the workers.
func New(cfg Config) (*Pool, error) {
	if cfg.Workers < 0 || cfg.QueueSize < 0 {
		return nil, fmt.Errorf("workpool %q: Workers and QueueSize must be >= 0", cfg.Name)
	}
	if cfg.Workers == 0 {
		cfg.Workers = 4
	}
	if cfg.QueueSize == 0 {
		cfg.QueueSize = 2 * cfg.Workers
	}
	cfg.Observer = resilience.OrNop(cfg.Observer)
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		cfg:     cfg,
		queue:   make(chan Job, cfg.QueueSize),
		ctx:     ctx,
		cancel:  cancel,
		closing: make(chan struct{}),
	}
	p.wg.Add(cfg.Workers)
	for range cfg.Workers {
		go p.worker()
	}
	return p, nil
}

// Submit enqueues job, waiting for queue space until ctx is done or the pool
// closes. It returns ErrPoolClosed after Close.
func (p *Pool) Submit(ctx context.Context, job Job) error {
	if job == nil {
		return fmt.Errorf("workpool %q: nil job", p.cfg.Name)
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return resilience.ErrPoolClosed
	}
	select {
	case p.queue <- job:
		return nil
	default:
	}
	select {
	case p.queue <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closing:
		return resilience.ErrPoolClosed
	}
}

// TrySubmit enqueues job without waiting. It returns ErrPoolFull when the
// queue has no room, so the caller can shed or dead-letter.
func (p *Pool) TrySubmit(job Job) error {
	if job == nil {
		return fmt.Errorf("workpool %q: nil job", p.cfg.Name)
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return resilience.ErrPoolClosed
	}
	select {
	case p.queue <- job:
		return nil
	default:
		p.cfg.Observer.ObserveDecision(context.Background(), resilience.Event{
			Dependency: p.cfg.Name,
			Policy:     resilience.PolicyWorkpool,
			Decision:   resilience.DecisionPoolFull,
			Err:        resilience.ErrPoolFull,
		})
		return resilience.ErrPoolFull
	}
}

// Close stops intake, lets queued jobs drain, and joins the workers. If ctx
// ends first, running jobs are cancelled through their context and Close
// still joins them before returning ctx.Err(). Close is idempotent.
func (p *Pool) Close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		close(p.closing) // wake blocked Submit calls before taking the write lock
		p.mu.Lock()
		p.closed = true
		close(p.queue)
		p.mu.Unlock()
	})

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		p.cancel()
		return nil
	case <-ctx.Done():
		p.cancel()
		<-done
		return ctx.Err()
	}
}

// Queued is the number of jobs waiting.
func (p *Pool) Queued() int { return len(p.queue) }

// Workers is the goroutine count.
func (p *Pool) Workers() int { return p.cfg.Workers }

// Capacity is the queue size.
func (p *Pool) Capacity() int { return p.cfg.QueueSize }

// Panics is how many jobs panicked since start.
func (p *Pool) Panics() int { return int(p.panics.Load()) }

// Name returns the pool label.
func (p *Pool) Name() string { return p.cfg.Name }

func (p *Pool) worker() {
	defer p.wg.Done()
	for job := range p.queue {
		p.run(job)
	}
}

func (p *Pool) run(job Job) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			p.panics.Add(1)
			err := fmt.Errorf("workpool %q: job panicked: %v", p.cfg.Name, r)
			p.cfg.Logger.Error("job panicked", "pool", p.cfg.Name, "panic", r, "stack", string(debug.Stack()))
			p.cfg.Observer.ObserveDecision(p.ctx, resilience.Event{
				Dependency: p.cfg.Name,
				Policy:     resilience.PolicyWorkpool,
				Decision:   resilience.DecisionPanic,
				Elapsed:    time.Since(start),
				Err:        err,
			})
		}
	}()
	job(p.ctx)
	p.cfg.Observer.ObserveDecision(p.ctx, resilience.Event{
		Dependency: p.cfg.Name,
		Policy:     resilience.PolicyWorkpool,
		Decision:   resilience.DecisionSuccess,
		Elapsed:    time.Since(start),
	})
}
