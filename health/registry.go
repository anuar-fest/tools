package health

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Check reports whether one thing is healthy. It must honour ctx: a check
// that ignores the deadline is reported as failed but keeps running.
type Check func(ctx context.Context) error

// Status is the closed set of outcomes.
type Status string

const (
	StatusOK       Status = "ok"
	StatusFail     Status = "fail"
	StatusStarting Status = "starting"
	StatusDraining Status = "draining"
)

// Config configures New. The zero value is usable.
type Config struct {
	// Timeout bounds every check of one evaluation; default 2s. Keep it
	// below the probe's timeoutSeconds.
	Timeout time.Duration
	// CacheTTL reuses a result for this long; default 1s, negative disables.
	CacheTTL time.Duration
	// StartupGate makes readiness fail until MarkStarted is called: the
	// service decides when it has loaded what it needs.
	StartupGate bool
	// Info is echoed in every report: name, version, build.
	Info map[string]string
	// Logger logs readiness transitions. nil discards.
	Logger *slog.Logger
	// OnReadinessChange is called when readiness flips, after the log line,
	// with the previous and the new report. Hook alarms here. It runs on
	// the probe's goroutine: keep it quick.
	OnReadinessChange func(from, to Report)
}

// Registry holds the checks and answers probes.
type Registry struct {
	cfg    Config
	logger *slog.Logger

	mu    sync.Mutex
	live  []namedCheck
	ready []namedCheck

	started  atomic.Bool
	draining atomic.Bool

	liveCache  cache
	readyCache cache
}

type namedCheck struct {
	name  string
	check Check
}

// Report is the body of a probe response.
type Report struct {
	Status Status                 `json:"status"`
	Checks map[string]CheckResult `json:"checks,omitempty"`
	Info   map[string]string      `json:"info,omitempty"`
}

// OK reports whether every check passed and no gate is closed.
func (r Report) OK() bool { return r.Status == StatusOK }

// CheckResult is one check's outcome.
type CheckResult struct {
	Status     Status  `json:"status"`
	Error      string  `json:"error,omitempty"`
	DurationMS float64 `json:"duration_ms"`
}

// New builds an empty registry.
func New(cfg Config) *Registry {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Registry{cfg: cfg, logger: cfg.Logger}
}

// AddLiveness registers a check that, when failing, means the process must
// be restarted. Never put a dependency here: a database outage would
// restart every pod. Panics on an empty or duplicate name (a wiring bug).
func (g *Registry) AddLiveness(name string, c Check) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.live = add(g.live, name, c)
}

// AddReadiness registers a check that, when failing, means the process must
// not receive traffic right now.
func (g *Registry) AddReadiness(name string, c Check) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ready = add(g.ready, name, c)
}

func add(list []namedCheck, name string, c Check) []namedCheck {
	if name == "" || c == nil {
		panic("health: check needs a name and a function")
	}
	for _, n := range list {
		if n.name == name {
			panic("health: duplicate check " + name)
		}
	}
	return append(list, namedCheck{name: name, check: c})
}

// MarkStarted opens the startup gate.
func (g *Registry) MarkStarted() { g.started.Store(true) }

// SetDraining makes readiness fail (true) so traffic stops arriving before
// the listener closes. Call it first thing on shutdown.
func (g *Registry) SetDraining(d bool) { g.draining.Store(d) }

// Draining reports the drain flag.
func (g *Registry) Draining() bool { return g.draining.Load() }

// Live evaluates the liveness checks.
func (g *Registry) Live(ctx context.Context) Report {
	g.mu.Lock()
	checks := g.live
	g.mu.Unlock()
	return g.liveCache.get(g.cfg.CacheTTL, func() Report { return g.run(ctx, checks) }, nil)
}

// Ready evaluates the readiness checks, unless the process is draining or
// has not started: those answer at once without running anything.
func (g *Registry) Ready(ctx context.Context) Report {
	if g.draining.Load() {
		return Report{Status: StatusDraining, Info: g.cfg.Info}
	}
	if g.cfg.StartupGate && !g.started.Load() {
		return Report{Status: StatusStarting, Info: g.cfg.Info}
	}
	g.mu.Lock()
	checks := g.ready
	g.mu.Unlock()
	return g.readyCache.get(g.cfg.CacheTTL, func() Report { return g.run(ctx, checks) }, g.logTransition)
}

func (g *Registry) logTransition(from, to Report) {
	if from.Status == "" || from.Status == to.Status {
		return
	}
	failed := make([]string, 0, len(to.Checks))
	for name, r := range to.Checks {
		if r.Status != StatusOK {
			failed = append(failed, name+": "+r.Error)
		}
	}
	sort.Strings(failed)
	if to.OK() {
		g.logger.Info("readiness restored")
	} else {
		g.logger.Warn("readiness lost", "failed", strings.Join(failed, "; "))
	}
	if g.cfg.OnReadinessChange != nil {
		g.cfg.OnReadinessChange(from, to)
	}
}

type result struct {
	name string
	res  CheckResult
}

// run evaluates checks concurrently under one deadline. A straggler that
// ignores ctx is reported as timed out and left to finish on its own.
func (g *Registry) run(ctx context.Context, checks []namedCheck) Report {
	rep := Report{Status: StatusOK, Checks: make(map[string]CheckResult, len(checks)), Info: g.cfg.Info}
	if len(checks) == 0 {
		return rep
	}
	ctx, cancel := context.WithTimeout(ctx, g.cfg.Timeout)
	defer cancel()
	chans := make([]chan result, len(checks))
	for i, c := range checks {
		ch := make(chan result, 1)
		chans[i] = ch
		go func() { ch <- runOne(ctx, c) }()
	}
	for i, c := range checks {
		rep.Checks[c.name] = g.collect(ctx, chans[i])
		if rep.Checks[c.name].Status != StatusOK {
			rep.Status = StatusFail
		}
	}
	return rep
}

// collect prefers a finished result over the shared deadline: once one
// check timed out, ctx is done for all of them, and select would otherwise
// pick the deadline at random over results that are already there.
func (g *Registry) collect(ctx context.Context, ch <-chan result) CheckResult {
	select {
	case r := <-ch:
		return r.res
	default:
	}
	select {
	case r := <-ch:
		return r.res
	case <-ctx.Done():
		return CheckResult{Status: StatusFail, Error: "timeout after " + g.cfg.Timeout.String(), DurationMS: ms(g.cfg.Timeout)}
	}
}

func runOne(ctx context.Context, c namedCheck) (r result) {
	start := time.Now()
	r.name = c.name
	defer func() {
		if p := recover(); p != nil {
			r.res = CheckResult{Status: StatusFail, Error: fmt.Sprintf("panic: %v", p), DurationMS: ms(time.Since(start))}
		}
	}()
	err := c.check(ctx)
	r.res = CheckResult{Status: StatusOK, DurationMS: ms(time.Since(start))}
	if err != nil {
		r.res.Status = StatusFail
		r.res.Error = err.Error()
	}
	return r
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// cache is a single-flight result cache: the mutex is held across the
// evaluation on purpose, so N probers arriving together cost one run and
// all read the same answer.
type cache struct {
	mu     sync.Mutex
	at     time.Time
	report Report
}

func (c *cache) get(ttl time.Duration, run func() Report, onChange func(from, to Report)) Report {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ttl > 0 && !c.at.IsZero() && time.Since(c.at) < ttl {
		return c.report
	}
	rep := run()
	if onChange != nil {
		onChange(c.report, rep)
	}
	c.at, c.report = time.Now(), rep
	return rep
}

// LiveHandler serves the liveness probe: 200 or 503 with a JSON Report.
func (g *Registry) LiveHandler() http.Handler { return handler(g.Live) }

// ReadyHandler serves the readiness probe.
func (g *Registry) ReadyHandler() http.Handler { return handler(g.Ready) }

func handler(eval func(context.Context) Report) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rep := eval(r.Context())
		h := w.Header()
		h.Set("Content-Type", "application/json")
		h.Set("Cache-Control", "no-store")
		code := http.StatusOK
		if !rep.OK() {
			code = http.StatusServiceUnavailable
		}
		w.WriteHeader(code)
		// The prober has the status code; a failed body write changes nothing.
		_ = json.NewEncoder(w).Encode(rep)
	})
}
