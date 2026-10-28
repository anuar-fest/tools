package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"slices"
	"sync"
	"syscall"
	"time"
)

// Component is one thing the process runs. At least one of Run and Stop
// must be set.
type Component struct {
	// Name appears in logs and errors.
	Name string
	// Run blocks until ctx is canceled or the component fails. Returning nil
	// before shutdown marks a one-shot component as finished; returning an
	// error starts the shutdown of everything else. Optional.
	Run func(ctx context.Context) error
	// Stop stops intake and drains in-flight work under ctx. It is called
	// before Run's context is canceled. Optional.
	Stop func(ctx context.Context) error
	// StopTimeout caps this component's stop; default: what is left of the
	// shutdown budget.
	StopTimeout time.Duration
}

// Config configures New. The zero value listens for SIGINT and SIGTERM
// with a 25 s shutdown budget.
type Config struct {
	// Logger logs component starts, stops and the shutdown reason. nil discards.
	Logger *slog.Logger
	// ShutdownTimeout is the whole budget for stopping; default 25s. Keep it
	// below terminationGracePeriodSeconds minus the preStop hook.
	ShutdownTimeout time.Duration
	// PreStopDelay waits after marking unready before stopping intake, so
	// endpoint changes propagate to the load balancer. Default 0; 3 to 5 s
	// on Kubernetes without a preStop sleep.
	PreStopDelay time.Duration
	// Signals that start the shutdown; default SIGINT and SIGTERM.
	Signals []os.Signal
	// NoSignals disables signal handling (tests, embedding).
	NoSignals bool
	// OnShutdown runs synchronously when the shutdown starts, before
	// anything stops: mark the readiness probe draining here.
	OnShutdown func(reason string)
}

// App owns the components.
type App struct {
	cfg    Config
	logger *slog.Logger

	mu         sync.Mutex
	components []Component
	running    bool
	reason     string

	shutdown     chan struct{}
	shutdownOnce sync.Once
}

// New builds an App.
func New(cfg Config) *App {
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 25 * time.Second
	}
	if len(cfg.Signals) == 0 {
		cfg.Signals = []os.Signal{os.Interrupt, syscall.SIGTERM}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &App{cfg: cfg, logger: cfg.Logger, shutdown: make(chan struct{})}
}

// Add registers a component. Components stop in reverse order of Add.
// It panics on an invalid component or after Run started: both are wiring
// bugs, not runtime conditions.
func (a *App) Add(c Component) {
	if c.Name == "" || (c.Run == nil && c.Stop == nil) {
		panic("lifecycle: component needs a name and Run or Stop")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running {
		panic("lifecycle: Add after Run")
	}
	a.components = append(a.components, c)
}

// Shutdown starts the shutdown from code, for example when a fatal error
// is detected outside any component. Idempotent.
func (a *App) Shutdown(reason string) {
	a.shutdownOnce.Do(func() {
		a.mu.Lock()
		a.reason = reason
		a.mu.Unlock()
		close(a.shutdown)
	})
}

// ComponentError says which component failed and in which phase.
type ComponentError struct {
	Name  string
	Phase string // "run" or "stop"
	Err   error
}

func (e *ComponentError) Error() string {
	return fmt.Sprintf("lifecycle: %s %s: %v", e.Name, e.Phase, e.Err)
}

func (e *ComponentError) Unwrap() error { return e.Err }

// ErrStopTimeout is wrapped when a component did not stop within budget.
var ErrStopTimeout = errors.New("did not stop within budget")

type exit struct {
	idx int
	err error
}

// Run starts every component's Run, waits for a signal, the parent context,
// Shutdown or a failing component, then stops everything in reverse order.
// It returns nil after a clean shutdown; otherwise the joined errors.
func (a *App) Run(ctx context.Context) error {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return errors.New("lifecycle: Run called twice")
	}
	a.running = true
	comps := slices.Clone(a.components)
	a.mu.Unlock()

	sigc := make(chan os.Signal, 2)
	if !a.cfg.NoSignals {
		signal.Notify(sigc, a.cfg.Signals...)
		defer signal.Stop(sigc)
	}

	base := context.WithoutCancel(ctx)
	exits := make(chan exit, len(comps))
	cancels := make([]context.CancelFunc, len(comps))
	done := make([]chan struct{}, len(comps))
	for i, c := range comps {
		if c.Run == nil {
			continue
		}
		cctx, cancel := context.WithCancel(base)
		cancels[i] = cancel
		d := make(chan struct{})
		done[i] = d
		go func() {
			defer close(d)
			exits <- exit{idx: i, err: guard(c.Name, "run", func() error { return c.Run(cctx) })}
		}()
		a.logger.Info("component started", "component", c.Name)
	}

	var cause error
	exited := make([]bool, len(comps))
	reason := a.await(ctx, sigc, exits, comps, exited, &cause)

	a.logger.Info("shutting down", "reason", reason, "budget", a.cfg.ShutdownTimeout)
	if a.cfg.OnShutdown != nil {
		a.cfg.OnShutdown(reason)
	}
	budget, cancelBudget := context.WithTimeoutCause(base, a.cfg.ShutdownTimeout, ErrStopTimeout)
	defer cancelBudget()
	go func() {
		// A second signal means "now": give up on the budget.
		select {
		case <-sigc:
			a.logger.Warn("second signal, abandoning graceful shutdown")
			cancelBudget()
		case <-budget.Done():
		}
	}()
	if a.cfg.PreStopDelay > 0 {
		t := time.NewTimer(a.cfg.PreStopDelay)
		select {
		case <-t.C:
		case <-budget.Done():
			t.Stop()
		}
	}

	var errs []error
	if cause != nil {
		errs = append(errs, cause)
	}
	for i := len(comps) - 1; i >= 0; i-- {
		errs = append(errs, a.stop(budget, comps[i], cancels[i], done[i], exited[i])...)
	}
	// Run errors reported while stopping, other than the cancellation we asked for.
	for {
		select {
		case e := <-exits:
			if e.err != nil && !errors.Is(e.err, context.Canceled) {
				errs = append(errs, &ComponentError{Name: comps[e.idx].Name, Phase: "run", Err: e.err})
			}
			continue
		default:
		}
		break
	}
	return errors.Join(errs...)
}

// await blocks until something ends the run phase and returns the reason.
func (a *App) await(ctx context.Context, sigc <-chan os.Signal, exits <-chan exit, comps []Component, exited []bool, cause *error) string {
	for {
		select {
		case sig := <-sigc:
			return "signal " + sig.String()
		case <-ctx.Done():
			return "context done: " + context.Cause(ctx).Error()
		case <-a.shutdown:
			a.mu.Lock()
			reason := a.reason
			a.mu.Unlock()
			return "requested: " + reason
		case e := <-exits:
			exited[e.idx] = true
			name := comps[e.idx].Name
			if e.err != nil {
				*cause = &ComponentError{Name: name, Phase: "run", Err: e.err}
				return "component failed: " + name
			}
			a.logger.Info("component finished", "component", name)
		}
	}
}

func (a *App) stop(budget context.Context, c Component, cancel context.CancelFunc, done chan struct{}, exited bool) []error {
	var errs []error
	stopCtx, cancelStop := budget, context.CancelFunc(func() {})
	if c.StopTimeout > 0 {
		stopCtx, cancelStop = context.WithTimeoutCause(budget, c.StopTimeout, ErrStopTimeout)
	}
	defer cancelStop()
	start := time.Now()
	if c.Stop != nil {
		if err := guard(c.Name, "stop", func() error { return c.Stop(stopCtx) }); err != nil {
			errs = append(errs, &ComponentError{Name: c.Name, Phase: "stop", Err: err})
		}
	}
	if cancel != nil {
		cancel()
	}
	if done != nil && !exited {
		select {
		case <-done:
		case <-stopCtx.Done():
			errs = append(errs, &ComponentError{Name: c.Name, Phase: "stop", Err: context.Cause(stopCtx)})
		}
	}
	if len(errs) == 0 {
		a.logger.Info("component stopped", "component", c.Name, "took", time.Since(start))
	} else {
		a.logger.Error("component stop failed", "component", c.Name, "took", time.Since(start), "err", errors.Join(errs...))
	}
	return errs
}

// guard runs f and turns a panic into an error so one component cannot take
// the process down without the others stopping.
func guard(name, phase string, f func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic in %s %s: %v\n%s", name, phase, p, debug.Stack())
		}
	}()
	return f()
}

// ExitCode maps Run's result to a process exit code: 0 for a clean
// shutdown, 1 otherwise, so the supervisor sees a half shutdown.
func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	return 1
}
