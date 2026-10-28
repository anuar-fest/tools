package alarms

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/anuar-fest/tools/events"
	"github.com/anuar-fest/tools/health"
	"github.com/anuar-fest/tools/resilience"
)

// Alarm types raised by the hooks in this file.
const (
	TypeResilience = "resilience"
	TypeEvents     = "events"
	TypeHealth     = "health"
)

var labelNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// builtin are the definitions behind the hooks; New adds them so Rules
// always renders them.
var builtin = []Definition{
	{Type: TypeResilience, Name: "breaker_opened", Severity: Warning, Kind: Event, Summary: "a circuit breaker opened"},
	{Type: TypeResilience, Name: "breaker_open", Severity: Warning, Kind: State, For: 2 * time.Minute, Summary: "a circuit breaker is open"},
	{Type: TypeEvents, Name: "dead_letter", Severity: Critical, Kind: Event, Summary: "a message was dead-lettered"},
	{Type: TypeEvents, Name: "handler_panic", Severity: Critical, Kind: Event, Summary: "an event handler panicked"},
	{Type: TypeEvents, Name: "undecodable", Severity: Warning, Kind: Event, Summary: "an undecodable message was received"},
	{Type: TypeHealth, Name: "readiness_lost", Severity: Critical, Kind: State, For: time.Minute, Summary: "readiness lost"},
}

// BuiltinDefinitions returns the definitions behind the hooks, for a
// service that renders rules without constructing Alarms.
func BuiltinDefinitions() []Definition {
	return append([]Definition(nil), builtin...)
}

// ResilienceObserver wraps inner (may be nil): every opening raises the
// resilience/breaker_opened event, and resilience/breaker_open is set
// while at least one breaker is open and cleared when the last one
// closes. Decisions pass through untouched; a rejected call is normal
// operation, an open breaker is a dependency incident.
func (a *Alarms) ResilienceObserver(inner resilience.Observer) resilience.Observer {
	return &resilienceObserver{a: a, inner: resilience.OrNop(inner), open: map[string]struct{}{}}
}

type resilienceObserver struct {
	a     *Alarms
	inner resilience.Observer
	open  map[string]struct{} // guarded by a.mu through the methods below
}

func (o *resilienceObserver) ObserveDecision(ctx context.Context, e resilience.Event) {
	o.inner.ObserveDecision(ctx, e)
}

func (o *resilienceObserver) ObserveBreakerState(ctx context.Context, c resilience.StateChange) {
	o.inner.ObserveBreakerState(ctx, c)
	dep := slog.String("dependency", c.Dependency)
	switch {
	case c.To == resilience.BreakerOpen:
		o.a.Raise(ctx, Alarm{Type: TypeResilience, Name: "breaker_opened", Detail: fmt.Sprintf("circuit breaker for %s opened", c.Dependency), Attrs: []slog.Attr{dep}})
		if o.track(c.Dependency, true) == 1 {
			o.a.Set(ctx, Alarm{Type: TypeResilience, Name: "breaker_open", Detail: fmt.Sprintf("circuit breaker for %s is open", c.Dependency), Attrs: []slog.Attr{dep}})
		}
	case c.From != resilience.BreakerClosed && c.To == resilience.BreakerClosed:
		if o.track(c.Dependency, false) == 0 {
			o.a.Clear(ctx, Alarm{Type: TypeResilience, Name: "breaker_open", Detail: fmt.Sprintf("circuit breaker for %s closed; none open", c.Dependency), Attrs: []slog.Attr{dep}})
		}
	}
}

// track records whether dep is open and returns how many are.
func (o *resilienceObserver) track(dep string, open bool) int {
	o.a.mu.Lock()
	defer o.a.mu.Unlock()
	if open {
		o.open[dep] = struct{}{}
	} else {
		delete(o.open, dep)
	}
	return len(o.open)
}

// EventsObserver wraps inner (may be nil) and raises events/dead_letter
// (Critical: a message left the main flow), events/handler_panic (Critical)
// and events/undecodable (Warning). Other outcomes pass through.
func (a *Alarms) EventsObserver(inner events.Observer) events.Observer {
	return &eventsObserver{a: a, inner: events.OrNop(inner)}
}

type eventsObserver struct {
	a     *Alarms
	inner events.Observer
}

func (o *eventsObserver) Observe(ctx context.Context, s events.Stat) {
	o.inner.Observe(ctx, s)
	attrs := []slog.Attr{slog.String("consumer", s.Consumer), slog.String("event_type", s.Type), slog.Int("attempt", s.Attempt)}
	switch s.Outcome {
	case events.OutcomeDeadLettered:
		o.a.Raise(ctx, Alarm{Type: TypeEvents, Name: "dead_letter", Err: s.Err,
			Detail: fmt.Sprintf("%s dead-lettered a %s message after %d attempts", s.Consumer, s.Type, s.Attempt), Attrs: attrs})
	case events.OutcomePanic:
		o.a.Raise(ctx, Alarm{Type: TypeEvents, Name: "handler_panic", Err: s.Err,
			Detail: fmt.Sprintf("%s panicked handling %s", s.Consumer, s.Type), Attrs: attrs})
	case events.OutcomeUndecodable:
		o.a.Raise(ctx, Alarm{Type: TypeEvents, Name: "undecodable", Err: s.Err,
			Detail: fmt.Sprintf("%s received an undecodable %s message", s.Consumer, s.Type), Attrs: attrs})
	}
}

// ReadinessChanged is a health.Config.OnReadinessChange hook: readiness
// lost sets health/readiness_lost with the failed checks; restored clears
// it.
func (a *Alarms) ReadinessChanged(from, to health.Report) {
	ctx := context.Background()
	if to.OK() {
		a.Clear(ctx, Alarm{Type: TypeHealth, Name: "readiness_lost", Detail: "readiness restored"})
		return
	}
	failed := make([]string, 0, len(to.Checks))
	for name, r := range to.Checks {
		if r.Status != health.StatusOK {
			failed = append(failed, name+": "+r.Error)
		}
	}
	sort.Strings(failed)
	detail := "readiness lost"
	if len(failed) > 0 {
		detail += ": " + strings.Join(failed, "; ")
	} else if to.Status != "" {
		detail += ": " + string(to.Status)
	}
	a.Set(ctx, Alarm{Type: TypeHealth, Name: "readiness_lost", Detail: detail,
		Attrs: []slog.Attr{slog.String("from", string(from.Status)), slog.String("to", string(to.Status))}})
}
