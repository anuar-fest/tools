// Package alarms raises application alarms the Prometheus way: a condition
// the code itself recognised as wrong (a breaker opened, a message was
// dead-lettered, readiness was lost, a tenant went over quota) becomes a
// metric that Prometheus alerting rules fire on, a structured log line with
// the trace id, and an event on the active span. Routing, grouping,
// deduplication and silences are Alertmanager's job; this package ships
// the rules (Rules) instead of notifying anyone itself.
//
// Two kinds: an Event is something that happened and is counted
// (alarms_total); a State is a condition that holds until cleared and is
// a 0/1 gauge (alarms_active). Type and Name are labels: closed sets of
// constants declared in Definitions, never dynamic values. Detail is free
// text and goes to logs and spans only.
package alarms

import (
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"sync"
	"time"

	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// InstrumentationName is the meter name.
const InstrumentationName = "github.com/anuar-fest/tools/alarms"

// Severity is the closed set of alarm levels; it becomes the severity
// label Alertmanager routes on.
type Severity string

const (
	// Critical pages someone: data is being lost or the service is down.
	Critical Severity = "critical"
	// Warning is looked at during working hours.
	Warning Severity = "warning"
	// Info records a notable state change, such as a recovery.
	Info Severity = "info"
)

// Kind says how an alarm is measured and therefore how its rule is written.
type Kind string

const (
	// Event alarms count occurrences: alarms_total; the rule fires on
	// increase() over a window.
	Event Kind = "event"
	// State alarms hold until cleared: alarms_active is 1 while set; the
	// rule fires when the gauge has been 1 for For.
	State Kind = "state"
)

// Definition declares one alarm upfront so Rules can render its alerting
// rule and New can refuse a bad label at boot.
type Definition struct {
	// Type groups alarms by subsystem: "resilience", "events", "health",
	// or a business area such as "billing".
	Type string
	// Name is the condition within the type: "breaker_open",
	// "dead_letter", "quota_exceeded".
	Name string
	// Severity defaults to Warning.
	Severity Severity
	// Kind defaults to Event.
	Kind Kind
	// Summary is the alert's summary annotation; default "type/name".
	Summary string
	// For is how long a State must hold before its alert fires; default 0.
	// Ignored for Event alarms.
	For time.Duration
	// Window is the increase() lookback for an Event alarm; default
	// RulesConfig.Window (5m). Ignored for State alarms.
	Window time.Duration
}

func (d Definition) key() string { return d.Type + "/" + d.Name }

func (d Definition) withDefaults() Definition {
	if d.Severity == "" {
		d.Severity = Warning
	}
	if d.Kind == "" {
		d.Kind = Event
	}
	if d.Summary == "" {
		d.Summary = d.Type + "/" + d.Name
	}
	return d
}

func (d Definition) validate() error {
	if !labelRe.MatchString(d.Type) {
		return fmt.Errorf("alarms: type %q must match %s", d.Type, labelRe)
	}
	if !labelRe.MatchString(d.Name) {
		return fmt.Errorf("alarms: name %q must match %s", d.Name, labelRe)
	}
	switch d.Severity {
	case Critical, Warning, Info:
	default:
		return fmt.Errorf("alarms: %s: severity %q is not critical, warning or info", d.key(), d.Severity)
	}
	switch d.Kind {
	case Event, State:
	default:
		return fmt.Errorf("alarms: %s: kind %q is not event or state", d.key(), d.Kind)
	}
	if d.For < 0 || d.Window < 0 {
		return fmt.Errorf("alarms: %s: negative duration", d.key())
	}
	return nil
}

// Alarm is one occurrence (Raise) or one condition (Set, Clear).
type Alarm struct {
	// Type and Name identify the alarm; see Definition.
	Type string
	Name string
	// Severity defaults to the definition's, then Warning.
	Severity Severity
	// Detail is the human message for this occurrence. Free text.
	Detail string
	// Err is attached to the log line when set.
	Err error
	// Attrs are extra structured fields for the log line.
	Attrs []slog.Attr
}

// Config configures New.
type Config struct {
	// Meter defaults to the global meter named InstrumentationName.
	Meter metric.Meter
	// Logger writes one line per alarm and per cleared state. nil discards.
	Logger *slog.Logger
	// Service is added to every log line.
	Service string
	// Definitions are the alarms this service raises; the built-in hooks'
	// are added by New. They are validated at boot and rendered by Rules.
	// Raising an undeclared alarm still works: it is counted and logged,
	// but has no rule.
	Definitions []Definition
	// MaxNames caps distinct (type, name) pairs seen at runtime; beyond it
	// names become "_other" so a bug cannot explode the label set. Default 200.
	MaxNames int
}

// Alarms is the facility. It owns no goroutine and needs no Close.
type Alarms struct {
	cfg    Config
	count  metric.Int64Counter
	active metric.Int64Gauge
	defs   map[string]Definition

	mu     sync.Mutex
	names  map[string]struct{} // distinct type/name seen
	states map[string]bool     // type/name → currently set
}

var labelRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// New validates the definitions and creates the instruments.
func New(cfg Config) (*Alarms, error) {
	if cfg.Meter == nil {
		cfg.Meter = otel.GetMeterProvider().Meter(InstrumentationName)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.MaxNames <= 0 {
		cfg.MaxNames = 200
	}
	defs := make(map[string]Definition, len(cfg.Definitions)+len(builtin))
	for _, d := range append(append([]Definition(nil), builtin...), cfg.Definitions...) {
		d = d.withDefaults()
		if err := d.validate(); err != nil {
			return nil, err
		}
		if _, dup := defs[d.key()]; dup {
			return nil, fmt.Errorf("alarms: %s defined twice", d.key())
		}
		defs[d.key()] = d
	}
	a := &Alarms{cfg: cfg, defs: defs, names: map[string]struct{}{}, states: map[string]bool{}}
	var err error
	a.count, err = cfg.Meter.Int64Counter("alarms", metric.WithDescription("Alarms raised by type, name and severity."), metric.WithUnit("{alarm}"))
	if err != nil {
		return nil, err
	}
	a.active, err = cfg.Meter.Int64Gauge("alarms.active", metric.WithDescription("1 while a state alarm is set, 0 once cleared."), metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	return a, nil
}

// Definitions returns every declared alarm, built-in hooks included,
// sorted by type and name. Rules renders them.
func (a *Alarms) Definitions() []Definition {
	out := make([]Definition, 0, len(a.defs))
	for _, d := range a.defs {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

// Rules renders the Prometheus alerting rules for Definitions; see the
// package-level Rules for the config.
func (a *Alarms) Rules(cfg RulesConfig) ([]byte, error) {
	if cfg.Service == "" {
		cfg.Service = a.cfg.Service
	}
	cfg.Definitions = a.Definitions()
	return Rules(cfg)
}

// Raise records one occurrence: the counter, a log line at the severity's
// level, and an event on the active span. It never blocks and never
// fails: raising an alarm must not be able to fail the caller.
func (a *Alarms) Raise(ctx context.Context, al Alarm) {
	al = a.sanitize(al)
	a.count.Add(ctx, 1, metric.WithAttributeSet(attrs(al)))
	a.emit(ctx, al, al.Detail)
}

// RaiseError is Raise with Detail from err.
func (a *Alarms) RaiseError(ctx context.Context, typ, name string, severity Severity, err error) {
	a.Raise(ctx, Alarm{Type: typ, Name: name, Severity: severity, Detail: err.Error(), Err: err})
}

// Set marks a state alarm active: on the inactive → active transition it
// counts, logs, adds the span event and records alarms_active = 1. While
// already active it does nothing, so a flapping check does not spam.
func (a *Alarms) Set(ctx context.Context, al Alarm) {
	al = a.sanitize(al)
	key := al.Type + "/" + al.Name
	a.mu.Lock()
	was := a.states[key]
	a.states[key] = true
	a.mu.Unlock()
	if was {
		return
	}
	set := metric.WithAttributeSet(attrs(al))
	a.count.Add(ctx, 1, set)
	a.active.Record(ctx, 1, set)
	a.emit(ctx, al, al.Detail)
}

// Clear ends a state alarm: alarms_active = 0 and an Info log line. It
// does nothing for a state that is not set.
func (a *Alarms) Clear(ctx context.Context, al Alarm) {
	al = a.sanitize(al)
	key := al.Type + "/" + al.Name
	a.mu.Lock()
	was := a.states[key]
	delete(a.states, key)
	a.mu.Unlock()
	if !was {
		return
	}
	a.active.Record(ctx, 0, metric.WithAttributeSet(attrs(al)))
	detail := al.Detail
	if detail == "" {
		detail = "alarm cleared"
	}
	al.Severity = Info
	a.emit(ctx, al, detail)
}

// Active reports whether the state alarm is set.
func (a *Alarms) Active(typ, name string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.states[typ+"/"+name]
}

func attrs(al Alarm) attribute.Set {
	return attribute.NewSet(attribute.String("alarm.type", al.Type), attribute.String("alarm.name", al.Name), attribute.String("alarm.severity", string(al.Severity)))
}

func (a *Alarms) emit(ctx context.Context, al Alarm, msg string) {
	logAttrs := make([]slog.Attr, 0, 6+len(al.Attrs))
	logAttrs = append(logAttrs, slog.Bool("alarm", true), slog.String("alarm.type", al.Type), slog.String("alarm.name", al.Name), slog.String("alarm.severity", string(al.Severity)))
	if a.cfg.Service != "" {
		logAttrs = append(logAttrs, slog.String("service", a.cfg.Service))
	}
	if al.Err != nil {
		logAttrs = append(logAttrs, slog.Any("err", al.Err))
	}
	logAttrs = append(logAttrs, al.Attrs...)
	a.cfg.Logger.LogAttrs(ctx, level(al.Severity), msg, logAttrs...)
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.AddEvent("alarm", trace.WithAttributes(attribute.String("alarm.type", al.Type), attribute.String("alarm.name", al.Name), attribute.String("alarm.severity", string(al.Severity)), attribute.String("alarm.detail", msg)))
	}
}

// sanitize keeps the label set closed: invalid names become _invalid,
// names beyond MaxNames become _other, the severity falls back to the
// definition's, then Warning.
func (a *Alarms) sanitize(al Alarm) Alarm {
	if !labelRe.MatchString(al.Type) {
		al.Type = "_invalid"
	}
	if !labelRe.MatchString(al.Name) {
		al.Name = "_invalid"
	}
	key := al.Type + "/" + al.Name
	if al.Severity == "" {
		if d, ok := a.defs[key]; ok {
			al.Severity = d.Severity
		}
	}
	switch al.Severity {
	case Critical, Warning, Info:
	default:
		al.Severity = Warning
	}
	a.mu.Lock()
	if _, ok := a.names[key]; !ok {
		if len(a.names) >= a.cfg.MaxNames {
			al.Name = "_other"
		} else {
			a.names[key] = struct{}{}
		}
	}
	a.mu.Unlock()
	return al
}

func level(s Severity) slog.Level {
	switch s {
	case Critical:
		return slog.LevelError
	case Info:
		return slog.LevelInfo
	default:
		return slog.LevelWarn
	}
}
