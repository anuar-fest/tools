package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Definition describes one business metric. Name and Labels are validated
// at creation so a typo fails the boot, not the dashboard.
type Definition struct {
	// Name without namespace or type suffix: "usage_events", not
	// "customers_usage_events_total" (the exporter adds _total, _seconds).
	Name string
	// Help is required.
	Help string
	// Unit in UCUM: "{event}", "s", "By". Default "1".
	Unit string
	// Labels is the closed set of label names. Values are passed
	// positionally to Inc/Observe/Set.
	Labels []string
	// Buckets for histograms; default LatencyBuckets.
	Buckets []float64
	// MaxCardinality caps distinct label-value combinations; beyond it the
	// values are replaced by "_other" and metrics.cardinality_limited
	// counts the overflow. Default BuilderConfig.DefaultMaxCardinality.
	MaxCardinality int
}

// BuilderConfig configures NewBuilder.
type BuilderConfig struct {
	// Meter defaults to the global meter named after Namespace.
	Meter metric.Meter
	// Namespace prefixes every metric: the service name ("customers").
	Namespace string
	// Logger receives one line per instrument the first time it is limited
	// or misused. nil discards.
	Logger *slog.Logger
	// DefaultMaxCardinality per instrument; default 1000.
	DefaultMaxCardinality int
}

// Builder creates validated business metrics on the OpenTelemetry API.
type Builder struct {
	meter   metric.Meter
	ns      string
	logger  *slog.Logger
	limited metric.Int64Counter
	defMax  int
}

var (
	nameRe  = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	labelRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	// suffixes the Prometheus exporter adds itself; a name that ends with
	// one would come out doubled.
	reservedSuffixes = []string{"_total", "_seconds", "_bytes", "_count", "_sum", "_bucket", "_info"}
	reservedLabels   = map[string]struct{}{"le": {}, "quantile": {}, "job": {}, "instance": {}}
)

// NewBuilder validates the namespace and creates the overflow counter.
func NewBuilder(cfg BuilderConfig) (*Builder, error) {
	if !nameRe.MatchString(cfg.Namespace) {
		return nil, fmt.Errorf("metrics: namespace %q must match %s", cfg.Namespace, nameRe)
	}
	if cfg.Meter == nil {
		cfg.Meter = otel.GetMeterProvider().Meter(cfg.Namespace)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.DefaultMaxCardinality <= 0 {
		cfg.DefaultMaxCardinality = 1000
	}
	limited, err := cfg.Meter.Int64Counter("metrics.cardinality_limited",
		metric.WithDescription("Observations whose label values were replaced by _other because the instrument reached its cardinality cap."),
		metric.WithUnit("{observation}"))
	if err != nil {
		return nil, err
	}
	return &Builder{meter: cfg.Meter, ns: cfg.Namespace, logger: cfg.Logger, limited: limited, defMax: cfg.DefaultMaxCardinality}, nil
}

func (b *Builder) validate(def Definition) (string, *labels, error) {
	if !nameRe.MatchString(def.Name) {
		return "", nil, fmt.Errorf("metrics: name %q must match %s", def.Name, nameRe)
	}
	for _, s := range reservedSuffixes {
		if strings.HasSuffix(def.Name, s) {
			return "", nil, fmt.Errorf("metrics: name %q must not end with %s; the exporter adds type suffixes", def.Name, s)
		}
	}
	if strings.TrimSpace(def.Help) == "" {
		return "", nil, fmt.Errorf("metrics: %s: Help is required", def.Name)
	}
	keys := make([]attribute.Key, 0, len(def.Labels))
	seen := map[string]struct{}{}
	for _, l := range def.Labels {
		if !labelRe.MatchString(l) {
			return "", nil, fmt.Errorf("metrics: %s: label %q must match %s", def.Name, l, labelRe)
		}
		if _, reserved := reservedLabels[l]; reserved {
			return "", nil, fmt.Errorf("metrics: %s: label %q is reserved by Prometheus", def.Name, l)
		}
		if _, dup := seen[l]; dup {
			return "", nil, fmt.Errorf("metrics: %s: duplicate label %q", def.Name, l)
		}
		seen[l] = struct{}{}
		keys = append(keys, attribute.Key(l))
	}
	full := b.ns + "." + def.Name
	maxCard := def.MaxCardinality
	if maxCard <= 0 {
		maxCard = b.defMax
	}
	return full, &labels{keys: keys, max: maxCard, seen: map[string]struct{}{}, name: full, b: b}, nil
}

// Counter creates a monotonic counter.
func (b *Builder) Counter(def Definition) (*Counter, error) {
	name, l, err := b.validate(def)
	if err != nil {
		return nil, err
	}
	c, err := b.meter.Int64Counter(name, metric.WithDescription(def.Help), metric.WithUnit(unitOr(def.Unit)))
	if err != nil {
		return nil, err
	}
	return &Counter{inst: c, labels: l}, nil
}

// Histogram creates a histogram; Buckets default to LatencyBuckets.
func (b *Builder) Histogram(def Definition) (*Histogram, error) {
	name, l, err := b.validate(def)
	if err != nil {
		return nil, err
	}
	buckets := def.Buckets
	if len(buckets) == 0 {
		buckets = LatencyBuckets
	}
	h, err := b.meter.Float64Histogram(name, metric.WithDescription(def.Help), metric.WithUnit(unitOr(def.Unit)), metric.WithExplicitBucketBoundaries(buckets...))
	if err != nil {
		return nil, err
	}
	return &Histogram{inst: h, labels: l}, nil
}

// Gauge creates a last-value gauge.
func (b *Builder) Gauge(def Definition) (*Gauge, error) {
	name, l, err := b.validate(def)
	if err != nil {
		return nil, err
	}
	g, err := b.meter.Float64Gauge(name, metric.WithDescription(def.Help), metric.WithUnit(unitOr(def.Unit)))
	if err != nil {
		return nil, err
	}
	return &Gauge{inst: g, labels: l}, nil
}

func unitOr(u string) string {
	if u == "" {
		return "1"
	}
	return u
}

// labels turns positional values into an attribute set, enforcing arity
// and the cardinality cap.
type labels struct {
	keys []attribute.Key
	max  int
	name string
	b    *Builder

	mu   sync.Mutex
	seen map[string]struct{}

	arityOnce, limitOnce sync.Once
}

// ErrArity is logged (once per instrument) when a call passes the wrong
// number of label values; the observation is recorded under "_invalid".
var ErrArity = errors.New("metrics: wrong number of label values")

func (l *labels) set(ctx context.Context, values []string) metric.MeasurementOption {
	if len(values) != len(l.keys) {
		l.arityOnce.Do(func() {
			l.b.logger.Error("metric misuse", "metric", l.name, "want_labels", len(l.keys), "got", len(values), "err", ErrArity)
		})
		return l.fill(ctx, "_invalid")
	}
	if len(values) == 0 {
		return metric.WithAttributeSet(*attribute.EmptySet())
	}
	key := strings.Join(values, "\x1f")
	l.mu.Lock()
	_, known := l.seen[key]
	if !known && len(l.seen) < l.max {
		l.seen[key] = struct{}{}
		known = true
	}
	l.mu.Unlock()
	if !known {
		l.limitOnce.Do(func() {
			l.b.logger.Warn("metric cardinality cap reached; further label combinations are recorded as _other", "metric", l.name, "max", l.max)
		})
		l.b.limited.Add(ctx, 1, metric.WithAttributes(attribute.String("metric", l.name)))
		return l.fill(ctx, "_other")
	}
	kvs := make([]attribute.KeyValue, len(l.keys))
	for i, k := range l.keys {
		kvs[i] = k.String(values[i])
	}
	return metric.WithAttributeSet(attribute.NewSet(kvs...))
}

func (l *labels) fill(_ context.Context, v string) metric.MeasurementOption {
	kvs := make([]attribute.KeyValue, len(l.keys))
	for i, k := range l.keys {
		kvs[i] = k.String(v)
	}
	return metric.WithAttributeSet(attribute.NewSet(kvs...))
}

// Counter is a validated monotonic counter.
type Counter struct {
	inst   metric.Int64Counter
	labels *labels
}

// Inc adds one.
func (c *Counter) Inc(ctx context.Context, values ...string) {
	c.inst.Add(ctx, 1, c.labels.set(ctx, values))
}

// Add adds n (n >= 0).
func (c *Counter) Add(ctx context.Context, n int64, values ...string) {
	c.inst.Add(ctx, n, c.labels.set(ctx, values))
}

// Histogram is a validated histogram.
type Histogram struct {
	inst   metric.Float64Histogram
	labels *labels
}

// Observe records v.
func (h *Histogram) Observe(ctx context.Context, v float64, values ...string) {
	h.inst.Record(ctx, v, h.labels.set(ctx, values))
}

// Since records the seconds elapsed since start.
func (h *Histogram) Since(ctx context.Context, start time.Time, values ...string) {
	h.Observe(ctx, time.Since(start).Seconds(), values...)
}

// Gauge is a validated last-value gauge.
type Gauge struct {
	inst   metric.Float64Gauge
	labels *labels
}

// Set records the current value.
func (g *Gauge) Set(ctx context.Context, v float64, values ...string) {
	g.inst.Record(ctx, v, g.labels.set(ctx, values))
}
