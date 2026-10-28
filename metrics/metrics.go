package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	metricsruntime "github.com/anuar-fest/tools/metrics/runtime"
)

// InstrumentationName is the meter name this package uses for its own
// instruments.
const InstrumentationName = "github.com/anuar-fest/tools/metrics"

// Config configures New. ServiceName is required unless Resource is set;
// everything else has a safe default: metrics are served for Prometheus on
// Handler, the cardinality Guard is installed, nothing is pushed.
type Config struct {
	// ServiceName becomes service.name on every metric.
	ServiceName string
	// ServiceVersion is the build version.
	ServiceVersion string
	// Environment becomes deployment.environment.name (prod, staging).
	Environment string
	// InstanceID becomes service.instance.id (the pod name). Optional.
	InstanceID string
	// Attributes are extra resource attributes.
	Attributes []attribute.KeyValue
	// Resource replaces the one built from the fields above. Pass the
	// tracing side's resource so spans and metrics carry one identity.
	Resource *resource.Resource

	// Disabled installs a provider without readers: instruments are no-ops
	// and Handler answers 404.
	Disabled bool
	// Prometheus configures the pull exporter; the zero value is a fresh
	// registry with the Go and process collectors.
	Prometheus PrometheusConfig
	// DisablePrometheus turns the pull exporter off (OTLP only).
	DisablePrometheus bool
	// OTLP pushes to a collector when Endpoint is set, in addition to or
	// instead of Prometheus.
	OTLP OTLPConfig
	// Readers are extra readers (tests use a manual reader).
	Readers []sdkmetric.Reader
	// Views rename, drop or re-bucket instruments. Guard is installed first
	// unless NoGuard; pass your own Guard with an allow list and set NoGuard.
	Views []sdkmetric.View
	// NoGuard disables the default Guard view.
	NoGuard bool
	// Runtime registers the Go runtime instruments (go.goroutine.count, ...)
	// for OTLP-only setups; with Prometheus on, the Go collectors already
	// cover it.
	Runtime bool
	// SkipGlobals leaves otel.GetMeterProvider untouched. Default: the
	// global is set, so a hook that was not handed a meter still works.
	SkipGlobals bool
	// Logger receives the SDK's own errors (export failures) at Warn through
	// the OpenTelemetry error handler. nil leaves the handler as it is, so
	// the tracing side's handler keeps working.
	Logger *slog.Logger
}

// OTLPConfig configures the OTLP/HTTP push exporter.
type OTLPConfig struct {
	// Endpoint of the collector: "otel-collector:4318" or a full URL.
	Endpoint string
	// Insecure uses plain HTTP for a host:port Endpoint.
	Insecure bool
	// Headers are sent with every export (tenant, auth).
	Headers map[string]string
	// Interval between pushes; default 60s.
	Interval time.Duration
	// Timeout bounds one export; default 10s.
	Timeout time.Duration
}

// Metrics owns the meter provider. Call Shutdown last on exit so the final
// values leave the process.
type Metrics struct {
	res      *resource.Resource
	mp       *sdkmetric.MeterProvider
	registry *prometheus.Registry
	handler  http.Handler
	runtime  metric.Registration

	once sync.Once
	err  error
}

// New builds the meter provider with its readers and, unless SkipGlobals,
// installs it as the OpenTelemetry global.
func New(ctx context.Context, cfg Config) (*Metrics, error) {
	res, err := buildResource(ctx, cfg)
	if err != nil {
		return nil, err
	}
	mp, reg, handler, err := buildMeterProvider(ctx, cfg, res)
	if err != nil {
		return nil, err
	}
	m := &Metrics{res: res, mp: mp, registry: reg, handler: handler}
	if cfg.Runtime && !cfg.Disabled {
		m.runtime, err = metricsruntime.Register(mp.Meter(InstrumentationName))
		if err != nil {
			_ = mp.Shutdown(ctx) // nothing was exported yet; the construction error is the one to report
			return nil, fmt.Errorf("metrics: runtime instruments: %w", err)
		}
	}
	if !cfg.SkipGlobals {
		otel.SetMeterProvider(mp)
		if cfg.Logger != nil {
			logger := cfg.Logger
			otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
				logger.Warn("opentelemetry sdk error", "err", err)
			}))
		}
	}
	return m, nil
}

func buildResource(ctx context.Context, cfg Config) (*resource.Resource, error) {
	if cfg.Resource != nil {
		return cfg.Resource, nil
	}
	if cfg.ServiceName == "" {
		return nil, errors.New("metrics: ServiceName is required")
	}
	attrs := []attribute.KeyValue{semconv.ServiceName(cfg.ServiceName)}
	if cfg.ServiceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersion(cfg.ServiceVersion))
	}
	if cfg.Environment != "" {
		attrs = append(attrs, semconv.DeploymentEnvironmentNameKey.String(cfg.Environment))
	}
	if cfg.InstanceID != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(cfg.InstanceID))
	}
	attrs = append(attrs, cfg.Attributes...)
	res, err := resource.New(ctx, resource.WithTelemetrySDK(), resource.WithAttributes(attrs...))
	if err != nil && !errors.Is(err, resource.ErrSchemaURLConflict) {
		return nil, fmt.Errorf("metrics: resource: %w", err)
	}
	return res, nil
}

func buildMeterProvider(ctx context.Context, cfg Config, res *resource.Resource) (*sdkmetric.MeterProvider, *prometheus.Registry, http.Handler, error) {
	opts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	if !cfg.NoGuard {
		opts = append(opts, sdkmetric.WithView(Guard(GuardConfig{})))
	}
	if len(cfg.Views) > 0 {
		opts = append(opts, sdkmetric.WithView(cfg.Views...))
	}
	handler := http.NotFoundHandler()
	if cfg.Disabled {
		return sdkmetric.NewMeterProvider(opts...), nil, handler, nil
	}
	var reg *prometheus.Registry
	if !cfg.DisablePrometheus {
		prom, err := NewPrometheus(cfg.Prometheus)
		if err != nil {
			return nil, nil, nil, err
		}
		reg, handler = prom.Registry(), prom.Handler()
		opts = append(opts, sdkmetric.WithReader(prom.Reader()))
	}
	if cfg.OTLP.Endpoint != "" {
		exp, err := newOTLPExporter(ctx, cfg.OTLP)
		if err != nil {
			return nil, nil, nil, err
		}
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(orDefault(cfg.OTLP.Interval, time.Minute)))))
	}
	for _, r := range cfg.Readers {
		opts = append(opts, sdkmetric.WithReader(r))
	}
	return sdkmetric.NewMeterProvider(opts...), reg, handler, nil
}

func newOTLPExporter(ctx context.Context, oc OTLPConfig) (sdkmetric.Exporter, error) {
	opts := []otlpmetrichttp.Option{otlpmetrichttp.WithTimeout(orDefault(oc.Timeout, 10*time.Second))}
	if strings.Contains(oc.Endpoint, "://") {
		opts = append(opts, otlpmetrichttp.WithEndpointURL(oc.Endpoint))
	} else {
		opts = append(opts, otlpmetrichttp.WithEndpoint(oc.Endpoint))
		if oc.Insecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}
	}
	if len(oc.Headers) > 0 {
		opts = append(opts, otlpmetrichttp.WithHeaders(oc.Headers))
	}
	exp, err := otlpmetrichttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("metrics: otlp exporter: %w", err)
	}
	return exp, nil
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// MeterProvider returns the SDK meter provider.
func (m *Metrics) MeterProvider() metric.MeterProvider { return m.mp }

// Meter is a shortcut for MeterProvider().Meter(name).
func (m *Metrics) Meter(name string) metric.Meter { return m.mp.Meter(name) }

// Resource returns the resource every metric carries.
func (m *Metrics) Resource() *resource.Resource { return m.res }

// Registry is the Prometheus registry, or nil when Prometheus is off. Hand
// it to code that still registers client_golang collectors directly.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves /metrics (404 when Prometheus is off). Mount it on the
// admin port, not the public API.
func (m *Metrics) Handler() http.Handler { return m.handler }

// Shutdown flushes the readers under ctx. Idempotent; later calls return
// the first result.
func (m *Metrics) Shutdown(ctx context.Context) error {
	m.once.Do(func() {
		var errs []error
		if m.runtime != nil {
			if err := m.runtime.Unregister(); err != nil {
				errs = append(errs, fmt.Errorf("metrics: runtime instruments: %w", err))
			}
		}
		if err := m.mp.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("metrics: shutdown: %w", err))
		}
		m.err = errors.Join(errs...)
	})
	return m.err
}
