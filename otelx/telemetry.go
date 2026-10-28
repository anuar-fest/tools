package otelx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// InstrumentationName is the tracer name this package uses for its own
// spans.
const InstrumentationName = "github.com/anuar-fest/tools/otelx"

// Config configures New. ServiceName is required; everything else has a
// safe default: no trace exporter means spans are never sampled (free).
type Config struct {
	// ServiceName becomes service.name on every span.
	ServiceName string
	// ServiceVersion is the build version.
	ServiceVersion string
	// Environment becomes deployment.environment.name (prod, staging).
	Environment string
	// InstanceID becomes service.instance.id (the pod name). Optional.
	InstanceID string
	// Attributes are extra resource attributes.
	Attributes []attribute.KeyValue

	Traces TracesConfig

	// Propagator defaults to W3C TraceContext plus Baggage.
	Propagator propagation.TextMapPropagator
	// SkipGlobals leaves otel.GetTracerProvider and friends untouched.
	// Default: the globals are set, so instrumentation that was not handed
	// a provider still works.
	SkipGlobals bool
	// Logger receives the SDK's own errors (export failures) at Warn.
	// nil discards.
	Logger *slog.Logger
}

// TracesConfig configures the tracer provider.
type TracesConfig struct {
	// Exporter overrides the OTLP/HTTP exporter (OTLP/gRPC, stdout, a test
	// exporter). When both Exporter and Endpoint are empty no span is
	// sampled and tracing costs nothing but context propagation.
	Exporter sdktrace.SpanExporter
	// Endpoint of the OTLP/HTTP collector: "otel-collector:4318" or a full
	// URL "https://otlp.example.com/v1/traces".
	Endpoint string
	// Insecure uses plain HTTP for a host:port Endpoint.
	Insecure bool
	// Headers are sent with every export (tenant, auth).
	Headers map[string]string
	// SampleRatio of root traces, 0 < ratio ≤ 1; 0 means 1 (sample all).
	// Child spans follow their parent's decision. Use Sampler for more.
	SampleRatio float64
	// Sampler overrides the parent-based ratio sampler.
	Sampler sdktrace.Sampler
	// BatchTimeout between exports; default 5s.
	BatchTimeout time.Duration
	// ExportTimeout bounds one export; default 10s.
	ExportTimeout time.Duration
	// MaxQueueSize of spans waiting to export; default 2048. Spans beyond
	// it are dropped, never blocking a request.
	MaxQueueSize int
	// Processors are extra span processors (tests use a sync exporter).
	Processors []sdktrace.SpanProcessor
}

// Telemetry owns the tracer provider. Call Shutdown last on exit so the
// final spans leave the process.
type Telemetry struct {
	res    *resource.Resource
	tp     *sdktrace.TracerProvider
	prop   propagation.TextMapPropagator
	traced bool

	once sync.Once
	err  error
}

// New builds the resource and the tracer provider and, unless SkipGlobals,
// installs them as the OpenTelemetry globals. Hand Resource() to
// metrics.Config.Resource so metrics carry the same identity.
func New(ctx context.Context, cfg Config) (*Telemetry, error) {
	if cfg.ServiceName == "" {
		return nil, errors.New("otelx: ServiceName is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	res, err := buildResource(ctx, cfg)
	if err != nil {
		return nil, err
	}
	tp, traced, err := buildTracerProvider(ctx, cfg.Traces, res)
	if err != nil {
		return nil, err
	}
	prop := cfg.Propagator
	if prop == nil {
		prop = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	}
	t := &Telemetry{res: res, tp: tp, prop: prop, traced: traced}
	if !cfg.SkipGlobals {
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(prop)
		otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
			logger.Warn("opentelemetry sdk error", "err", err)
		}))
	}
	return t, nil
}

func buildResource(ctx context.Context, cfg Config) (*resource.Resource, error) {
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
		return nil, fmt.Errorf("otelx: resource: %w", err)
	}
	return res, nil
}

func buildTracerProvider(ctx context.Context, tc TracesConfig, res *resource.Resource) (*sdktrace.TracerProvider, bool, error) {
	exp := tc.Exporter
	if exp == nil && tc.Endpoint != "" {
		opts := []otlptracehttp.Option{otlptracehttp.WithTimeout(orDefault(tc.ExportTimeout, 10*time.Second))}
		if strings.Contains(tc.Endpoint, "://") {
			opts = append(opts, otlptracehttp.WithEndpointURL(tc.Endpoint))
		} else {
			opts = append(opts, otlptracehttp.WithEndpoint(tc.Endpoint))
			if tc.Insecure {
				opts = append(opts, otlptracehttp.WithInsecure())
			}
		}
		if len(tc.Headers) > 0 {
			opts = append(opts, otlptracehttp.WithHeaders(tc.Headers))
		}
		var err error
		exp, err = otlptracehttp.New(ctx, opts...)
		if err != nil {
			return nil, false, fmt.Errorf("otelx: trace exporter: %w", err)
		}
	}
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if exp == nil && len(tc.Processors) == 0 {
		// Nobody listens: never sample, so a span costs an allocation-free
		// non-recording stub while propagation keeps working.
		opts = append(opts, sdktrace.WithSampler(sdktrace.NeverSample()))
		return sdktrace.NewTracerProvider(opts...), false, nil
	}
	sampler := tc.Sampler
	if sampler == nil {
		ratio := tc.SampleRatio
		if ratio == 0 {
			ratio = 1
		}
		if ratio < 0 || ratio > 1 {
			return nil, false, fmt.Errorf("otelx: SampleRatio %v out of (0, 1]", ratio)
		}
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	}
	opts = append(opts, sdktrace.WithSampler(sampler))
	if exp != nil {
		opts = append(opts, sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(orDefault(tc.BatchTimeout, 5*time.Second)),
			sdktrace.WithExportTimeout(orDefault(tc.ExportTimeout, 10*time.Second)),
			sdktrace.WithMaxQueueSize(orDefaultInt(tc.MaxQueueSize, 2048)),
		))
	}
	for _, p := range tc.Processors {
		opts = append(opts, sdktrace.WithSpanProcessor(p))
	}
	return sdktrace.NewTracerProvider(opts...), true, nil
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

func orDefaultInt(n, def int) int {
	if n <= 0 {
		return def
	}
	return n
}

// TracerProvider returns the SDK tracer provider.
func (t *Telemetry) TracerProvider() trace.TracerProvider { return t.tp }

// Tracer is a shortcut for TracerProvider().Tracer(name).
func (t *Telemetry) Tracer(name string) trace.Tracer { return t.tp.Tracer(name) }

// Propagator returns the configured propagator.
func (t *Telemetry) Propagator() propagation.TextMapPropagator { return t.prop }

// Resource returns the resource every span carries. Pass it to
// metrics.Config.Resource.
func (t *Telemetry) Resource() *resource.Resource { return t.res }

// TracingEnabled reports whether spans are exported anywhere.
func (t *Telemetry) TracingEnabled() bool { return t.traced }

// Shutdown flushes the spans under ctx. Idempotent; later calls return the
// first result.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	t.once.Do(func() {
		if err := t.tp.Shutdown(ctx); err != nil {
			t.err = fmt.Errorf("otelx: traces: %w", err)
		}
	})
	return t.err
}
