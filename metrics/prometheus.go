package metrics

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// PrometheusConfig configures NewPrometheus. The zero value is a fresh
// registry with the Go and process collectors and no scope labels.
type PrometheusConfig struct {
	// Registry to export into; nil creates one. Pass the registry that
	// holds promauto metrics during a migration so one /metrics serves both.
	Registry *prometheus.Registry
	// WithoutGoCollectors skips go_goroutines, process_resident_memory_bytes
	// and friends.
	WithoutGoCollectors bool
	// Namespace prefixes every metric name.
	Namespace string
	// WithScopeInfo adds otel_scope_name/otel_scope_version labels.
	WithScopeInfo bool
}

// Prometheus is the pull exporter: a Reader for the MeterProvider, the
// registry it writes into and the handler that serves it.
type Prometheus struct {
	registry *prometheus.Registry
	exporter *otelprom.Exporter
	handler  http.Handler
}

// NewPrometheus builds the exporter.
func NewPrometheus(cfg PrometheusConfig) (*Prometheus, error) {
	reg := cfg.Registry
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	if !cfg.WithoutGoCollectors {
		for _, c := range []prometheus.Collector{collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})} {
			var already prometheus.AlreadyRegisteredError
			if err := reg.Register(c); err != nil && !errors.As(err, &already) {
				return nil, fmt.Errorf("metrics: prometheus collectors: %w", err)
			}
		}
	}
	opts := []otelprom.Option{otelprom.WithRegisterer(reg)}
	if !cfg.WithScopeInfo {
		opts = append(opts, otelprom.WithoutScopeInfo())
	}
	if cfg.Namespace != "" {
		opts = append(opts, otelprom.WithNamespace(cfg.Namespace))
	}
	exp, err := otelprom.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("metrics: prometheus exporter: %w", err)
	}
	return &Prometheus{
		registry: reg,
		exporter: exp,
		handler: promhttp.HandlerFor(reg, promhttp.HandlerOpts{
			ErrorHandling:       promhttp.ContinueOnError,
			MaxRequestsInFlight: 2,
			Timeout:             10 * time.Second,
			EnableOpenMetrics:   true, // exemplars: a trace id on every histogram bucket
		}),
	}, nil
}

// Reader is what sdkmetric.WithReader takes.
func (p *Prometheus) Reader() sdkmetric.Reader { return p.exporter }

// Registry is the underlying registry, for code that still registers
// client_golang collectors directly.
func (p *Prometheus) Registry() *prometheus.Registry { return p.registry }

// Handler serves /metrics. Mount it on the admin port, not the public API.
func (p *Prometheus) Handler() http.Handler { return p.handler }
