package servergrpc

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/anuar-fest/tools/health"
)

// Service names the health service answers. "" and "ready" report
// readiness (the load balancer's question); "live" reports liveness.
const (
	ServiceReady = "ready"
	ServiceLive  = "live"
)

// HealthServer bridges health.Registry to grpc.health.v1, so gRPC clients
// and the Kubernetes gRPC probe see the same readiness as /healthz/ready.
type HealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	reg      *health.Registry
	interval time.Duration
}

// NewHealthServer builds the service; interval is how often Watch
// re-evaluates (default 5s). Register it with
// grpc_health_v1.RegisterHealthServer(srv, hs).
func NewHealthServer(reg *health.Registry, interval time.Duration) *HealthServer {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &HealthServer{reg: reg, interval: interval}
}

func (h *HealthServer) evaluate(ctx context.Context, service string) (grpc_health_v1.HealthCheckResponse_ServingStatus, error) {
	var rep health.Report
	switch service {
	case "", ServiceReady:
		rep = h.reg.Ready(ctx)
	case ServiceLive:
		rep = h.reg.Live(ctx)
	default:
		return grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN, status.Error(codes.NotFound, "unknown service")
	}
	if rep.OK() {
		return grpc_health_v1.HealthCheckResponse_SERVING, nil
	}
	return grpc_health_v1.HealthCheckResponse_NOT_SERVING, nil
}

// Check implements grpc.health.v1.Health.
func (h *HealthServer) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	st, err := h.evaluate(ctx, req.GetService())
	if err != nil {
		return nil, err
	}
	return &grpc_health_v1.HealthCheckResponse{Status: st}, nil
}

// Watch implements grpc.health.v1.Health: the first status at once, then
// every change, until the client goes away.
func (h *HealthServer) Watch(req *grpc_health_v1.HealthCheckRequest, stream grpc_health_v1.Health_WatchServer) error {
	ctx := stream.Context()
	last, err := h.evaluate(ctx, req.GetService())
	if err != nil {
		return err
	}
	if err := stream.Send(&grpc_health_v1.HealthCheckResponse{Status: last}); err != nil {
		return err
	}
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			st, err := h.evaluate(ctx, req.GetService())
			if err != nil {
				return err
			}
			if st == last {
				continue
			}
			last = st
			if err := stream.Send(&grpc_health_v1.HealthCheckResponse{Status: st}); err != nil {
				return err
			}
		}
	}
}

var _ grpc_health_v1.HealthServer = (*HealthServer)(nil)
