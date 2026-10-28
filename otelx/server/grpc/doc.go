// Package servergrpc is the edge for a gRPC server you own: interceptors
// that trace every call with the RPC semantic conventions and turn a panic
// into codes.Internal; a Server with keepalive, size and shutdown defaults
// that fits lifecycle; and a grpc.health.v1 service backed by
// health.Registry so one readiness decision serves both the HTTP probe and
// gRPC clients. The rpc.server.* metrics are metrics/grpc; pass its
// ServerOptions through ServerConfig.Options so they chain after these.
//
// Shaffra owns no gRPC server today; this is where one starts when it does.
package servergrpc
