package metrics

// Bucket boundaries in seconds and bytes. Every hook under metrics/ uses one of
// these so histograms from different integrations line up on one panel.
var (
	// LatencyBuckets suit request-scale work: HTTP and RPC calls, message
	// handlers. 5 ms to 30 s.
	LatencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 30}
	// FastBuckets suit in-datacenter round trips: database queries, cache
	// commands, broker publishes, pool waits. 100 µs to 10 s.
	FastBuckets = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	// SizeBuckets suit payload sizes: 256 B to 64 MiB.
	SizeBuckets = []float64{256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864}
)
