package metrics

import (
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// HighCardinalityKeys belong on spans and logs, never on metrics. Guard
// drops them from every instrument so a business metric that was given a
// user id does not grow a series per user.
var HighCardinalityKeys = []attribute.Key{
	"url.path", "url.full", "url.query", "http.target", "http.route.raw",
	"db.query.text", "db.statement", "messaging.message.id", "messaging.message.conversation_id",
	"user.id", "enduser.id", "tenant.id", "session.id", "request.id", "trace.id", "span.id",
	"client.address", "peer.address", "net.peer.ip", "user_agent.original",
}

// GuardConfig configures Guard.
type GuardConfig struct {
	// Deny is removed from every instrument; default HighCardinalityKeys.
	Deny []attribute.Key
	// Allow, per instrument name, keeps only these keys (after Deny).
	Allow map[string][]attribute.Key
}

// Guard is one View that matches every instrument and filters its
// attributes: Deny everywhere, Allow where configured. One view rather
// than one per rule, because several matching views would give the
// instrument several streams and the exporter duplicate series.
// Explicit bucket boundaries set on an instrument are kept.
func Guard(cfg GuardConfig) sdkmetric.View {
	deny := cfg.Deny
	if deny == nil {
		deny = HighCardinalityKeys
	}
	denyFilter := attribute.NewDenyKeysFilter(deny...)
	allowFilters := make(map[string]attribute.Filter, len(cfg.Allow))
	for name, keys := range cfg.Allow {
		allow := attribute.NewAllowKeysFilter(keys...)
		allowFilters[name] = func(kv attribute.KeyValue) bool { return allow(kv) && denyFilter(kv) }
	}
	return func(inst sdkmetric.Instrument) (sdkmetric.Stream, bool) {
		filter := denyFilter
		if f, ok := allowFilters[inst.Name]; ok {
			filter = f
		}
		return sdkmetric.Stream{Name: inst.Name, Description: inst.Description, Unit: inst.Unit, AttributeFilter: filter}, true
	}
}
