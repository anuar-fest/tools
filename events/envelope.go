package events

import (
	"errors"
	"fmt"
	"time"
)

// Content types for Envelope.ContentType.
const (
	ContentTypeJSON     = "application/json"
	ContentTypeProtobuf = "application/protobuf"
)

// Envelope is the unit of publication. Data is already encoded; the envelope
// carries the metadata that transports map to headers so consumers can
// deduplicate, correlate and trace without decoding the payload.
type Envelope struct {
	// ID is unique per event (UUID v7). Transports use it for broker-side
	// deduplication; the Inbox uses it for consumer-side idempotency.
	ID string
	// Type is the event type and the subject it is published on. See ParseType.
	Type string
	// Source is the producing service.
	Source string
	// Subject identifies the aggregate the event is about ("customer/42"),
	// not the broker subject. Optional.
	Subject string
	// Time is when the event happened, UTC.
	Time time.Time
	// TenantID scopes the event to one customer. Optional but strongly
	// recommended: every multi-tenant bug starts with a missing tenant.
	TenantID string
	// CorrelationID ties every event of one business flow together; it is
	// inherited from the triggering request or event. CausationID is the ID
	// of the event (or request) that directly caused this one.
	CorrelationID string
	CausationID   string
	// ContentType says how Data is encoded (default ContentTypeJSON).
	ContentType string
	// DataSchema names the schema of Data (a proto full name, a JSON schema
	// id). Optional; the major version lives in Type.
	DataSchema string
	// TraceParent and TraceState carry the W3C trace context.
	TraceParent string
	TraceState  string
	// Headers are extension attributes. Keys are lowercase; values must be
	// small: they travel in broker headers.
	Headers map[string]string
	Data    []byte
}

// Option customises New.
type Option func(*Envelope)

// WithTenant sets TenantID.
func WithTenant(id string) Option { return func(e *Envelope) { e.TenantID = id } }

// WithSubject sets the aggregate Subject.
func WithSubject(s string) Option { return func(e *Envelope) { e.Subject = s } }

// WithCorrelation sets CorrelationID and CausationID explicitly.
func WithCorrelation(correlation, causation string) Option {
	return func(e *Envelope) { e.CorrelationID, e.CausationID = correlation, causation }
}

// WithContentType sets ContentType and DataSchema.
func WithContentType(ct, schema string) Option {
	return func(e *Envelope) { e.ContentType, e.DataSchema = ct, schema }
}

// WithHeader adds one extension header.
func WithHeader(k, v string) Option {
	return func(e *Envelope) {
		if e.Headers == nil {
			e.Headers = map[string]string{}
		}
		e.Headers[k] = v
	}
}

// WithTime overrides the event time (tests, replays).
func WithTime(t time.Time) Option { return func(e *Envelope) { e.Time = t } }

// New builds an envelope with a fresh ID, the current time and JSON content
// type. The correlation id defaults to the event's own id so a flow started
// by this event is traceable from it.
func New(typ, source string, data []byte, opts ...Option) Envelope {
	e := Envelope{
		ID:          NewID(),
		Type:        typ,
		Source:      source,
		Time:        time.Now().UTC(),
		ContentType: ContentTypeJSON,
		Data:        data,
	}
	for _, o := range opts {
		o(&e)
	}
	if e.CorrelationID == "" {
		e.CorrelationID = e.ID
	}
	return e
}

// Derive builds an envelope caused by parent: it inherits the correlation id
// and tenant, and sets the causation id to parent.ID. Use it inside handlers
// that publish follow-up events.
func Derive(parent Envelope, typ, source string, data []byte, opts ...Option) Envelope {
	all := make([]Option, 0, 2+len(opts))
	all = append(all, WithCorrelation(parent.CorrelationID, parent.ID), WithTenant(parent.TenantID))
	all = append(all, opts...)
	return New(typ, source, data, all...)
}

// Validate reports the first problem with the envelope.
func (e Envelope) Validate() error {
	switch {
	case e.ID == "":
		return errors.New("events: envelope has no ID")
	case e.Type == "":
		return errors.New("events: envelope has no Type")
	case !ValidType(e.Type):
		return fmt.Errorf("events: envelope type %q is not {domain}.{entity}.{event}.v{major}", e.Type)
	case e.Source == "":
		return errors.New("events: envelope has no Source")
	case e.Time.IsZero():
		return errors.New("events: envelope has no Time")
	}
	return nil
}
