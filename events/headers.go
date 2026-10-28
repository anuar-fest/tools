package events

import (
	"fmt"
	"strings"
	"time"
)

// Header names used by every transport adapter, in the CloudEvents binary
// content mode spelling. Transports that need case-insensitive keys
// (NATS) match them as given.
const (
	HeaderID            = "ce-id"
	HeaderType          = "ce-type"
	HeaderSource        = "ce-source"
	HeaderSubject       = "ce-subject"
	HeaderTime          = "ce-time"
	HeaderTenant        = "ce-tenantid"
	HeaderCorrelationID = "ce-correlationid"
	HeaderCausationID   = "ce-causationid"
	HeaderContentType   = "content-type"
	HeaderDataSchema    = "ce-dataschema"
	HeaderTraceParent   = "traceparent"
	HeaderTraceState    = "tracestate"
	// HeaderExtPrefix prefixes extension headers from Envelope.Headers.
	HeaderExtPrefix = "ce-x-"
)

// ToHeaders maps the envelope metadata to transport headers. Data is not
// included.
func ToHeaders(e Envelope) map[string]string {
	h := make(map[string]string, 12+len(e.Headers))
	set := func(k, v string) {
		if v != "" {
			h[k] = v
		}
	}
	set(HeaderID, e.ID)
	set(HeaderType, e.Type)
	set(HeaderSource, e.Source)
	set(HeaderSubject, e.Subject)
	if !e.Time.IsZero() {
		h[HeaderTime] = e.Time.UTC().Format(time.RFC3339Nano)
	}
	set(HeaderTenant, e.TenantID)
	set(HeaderCorrelationID, e.CorrelationID)
	set(HeaderCausationID, e.CausationID)
	set(HeaderContentType, e.ContentType)
	set(HeaderDataSchema, e.DataSchema)
	set(HeaderTraceParent, e.TraceParent)
	set(HeaderTraceState, e.TraceState)
	for k, v := range e.Headers {
		set(HeaderExtPrefix+strings.ToLower(k), v)
	}
	return h
}

// FromHeaders rebuilds an envelope from transport headers and the payload.
// Header keys are matched case-insensitively. The result is validated.
func FromHeaders(h map[string]string, data []byte) (Envelope, error) {
	norm := make(map[string]string, len(h))
	for k, v := range h {
		norm[strings.ToLower(k)] = v
	}
	e := Envelope{
		ID:            norm[HeaderID],
		Type:          norm[HeaderType],
		Source:        norm[HeaderSource],
		Subject:       norm[HeaderSubject],
		TenantID:      norm[HeaderTenant],
		CorrelationID: norm[HeaderCorrelationID],
		CausationID:   norm[HeaderCausationID],
		ContentType:   norm[HeaderContentType],
		DataSchema:    norm[HeaderDataSchema],
		TraceParent:   norm[HeaderTraceParent],
		TraceState:    norm[HeaderTraceState],
		Data:          data,
	}
	if ts := norm[HeaderTime]; ts != "" {
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			return Envelope{}, fmt.Errorf("events: bad %s header %q: %w", HeaderTime, ts, err)
		}
		e.Time = t
	}
	for k, v := range norm {
		if strings.HasPrefix(k, HeaderExtPrefix) {
			if e.Headers == nil {
				e.Headers = map[string]string{}
			}
			e.Headers[strings.TrimPrefix(k, HeaderExtPrefix)] = v
		}
	}
	if err := e.Validate(); err != nil {
		return Envelope{}, err
	}
	return e, nil
}
