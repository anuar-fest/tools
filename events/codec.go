package events

import (
	"encoding/json"
	"fmt"
)

// Codec encodes payloads. Keep one codec per event type family; mixing
// encodings on one subject is a migration, not a feature.
type Codec interface {
	ContentType() string
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSON is the default codec.
type JSON struct{}

func (JSON) ContentType() string                { return ContentTypeJSON }
func (JSON) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (JSON) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// Encode builds an envelope from a value with c. The envelope's content
// type is set from the codec.
func Encode(c Codec, typ, source string, v any, opts ...Option) (Envelope, error) {
	data, err := c.Marshal(v)
	if err != nil {
		return Envelope{}, fmt.Errorf("events: encode %s: %w", typ, err)
	}
	e := New(typ, source, data, opts...)
	e.ContentType = c.ContentType()
	return e, nil
}

// Decode unmarshals e.Data into v, refusing a content type the codec does
// not produce. A mismatch is a Drop: redelivery will not change the bytes.
func Decode(c Codec, e Envelope, v any) error {
	if e.ContentType != "" && e.ContentType != c.ContentType() {
		return Drop(fmt.Errorf("events: %s has content type %q, codec expects %q", e.Type, e.ContentType, c.ContentType()))
	}
	if err := c.Unmarshal(e.Data, v); err != nil {
		return Drop(fmt.Errorf("events: decode %s %s: %w", e.Type, e.ID, err))
	}
	return nil
}
