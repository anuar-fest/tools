package events

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Type is a parsed event type: {domain}.{entity}.{event}.v{major}, for
// example customers.customer.created.v1. The type is also the subject or
// topic the event is published on, so the rule is enforced on every envelope:
// lowercase, underscores allowed, four segments, explicit major version.
type Type struct {
	Domain  string
	Entity  string
	Event   string
	Version int
}

var typeRE = regexp.MustCompile(`^([a-z][a-z0-9_]*)\.([a-z][a-z0-9_]*)\.([a-z][a-z0-9_]*)\.v([1-9][0-9]*)$`)

// ParseType validates and splits an event type.
func ParseType(s string) (Type, error) {
	m := typeRE.FindStringSubmatch(s)
	if m == nil {
		return Type{}, fmt.Errorf("events: invalid type %q: want {domain}.{entity}.{event}.v{major}, lowercase", s)
	}
	v, _ := strconv.Atoi(m[4]) // the regexp guarantees digits
	return Type{Domain: m[1], Entity: m[2], Event: m[3], Version: v}, nil
}

// ValidType reports whether s follows the naming rule.
func ValidType(s string) bool { return typeRE.MatchString(s) }

// String renders the type back to its wire form.
func (t Type) String() string {
	return strings.Join([]string{t.Domain, t.Entity, t.Event, "v" + strconv.Itoa(t.Version)}, ".")
}

// Prefix is "{domain}.{entity}." for building subject filters such as
// "customers.customer.>" in NATS.
func (t Type) Prefix() string { return t.Domain + "." + t.Entity + "." }
