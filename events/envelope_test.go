package events_test

import (
	"errors"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/anuar-fest/tools/events"
)

var uuidV7 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewID_IsV7AndTimeOrdered(t *testing.T) {
	ids := make([]string, 50)
	for i := range ids {
		ids[i] = events.NewID()
		if !uuidV7.MatchString(ids[i]) {
			t.Fatalf("not a v7 uuid: %s", ids[i])
		}
		time.Sleep(time.Millisecond)
	}
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	for i := range ids {
		if ids[i] != sorted[i] {
			t.Fatalf("ids generated 1ms apart must sort by time: %v", ids)
		}
	}
	seen := map[string]bool{}
	for range 10000 {
		id := events.NewID()
		if seen[id] {
			t.Fatal("duplicate id")
		}
		seen[id] = true
	}
}

func TestParseType(t *testing.T) {
	ty, err := events.ParseType("customers.customer.created.v1")
	if err != nil || ty.Domain != "customers" || ty.Entity != "customer" || ty.Event != "created" || ty.Version != 1 {
		t.Fatalf("%+v err=%v", ty, err)
	}
	if ty.String() != "customers.customer.created.v1" || ty.Prefix() != "customers.customer." {
		t.Fatalf("String=%q Prefix=%q", ty.String(), ty.Prefix())
	}
	for _, bad := range []string{"", "customers.created", "Customers.customer.created.v1", "customers.customer.created", "customers.customer.created.v0", "customers.customer.created.v1.extra", "cust-omers.customer.created.v1"} {
		if _, err := events.ParseType(bad); err == nil {
			t.Fatalf("%q should be invalid", bad)
		}
	}
	if !events.ValidType("pmo_tasks.task.completed.v12") {
		t.Fatal("underscores and multi-digit versions are valid")
	}
}

func TestNewAndDerive(t *testing.T) {
	e := events.New("customers.customer.created.v1", "customers-api", []byte(`{}`), events.WithTenant("t1"), events.WithSubject("customer/1"), events.WithHeader("Region", "eu"))
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
	if e.CorrelationID != e.ID || e.CausationID != "" || e.ContentType != events.ContentTypeJSON || e.Headers["Region"] != "eu" {
		t.Fatalf("%+v", e)
	}
	child := events.Derive(e, "customers.account.created.v1", "customers-api", nil)
	if child.CorrelationID != e.ID || child.CausationID != e.ID || child.TenantID != "t1" || child.ID == e.ID {
		t.Fatalf("derive: %+v", child)
	}
}

func TestValidate(t *testing.T) {
	good := events.New("a.b.c.v1", "svc", nil)
	cases := map[string]func(e *events.Envelope){
		"no id":     func(e *events.Envelope) { e.ID = "" },
		"no type":   func(e *events.Envelope) { e.Type = "" },
		"bad type":  func(e *events.Envelope) { e.Type = "A.B" },
		"no source": func(e *events.Envelope) { e.Source = "" },
		"no time":   func(e *events.Envelope) { e.Time = time.Time{} },
	}
	for name, mutate := range cases {
		e := good
		mutate(&e)
		if e.Validate() == nil {
			t.Fatalf("%s should fail", name)
		}
	}
}

func TestHeadersRoundTrip(t *testing.T) {
	at := time.Date(2026, 5, 1, 12, 0, 0, 123456789, time.UTC)
	e := events.New("orders.order.paid.v2", "payments", []byte("payload"),
		events.WithTenant("t1"), events.WithSubject("order/9"), events.WithCorrelation("corr", "cause"),
		events.WithContentType(events.ContentTypeProtobuf, "orders.v2.OrderPaid"), events.WithHeader("region", "eu"), events.WithTime(at))
	e.TraceParent = "00-abc-def-01"
	e.TraceState = "vendor=1"

	h := events.ToHeaders(e)
	if h[events.HeaderID] != e.ID || h[events.HeaderType] != e.Type || h[events.HeaderTime] != "2026-05-01T12:00:00.123456789Z" || h["ce-x-region"] != "eu" {
		t.Fatalf("headers = %v", h)
	}
	// Transports may upper-case keys.
	upper := map[string]string{}
	for k, v := range h {
		upper[strings.ToUpper(k)] = v
	}
	back, err := events.FromHeaders(upper, e.Data)
	if err != nil {
		t.Fatal(err)
	}
	e.Headers = map[string]string{"region": "eu"}
	if back.ID != e.ID || back.Type != e.Type || back.Source != e.Source || back.Subject != e.Subject || !back.Time.Equal(e.Time) ||
		back.TenantID != e.TenantID || back.CorrelationID != "corr" || back.CausationID != "cause" || back.ContentType != e.ContentType ||
		back.DataSchema != e.DataSchema || back.TraceParent != e.TraceParent || back.TraceState != e.TraceState ||
		back.Headers["region"] != "eu" || string(back.Data) != "payload" {
		t.Fatalf("round trip mismatch:\n%+v\n%+v", e, back)
	}
}

func TestFromHeaders_Rejects(t *testing.T) {
	if _, err := events.FromHeaders(map[string]string{events.HeaderID: "x"}, nil); err == nil {
		t.Fatal("missing type must fail")
	}
	if _, err := events.FromHeaders(map[string]string{events.HeaderID: "x", events.HeaderType: "a.b.c.v1", events.HeaderSource: "s", events.HeaderTime: "yesterday"}, nil); err == nil {
		t.Fatal("bad time must fail")
	}
}

func TestDropAndRetryAfter(t *testing.T) {
	base := errors.New("boom")
	if events.Drop(nil) != nil || events.RetryAfter(nil, time.Second) != nil {
		t.Fatal("nil in, nil out")
	}
	d := events.Drop(base)
	if !events.IsDrop(d) || !errors.Is(d, base) || d.Error() != "boom" {
		t.Fatalf("drop: %v", d)
	}
	r := events.RetryAfter(base, 5*time.Second)
	if delay, ok := events.RetryDelay(r); !ok || delay != 5*time.Second || !errors.Is(r, base) || events.IsDrop(r) {
		t.Fatalf("retry after: %v", r)
	}
	if _, ok := events.RetryDelay(base); ok {
		t.Fatal("plain error has no delay")
	}
}

func TestCodec(t *testing.T) {
	type payload struct {
		N int `json:"n"`
	}
	e, err := events.Encode(events.JSON{}, "a.b.c.v1", "svc", payload{N: 3})
	if err != nil || e.ContentType != events.ContentTypeJSON || string(e.Data) != `{"n":3}` {
		t.Fatalf("%+v err=%v", e, err)
	}
	var p payload
	if err := events.Decode(events.JSON{}, e, &p); err != nil || p.N != 3 {
		t.Fatalf("p=%+v err=%v", p, err)
	}
	e.ContentType = events.ContentTypeProtobuf
	if err := events.Decode(events.JSON{}, e, &p); !events.IsDrop(err) {
		t.Fatalf("content type mismatch must be a Drop, got %v", err)
	}
	e.ContentType = events.ContentTypeJSON
	e.Data = []byte("not json")
	if err := events.Decode(events.JSON{}, e, &p); !events.IsDrop(err) {
		t.Fatalf("undecodable must be a Drop, got %v", err)
	}
}
