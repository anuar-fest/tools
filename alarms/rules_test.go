package alarms_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/anuar-fest/tools/alarms"
)

func TestRules_RendersEventAndStateAlerts(t *testing.T) {
	a, _ := newAlarms(t, nil,
		alarms.Definition{Type: "billing", Name: "quota_exceeded", Severity: alarms.Critical, Summary: "a tenant went over its hard quota", Window: 10 * time.Minute},
		alarms.Definition{Type: "cache", Name: "unreachable", Kind: alarms.State, For: 90 * time.Second},
	)
	out, err := a.Rules(alarms.RulesConfig{Labels: map[string]string{"team": "payments"}, Selector: `job="customers"`})
	if err != nil {
		t.Fatal(err)
	}
	text := string(out)
	for _, want := range []string{
		"  - name: \"svc-alarms\"\n",
		"      - alert: BillingQuotaExceeded\n        expr: increase(alarms_total{alarm_type=\"billing\",alarm_name=\"quota_exceeded\",job=\"customers\"}[10m]) > 0\n        for: 0s\n",
		"          severity: critical\n          alarm_type: billing\n          alarm_name: quota_exceeded\n          service: \"svc\"\n          team: \"payments\"\n",
		"          summary: \"a tenant went over its hard quota\"\n",
		"      - alert: CacheUnreachable\n        expr: alarms_active{alarm_type=\"cache\",alarm_name=\"unreachable\",job=\"customers\"} == 1\n        for: 1m30s\n",
		"          summary: \"cache/unreachable\"\n",
		"      - alert: HealthReadinessLost\n        expr: alarms_active{alarm_type=\"health\",alarm_name=\"readiness_lost\",job=\"customers\"} == 1\n        for: 1m\n",
		"      - alert: ResilienceBreakerOpened\n        expr: increase(alarms_total{alarm_type=\"resilience\",alarm_name=\"breaker_opened\",job=\"customers\"}[5m]) > 0\n",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing:\n%s\nin:\n%s", want, text)
		}
	}
	// Deterministic: sorted by type/name, so the file diffs cleanly in a chart.
	if strings.Index(text, "alert: BillingQuotaExceeded") > strings.Index(text, "alert: CacheUnreachable") || strings.Index(text, "alert: CacheUnreachable") > strings.Index(text, "alert: EventsDeadLetter") {
		t.Fatal("rules must be sorted")
	}
	again, _ := a.Rules(alarms.RulesConfig{Labels: map[string]string{"team": "payments"}, Selector: `job="customers"`})
	if !bytes.Equal(out, again) {
		t.Fatal("rendering must be stable")
	}
}

func TestRules_Validation(t *testing.T) {
	ok := []alarms.Definition{{Type: "t", Name: "n"}}
	cases := map[string]alarms.RulesConfig{
		"no service":      {Definitions: ok},
		"no definitions":  {Service: "s"},
		"bad label":       {Service: "s", Definitions: ok, Labels: map[string]string{"bad-key": "x"}},
		"generated label": {Service: "s", Definitions: ok, Labels: map[string]string{"severity": "x"}},
		"duplicate":       {Service: "s", Definitions: []alarms.Definition{{Type: "t", Name: "n"}, {Type: "t", Name: "n"}}},
		"bad definition":  {Service: "s", Definitions: []alarms.Definition{{Type: "T", Name: "n"}}},
	}
	for name, cfg := range cases {
		if _, err := alarms.Rules(cfg); err == nil {
			t.Errorf("%s must fail", name)
		}
	}
	out, err := alarms.Rules(alarms.RulesConfig{Service: "s", Group: "custom", Definitions: alarms.BuiltinDefinitions()})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "- name: \"custom\"") || strings.Count(string(out), "- alert: ") != len(alarms.BuiltinDefinitions()) {
		t.Fatalf("built-in rules:\n%s", out)
	}
}

func TestAlertName(t *testing.T) {
	for in, want := range map[alarms.Definition]string{
		{Type: "billing", Name: "quota_exceeded"}: "BillingQuotaExceeded",
		{Type: "health", Name: "readiness_lost"}:  "HealthReadinessLost",
		{Type: "t", Name: "n"}:                    "TN",
	} {
		if got := alarms.AlertName(in); got != want {
			t.Errorf("AlertName(%v) = %q, want %q", in, got, want)
		}
	}
}
