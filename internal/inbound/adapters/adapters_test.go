package adapters_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/biffsocko/prm/internal/inbound"
	_ "github.com/biffsocko/prm/internal/inbound/adapters" // register
)

func mustLookup(t *testing.T, name string) inbound.Adapter {
	t.Helper()
	a, err := inbound.Lookup(name)
	if err != nil {
		t.Fatalf("Lookup(%q): %v", name, err)
	}
	return a
}

func TestSplunkNormalizeHappyPath(t *testing.T) {
	body := []byte(`{
		"sid":          "scheduler__admin__abc",
		"search_name":  "Auth API 5xx Spike",
		"app":          "search",
		"owner":        "admin",
		"results_link": "https://splunk.example.com/r",
		"result": {"status_code":"503","service":"auth-api","count":"47"}
	}`)
	ev, err := mustLookup(t, "splunk").Normalize(body, http.Header{}, nil)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if ev.Source != "splunk" {
		t.Errorf("source")
	}
	if ev.Service != "auth-api" {
		t.Errorf("service: got %q", ev.Service)
	}
	// search_name "Auth API 5xx Spike" -> contains "5xx" -> "error"
	if ev.Severity != inbound.SeverityError {
		t.Errorf("severity: got %q want error", ev.Severity)
	}
	if !strings.Contains(ev.Summary, "Auth API 5xx Spike") {
		t.Errorf("summary missing search_name: %q", ev.Summary)
	}
	if !strings.Contains(ev.Summary, "count=47") {
		t.Errorf("summary missing count: %q", ev.Summary)
	}
	if ev.Fields["sid"] != "scheduler__admin__abc" {
		t.Errorf("sid missing")
	}
	if ev.Fields["service"] != "auth-api" {
		t.Errorf("service field missing")
	}
}

func TestSplunkRejectsMissingSearchName(t *testing.T) {
	body := []byte(`{"result":{}}`)
	_, err := mustLookup(t, "splunk").Normalize(body, http.Header{}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSplunkRespectsServiceFieldOverride(t *testing.T) {
	body := []byte(`{"search_name":"x","result":{"svc":"my-svc"}}`)
	settings := []byte(`{"service_field":"svc"}`)
	ev, err := mustLookup(t, "splunk").Normalize(body, http.Header{}, settings)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Service != "my-svc" {
		t.Errorf("service override failed: %q", ev.Service)
	}
}

func TestGraylogNormalizeHappyPath(t *testing.T) {
	body := []byte(`{
		"event_definition_id":    "ed1",
		"event_definition_type":  "aggregation-v1",
		"event_definition_title": "Auth API error rate",
		"event": {
			"timestamp": "2026-05-18T03:00:00Z",
			"message":   "Auth API error rate > 5/min",
			"fields":    {"service":"auth-api","level":"ERROR"},
			"priority":  3
		}
	}`)
	ev, err := mustLookup(t, "graylog").Normalize(body, http.Header{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Source != "graylog" {
		t.Errorf("source")
	}
	if ev.Service != "auth-api" {
		t.Errorf("service: %q", ev.Service)
	}
	// fields.level "ERROR" overrides priority -> "error"
	if ev.Severity != inbound.SeverityError {
		t.Errorf("severity: %q", ev.Severity)
	}
	if ev.Summary != "Auth API error rate > 5/min" {
		t.Errorf("summary: %q", ev.Summary)
	}
	want, _ := time.Parse(time.RFC3339, "2026-05-18T03:00:00Z")
	if !ev.OccurredAt.Equal(want) {
		t.Errorf("occurred_at: got %v want %v", ev.OccurredAt, want)
	}
	if ev.Fields["priority"] != 3 {
		t.Errorf("priority field missing or wrong: got %v (%T)", ev.Fields["priority"], ev.Fields["priority"])
	}
}

func TestGraylogPriorityFallback(t *testing.T) {
	// No level in fields; priority drives severity.
	body := []byte(`{
		"event": {"message":"ok","priority":1,"fields":{"service":"x"}}
	}`)
	ev, _ := mustLookup(t, "graylog").Normalize(body, http.Header{}, nil)
	if ev.Severity != inbound.SeverityInfo {
		t.Errorf("priority=1 should be info, got %q", ev.Severity)
	}
}

func TestGenericWithJSONPathSettings(t *testing.T) {
	body := []byte(`{
		"alert": {
			"summary": "Production DB CPU > 95%",
			"service": "db-primary",
			"severity": "P1",
			"timestamp": "2026-05-18T03:00:00Z"
		},
		"meta": {"region":"us-east"}
	}`)
	settings := []byte(`{
		"summary_path":     "$.alert.summary",
		"service_path":     "$.alert.service",
		"severity_path":    "$.alert.severity",
		"occurred_at_path": "$.alert.timestamp",
		"severity_map":     {"P1":"critical","P2":"error"}
	}`)
	ev, err := mustLookup(t, "generic").Normalize(body, http.Header{}, settings)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Service != "db-primary" {
		t.Errorf("service: %q", ev.Service)
	}
	if ev.Severity != inbound.SeverityCritical {
		t.Errorf("severity: %q", ev.Severity)
	}
	if ev.Summary != "Production DB CPU > 95%" {
		t.Errorf("summary: %q", ev.Summary)
	}
	if !ev.OccurredAt.UTC().Equal(time.Date(2026, 5, 18, 3, 0, 0, 0, time.UTC)) {
		t.Errorf("occurred_at: %v", ev.OccurredAt)
	}
}

func TestGenericRequiresSummaryPath(t *testing.T) {
	body := []byte(`{}`)
	_, err := mustLookup(t, "generic").Normalize(body, http.Header{}, nil)
	if err == nil {
		t.Fatal("expected error for missing summary_path")
	}
}

func TestGenericArrayIndexInPath(t *testing.T) {
	body := []byte(`{"items":[{"name":"first"},{"name":"second"}]}`)
	settings := []byte(`{"summary_path":"$.items.1.name"}`)
	ev, err := mustLookup(t, "generic").Normalize(body, http.Header{}, settings)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Summary != "second" {
		t.Errorf("array index path didn't resolve: %q", ev.Summary)
	}
}

func TestRegistryAllAdaptersRegistered(t *testing.T) {
	names := map[string]bool{}
	for _, n := range inbound.ListNames() {
		names[n] = true
	}
	for _, want := range []string{"splunk", "graylog", "generic"} {
		if !names[want] {
			t.Errorf("expected adapter %q registered; got %v", want, inbound.ListNames())
		}
	}
}
