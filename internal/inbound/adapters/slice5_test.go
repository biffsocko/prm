package adapters_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/biffsocko/prm/internal/inbound"
)

func TestDatadogMetricAlertHappyPath(t *testing.T) {
	body := []byte(`{
		"id":         "evt-1",
		"alert_type": "error",
		"alert_title": "[Triggered on i-deadbeef] CPU > 90%",
		"title":      "CPU > 90% on i-deadbeef",
		"body":       "CPU exceeded threshold",
		"date":       "2026-05-18T03:00:00Z",
		"event_type": "metric_alert",
		"hostname":   "i-deadbeef",
		"tags":       "env:prod,service:auth-api,team:platform",
		"org":        {"id":"42","name":"acme"},
		"link":       "https://app.datadoghq.com/event/42"
	}`)
	ev, err := mustLookup(t, "datadog").Normalize(body, http.Header{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Source != "datadog" {
		t.Errorf("source")
	}
	if ev.Service != "auth-api" {
		t.Errorf("service tag extraction failed: %q", ev.Service)
	}
	if ev.Severity != inbound.SeverityError {
		t.Errorf("alert_type=error should map to severity error; got %q", ev.Severity)
	}
	if !strings.Contains(ev.Summary, "CPU > 90%") {
		t.Errorf("summary: %q", ev.Summary)
	}
	tagsMap, ok := ev.Fields["tags"].(map[string]string)
	if !ok || tagsMap["env"] != "prod" || tagsMap["team"] != "platform" {
		t.Errorf("tags parse: %v", ev.Fields["tags"])
	}
}

func TestDatadogAlertTypeMapping(t *testing.T) {
	cases := map[string]string{
		"error":   inbound.SeverityError,
		"warning": inbound.SeverityWarn,
		"info":    inbound.SeverityInfo,
		"success": inbound.SeverityInfo,
		"weird":   inbound.SeverityWarn,
	}
	for alertType, want := range cases {
		body := []byte(`{"alert_type":"` + alertType + `","title":"x"}`)
		ev, err := mustLookup(t, "datadog").Normalize(body, http.Header{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Severity != want {
			t.Errorf("alert_type=%q: got severity %q want %q", alertType, ev.Severity, want)
		}
	}
}

func TestGitHubPushEvent(t *testing.T) {
	body := []byte(`{
		"pusher":   {"name":"alice"},
		"sender":   {"login":"alice"},
		"ref":      "refs/heads/main",
		"commits":  [{"id":"abc"},{"id":"def"}],
		"repository": {"full_name":"biffsocko/prm"}
	}`)
	h := http.Header{}
	h.Set("X-GitHub-Event", "push")
	ev, err := mustLookup(t, "github").Normalize(body, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Source != "github" {
		t.Errorf("source")
	}
	if ev.Service != "biffsocko/prm" {
		t.Errorf("repo as service: %q", ev.Service)
	}
	if !strings.Contains(ev.Summary, "pushed 2 commit") {
		t.Errorf("summary: %q", ev.Summary)
	}
	if ev.Fields["github_event"] != "push" {
		t.Errorf("event tag missing")
	}
	if ev.Fields["commits"] != 2 {
		t.Errorf("commits count: %v", ev.Fields["commits"])
	}
}

func TestGitHubPRMergedSeverity(t *testing.T) {
	body := []byte(`{
		"action": "closed",
		"pull_request": {"number": 42, "title": "Fix the thing", "merged": true},
		"sender": {"login":"alice"},
		"repository": {"full_name":"acme/svc"}
	}`)
	h := http.Header{}
	h.Set("X-GitHub-Event", "pull_request")
	ev, err := mustLookup(t, "github").Normalize(body, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(ev.Summary, "merged") {
		t.Errorf("merged PR should say so: %q", ev.Summary)
	}
}

func TestGitHubDeploymentStatusFailure(t *testing.T) {
	body := []byte(`{
		"deployment": {"environment": "prod"},
		"deployment_status": {"state": "failure"},
		"sender": {"login":"deploybot"},
		"repository": {"full_name":"acme/svc"}
	}`)
	h := http.Header{}
	h.Set("X-GitHub-Event", "deployment_status")
	ev, err := mustLookup(t, "github").Normalize(body, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Severity != inbound.SeverityError {
		t.Errorf("failure deployment should be error severity, got %q", ev.Severity)
	}
	if !strings.Contains(ev.Summary, "prod") || !strings.Contains(ev.Summary, "failure") {
		t.Errorf("summary missing details: %q", ev.Summary)
	}
}

func TestGitHubMissingEventHeader(t *testing.T) {
	body := []byte(`{}`)
	_, err := mustLookup(t, "github").Normalize(body, http.Header{}, nil)
	if err == nil {
		t.Fatal("expected error when X-GitHub-Event header is missing")
	}
}

func TestRegistryIncludesNewAdapters(t *testing.T) {
	names := map[string]bool{}
	for _, n := range inbound.ListNames() {
		names[n] = true
	}
	for _, want := range []string{"splunk", "graylog", "generic", "datadog", "github"} {
		if !names[want] {
			t.Errorf("adapter %q not registered; got %v", want, inbound.ListNames())
		}
	}
}
