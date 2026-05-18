// Package adapters holds the inbound adapter implementations. Importing
// any of these subpackages auto-registers them via init() with the
// parent inbound package.
package adapters

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/biffsocko/prm/internal/inbound"
)

// Splunk adapter. Splunk's Webhook alert action POSTs a JSON object
// shaped like:
//
//	{
//	  "sid":          "scheduler__admin__...",
//	  "search_name":  "Auth API 5xx Spike",
//	  "app":          "search",
//	  "owner":        "admin",
//	  "results_link": "https://splunk.example.com/...",
//	  "result":       { "status_code": "503", "service": "auth-api", "count": "47" }
//	}
type Splunk struct{}

func (Splunk) Name() string { return "splunk" }

// splunkPayload is what Splunk's webhook alert action sends.
type splunkPayload struct {
	SID         string         `json:"sid"`
	SearchName  string         `json:"search_name"`
	App         string         `json:"app"`
	Owner       string         `json:"owner"`
	ResultsLink string         `json:"results_link"`
	Result      map[string]any `json:"result"`
}

// splunkSettings is the per-integration config. ServiceField overrides
// the default "service" key for pulling the affected-service name out
// of result; SeverityField overrides default severity-source selection.
type splunkSettings struct {
	ServiceField  string `json:"service_field"`  // default "service"
	SeverityField string `json:"severity_field"` // default ""; if empty, derive from search_name
	SummaryFmt    string `json:"summary_fmt"`    // optional Go text/template-style; default uses search_name
}

func (Splunk) Normalize(body []byte, _ http.Header, settingsJSON []byte) (inbound.Event, error) {
	var p splunkPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return inbound.Event{}, fmt.Errorf("%w: parse splunk payload: %v", inbound.ErrAdapterBadInput, err)
	}
	if p.SearchName == "" {
		return inbound.Event{}, fmt.Errorf("%w: search_name", inbound.ErrAdapterMissing)
	}

	var s splunkSettings
	if len(settingsJSON) > 0 {
		_ = json.Unmarshal(settingsJSON, &s)
	}
	serviceKey := s.ServiceField
	if serviceKey == "" {
		serviceKey = "service"
	}

	service := ""
	if p.Result != nil {
		if v, ok := p.Result[serviceKey].(string); ok {
			service = v
		}
	}
	severity := ""
	if s.SeverityField != "" && p.Result != nil {
		if v, ok := p.Result[s.SeverityField].(string); ok {
			severity = inbound.NormalizeSeverity(v)
		}
	}
	if severity == "" {
		severity = deriveSplunkSeverity(p.SearchName)
	}

	summary := p.SearchName
	if v, ok := p.Result["count"]; ok {
		summary = fmt.Sprintf("%s (count=%v)", summary, v)
	}
	summary = inbound.Truncate(summary, 200)

	fields := map[string]any{}
	for k, v := range p.Result {
		fields[k] = v
	}
	if p.SID != "" {
		fields["sid"] = p.SID
	}
	if p.App != "" {
		fields["app"] = p.App
	}
	if p.Owner != "" {
		fields["owner"] = p.Owner
	}
	if p.ResultsLink != "" {
		fields["results_link"] = p.ResultsLink
	}

	return inbound.Event{
		Source:     "splunk",
		Service:    service,
		Severity:   severity,
		Summary:    summary,
		Fields:     fields,
		OccurredAt: time.Now().UTC(), // Splunk payload doesn't carry a reliable trigger ts
		Raw:        json.RawMessage(body),
	}, nil
}

// deriveSplunkSeverity is a best-effort severity classifier when the
// caller hasn't given us a SeverityField. Looks at the search name for
// common conventions.
func deriveSplunkSeverity(name string) string {
	low := toLower(name)
	switch {
	case contains(low, "critical"), contains(low, "fatal"), contains(low, "outage"), contains(low, "down"):
		return inbound.SeverityCritical
	case contains(low, "error"), contains(low, "5xx"), contains(low, "failure"):
		return inbound.SeverityError
	case contains(low, "warn"), contains(low, "anomaly"), contains(low, "spike"):
		return inbound.SeverityWarn
	default:
		return inbound.SeverityInfo
	}
}

func toLower(s string) string {
	b := make([]byte, len(s))
	for i := range s {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

func contains(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(haystack) < len(needle) {
		return false
	}
outer:
	for i := 0; i <= len(haystack)-len(needle); i++ {
		for j := 0; j < len(needle); j++ {
			if haystack[i+j] != needle[j] {
				continue outer
			}
		}
		return true
	}
	return false
}

func init() {
	inbound.Register(Splunk{})
}
