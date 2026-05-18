package adapters

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/biffsocko/prm/internal/inbound"
)

// Datadog adapter. Datadog's Webhooks integration POSTs a configurable
// payload; the default JSON template includes fields like:
//
//	{
//	  "id":              "event-id",
//	  "alert_type":      "error" | "warning" | "info" | "success",
//	  "alert_title":     "[Triggered on {host}] CPU > 90%",
//	  "title":           "Re: CPU > 90%",
//	  "body":            "Markdown body of the alert",
//	  "date":            "2026-05-18T03:00:00Z",
//	  "event_msg":       "human-readable",
//	  "event_type":      "metric_alert",
//	  "hostname":        "i-deadbeef",
//	  "tags":            "env:prod,service:auth-api",
//	  "org": { "id": "...", "name": "..." }
//	}
//
// Different Datadog event types nest differently; this adapter aims for
// the common-case "metric_alert" / "service_check" shapes and falls back
// gracefully on unrecognized fields.
type Datadog struct{}

func (Datadog) Name() string { return "datadog" }

type datadogPayload struct {
	ID         string `json:"id"`
	AlertType  string `json:"alert_type"`
	AlertTitle string `json:"alert_title"`
	Title      string `json:"title"`
	Body       string `json:"body"`
	Date       string `json:"date"`
	EventMsg   string `json:"event_msg"`
	EventType  string `json:"event_type"`
	Hostname   string `json:"hostname"`
	Tags       string `json:"tags"`
	Org        struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"org"`
	Link string `json:"link"`
}

type datadogSettings struct {
	// ServiceTag is the Datadog tag key whose value is taken as the
	// PRM service identifier. Defaults to "service".
	ServiceTag string `json:"service_tag"`
}

func (Datadog) Normalize(body []byte, _ http.Header, settingsJSON []byte) (inbound.Event, error) {
	var p datadogPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return inbound.Event{}, fmt.Errorf("%w: parse datadog payload: %v", inbound.ErrAdapterBadInput, err)
	}
	// Title is the most informative summary source; fall back to alert_title,
	// event_msg, or body in that order.
	summary := p.Title
	if summary == "" {
		summary = p.AlertTitle
	}
	if summary == "" {
		summary = p.EventMsg
	}
	if summary == "" {
		summary = firstLine(p.Body)
	}
	if summary == "" {
		return inbound.Event{}, fmt.Errorf("%w: title/alert_title/event_msg/body", inbound.ErrAdapterMissing)
	}
	summary = inbound.Truncate(summary, 200)

	var s datadogSettings
	if len(settingsJSON) > 0 {
		_ = json.Unmarshal(settingsJSON, &s)
	}
	tagKey := s.ServiceTag
	if tagKey == "" {
		tagKey = "service"
	}
	tagMap := parseDDTags(p.Tags)
	service := tagMap[tagKey]

	severity := datadogAlertTypeToSeverity(p.AlertType)

	ts := time.Now().UTC()
	if p.Date != "" {
		// Datadog dates may be RFC3339 or numeric epoch in different
		// integrations. Try both.
		if parsed, err := time.Parse(time.RFC3339, p.Date); err == nil {
			ts = parsed.UTC()
		}
	}

	fields := map[string]any{
		"alert_type": p.AlertType,
		"event_type": p.EventType,
		"hostname":   p.Hostname,
		"tags":       tagMap,
		"event_id":   p.ID,
	}
	if p.Org.Name != "" {
		fields["org_name"] = p.Org.Name
	}
	if p.Link != "" {
		fields["link"] = p.Link
	}

	return inbound.Event{
		Source: "datadog", Service: service, Severity: severity,
		Summary: summary, Fields: fields, OccurredAt: ts,
		Raw: json.RawMessage(body),
	}, nil
}

func datadogAlertTypeToSeverity(t string) string {
	switch t {
	case "error":
		return inbound.SeverityError
	case "warning":
		return inbound.SeverityWarn
	case "success":
		return inbound.SeverityInfo
	case "info":
		return inbound.SeverityInfo
	default:
		return inbound.SeverityWarn
	}
}

func parseDDTags(tags string) map[string]string {
	out := map[string]string{}
	if tags == "" {
		return out
	}
	// Datadog tags are comma-separated "key:value" pairs, possibly with
	// bare keys (no value) for tag-set membership. Skip those.
	start := 0
	for i := 0; i <= len(tags); i++ {
		if i == len(tags) || tags[i] == ',' {
			pair := tags[start:i]
			start = i + 1
			if pair == "" {
				continue
			}
			colon := -1
			for j := 0; j < len(pair); j++ {
				if pair[j] == ':' {
					colon = j
					break
				}
			}
			if colon < 0 {
				continue
			}
			out[pair[:colon]] = pair[colon+1:]
		}
	}
	return out
}

func firstLine(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			return s[:i]
		}
	}
	return s
}

func init() { inbound.Register(Datadog{}) }
