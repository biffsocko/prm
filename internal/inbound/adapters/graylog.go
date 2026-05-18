package adapters

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/biffsocko/prm/internal/inbound"
)

// Graylog adapter. Graylog's HTTP Notification (Event Definitions) POSTs
// a JSON object shaped like:
//
//	{
//	  "event_definition_id":    "...",
//	  "event_definition_type":  "aggregation-v1",
//	  "event_definition_title": "Auth API error rate",
//	  "event": {
//	    "timestamp": "2026-05-18T03:00:00.000Z",
//	    "message":   "Auth API error rate > 5/min",
//	    "fields":    { "service": "auth-api", "level": "ERROR" },
//	    "priority":  3
//	  }
//	}
//
// Graylog's `event.priority` is 1=LOW, 2=NORMAL, 3=HIGH. We translate
// 1->info, 2->warning, 3->error per the convention in DESIGN.md.
type Graylog struct{}

func (Graylog) Name() string { return "graylog" }

type graylogPayload struct {
	EventDefinitionID    string        `json:"event_definition_id"`
	EventDefinitionType  string        `json:"event_definition_type"`
	EventDefinitionTitle string        `json:"event_definition_title"`
	Event                graylogInner  `json:"event"`
}

type graylogInner struct {
	Timestamp time.Time      `json:"timestamp"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields"`
	Priority  int            `json:"priority"`
}

type graylogSettings struct {
	ServiceField string `json:"service_field"` // default "service"
}

func (Graylog) Normalize(body []byte, _ http.Header, settingsJSON []byte) (inbound.Event, error) {
	var p graylogPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return inbound.Event{}, fmt.Errorf("%w: parse graylog payload: %v", inbound.ErrAdapterBadInput, err)
	}
	if p.Event.Message == "" && p.EventDefinitionTitle == "" {
		return inbound.Event{}, fmt.Errorf("%w: event.message or event_definition_title", inbound.ErrAdapterMissing)
	}

	var s graylogSettings
	if len(settingsJSON) > 0 {
		_ = json.Unmarshal(settingsJSON, &s)
	}
	serviceKey := s.ServiceField
	if serviceKey == "" {
		serviceKey = "service"
	}

	service := ""
	if p.Event.Fields != nil {
		if v, ok := p.Event.Fields[serviceKey].(string); ok {
			service = v
		}
	}

	severity := graylogPriorityToSeverity(p.Event.Priority)
	if p.Event.Fields != nil {
		if v, ok := p.Event.Fields["level"].(string); ok {
			severity = inbound.NormalizeSeverity(v)
		}
	}

	summary := p.Event.Message
	if summary == "" {
		summary = p.EventDefinitionTitle
	}
	summary = inbound.Truncate(summary, 200)

	fields := map[string]any{}
	for k, v := range p.Event.Fields {
		fields[k] = v
	}
	if p.EventDefinitionID != "" {
		fields["event_definition_id"] = p.EventDefinitionID
	}
	if p.EventDefinitionType != "" {
		fields["event_definition_type"] = p.EventDefinitionType
	}
	if p.EventDefinitionTitle != "" {
		fields["event_definition_title"] = p.EventDefinitionTitle
	}
	fields["priority"] = p.Event.Priority

	ts := p.Event.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	return inbound.Event{
		Source:     "graylog",
		Service:    service,
		Severity:   severity,
		Summary:    summary,
		Fields:     fields,
		OccurredAt: ts,
		Raw:        json.RawMessage(body),
	}, nil
}

func graylogPriorityToSeverity(p int) string {
	switch p {
	case 1:
		return inbound.SeverityInfo
	case 2:
		return inbound.SeverityWarn
	case 3:
		return inbound.SeverityError
	default:
		return inbound.SeverityWarn
	}
}

func init() {
	inbound.Register(Graylog{})
}
