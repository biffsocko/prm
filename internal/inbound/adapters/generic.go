package adapters

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/biffsocko/prm/internal/inbound"
)

// Generic adapter. For any system that POSTs JSON. Configured via the
// integration's settings_json with dotted-path field selectors:
//
//	{
//	  "service_path":  "$.alert.service",        // dotted path; "$" is root
//	  "severity_path": "$.alert.severity",       // optional
//	  "summary_path":  "$.alert.summary",        // required
//	  "occurred_at_path": "$.alert.timestamp",   // optional; falls back to now
//	  "severity_map": {"P1": "critical", "P2":"error"}, // optional override
//	  "include_raw":  true                       // optional; default false
//	}
//
// Paths use a tiny JSON-path subset:  "$.foo.bar.baz" or "$.items.0.name".
// No wildcards / no predicates -- it's a literal traversal. If a path
// resolves to a non-string for service/severity/summary, we coerce to
// string via fmt.Sprintf("%v", ...).
type Generic struct{}

func (Generic) Name() string { return "generic" }

type genericSettings struct {
	ServicePath    string            `json:"service_path"`
	SeverityPath   string            `json:"severity_path"`
	SummaryPath    string            `json:"summary_path"`
	OccurredAtPath string            `json:"occurred_at_path"`
	SeverityMap    map[string]string `json:"severity_map"`
	IncludeRaw     bool              `json:"include_raw"`
}

func (Generic) Normalize(body []byte, _ http.Header, settingsJSON []byte) (inbound.Event, error) {
	var s genericSettings
	if len(settingsJSON) > 0 {
		if err := json.Unmarshal(settingsJSON, &s); err != nil {
			return inbound.Event{}, fmt.Errorf("%w: parse settings: %v", inbound.ErrAdapterBadInput, err)
		}
	}
	if s.SummaryPath == "" {
		return inbound.Event{}, fmt.Errorf("%w: settings.summary_path", inbound.ErrAdapterMissing)
	}

	var root any
	if err := json.Unmarshal(body, &root); err != nil {
		return inbound.Event{}, fmt.Errorf("%w: parse body: %v", inbound.ErrAdapterBadInput, err)
	}

	summary := pathToString(root, s.SummaryPath)
	if summary == "" {
		return inbound.Event{}, fmt.Errorf("%w: summary_path resolved to empty", inbound.ErrAdapterMissing)
	}
	summary = inbound.Truncate(summary, 200)

	service := ""
	if s.ServicePath != "" {
		service = pathToString(root, s.ServicePath)
	}

	severity := ""
	if s.SeverityPath != "" {
		raw := pathToString(root, s.SeverityPath)
		if mapped, ok := s.SeverityMap[raw]; ok {
			severity = inbound.NormalizeSeverity(mapped)
		} else {
			severity = inbound.NormalizeSeverity(raw)
		}
	}

	ts := time.Now().UTC()
	if s.OccurredAtPath != "" {
		if str := pathToString(root, s.OccurredAtPath); str != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, str); err == nil {
				ts = parsed.UTC()
			} else if parsed, err := time.Parse(time.RFC3339, str); err == nil {
				ts = parsed.UTC()
			}
		}
	}

	// Flatten the top-level object into Fields. If root isn't a map (e.g.,
	// an array), wrap it.
	fields := map[string]any{}
	if m, ok := root.(map[string]any); ok {
		for k, v := range m {
			fields[k] = v
		}
	} else {
		fields["body"] = root
	}

	var raw json.RawMessage
	if s.IncludeRaw {
		raw = json.RawMessage(body)
	}

	return inbound.Event{
		Source:     "generic",
		Service:    service,
		Severity:   severity,
		Summary:    summary,
		Fields:     fields,
		OccurredAt: ts,
		Raw:        raw,
	}, nil
}

// pathToString resolves a tiny "$.a.b.0.c" path against a parsed JSON
// value and stringifies the result. Returns "" on miss.
func pathToString(root any, path string) string {
	v := pathLookup(root, path)
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	default:
		return fmt.Sprintf("%v", v)
	}
}

func pathLookup(root any, path string) any {
	if path == "" || path == "$" {
		return root
	}
	if !strings.HasPrefix(path, "$.") {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(path, "$."), ".")
	cur := root
	for _, p := range parts {
		switch v := cur.(type) {
		case map[string]any:
			cur = v[p]
		case []any:
			i, err := parseUint(p)
			if err != nil || i >= len(v) {
				return nil
			}
			cur = v[i]
		default:
			return nil
		}
	}
	return cur
}

func parseUint(s string) (int, error) {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("not numeric")
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

func init() {
	inbound.Register(Generic{})
}
