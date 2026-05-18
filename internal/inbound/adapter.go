// Package inbound is the framework for inbound integrations: external
// systems (Splunk, Graylog, GitHub, etc.) POST to
// /v1/inbound/{integration_id}, an adapter normalizes the payload to a
// common Event shape, and PRM republishes onto the bound channel.
//
// Adapters are stateless. New adapters add a file under
// internal/inbound/adapters/ and register via inbound.Register.
package inbound

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Adapter normalizes an inbound HTTP request into a common Event. Adapters
// are stateless; per-integration config (e.g., JSON paths for the generic
// adapter) is supplied as a settings JSON blob at Normalize time.
type Adapter interface {
	// Name returns the adapter identifier as stored in
	// storage.Integration.Adapter. Must be stable across versions.
	Name() string
	// Normalize parses the HTTP body + headers and emits an Event.
	// settings is the integration's SettingsJSON (may be empty).
	Normalize(body []byte, headers http.Header, settings []byte) (Event, error)
}

// Event is the common shape every adapter emits. PRM republishes this as
// a chat message on the bound channel; structured Fields are also kept
// for bot consumption.
type Event struct {
	// Source identifies which adapter produced this Event. Echoed into
	// the channel summary and the structured fields.
	Source string `json:"source"`
	// Service is a short identifier for the affected system. Adapters
	// typically pull this from the upstream payload (e.g.,
	// Splunk: result.service; Graylog: event.fields.service).
	Service string `json:"service,omitempty"`
	// Severity is the normalized urgency: "info" | "warn" | "error" |
	// "critical". Adapters translate upstream conventions into one of
	// these four buckets.
	Severity string `json:"severity,omitempty"`
	// Summary is a short, human-readable one-liner. This is what shows
	// up in chat as the broadcast Msg.Body. Keep it under ~200 chars.
	Summary string `json:"summary"`
	// Fields is the structured payload, preserved from the upstream
	// (modulo adapter-specific shaping). Bots that need the raw
	// numbers/IDs/etc. read these.
	Fields map[string]any `json:"fields,omitempty"`
	// OccurredAt is the upstream-reported timestamp if present, else
	// the server's clock at receive time.
	OccurredAt time.Time `json:"occurred_at"`
	// Raw is the original request body, retained for debugging /
	// audit. Adapters should pass this through unmodified.
	Raw json.RawMessage `json:"-"`
}

// Sentinel errors. Inbound HTTP handlers map these to status codes.
var (
	ErrAdapterUnknown   = errors.New("inbound: unknown adapter")
	ErrAdapterBadInput  = errors.New("inbound: bad input")
	ErrAdapterMissing   = errors.New("inbound: missing required field")
	ErrAdapterRateLimit = errors.New("inbound: rate limited")
)

// SeverityInfo / Warn / Error / Critical are the canonical normalized
// severity strings adapters emit. Other values are allowed but bots
// that filter by severity should expect these four.
const (
	SeverityInfo     = "info"
	SeverityWarn     = "warn"
	SeverityError    = "error"
	SeverityCritical = "critical"
)

// --- registry ---

var (
	registryMu sync.RWMutex
	registry   = map[string]Adapter{}
)

// Register makes an adapter available under its Name. Re-registering the
// same name replaces the previous entry. Safe for concurrent use.
func Register(a Adapter) {
	registryMu.Lock()
	registry[a.Name()] = a
	registryMu.Unlock()
}

// Lookup returns the adapter with the given name, or
// ErrAdapterUnknown.
func Lookup(name string) (Adapter, error) {
	registryMu.RLock()
	a, ok := registry[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrAdapterUnknown, name)
	}
	return a, nil
}

// ListNames returns a snapshot of registered adapter names. Order is
// not guaranteed.
func ListNames() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	return out
}

// --- helpers shared by adapter implementations ---

// Truncate caps a string at n runes (not bytes) with a trailing "..."
// when truncated. Adapters use this on Summary to keep chat readable.
func Truncate(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-3]) + "..."
}

// NormalizeSeverity maps an arbitrary upstream label to one of the four
// canonical buckets. Unknown labels default to "warn" (the safe middle).
// Adapters can override per-source rules before calling this.
func NormalizeSeverity(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "info", "informational", "notice", "low":
		return SeverityInfo
	case "warn", "warning", "medium":
		return SeverityWarn
	case "err", "error", "high":
		return SeverityError
	case "crit", "critical", "fatal", "emerg", "emergency":
		return SeverityCritical
	default:
		return SeverityWarn
	}
}
