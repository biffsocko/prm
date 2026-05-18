package webhook

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Transport is the abstraction over webhook delivery channels. HTTP is
// the default and the only required implementation; AMQP and MQTT are
// alternative transports for environments that already standardize on
// a message broker.
//
// A subscription's URL scheme selects the transport:
//
//	https://bot.example.com/webhook          -> HTTP POST (default)
//	http://localhost:9090/dev-only           -> HTTP POST (allowed for dev)
//	amqp://user:pass@broker:5672/vhost?exchange=alerts&routing_key=ops
//	                                         -> AMQP publish
//	amqps://...                              -> AMQP publish over TLS
//	mqtt://user:pass@broker:1883/topic/path?qos=1
//	                                         -> MQTT publish
//	mqtts://...                              -> MQTT publish over TLS
//
// All transports receive the same signed PRM payload bytes and report
// success/failure with the same retry semantics (transient -> retry
// with exponential backoff; permanent -> auto-disable thresholds).
// Transport-specific carry-channels for the HMAC signature are
// documented per implementation.
type Transport interface {
	// Scheme returns the URL scheme(s) this transport handles, e.g.
	// "https" or "amqps". Used by the registry to dispatch.
	Schemes() []string

	// Send performs one delivery attempt. Returns:
	//   - kind == DeliveryOK            : delivered; record + stop retrying.
	//   - kind == DeliveryTransient     : transient failure (network,
	//                                     5xx, broker reconnect); retry
	//                                     per the worker's backoff loop.
	//   - kind == DeliveryPermanent     : permanent (auth, 4xx, bad URL);
	//                                     do NOT retry; counts toward the
	//                                     auto-disable threshold.
	//
	// statusDetail is a short human-readable explanation suitable for
	// the SubscriptionFire.LastError column. err, when non-nil, is the
	// underlying transport error; it is logged but not retried beyond
	// the kind directive.
	Send(ctx context.Context, t Target, body []byte, sig Signature) DeliveryResult

	// Close releases any persistent connections, channels, or
	// goroutines held by the transport. Called from Manager.Stop.
	Close() error
}

// DeliveryKind classifies the outcome of one Send attempt.
type DeliveryKind int

const (
	DeliveryOK DeliveryKind = iota
	DeliveryTransient
	DeliveryPermanent
)

// DeliveryResult is what Send returns. StatusDetail is short text
// (HTTP status, AMQP error code, MQTT connack reason) recorded with
// the subscription fire for operator diagnostics.
type DeliveryResult struct {
	Kind         DeliveryKind
	StatusDetail string
	Err          error
}

// Target carries the subscription's destination URL plus the
// per-subscription identity that some transports need (e.g. MQTT
// uses subscription ID as the default client id).
type Target struct {
	URL            string
	SubscriptionID string
}

// schemeOf returns the lowercased URL scheme, or "" if the URL is
// unparseable. Used by both the registry and subops URL validation.
func schemeOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Scheme)
}

// ValidateURL is called by subops at subscription-create time. It
// rejects URLs whose scheme has no registered transport, or whose
// shape is missing transport-required fields (e.g. an amqp URL with
// no routing key in a context that needs one). Errors are returned
// as plain strings so subops can wrap them in its own error sentinel.
func ValidateURL(rawURL string) error {
	if rawURL == "" {
		return fmt.Errorf("url is required")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("malformed url: %v", err)
	}
	scheme := strings.ToLower(u.Scheme)
	switch scheme {
	case "https", "http":
		if u.Host == "" {
			return fmt.Errorf("http(s) url is missing host")
		}
		return nil
	case "amqp", "amqps":
		if u.Host == "" {
			return fmt.Errorf("amqp url is missing broker host")
		}
		q := u.Query()
		exchange := q.Get("exchange")
		routingKey := q.Get("routing_key")
		// Either (exchange + routing_key) OR (default exchange "" +
		// routing_key = queue name). routing_key must be present.
		if routingKey == "" {
			return fmt.Errorf("amqp url is missing ?routing_key= (the queue name when no exchange is given)")
		}
		_ = exchange
		return nil
	case "mqtt", "mqtts":
		if u.Host == "" {
			return fmt.Errorf("mqtt url is missing broker host")
		}
		topic := strings.TrimPrefix(u.Path, "/")
		if topic == "" {
			return fmt.Errorf("mqtt url is missing topic in path (e.g. mqtt://broker/topic/path)")
		}
		if q := u.Query().Get("qos"); q != "" {
			switch q {
			case "0", "1", "2":
			default:
				return fmt.Errorf("mqtt qos must be 0, 1, or 2; got %q", q)
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported url scheme %q (supported: https, http, amqp, amqps, mqtt, mqtts)", scheme)
	}
}

// registry holds the active Transport implementations keyed by
// scheme. Transports register themselves via Register on package init
// or are wired explicitly by the manager.
type registry struct {
	mu   sync.RWMutex
	byScheme map[string]Transport
}

func newRegistry() *registry {
	return &registry{byScheme: map[string]Transport{}}
}

func (r *registry) register(t Transport) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range t.Schemes() {
		r.byScheme[strings.ToLower(s)] = t
	}
}

func (r *registry) for_(scheme string) (Transport, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.byScheme[strings.ToLower(scheme)]
	return t, ok
}

func (r *registry) all() []Transport {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := map[Transport]struct{}{}
	out := make([]Transport, 0, len(r.byScheme))
	for _, t := range r.byScheme {
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

func (r *registry) closeAll() error {
	var firstErr error
	for _, t := range r.all() {
		if err := t.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// transportDialTimeout is the default for opening a new broker
// connection (AMQP / MQTT). HTTP uses the http.Client's Timeout.
const transportDialTimeout = 10 * time.Second
