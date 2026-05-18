package webhook

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// mqttTransport publishes the signed PRM payload to an MQTT 3.1.1
// broker. (We avoid MQTT 5 user properties so the same envelope works
// on every broker; bots can verify HMAC without knowing the protocol
// version of the broker.)
//
// URL shape:
//
//	mqtt://user:pass@broker:1883/topic/path?qos=1
//	mqtts://...                              (TLS)
//
// Path syntax: the URL Path becomes the MQTT topic (leading "/" is
// stripped). MQTT topic levels use "/" as their separator already, so
// `/alerts/ops/auth-api` is published to topic `alerts/ops/auth-api`.
// To use other separators, URL-encode them.
//
// Query params:
//   - qos        : 0, 1, or 2. Default 1 (at least once -- the right
//                  default for state-changing alerts).
//   - retain     : "true" to set the MQTT retain flag. Default false.
//   - client_id  : override the auto-derived client id (defaults to
//                  "prmd-<short-subscription-id>"). Useful when your
//                  broker rejects duplicate client ids and you have
//                  multiple PRM nodes publishing.
//
// Signature carriage:
//
//	Because MQTT 3.1.1 has no headers, the payload is wrapped in an
//	envelope JSON:
//
//	  {
//	    "prm_signature": "t=<unix>,v1=<hex>",
//	    "prm_subscription_id": "<uuid>",
//	    "payload_b64": "<base64 of the raw signed payload bytes>"
//	  }
//
//	Bots decode payload_b64 to recover the exact JSON bytes the
//	HTTP transport would have POSTed, then verify prm_signature
//	against those bytes using the existing webhook.Verify rules.
//	Documented in docs/WEBHOOKS.md under "MQTT transport".
//
// Connection pooling: one client per (host, user, client-id) key.
// Lazy connect; on connection-lost the paho client auto-reconnects.
// Successful PUBACK for QoS 1/2 is required for DeliveryOK.
type mqttTransport struct {
	mu      sync.Mutex
	clients map[string]mqtt.Client
	// dialTimeout caps the initial broker connect attempt.
	dialTimeout time.Duration
}

func newMQTTTransport() *mqttTransport {
	return &mqttTransport{
		clients:     map[string]mqtt.Client{},
		dialTimeout: transportDialTimeout,
	}
}

func (mqttTransport) Schemes() []string { return []string{"mqtt", "mqtts"} }

func (m *mqttTransport) Send(ctx context.Context, t Target, body []byte, sig Signature) DeliveryResult {
	u, err := url.Parse(t.URL)
	if err != nil {
		return DeliveryResult{Kind: DeliveryPermanent, StatusDetail: "url parse", Err: err}
	}
	topic := strings.TrimPrefix(u.Path, "/")
	if topic == "" {
		return DeliveryResult{Kind: DeliveryPermanent, StatusDetail: "missing topic"}
	}
	q := u.Query()
	qos := byte(1)
	if s := q.Get("qos"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 || n > 2 {
			return DeliveryResult{Kind: DeliveryPermanent, StatusDetail: "invalid qos"}
		}
		qos = byte(n)
	}
	retain := strings.EqualFold(q.Get("retain"), "true")

	envelope := mqttEnvelope{
		Signature:      sig.Header(),
		SubscriptionID: t.SubscriptionID,
		PayloadB64:     base64.StdEncoding.EncodeToString(body),
	}
	envBytes, err := json.Marshal(envelope)
	if err != nil {
		return DeliveryResult{Kind: DeliveryPermanent, StatusDetail: "envelope marshal", Err: err}
	}

	client, err := m.getOrConnect(u, t.SubscriptionID)
	if err != nil {
		return DeliveryResult{Kind: DeliveryTransient, StatusDetail: "mqtt connect", Err: err}
	}

	// Publish returns a Token; we wait on it under the parent context's
	// deadline so a hung broker doesn't pin a worker goroutine.
	pubCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	tok := client.Publish(topic, qos, retain, envBytes)
	done := make(chan struct{})
	go func() { tok.Wait(); close(done) }()
	select {
	case <-done:
		if err := tok.Error(); err != nil {
			m.dropClient(mqttKey(u, t.SubscriptionID))
			return DeliveryResult{Kind: DeliveryTransient, StatusDetail: "mqtt publish", Err: err}
		}
		return DeliveryResult{Kind: DeliveryOK, StatusDetail: fmt.Sprintf("mqtt qos %d", qos)}
	case <-pubCtx.Done():
		m.dropClient(mqttKey(u, t.SubscriptionID))
		return DeliveryResult{Kind: DeliveryTransient, StatusDetail: "mqtt publish timeout", Err: pubCtx.Err()}
	}
}

func (m *mqttTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, c := range m.clients {
		if c.IsConnected() {
			c.Disconnect(250)
		}
		delete(m.clients, k)
	}
	return nil
}

func (m *mqttTransport) getOrConnect(u *url.URL, subscriptionID string) (mqtt.Client, error) {
	key := mqttKey(u, subscriptionID)
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.clients[key]; ok && c.IsConnected() {
		return c, nil
	}
	delete(m.clients, key)

	opts := mqtt.NewClientOptions()
	brokerScheme := "tcp"
	if strings.EqualFold(u.Scheme, "mqtts") {
		brokerScheme = "ssl"
		opts.SetTLSConfig(&tls.Config{ServerName: u.Hostname()})
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		// Default ports.
		if brokerScheme == "ssl" {
			host += ":8883"
		} else {
			host += ":1883"
		}
	}
	opts.AddBroker(brokerScheme + "://" + host)
	if u.User != nil {
		opts.SetUsername(u.User.Username())
		if pw, ok := u.User.Password(); ok {
			opts.SetPassword(pw)
		}
	}
	clientID := u.Query().Get("client_id")
	if clientID == "" {
		shortID := subscriptionID
		if len(shortID) > 8 {
			shortID = shortID[:8]
		}
		clientID = "prmd-" + shortID
	}
	opts.SetClientID(clientID)
	opts.SetAutoReconnect(true)
	opts.SetConnectTimeout(m.dialTimeout)
	opts.SetCleanSession(true)
	opts.SetOrderMatters(false)

	c := mqtt.NewClient(opts)
	tok := c.Connect()
	done := make(chan struct{})
	go func() { tok.Wait(); close(done) }()
	select {
	case <-done:
		if err := tok.Error(); err != nil {
			return nil, fmt.Errorf("mqtt connect: %w", err)
		}
	case <-time.After(m.dialTimeout + time.Second):
		return nil, fmt.Errorf("mqtt connect timeout")
	}
	m.clients[key] = c
	return c, nil
}

func (m *mqttTransport) dropClient(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.clients[key]
	if !ok {
		return
	}
	if c.IsConnected() {
		c.Disconnect(100)
	}
	delete(m.clients, key)
}

// mqttKey pools clients per broker+identity. Multiple subscriptions
// landing on the same broker can share one client (different topics
// are fine through the same client) provided they don't override
// client_id. When client_id IS overridden, we key on it to allow
// per-subscription identity isolation -- a common MQTT pattern when
// brokers reject duplicate client ids.
func mqttKey(u *url.URL, subscriptionID string) string {
	user := ""
	if u.User != nil {
		user = u.User.Username()
	}
	cid := u.Query().Get("client_id")
	if cid == "" {
		// Pool across subscriptions hitting the same broker+user.
		return strings.ToLower(u.Scheme) + "://" + user + "@" + u.Host
	}
	return strings.ToLower(u.Scheme) + "://" + user + "@" + u.Host + "/" + cid
}

// mqttEnvelope is the JSON wrapper that carries the HMAC signature
// alongside the original signed payload for transports that lack a
// header-like channel.
type mqttEnvelope struct {
	Signature      string `json:"prm_signature"`
	SubscriptionID string `json:"prm_subscription_id"`
	PayloadB64     string `json:"payload_b64"`
}
