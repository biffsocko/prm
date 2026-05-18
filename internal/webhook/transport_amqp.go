package webhook

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// amqpTransport publishes the signed PRM payload to an AMQP 0.9.1
// broker (RabbitMQ and compatible).
//
// URL shape:
//
//	amqp://user:pass@host:5672/vhost?exchange=alerts&routing_key=ops.deploy
//	amqps://...                              (TLS)
//
// Query params:
//   - exchange     : AMQP exchange to publish to. Empty string is the
//                    default exchange, in which case routing_key MUST
//                    be a queue name. Required if you want to fan out.
//   - routing_key  : routing key for the publish; queue name when
//                    using the default exchange. Required.
//   - mandatory    : "true" sets the AMQP mandatory flag (broker
//                    returns a basic.return if no queue is bound).
//                    Default "false".
//   - persistent   : "true" marks the message as DeliveryMode 2
//                    (broker persists it to disk). Default "true"
//                    because PRM webhooks are state-changing events.
//
// Signature: the HMAC signature is carried in the message Headers
// table as `prm-signature` (string). Body is the raw signed payload
// JSON bytes. Same payload as the HTTP transport sends.
//
// Connection pooling: one Connection + one Channel per (host, user,
// vhost) tuple, lazily opened on first delivery to that broker.
// Publisher confirms are enabled so Send waits for an ACK before
// returning OK; a NACK or close-during-confirm becomes transient.
// On any connection error the pool entry is discarded; the next
// delivery reopens.
type amqpTransport struct {
	mu    sync.Mutex
	conns map[string]*amqpConn
	// dialTimeout overrides the package default for tests.
	dialTimeout time.Duration
}

type amqpConn struct {
	conn    *amqp.Connection
	ch      *amqp.Channel
	confirm chan amqp.Confirmation
}

func newAMQPTransport() *amqpTransport {
	return &amqpTransport{
		conns:       map[string]*amqpConn{},
		dialTimeout: transportDialTimeout,
	}
}

func (amqpTransport) Schemes() []string { return []string{"amqp", "amqps"} }

func (a *amqpTransport) Send(ctx context.Context, t Target, body []byte, sig Signature) DeliveryResult {
	u, err := url.Parse(t.URL)
	if err != nil {
		return DeliveryResult{Kind: DeliveryPermanent, StatusDetail: "url parse", Err: err}
	}
	q := u.Query()
	exchange := q.Get("exchange")
	routingKey := q.Get("routing_key")
	if routingKey == "" {
		return DeliveryResult{Kind: DeliveryPermanent, StatusDetail: "missing routing_key"}
	}
	mandatory := strings.EqualFold(q.Get("mandatory"), "true")
	persistent := !strings.EqualFold(q.Get("persistent"), "false") // default true

	c, err := a.getOrDial(u)
	if err != nil {
		return DeliveryResult{Kind: DeliveryTransient, StatusDetail: "amqp dial", Err: err}
	}

	deliveryMode := uint8(amqp.Transient)
	if persistent {
		deliveryMode = amqp.Persistent
	}
	pub := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: deliveryMode,
		Timestamp:    time.Now().UTC(),
		MessageId:    sig.Header(),
		Headers: amqp.Table{
			"prm-signature":       sig.Header(),
			"prm-subscription-id": t.SubscriptionID,
		},
		Body: body,
	}

	pubCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := c.ch.PublishWithContext(pubCtx, exchange, routingKey, mandatory, false, pub); err != nil {
		// Publish failure usually means the channel is broken; drop
		// the cached conn so the next attempt reopens.
		a.dropConn(amqpKey(u))
		return DeliveryResult{Kind: DeliveryTransient, StatusDetail: "amqp publish", Err: err}
	}

	// Wait for publisher confirm. A nack or a closed channel is
	// transient (broker chose not to enqueue, e.g. memory backpressure).
	select {
	case conf, ok := <-c.confirm:
		if !ok {
			a.dropConn(amqpKey(u))
			return DeliveryResult{Kind: DeliveryTransient, StatusDetail: "amqp confirm channel closed"}
		}
		if !conf.Ack {
			return DeliveryResult{Kind: DeliveryTransient, StatusDetail: "amqp nack"}
		}
		return DeliveryResult{Kind: DeliveryOK, StatusDetail: "amqp ack"}
	case <-pubCtx.Done():
		a.dropConn(amqpKey(u))
		return DeliveryResult{Kind: DeliveryTransient, StatusDetail: "amqp confirm timeout", Err: pubCtx.Err()}
	}
}

func (a *amqpTransport) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	var firstErr error
	for k, c := range a.conns {
		if c.ch != nil {
			if err := c.ch.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if c.conn != nil && !c.conn.IsClosed() {
			if err := c.conn.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		delete(a.conns, k)
	}
	return firstErr
}

// getOrDial returns a pooled connection for the broker URL, opening
// one if needed. The pool key intentionally ignores query params so
// many subscriptions to the same broker share one connection.
func (a *amqpTransport) getOrDial(u *url.URL) (*amqpConn, error) {
	key := amqpKey(u)
	a.mu.Lock()
	defer a.mu.Unlock()
	if c, ok := a.conns[key]; ok {
		if !c.conn.IsClosed() {
			return c, nil
		}
		// Stale; recreate below.
		delete(a.conns, key)
	}

	dialURL := *u
	dialURL.RawQuery = ""
	cfg := amqp.Config{Dial: amqp.DefaultDial(a.dialTimeout)}
	if strings.EqualFold(u.Scheme, "amqps") {
		cfg.TLSClientConfig = &tls.Config{ServerName: u.Hostname()}
	}
	conn, err := amqp.DialConfig(dialURL.String(), cfg)
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("amqp channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("amqp confirm mode: %w", err)
	}
	confirm := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	c := &amqpConn{conn: conn, ch: ch, confirm: confirm}
	a.conns[key] = c
	return c, nil
}

func (a *amqpTransport) dropConn(key string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c, ok := a.conns[key]
	if !ok {
		return
	}
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil && !c.conn.IsClosed() {
		_ = c.conn.Close()
	}
	delete(a.conns, key)
}

// amqpKey is the pool key. We deliberately ignore the query string so
// many subscriptions hitting the same broker reuse one connection.
// Different vhosts (different path) get different connections, which
// matches AMQP's connection-per-vhost convention.
func amqpKey(u *url.URL) string {
	user := ""
	if u.User != nil {
		user = u.User.Username()
	}
	return strings.ToLower(u.Scheme) + "://" + user + "@" + u.Host + u.Path
}
