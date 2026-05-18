package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestValidateURL(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
		errSub  string // optional substring
	}{
		{"", true, "required"},
		{"https://bot.example.com/hook", false, ""},
		{"http://localhost:9090/dev", false, ""},
		{"https://", true, "host"},

		// AMQP
		{"amqp://user:pw@rmq:5672/%2f?exchange=alerts&routing_key=ops", false, ""},
		{"amqps://rmq.cloud.example/vh?exchange=&routing_key=q.deploys", false, ""},
		{"amqp://rmq/vh?exchange=alerts", true, "routing_key"},
		{"amqp://?routing_key=q", true, "broker host"},

		// MQTT
		{"mqtt://user:pw@broker:1883/alerts/ops?qos=1", false, ""},
		{"mqtts://broker/topic", false, ""},
		{"mqtt://broker", true, "topic"},
		{"mqtt://broker/topic?qos=7", true, "qos"},

		// Unknown
		{"redis://x/y", true, "unsupported"},
		{"ftp://example.com", true, "unsupported"},
	}
	for _, tc := range cases {
		err := ValidateURL(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ValidateURL(%q) expected error, got nil", tc.in)
				continue
			}
			if tc.errSub != "" && !strings.Contains(err.Error(), tc.errSub) {
				t.Errorf("ValidateURL(%q) err = %v; want substring %q", tc.in, err, tc.errSub)
			}
		} else if err != nil {
			t.Errorf("ValidateURL(%q) unexpected err: %v", tc.in, err)
		}
	}
}

func TestRegistryDispatchesByScheme(t *testing.T) {
	r := newRegistry()
	httpT := newHTTPTransport(&http.Client{Timeout: time.Second})
	amqpT := newAMQPTransport()
	mqttT := newMQTTTransport()
	r.register(httpT)
	r.register(amqpT)
	r.register(mqttT)

	for _, s := range []string{"https", "http", "amqp", "amqps", "mqtt", "mqtts"} {
		if _, ok := r.for_(s); !ok {
			t.Errorf("scheme %q not registered", s)
		}
	}
	if _, ok := r.for_("ftp"); ok {
		t.Errorf("ftp should not be registered")
	}
}

// TestHTTPTransportSignsAndReports verifies the refactored HTTP path
// still: (a) carries the HMAC sig in PRM-Signature header, (b) maps
// 2xx -> OK, 4xx -> Permanent, 5xx -> Transient.
func TestHTTPTransportSignsAndReports(t *testing.T) {
	var mu sync.Mutex
	var lastSig string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		lastSig = r.Header.Get("PRM-Signature")
		_, _ = io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(202)
		case "/bad":
			w.WriteHeader(404)
		default:
			w.WriteHeader(500)
		}
	}))
	t.Cleanup(srv.Close)

	tr := newHTTPTransport(&http.Client{Timeout: 5 * time.Second})
	body := []byte(`{"ping":1}`)
	secret := []byte("s3cret")
	sig := Sign(body, secret, time.Now().Unix())

	// OK
	res := tr.Send(context.Background(), Target{URL: srv.URL + "/ok"}, body, sig)
	if res.Kind != DeliveryOK {
		t.Errorf("expected DeliveryOK, got %v (%s)", res.Kind, res.StatusDetail)
	}
	mu.Lock()
	got := lastSig
	mu.Unlock()
	if got != sig.Header() {
		t.Errorf("PRM-Signature mismatch: got %q want %q", got, sig.Header())
	}

	// 4xx permanent
	res = tr.Send(context.Background(), Target{URL: srv.URL + "/bad"}, body, sig)
	if res.Kind != DeliveryPermanent {
		t.Errorf("4xx should be permanent; got %v (%s)", res.Kind, res.StatusDetail)
	}

	// 5xx transient
	res = tr.Send(context.Background(), Target{URL: srv.URL + "/boom"}, body, sig)
	if res.Kind != DeliveryTransient {
		t.Errorf("5xx should be transient; got %v (%s)", res.Kind, res.StatusDetail)
	}
}

// TestMQTTEnvelopeRoundTrip verifies the MQTT envelope shape: callers
// receive {prm_signature, prm_subscription_id, payload_b64} where
// base64-decoding payload_b64 yields the exact bytes a HTTP transport
// would have POSTed. This is the contract bots verify against.
func TestMQTTEnvelopeRoundTrip(t *testing.T) {
	originalPayload := []byte(`{"event":"deploy","details":{"svc":"auth-api"}}`)
	secret := []byte("hmac-secret")
	sig := Sign(originalPayload, secret, 1700000000)

	env := mqttEnvelope{
		Signature:      sig.Header(),
		SubscriptionID: "sub-uuid-here",
		PayloadB64:     base64.StdEncoding.EncodeToString(originalPayload),
	}
	wire, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}

	// Receiver-side: parse envelope, base64-decode payload, re-sign and
	// constant-time compare.
	var got mqttEnvelope
	if err := json.Unmarshal(wire, &got); err != nil {
		t.Fatal(err)
	}
	bodyBack, err := base64.StdEncoding.DecodeString(got.PayloadB64)
	if err != nil {
		t.Fatal(err)
	}
	if string(bodyBack) != string(originalPayload) {
		t.Fatalf("base64 round-trip mismatch")
	}
	// Independently recompute the HMAC and check it matches the envelope
	// signature. We're not calling Verify because that needs the ts/v1
	// header parse, but the principle is the same.
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte("1700000000."))
	mac.Write(bodyBack)
	wantHex := hex.EncodeToString(mac.Sum(nil))
	expectHeader := "t=1700000000,v1=" + wantHex
	if got.Signature != expectHeader {
		t.Fatalf("signature mismatch: got %q want %q", got.Signature, expectHeader)
	}
}
