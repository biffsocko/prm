package webhook_test

import (
	"strings"
	"testing"

	"github.com/biffsocko/prm/internal/webhook"
)

func TestSignAndVerifyRoundTrip(t *testing.T) {
	body := []byte(`{"hello":"world"}`)
	secret := []byte("shhhh")
	sig := webhook.Sign(body, secret, 1700000000)
	hdr := sig.Header()
	if !strings.HasPrefix(hdr, "t=1700000000,v1=") {
		t.Errorf("unexpected header format: %q", hdr)
	}
	if !webhook.Verify(hdr, body, secret) {
		t.Errorf("Verify on identical input should succeed")
	}
}

func TestVerifyRejectsTamperedBody(t *testing.T) {
	body := []byte(`{"hello":"world"}`)
	secret := []byte("shhhh")
	hdr := webhook.Sign(body, secret, 1700000000).Header()
	tampered := []byte(`{"hello":"evil"}`)
	if webhook.Verify(hdr, tampered, secret) {
		t.Errorf("Verify should reject tampered body")
	}
}

func TestVerifyRejectsWrongSecret(t *testing.T) {
	body := []byte(`x`)
	hdr := webhook.Sign(body, []byte("real-secret"), 1700000000).Header()
	if webhook.Verify(hdr, body, []byte("guess-secret")) {
		t.Errorf("Verify should reject wrong secret")
	}
}

func TestVerifyRejectsMalformedHeader(t *testing.T) {
	body := []byte(`x`)
	for _, h := range []string{
		"",
		"junk",
		"t=,v1=abc",
		"t=abc,v1=abc",
		"v1=onlyv",
		"t=1234567890", // no v1
	} {
		if webhook.Verify(h, body, []byte("s")) {
			t.Errorf("Verify should reject malformed header %q", h)
		}
	}
}

func TestSignIsDeterministicForSameInputs(t *testing.T) {
	body := []byte("body")
	secret := []byte("secret")
	a := webhook.Sign(body, secret, 42)
	b := webhook.Sign(body, secret, 42)
	if a.V != b.V {
		t.Errorf("HMAC should be deterministic; got different values")
	}
}

func TestSignDiffersAcrossTimestamps(t *testing.T) {
	body := []byte("body")
	secret := []byte("secret")
	a := webhook.Sign(body, secret, 1)
	b := webhook.Sign(body, secret, 2)
	if a.V == b.V {
		t.Errorf("HMAC should differ across timestamps (replay prevention)")
	}
}
