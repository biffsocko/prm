// Package webhook implements outbound webhook delivery: HMAC signing,
// retry policy, debounce + cooldown + budget enforcement.
//
// The hot path NEVER blocks on this package. Server callers dispatch
// matched events into the worker pool via Manager.Notify; all delivery
// runs on the pool's goroutines.
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/channels"
)

// Payload is what gets HMAC-signed and POSTed to the bot's webhook URL.
//
// Wire shape (JSON):
//
//	{
//	  "event_id":     "<uuid v7 of this fire>",
//	  "subscription_id": "<uuid>",
//	  "tenant_id":   "<uuid>",
//	  "channel_id":  "<uuid>",
//	  "channel_name": "ops",
//	  "ts":           "2026-05-18T03:00:00.000Z",
//	  "matches":      [{"from":"...", "display_name":"...", "ts":"...", "body":"..."}, ...],
//	  "context":      [...same shape as matches...]
//	}
//
// The "matches" array holds the messages that triggered this fire
// (after debounce-batching, typically 1+). The "context" array holds
// the channel's last N messages preceding the first match, for the LLM
// or whatever the bot is doing.
type Payload struct {
	EventID        string    `json:"event_id"`
	SubscriptionID string    `json:"subscription_id"`
	TenantID       string    `json:"tenant_id"`
	ChannelID      string    `json:"channel_id"`
	ChannelName    string    `json:"channel_name"`
	TS             time.Time `json:"ts"`
	Matches        []Message `json:"matches"`
	Context        []Message `json:"context"`
}

// Message is one chat message inside a Payload (either a triggering match
// or a piece of context). Same shape for both arrays for symmetry.
type Message struct {
	From        string    `json:"from"` // account_id
	DisplayName string    `json:"display_name,omitempty"`
	TS          time.Time `json:"ts"`
	Body        string    `json:"body"`
}

// MessageFromHistory converts a channels.HistoryEntry to a wire Message.
func MessageFromHistory(e channels.HistoryEntry) Message {
	return Message{
		From:        e.From.String(),
		DisplayName: e.DisplayName,
		TS:          e.TS,
		Body:        e.Body,
	}
}

// Marshal returns the canonical JSON encoding of a payload (used for HMAC
// signing and for the request body).
func Marshal(p *Payload) ([]byte, error) {
	return json.Marshal(p)
}

// Signature is the value of the PRM-Signature header.
// Format:
//
//	t=<unix-seconds>,v1=<hex-hmac>
//
// where v1 = hex(HMAC-SHA256(secret, "<t>.<body>"))
//
// The leading "t=" timestamp lets receivers reject replays older than a
// tolerance window without verifying the signature first.
type Signature struct {
	T int64
	V string // hex of HMAC-SHA256
}

// Sign computes the signature for a body + secret + timestamp. Timestamp
// is unix-seconds; pass time.Now().Unix() unless re-signing for replay.
func Sign(body, secret []byte, ts int64) Signature {
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%d.", ts)
	mac.Write(body)
	sum := mac.Sum(nil)
	return Signature{T: ts, V: hex.EncodeToString(sum)}
}

// Header returns the PRM-Signature header value for a Signature.
func (s Signature) Header() string {
	return "t=" + strconv.FormatInt(s.T, 10) + ",v1=" + s.V
}

// Verify returns true if the given header value validates against the
// body+secret. Exposed so bot SDKs can use the same code path the server
// signs with. Constant-time compare.
func Verify(header string, body, secret []byte) bool {
	t, v, ok := parseSignatureHeader(header)
	if !ok {
		return false
	}
	want := Sign(body, secret, t).V
	return hmac.Equal([]byte(want), []byte(v))
}

func parseSignatureHeader(h string) (t int64, v string, ok bool) {
	var tStr string
	for _, part := range strings.Split(h, ",") {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "t="):
			tStr = strings.TrimPrefix(part, "t=")
		case strings.HasPrefix(part, "v1="):
			v = strings.TrimPrefix(part, "v1=")
		}
	}
	if tStr == "" || v == "" {
		return 0, "", false
	}
	var err error
	t, err = strconv.ParseInt(tStr, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return t, v, true
}

// NewEventID returns a fresh UUID v7 for the EventID field. Exposed so
// tests can stub it.
func NewEventID() string {
	id, err := uuid.NewV7()
	if err != nil {
		// UUID generation only fails on rand.Read errors, which we treat as
		// fatal in production. Tests can override.
		return ""
	}
	return id.String()
}
