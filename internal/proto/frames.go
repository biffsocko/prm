// Package proto defines the PRM wire-protocol frame types and a JSON-line
// codec. Every frame is a JSON object terminated by a newline; the "type"
// field selects the schema for the rest of the fields.
//
// Slice 1 verbs: hello, welcome, auth_request, auth_challenge,
// auth_response, auth_ok, auth_err, join, part, msg, presence, ping, pong,
// error.
package proto

import (
	"encoding/json"
	"time"
)

// Frame is anything that can be sent over the wire.
type Frame interface {
	FrameType() string
}

// Verb type constants.
const (
	TypeHello                = "hello"
	TypeWelcome              = "welcome"
	TypeAuthRequest          = "auth_request"
	TypeAuthChallenge        = "auth_challenge"
	TypeAuthResponse         = "auth_response"
	TypeAuthOK               = "auth_ok"
	TypeAuthErr              = "auth_err"
	TypeJoin                 = "join"
	TypePart                 = "part"
	TypeMsg                  = "msg"
	TypePresence             = "presence"
	TypePing                 = "ping"
	TypePong                 = "pong"
	TypeError                = "error"
	TypeSubscriptionCreate   = "subscription_create"
	TypeSubscriptionList     = "subscription_list"
	TypeSubscriptionGet      = "subscription_get"
	TypeSubscriptionUpdate   = "subscription_update"
	TypeSubscriptionDelete   = "subscription_delete"
	TypeSubscriptionOK       = "subscription_ok"
	TypeSubscriptionListOK   = "subscription_list_ok"
	TypeSubscriptionDeleted  = "subscription_deleted"
)

// Auth methods.
const (
	AuthMethodPassword = "password"
	AuthMethodToken    = "token"
)

// Presence kinds.
const (
	PresenceJoin = "join"
	PresencePart = "part"
)

// Hello is the first frame sent by the client after TLS handshake.
// It advertises client capabilities and is followed by a Welcome from the server.
type Hello struct {
	Type          string   `json:"type"`
	ID            string   `json:"id,omitempty"`
	ClientName    string   `json:"client_name,omitempty"`
	ClientVersion string   `json:"client_version,omitempty"`
	CapVersion    string   `json:"cap_version"`
	Capabilities  []string `json:"capabilities,omitempty"`
}

func (Hello) FrameType() string { return TypeHello }

// Welcome is the server's reply to Hello. It echoes the negotiated capability
// version and the union of capabilities both sides agreed to.
type Welcome struct {
	Type          string   `json:"type"`
	ID            string   `json:"id,omitempty"`
	ServerName    string   `json:"server_name"`
	ServerVersion string   `json:"server_version"`
	CapVersion    string   `json:"cap_version"`
	Capabilities  []string `json:"capabilities,omitempty"`
}

func (Welcome) FrameType() string { return TypeWelcome }

// AuthRequest begins the auth handshake. The Tenant field is required for
// password method and ignored for token method (token carries tenant binding
// internally).
type AuthRequest struct {
	Type     string `json:"type"`
	ID       string `json:"id,omitempty"`
	Method   string `json:"method"`
	Tenant   string `json:"tenant,omitempty"`
	Username string `json:"username,omitempty"`
	Token    string `json:"token,omitempty"`
}

func (AuthRequest) FrameType() string { return TypeAuthRequest }

// AuthChallenge is the server's challenge for password method. The client
// must compute Argon2id(password, salt, nonce) and reply with AuthResponse.
type AuthChallenge struct {
	Type   string `json:"type"`
	ID     string `json:"id,omitempty"`
	Salt   string `json:"salt"`
	Nonce  string `json:"nonce"`
	Params string `json:"params"` // "argon2id,m=65536,t=3,p=1"
}

func (AuthChallenge) FrameType() string { return TypeAuthChallenge }

// AuthResponse is the client's reply to a password challenge.
type AuthResponse struct {
	Type  string `json:"type"`
	ID    string `json:"id,omitempty"`
	Proof string `json:"proof"`
}

func (AuthResponse) FrameType() string { return TypeAuthResponse }

// AuthOK reports successful authentication.
type AuthOK struct {
	Type        string `json:"type"`
	ID          string `json:"id,omitempty"`
	AccountID   string `json:"account_id"`
	TenantID    string `json:"tenant_id"`
	AccountType string `json:"account_type"` // "human" | "bot"
	DisplayName string `json:"display_name"`
}

func (AuthOK) FrameType() string { return TypeAuthOK }

// AuthErr reports failed authentication. Reason is a short machine-readable
// code; Detail is optional human text. The server should never leak whether
// the failure was bad-username vs bad-password (use a single reason for both).
type AuthErr struct {
	Type   string `json:"type"`
	ID     string `json:"id,omitempty"`
	Reason string `json:"reason"`
	Detail string `json:"detail,omitempty"`
}

func (AuthErr) FrameType() string { return TypeAuthErr }

// Join asks to join a channel by name. In slice 1 there is one public
// channel per tenant; later slices add private channels and ACL enforcement.
type Join struct {
	Type    string `json:"type"`
	ID      string `json:"id,omitempty"`
	Channel string `json:"channel"`
}

func (Join) FrameType() string { return TypeJoin }

// Part leaves a channel.
type Part struct {
	Type    string `json:"type"`
	ID      string `json:"id,omitempty"`
	Channel string `json:"channel"`
}

func (Part) FrameType() string { return TypePart }

// Msg is the chat-message frame, in both directions.
//
// Outbound from client: From is empty (server stamps it). TS is empty.
// Outbound from server (broadcast): From and TS are set by the server.
type Msg struct {
	Type    string    `json:"type"`
	ID      string    `json:"id,omitempty"`
	Channel string    `json:"channel,omitempty"` // empty for direct messages
	To      string    `json:"to,omitempty"`      // account_id for direct messages
	From    string    `json:"from,omitempty"`    // account_id (server-stamped)
	TS      time.Time `json:"ts,omitempty"`      // server-stamped
	Body    string    `json:"body"`
}

func (Msg) FrameType() string { return TypeMsg }

// Presence reports a membership change in a channel.
type Presence struct {
	Type        string `json:"type"`
	ID          string `json:"id,omitempty"`
	Channel     string `json:"channel"`
	Kind        string `json:"kind"` // "join" | "part"
	AccountID   string `json:"account_id"`
	DisplayName string `json:"display_name,omitempty"`
}

func (Presence) FrameType() string { return TypePresence }

// Ping/Pong are the keepalive frames. The server initiates; the client
// echoes back the same Token in a Pong.
type Ping struct {
	Type  string `json:"type"`
	ID    string `json:"id,omitempty"`
	Token string `json:"token"`
}

func (Ping) FrameType() string { return TypePing }

type Pong struct {
	Type  string `json:"type"`
	ID    string `json:"id,omitempty"`
	Token string `json:"token"`
}

func (Pong) FrameType() string { return TypePong }

// --- Subscription management verbs (slice 3b) ---
//
// These mirror the REST control plane's subscription CRUD but live on the
// realtime PRM protocol so a bot can manage subscriptions over the same
// authenticated TLS socket it uses for chat. Auth is implicit via the
// existing AuthOK on the connection; the connection's account must be a
// bot.

// SubscriptionInfo is the wire representation of a subscription. Returned
// in subscription_ok and inside subscription_list_ok. The Secret field is
// populated ONLY in the response to subscription_create -- never in get/list.
type SubscriptionInfo struct {
	ID           string          `json:"id"`
	TenantID     string          `json:"tenant_id"`
	AccountID    string          `json:"account_id"`
	ChannelID    string          `json:"channel_id"`
	URL          string          `json:"url"`
	Secret       string          `json:"secret,omitempty"` // base64-url, returned once on create
	Match        json.RawMessage `json:"match"`
	Events       []string        `json:"events"`
	ContextLines int             `json:"context_lines"`
	DebounceMs   int             `json:"debounce_ms"`
	CooldownMs   int             `json:"cooldown_ms"`
	Budget       json.RawMessage `json:"budget,omitempty"`
	DisabledAt   *time.Time      `json:"disabled_at,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
}

// SubscriptionCreate asks the server to create a subscription on the
// given channel. Exactly one of ChannelName or ChannelID must be set.
type SubscriptionCreate struct {
	Type         string          `json:"type"`
	ID           string          `json:"id,omitempty"`
	ChannelName  string          `json:"channel_name,omitempty"`
	ChannelID    string          `json:"channel_id,omitempty"`
	URL          string          `json:"url"`
	Match        json.RawMessage `json:"match"`
	Events       []string        `json:"events,omitempty"`
	ContextLines int             `json:"context_lines,omitempty"`
	DebounceMs   int             `json:"debounce_ms,omitempty"`
	CooldownMs   int             `json:"cooldown_ms,omitempty"`
	Budget       json.RawMessage `json:"budget,omitempty"`
}

func (SubscriptionCreate) FrameType() string { return TypeSubscriptionCreate }

// SubscriptionList asks the server to return all subscriptions owned by
// the connection's account in the connection's tenant.
type SubscriptionList struct {
	Type string `json:"type"`
	ID   string `json:"id,omitempty"`
}

func (SubscriptionList) FrameType() string { return TypeSubscriptionList }

// SubscriptionGet fetches one subscription by ID.
type SubscriptionGet struct {
	Type           string `json:"type"`
	ID             string `json:"id,omitempty"`
	SubscriptionID string `json:"subscription_id"`
}

func (SubscriptionGet) FrameType() string { return TypeSubscriptionGet }

// SubscriptionUpdate patches a subscription. Pointers for primitive
// fields so absent fields can be distinguished from zero-valued ones on
// the wire.
type SubscriptionUpdate struct {
	Type           string          `json:"type"`
	ID             string          `json:"id,omitempty"`
	SubscriptionID string          `json:"subscription_id"`
	URL            *string         `json:"url,omitempty"`
	Match          json.RawMessage `json:"match,omitempty"`
	Events         []string        `json:"events,omitempty"`
	ContextLines   *int            `json:"context_lines,omitempty"`
	DebounceMs     *int            `json:"debounce_ms,omitempty"`
	CooldownMs     *int            `json:"cooldown_ms,omitempty"`
	Budget         json.RawMessage `json:"budget,omitempty"`
	Disabled       *bool           `json:"disabled,omitempty"`
}

func (SubscriptionUpdate) FrameType() string { return TypeSubscriptionUpdate }

// SubscriptionDelete removes a subscription.
type SubscriptionDelete struct {
	Type           string `json:"type"`
	ID             string `json:"id,omitempty"`
	SubscriptionID string `json:"subscription_id"`
}

func (SubscriptionDelete) FrameType() string { return TypeSubscriptionDelete }

// SubscriptionOK is the server's response to create / get / update.
type SubscriptionOK struct {
	Type         string           `json:"type"`
	ID           string           `json:"id,omitempty"`
	Subscription SubscriptionInfo `json:"subscription"`
}

func (SubscriptionOK) FrameType() string { return TypeSubscriptionOK }

// SubscriptionListOK is the server's response to list.
type SubscriptionListOK struct {
	Type          string             `json:"type"`
	ID            string             `json:"id,omitempty"`
	Subscriptions []SubscriptionInfo `json:"subscriptions"`
}

func (SubscriptionListOK) FrameType() string { return TypeSubscriptionListOK }

// SubscriptionDeleted is the server's response to delete.
type SubscriptionDeleted struct {
	Type           string `json:"type"`
	ID             string `json:"id,omitempty"`
	SubscriptionID string `json:"subscription_id"`
}

func (SubscriptionDeleted) FrameType() string { return TypeSubscriptionDeleted }

// Error is the generic error frame. Reason is a stable machine-readable code
// (e.g., "not_authenticated", "channel_not_found", "rate_limited"); Detail
// is optional human text safe to surface in a client.
type Error struct {
	Type   string `json:"type"`
	ID     string `json:"id,omitempty"` // correlation id of the request that errored, if any
	Reason string `json:"reason"`
	Detail string `json:"detail,omitempty"`
}

func (Error) FrameType() string { return TypeError }
