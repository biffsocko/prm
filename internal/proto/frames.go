// Package proto defines the PRM wire-protocol frame types and a JSON-line
// codec. Every frame is a JSON object terminated by a newline; the "type"
// field selects the schema for the rest of the fields.
//
// Slice 1 verbs: hello, welcome, auth_request, auth_challenge,
// auth_response, auth_ok, auth_err, join, part, msg, presence, ping, pong,
// error.
package proto

import "time"

// Frame is anything that can be sent over the wire.
type Frame interface {
	FrameType() string
}

// Verb type constants.
const (
	TypeHello          = "hello"
	TypeWelcome        = "welcome"
	TypeAuthRequest    = "auth_request"
	TypeAuthChallenge  = "auth_challenge"
	TypeAuthResponse   = "auth_response"
	TypeAuthOK         = "auth_ok"
	TypeAuthErr        = "auth_err"
	TypeJoin           = "join"
	TypePart           = "part"
	TypeMsg            = "msg"
	TypePresence       = "presence"
	TypePing           = "ping"
	TypePong           = "pong"
	TypeError          = "error"
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
