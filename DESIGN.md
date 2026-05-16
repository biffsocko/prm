# PRM Design

This document is the architectural source of truth. README is the elevator pitch; DESIGN is the contract.

## Goals

- **High-speed message fan-out.** Sub-millisecond p50 from `PRIVMSG` arrival on one connection to the same bytes being written to all subscribed connections on the same node. The hot path never touches disk or a database.
- **Bots as first-class users.** Bot identities are distinct from human identities. Bots can connect like any client *or* skip the connection entirely and integrate via webhook subscriptions.
- **LLM-token economy.** Filtering happens server-side so an LLM-backed bot only pays tokens on messages that already matched a declared trigger. See "Cost savings model" below.
- **Auth required everywhere.** No anonymous identities, no plaintext transport. TLS-only.
- **Simple to operate.** Single binary, embedded SQLite, no external dependencies for v0.

## Non-goals (v0)

- IRC wire compatibility. Existing IRC clients will not connect.
- Federation / server-to-server. Single-node only.
- Persistent message history. Channels live in memory; closing all clients does not lose state, but server restart does.
- Operator console / admin UI.
- OAuth or SSO.

## Wire protocol

**Transport:** TLS 1.3 over TCP. Same port also accepts WebSocket Upgrade for browser clients. Plaintext TCP is not supported.

**Framing:** line-delimited JSON. Each frame is one JSON object terminated by `\n`. UTF-8 throughout. Maximum frame size 64 KB (configurable).

**Frame shape:**

```json
{"type": "verb", "id": "client-correlation-id", "...": "..."}
```

The `type` field is mandatory and selects the schema for the remaining fields. `id` is an optional client-supplied correlation token; the server echoes it back in any response, error, or ack frame so the client can match replies to requests.

**Verbs (initial set):**

| Verb | Direction | Purpose |
|---|---|---|
| `hello` | C→S | Capability advertisement on connect |
| `welcome` | S→C | Server hello reply with negotiated capabilities |
| `auth_request` | C→S | Begin auth (`method`: `password` \| `token`) |
| `auth_challenge` | S→C | Server challenge (when applicable) |
| `auth_response` | C→S | Client response to challenge |
| `auth_ok` / `auth_err` | S→C | Result |
| `join` | C→S | Join a channel |
| `part` | C→S | Leave a channel |
| `msg` | C↔S | Channel or direct message |
| `presence` | S→C | Member join/leave/role-change events |
| `ping` / `pong` | C↔S | Keepalive (server initiates, client echoes) |
| `error` | S→C | Generic error frame |

The catalog will grow but stays small. Anything bot-related (subscription management, webhook secrets, etc.) lives on the REST control plane, not the realtime protocol.

## Identity model

Accounts only. There is no concept of a nickname being claimable on a first-come basis the way IRC does it.

- Every account has a stable, opaque `account_id` (UUID v7).
- Every account has an editable `display_name` shown in clients.
- Display names are not unique across the server. The `account_id` is what disambiguates.
- Account creation requires a password (Argon2id) and optionally a recovery email.
- An account has a `type`: `human` or `bot`. Bots get an additional API token at creation; humans don't.

This avoids the entire NickServ ghosting / collision problem space. Two users can both be named "alex" with no conflict.

## Channels and ACLs

Each channel has:

- `channel_id` — opaque UUID, stable
- `name` — human-readable, mutable, not unique
- `owner_id` — account UUID
- `visibility` — `private` (must be in ACL to join) or `public` (any authenticated account may join)
- `acl` — list of `(account_id, role)` pairs where role ∈ {`owner`, `admin`, `member`, `banned`}

Joining requires:

1. Account is authenticated.
2. Channel exists.
3. Either the channel is `public`, or the account's role in the ACL is in `{owner, admin, member}` (and not `banned`).

The ACL is the only access control. There is no `+i` invite-only mode bolted on top — invite-only is just `visibility=private` with an empty default ACL. There is no channel password — password-as-access-control is replaced by the ACL.

## Bot accounts

A `type: "bot"` account has:

- An **API token** at creation, in addition to a password. Tokens are opaque random strings hashed at rest. The bot uses the token to authenticate either on the realtime protocol (`method: "token"`) or to the REST control plane.
- Permission to register **webhook subscriptions** (see below).
- Optional metadata: maintainer, description, homepage URL, displayed in channel member lists.

Bots are clients otherwise. They join channels, get/give messages, leave. The bot account type is what unlocks the webhook subscription endpoints.

## REST control plane

Separate HTTP listener on a different port (default 8443, TLS-only). Authenticates with API tokens. Endpoints (initial set):

```
POST   /v1/accounts                          create account (rate-limited)
POST   /v1/accounts/{id}/tokens              issue a new API token (bot accounts)
DELETE /v1/accounts/{id}/tokens/{token_id}   revoke a token

POST   /v1/channels                          create channel
PATCH  /v1/channels/{id}                     update name / visibility
PUT    /v1/channels/{id}/acl/{account_id}    set role
DELETE /v1/channels/{id}/acl/{account_id}    remove from ACL

POST   /v1/subscriptions                     create webhook subscription
GET    /v1/subscriptions                     list (scoped to caller's bot account)
PATCH  /v1/subscriptions/{id}                update match rules / url / budget
DELETE /v1/subscriptions/{id}                remove
```

## Webhook subscriptions

A subscription is the bot's declarative "wake me when..." statement:

```json
{
  "channel_id": "...",
  "match": {
    "any_of": [
      {"type": "mention", "account_id": "<this bot>"},
      {"type": "regex", "pattern": "(?i)^deploy\\b"},
      {"type": "glob", "pattern": "build #*"}
    ]
  },
  "url": "https://my-bot.example.com/prm-webhook",
  "events": ["message"],
  "context_lines": 8,
  "debounce_ms": 750,
  "cooldown_ms": 5000,
  "budget": {"daily_max_fires": 500, "estimated_cost_per_fire_usd": 0.02}
}
```

When a frame on the channel matches `any_of`:

1. **Cooldown check.** If the previous fire for this subscription was less than `cooldown_ms` ago, skip.
2. **Debounce buffer.** Hold the match for `debounce_ms`. If additional matching messages arrive in the window, batch them all into one fire.
3. **Budget check.** If `daily_max_fires` for this subscription has been reached, suppress the fire and emit a single `budget_exhausted` event (per day) so the bot owner knows.
4. **Context attach.** Pull the last `context_lines` channel messages from in-memory state and bundle them into the payload.
5. **Sign and POST.** Payload is signed with HMAC-SHA256 using the subscription's secret. Header: `PRM-Signature: t=<unix>,v1=<hex>`.
6. **Retry policy.** On 5xx or timeout: exponential backoff, up to 3 retries, then give up and record the failure. On 4xx: do not retry; flag the subscription as broken after N consecutive 4xx and auto-disable.

Webhook delivery runs on a separate worker pool. **It never blocks the realtime fan-out path.**

## Cost savings model

The first-order win is filter pushdown: the server's regex match is free relative to a model call, and most messages don't match anything.

### Worked example

Assume:

- A channel `#general` with 50,000 messages per day across all senders.
- A bot that wants to respond when `@bot` is mentioned or messages start with `!cmd `.
- ~200 messages per day match those criteria.
- Bot uses an LLM to decide *and* generate the response.

**Traditional always-on bot, naive "ask the model on every message" pattern:**

- 50,000 model calls per day.
- Even with a small classifier (say, $0.0001 per call) that's **$5/day** just to say "no" to 49,800 messages.
- With a real model (Claude Haiku-tier at ~$0.0015 per short call) that's **$75/day** of "no" answers.

**Traditional bot, client-side filter then model:**

- Bot maintains a persistent connection, reads every message, runs its own regex in code, only calls the model on matches.
- 200 model calls per day; cost matches the "only what's needed" baseline.
- But: bot has to keep a connection up 24/7 (idle compute), maintain reconnect logic, handle scrollback for context. Infrastructure cost is non-zero and operationally annoying.

**PRM with webhook subscriptions:**

- 0 connections idle.
- ~200 webhooks fired per day, each with context already attached.
- 200 model calls per day, each cleanly scoped.
- Debounce collapses bursts: a 10-message back-and-forth that all mentions the bot becomes 1 fire, not 10. If 30% of fires would have been bursts, the effective fire count drops to ~140.
- Budget cap caps the worst case if the channel suddenly goes nuts.

The savings versus naive always-on are between **100×** and **350×** in model cost depending on tier. The savings versus a client-side-filter bot are smaller in *model* cost (already minimized) but eliminate the persistent-connection infrastructure entirely — bot runs as a serverless function with zero idle cost.

### Why server-side filter is better than "smart client-side filter"

A client could always run the same regex. The argument for moving it server-side:

1. **Compute pushdown.** The bot's runtime can be cold-start serverless. No always-on container.
2. **Bandwidth.** Filter happens before any payload is shipped to the bot's network. For high-volume channels, that's real.
3. **Centralized policy.** Server enforces cooldown / debounce / budget. A bot author can't accidentally write a tight loop.
4. **Declarative.** Subscription is a config record, not code. Easy to inspect, audit, and change without redeploying the bot.

## Auth

SASL-style three-way handshake over the JSON protocol:

```
C: {"type":"auth_request","method":"password","username":"alex"}
S: {"type":"auth_challenge","nonce":"<server-nonce>","salt":"<argon2-salt>"}
C: {"type":"auth_response","proof":"<argon2id(password+nonce+salt)>"}
S: {"type":"auth_ok","account_id":"..."} | {"type":"auth_err","reason":"..."}
```

For bots:

```
C: {"type":"auth_request","method":"token","token":"<opaque-token>"}
S: {"type":"auth_ok","account_id":"...","type":"bot"}
```

Password hashing: Argon2id, `memory=64 MiB`, `iterations=3`, `parallelism=1` at rest. Per-account salt.

Token storage: SHA-256 hash at rest; tokens are shown to the user exactly once at issuance.

## Storage

SQLite (embedded). Schemas:

- `accounts` (id, display_name, type, password_hash, password_salt, recovery_email, created_at, ...)
- `tokens` (id, account_id, hash, created_at, last_used_at, revoked_at)
- `channels` (id, name, owner_id, visibility, created_at)
- `channel_acl` (channel_id, account_id, role, granted_at, granted_by)
- `subscriptions` (id, account_id, channel_id, match_json, url, secret_hash, events_json, context_lines, debounce_ms, cooldown_ms, budget_json, disabled_at, ...)
- `subscription_fires` (subscription_id, fired_at, status, attempts) — for budget accounting and debugging

Channel membership and presence are **not** in SQLite. They live in memory and rebuild on server restart from `channel_acl` plus reconnecting clients.

## Performance design

**Hot path: a PRIVMSG arrives and must reach all channel members.**

1. Parse JSON frame on the inbound connection's read goroutine.
2. Look up channel by id in a sharded map (sharded by hash of channel_id; one RWMutex per shard, no global lock).
3. Hold the channel's read lock. The member list is a slice of connection refs. Iterate, push the precomputed serialized frame onto each member connection's outbound queue (`chan []byte` with bounded capacity).
4. Each connection has its own write goroutine that drains the outbound queue, batching with `Writev` or `bufio.Writer` flushed at message boundaries. TCP_NODELAY on. No write blocks under load: if a member's outbound queue is full (slow consumer), drop oldest non-system messages and tag the connection as lagging.
5. Webhook delivery: a parallel goroutine on the channel side scans subscriptions matching this message and pushes onto the webhook worker pool. **Webhook delivery never blocks step 4.**

Expected p50 fan-out from arrival → bytes-on-wire at all members: tens of microseconds. Expected p99 under load: low milliseconds, bounded by GC pauses (will tune `GOGC` and the message frame allocator).

**Allocator notes:**

- Message frames are pooled via `sync.Pool` of `[]byte` with capacity 4 KB. Frames larger than 4 KB allocate fresh.
- JSON encoding uses a precomputed envelope template + `append` for hot fields, not `encoding/json` reflection, on the broadcast path. Inbound parse can use `encoding/json` since it's one-per-client.

## What's not designed yet

Deliberately deferred to v1+ and not blocking v0 implementation:

- Chat history persistence and a `chathistory` retrieval verb
- Server-to-server federation
- Operator framework (admin commands, kick/ban beyond channel scope)
- OAuth / SSO integration
- File/attachment storage
- Voice/video (out of scope, probably forever for PRM)

## Open questions

- **Mention syntax.** `@display_name` (fragile, names aren't unique) vs `@account_id` (unfriendly to type) vs `@display_name#tag` (Discord-style). Leaning Discord-style.
- **Multi-device for a single account.** Allow N concurrent connections for one account_id, or enforce single-session? Leaning N concurrent.
- **Bot identity in channel.** When a webhook-only bot has no live connection, should it appear in the channel member list? Leaning yes, with a "ghost" indicator.
- **Wire protocol versioning.** Embed in `hello` capability negotiation, or in TLS ALPN? Leaning capability negotiation.
