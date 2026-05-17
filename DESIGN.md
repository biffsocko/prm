# PRM Design

This document is the architectural source of truth. README is the elevator pitch; DESIGN is the contract.

## Goals

- **High-speed message fan-out.** Sub-millisecond p50 from a `msg` arrival on one connection to the same bytes being written to all subscribed connections on the same node. The hot path never touches disk or a database.
- **Multi-tenant by design.** A single PRM deployment hosts many isolated organizations / workspaces. Tenant isolation is enforced at every API boundary; no tenant can see another's data.
- **Bots as first-class users.** Bot identities are distinct from human identities. Bots can connect like any client *or* skip the connection entirely and integrate via webhook subscriptions.
- **LLM-token economy.** Filtering happens server-side so an LLM-backed bot only pays tokens on messages that already matched a declared trigger. See "Cost savings model" below.
- **Production-grade resilience.** Hot-standby high availability and disaster-recovery from day one of v1. RPO measured in seconds; RTO under a minute. See "High availability and disaster recovery" below.
- **Auth required everywhere.** No anonymous identities, no plaintext transport. TLS-only.
- **Operationally honest.** The PRM binary is single-file. The production deployment is `PRM + Postgres` — that pair, not "PRM alone." Said up front so nobody is surprised.

## Non-goals (v0 / v1)

- IRC wire compatibility. Existing IRC clients will not connect.
- Federation / server-to-server across organizations. PRM does not yet link to other PRM (or other chat) servers.
- Active-active multi-node within a single deployment. Tier 3 of the redundancy plan; deferred until usage justifies it.
- Multi-region geo-replication. Tier 4; further deferred.
- Persistent chat-message history. Channels live in memory; closing all clients does not lose state, but server restart does. (Logs / events through inbound integrations are different — they're not chat.)
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

## Multi-tenancy

A single PRM deployment hosts many independent organizations / workspaces. Each is a **tenant** with its own users, channels, ACLs, bots, subscriptions, and inbound integrations. Tenants cannot see each other's data under any circumstance.

### Tenant model

- A **tenant** has: `tenant_id` (UUID v7), human-readable `display_name`, URL-safe `slug` (used in auth and integration URLs), creation timestamp, settings (quotas, rate limits), status (`active` / `suspended`).
- Every domain object — account, channel, ACL entry, bot, subscription, integration — belongs to exactly one tenant via `tenant_id`. UUIDs of those objects remain globally unique, but tenancy is the routing key.
- Tenant creation is **out of band** in v0 / v1 — a platform operator runs `prmd admin create-tenant <slug>`. Self-service signup is a v2+ concern.
- A special platform-operator scope exists for cross-tenant administration (creating tenants, setting quotas, suspending tenants). Platform operators do not appear inside tenant ACLs or channel member lists; they live in a separate `platform_admins` table.

### Tenant resolution

Every authenticated connection is **bound to exactly one tenant for its lifetime**. Tenant is resolved at auth time:

- **Password auth:** `auth_request` carries `tenant` (slug or UUID) alongside `username`. Server scopes the credential lookup to that tenant.
- **Token auth (bots, integrations):** the API token has the `tenant_id` baked in at issuance. Server resolves the tenant from the token; the client does not send it.
- **Inbound integration webhooks:** the integration's token determines the tenant; the URL path `/v1/inbound/{integration_id}` is per-tenant by construction (integration_id is unique within tenant).

A client that wants to switch tenants disconnects and reconnects with new credentials.

### Storage shape

- `tenant_id` is a column on every domain table. Composite indexes lead with `tenant_id`.
- Default deployment: one Postgres database with row-level tenancy. For very large or strongly-isolated tenants, optional **schema-per-tenant** mode (one Postgres schema per tenant) is supported by a runtime config flag — same code, different routing.
- In-memory state: channel registry keyed by `(tenant_id, channel_id)`; shard hash is `hash(tenant_id, channel_id)` so the broadcast path stays sharded without a global lock.

### Per-tenant operational knobs

Each tenant has independently-configurable limits:

- Max concurrent connections
- Max channels
- Max bots
- Max webhook subscriptions
- Inbound integration rate limits
- Outbound webhook delivery rate limits
- Daily / monthly message ceilings (soft, for billing/usage reporting; hard, for abuse prevention)

A platform operator sets defaults; per-tenant overrides land in the `tenants.settings` JSON column.

### Cross-tenant isolation guarantees

The implementation rule: **no domain query is written without `tenant_id` as a leading predicate.** This is enforced at the storage-package boundary — every repository function takes a `tenantID` first-class argument and there is no `getChannelByID(id)` API, only `getChannelByID(tenantID, id)`. Reviews and linters should refuse any new API that lacks this scoping.

Identity model (next section) is defined *within* a tenant; account uniqueness is per-tenant, not global.

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

## Inbound integrations

PRM is not a log platform, an alert engine, or an event store. It is the *bot orchestration layer* on top of those systems. To make events from external tools show up as PRM channel events — so bots can subscribe to them with the same model as chat messages — PRM exposes a small inbound webhook API.

### The pattern

```
POST /v1/inbound/{integration_id}
Authorization: Bearer <integration-token>
Content-Type: application/json
```

- `integration_id` is created once via the REST control plane (`POST /v1/integrations`) and is bound to:
  - A channel that received events will be republished to
  - An adapter (`splunk` | `graylog` | `datadog` | `github` | `generic` | …)
  - A scoped API token shown exactly once at creation time
- The handler receives whatever JSON the calling system sends, runs the adapter's normalize function, republishes the result as a PRM `event` message on the bound channel, and returns `202 Accepted`.
- All downstream behavior — webhook subscriptions, debounce, cooldown, budget caps, context attach — is unchanged. The event looks like any other channel message to a bot.

### Adapter contract

```go
type Adapter interface {
    Name() string
    Normalize(body []byte, headers http.Header) (Event, error)
}

type Event struct {
    Source     string         // e.g. "splunk", "graylog"
    Service    string         // e.g. "auth-api"
    Severity   string         // "info" | "warn" | "error" | "critical"
    Summary    string         // short human-readable line
    Fields     map[string]any // structured fields preserved from upstream
    OccurredAt time.Time      // upstream timestamp if present, else now
    Raw        json.RawMessage // original payload, for debugging
}
```

Adapters are stateless. New integrations add an adapter file in `internal/inbound/adapters/` and register it on startup. No protocol changes required.

### Splunk adapter

Splunk's Webhook alert action posts JSON of this shape:

```json
{
  "sid": "scheduler__admin__...",
  "search_name": "Auth API 5xx Spike",
  "app": "search",
  "owner": "admin",
  "results_link": "https://splunk.example.com/...",
  "result": { "status_code": "503", "service": "auth-api", "count": "47" }
}
```

Normalize maps:

- `Source` → `"splunk"`
- `Service` → `result.service` (JSON path is configurable per integration)
- `Summary` → `search_name`, optionally interpolated with `result` fields (e.g. `"{search_name}: {result.count} 5xx in window"`)
- `Severity` → derived from search-name conventions or an explicit `severity` field on the search results; defaults to `warning`
- `Fields` → the entire `result` object plus `results_link`
- `OccurredAt` → `now()` (Splunk's payload doesn't include a reliable trigger timestamp)

### Graylog adapter

Graylog's HTTP Notification (Event Definitions) posts JSON:

```json
{
  "event_definition_id": "...",
  "event_definition_type": "aggregation-v1",
  "event_definition_title": "Auth API error rate",
  "event": {
    "timestamp": "2026-05-16T12:30:01.000Z",
    "message": "Auth API error rate > 5/min",
    "fields": { "service": "auth-api", "level": "ERROR" },
    "priority": 3
  }
}
```

Normalize maps:

- `Source` → `"graylog"`
- `Service` → `event.fields.service`
- `Summary` → `event.message` (falls back to `event_definition_title`)
- `Severity` → translation of `event.priority` (Graylog uses 1=low / 2=normal / 3=high → PRM `info` / `warning` / `error`)
- `Fields` → `event.fields`
- `OccurredAt` → `event.timestamp`

### Generic adapter

For any system that can POST JSON — GitHub webhooks, Jenkins post-build hooks, Kubernetes Events, cron jobs, custom scripts. The generic adapter is configured at integration-creation time with:

- JSON-path expressions for each `Event` field (e.g., `summary_path: "$.alert.title"`)
- Optional severity mapping table
- Optional pre-filter (skip events where a JSON path matches or doesn't match)

This makes the long tail of "stuff that can POST JSON" trivially supported without writing Go code per source.

### Security

- Tokens are bearer tokens, hashed at rest with SHA-256, displayed exactly once at creation.
- Tokens are scoped to one integration and one channel; revoking the token deletes the binding.
- An optional **shared-secret signature** mode (`X-PRM-Signature: sha256=...`) is supported for callers that prefer HMAC over bearer tokens — matches GitHub's webhook signing style.
- Per-integration rate limit: default 10 events/sec sustained, 100 burst. Configurable. Excess returns `429 Too Many Requests`.
- Payload size cap: 64 KB by default. Larger payloads return `413 Payload Too Large`; an adapter can opt into a higher cap.

### Why this is strictly better than "PRM is also a log platform"

- **Zero observability code to maintain.** No parsers, indexers, retention tiers, query languages. The existing platforms already do that better than PRM ever would.
- **Works with whatever the user already has.** No migration required. Drop the inbound URL into Splunk's alert action and you're done.
- **One mental model for bot authors.** Chat message, log alert, GitHub PR, deploy notification — all look the same to a subscription rule. Same match shape, same payload-with-context, same LLM-cost story.
- **Reuses the entire PRM stack.** No new auth model, no new storage decisions, no new retention policy.
- **Sharpens the bot pitch.** PRM isn't competing with Splunk; it's the layer that lets an LLM bot triage Splunk's alerts with chat-channel context attached.

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

**Primary: PostgreSQL.** Production deployments target Postgres 15+. Replication for the hot-standby HA tier (below) requires streaming replication, which Postgres handles natively. Schema migrations across many tenants need real DB tooling, which Postgres has.

**Optional alternative: SQLite** (via `modernc.org/sqlite`). Available for small-deployment scenarios — single-tenant homelab, friend group, dev environment — where the "single binary, no dependencies" simplicity is worth giving up multi-instance HA. Same schema, same migrations, same code paths; the storage package abstracts the backend.

The storage choice is configured at startup via `--storage postgres://...` or `--storage sqlite:./prm.db`. Same binary, same code, different URL.

### Schemas

All domain tables include `tenant_id uuid not null` as the first column, indexed.

- `tenants` (id, slug, display_name, settings_json, status, created_at)
- `accounts` (id, tenant_id, display_name, type, password_hash, password_salt, recovery_email, created_at, ...)
- `tokens` (id, tenant_id, account_id, hash, created_at, last_used_at, revoked_at)
- `channels` (id, tenant_id, name, owner_id, visibility, created_at)
- `channel_acl` (tenant_id, channel_id, account_id, role, granted_at, granted_by)
- `subscriptions` (id, tenant_id, account_id, channel_id, match_json, url, secret_hash, events_json, context_lines, debounce_ms, cooldown_ms, budget_json, disabled_at, ...)
- `subscription_fires` (tenant_id, subscription_id, fired_at, status, attempts) — for budget accounting and debugging
- `integrations` (id, tenant_id, channel_id, adapter, token_hash, settings_json, disabled_at, ...)
- `platform_admins` (account_id, granted_at) — global, cross-tenant; intentionally not tenant-scoped

Composite indexes lead with `tenant_id`. Queries always scope by `tenant_id` first; the storage-package API enforces this at the function signature.

### What's not in durable storage

Channel membership and presence **live only in memory** and rebuild on server restart from `channel_acl` (durable) plus reconnecting clients. Same shape as v0 — durable state is "who can talk to whom"; transient state is "who is currently connected." This separation is what makes hot-standby HA work cleanly: durable state replicates, transient state rebuilds.

## High availability and disaster recovery

PRM ships with a tiered redundancy plan. Each tier is operationally meaningful on its own; later tiers are not blocked by earlier ones being missing.

### Tier 1 — Backup + restore (slice 1+)

- Postgres continuously archives WAL segments to object storage (S3 / MinIO / equivalent). The PRM repo ships an example Postgres configuration; nothing PRM-specific to maintain.
- **RPO:** seconds (WAL ship interval).
- **RTO:** minutes (manual restore + PRM restart).
- Single PRM node. Box dies → restore Postgres from latest base + WAL, point PRM at the restored DB, clients reconnect.

### Tier 2 — Hot-standby active-passive (slice 2)

- Two PRM nodes deployed. Postgres primary + streaming-replication standby.
- Leader election via a Postgres advisory lock: `SELECT pg_try_advisory_lock(N)`. Whoever holds the lock serves traffic. Loser sits idle waiting on its own lock attempt.
- L4 load balancer (HAProxy / nginx / cloud LB) in front of both PRM processes; health check returns 200 only from the lock-holder.
- On primary PRM failure: lock TTL expires → standby acquires → load balancer flips → standby serves. Postgres-side: standby gets promoted via your preferred mechanism (Patroni, manual `pg_ctl promote`, etc.).
- **RPO:** zero for committed Postgres writes (synchronous replication slot).
- **RTO:** under 60 seconds end-to-end (lock TTL + LB health-check cycle + Postgres promotion).
- During failover: in-memory channel state is **cold on the standby**. Clients reconnect; membership and presence rebuild from `channel_acl` + reconnecting clients. The blip is visible to users (~10–30s of disconnect), but no data is lost.

### Tier 3 — Active-active multi-node (deferred, v2+)

- N PRM nodes all serve traffic. Channels sharded by `hash(tenant_id, channel_id) % N`; each node "owns" a slice.
- Cross-node messages routed via an internal pub/sub fabric (NATS Jetstream, or a small protocol between PRM nodes over TLS).
- Presence propagated across nodes. Cross-shard joins do an internal handshake.
- Adds 100–500 μs cross-node hop for some messages; sub-ms target preserved for same-shard messages.
- Not in v0 / v1. **Mentioned here so v0 design choices don't preclude it** — sharding by `(tenant_id, channel_id)` already matches the would-be Tier 3 shard key.

### Tier 4 — Multi-region (deferred, further)

- Geo-distributed Postgres or a globally replicated DB.
- Edge PRM nodes that route to the home region for each tenant.
- Real-world: only matters at significant scale with latency-sensitive global users. Don't design for it now; just don't paint into a corner that forbids it (no assumptions about single-region clock sync, etc.).

### Backup verification

- Backups that haven't been restored are theater. The PRM repo ships a restore-test script and a documented runbook: at minimum monthly, restore the most recent base+WAL to a scratch Postgres, spin up a PRM process pointing at it, verify a known-good test tenant comes up.

### Operational footprint of HA

Honest framing:

- **Tier 1** adds: Postgres backup configuration + object storage bucket. A few hours one-time setup.
- **Tier 2** adds: a second PRM process, a second Postgres instance with replication, a load balancer, the lock-election sidecar logic. A day or two of standup, ongoing monitoring.
- Tier 3+ is significantly more operational work and requires capacity planning that depends on actual usage. Don't pay this cost speculatively.

### Performance impact of HA

| Operation | Tier 1 (backup only) | Tier 2 (hot standby) |
|---|---|---|
| `msg` fan-out p50/p99 | unchanged | unchanged |
| Connection establish + auth | +1–5 ms (Postgres vs SQLite) | same as Tier 1 |
| JOIN (ACL lookup) | +1–5 ms | same as Tier 1 |
| Steady-state CPU | unchanged | unchanged (standby idle) |
| Steady-state memory | +5–10% (tenant_id everywhere) | same as Tier 1 |
| Failover blip | N/A | 10–30s, one-time per disaster |

The headline performance number (sub-ms p50 fan-out) is preserved because the hot path never touches durable storage. Multi-tenancy adds one hash dimension; HA adds zero cost during steady state. The slower operations (auth, JOIN) are off the hot path and happen rarely per connection.

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

## Comparison to Redis and RabbitMQ

These come up because both are commonly used as pub/sub layers for chat-adjacent systems. The honest comparison depends on what dimension you measure.

### Raw publish-to-N-subscribers fan-out, single node

- **Redis pub/sub**: tight C, single-threaded core, very small per-message path. Sub-100μs p50 on a LAN is typical. PRM with careful Go tuning can match this but probably won't beat it — JSON framing and Go GC are real costs Redis doesn't pay.
- **RabbitMQ**: optimized for delivery guarantees, not latency. p50 is 1–10ms, higher with persistence. PRM will beat it on latency by skipping persistence, acks, exchange routing, and dead-letter queues.

If your benchmark is `PUBLISH foo bar` → "subscriber reads bar," Redis wins.

### End-to-end "chat message reaches all channel members"

This is what PRM actually optimizes for, and it's not the same workload as raw pub/sub. PRM amortizes:

- Serialize the broadcast frame once per inbound message.
- Push precomputed bytes onto each member's outbound queue (sharded channel state, no global lock).
- Each connection has its own write goroutine that drains the queue with `Writev` and TCP_NODELAY.

Target: tens of microseconds p50 fan-out, low-ms p99 under load. Should be competitive with Redis here and well ahead of RabbitMQ.

JSON framing cost: a 200-byte chat message is maybe 30% bigger than the RESP equivalent. For chat-shaped workloads (many connections, modest per-message rate per channel) wire bytes/sec is rarely the bottleneck. If it becomes one, the answer is binary framing in PRM, not abandoning PRM for a different broker.

### Total system cost for an LLM-powered bot

This is where the comparison stops being broker-vs-broker. Neither Redis nor RabbitMQ has server-side filter pushdown for bot subscriptions, debounce, cooldown, budget caps, or context attach. To build "IRC for bots" on top of either, you would:

- **Redis pub/sub:** write a consumer that reads every message and filters in code. Back to paying tokens per message if your filter is the LLM, or maintaining your own filter logic. No durable subscription state; reconnect logic on you.
- **RabbitMQ:** write a routing topology of exchanges and queues to pre-filter. Feasible, but you're spinning up one queue per bot per filter rule and managing the topology lifecycle.
- **Either way:** write the webhook delivery worker pool, the debounce window, the per-subscription cooldown, the budget cap accounting, the signed payload format, the retry policy, and the per-bot context attach.

PRM puts all of that in the server. The LLM-token savings documented in the "Cost savings model" section come from filtering happening *before* the bot's runtime ever sees the message, and that savings is independent of which underlying broker you'd otherwise pick. PRM packages the savings into the platform; Redis and RabbitMQ leave it as homework.

### When to use Redis or RabbitMQ instead

- **Service-to-service messaging between backend services.** PRM is the wrong tool. Use Redis (transient pub/sub) or RabbitMQ (durable queues).
- **Need at-least-once delivery semantics with durable queues.** PRM doesn't do that in v0. Use RabbitMQ or Kafka.
- **Need a generic key-value store, cache, or stream processing.** Redis or Kafka, obviously.
- **Building a chat system with LLM-powered bots and you don't want to write the bot integration layer yourself.** PRM.

The PRM bet is that "chat with bots as first-class users" is workload-specific enough to justify a purpose-built relay rather than building chat-plus-bot semantics on top of a generic broker.

## Implementation slices

Slicing the build so each step ships something useful and validates the next:

**Slice 1 — Minimum viable PRM, multi-tenant from day one.**
- TLS server + connection accept + hot fan-out path
- `hello` / `welcome` capability negotiation (including tenant resolution)
- Password auth handshake with tenant-scoped accounts
- Verb subset: `hello`, `welcome`, `auth_*`, `join`, `part`, `msg`, `presence`, `ping`, `pong`
- One public channel per tenant; no ACLs yet (channels are slice 2)
- **PostgreSQL** as primary storage; SQLite available as alt for tiny deploys
- In-memory channel state keyed by `(tenant_id, channel_id)`
- Bare TUI client (`cmd/prm`)
- Fan-out benchmark harness — proves sub-ms p50 with N synthetic clients before continuing
- Admin CLI: `prmd admin create-tenant`
- Tier 1 backup config (Postgres WAL archive to object storage) documented in runbook

**Slice 2 — Auth surface and HA. ✅ Implemented.**
- Channel ACLs persisted in storage (SQLite full, Postgres stub) — enforced on JOIN; channels must exist explicitly (no implicit creation on first JOIN as in slice 1).
- Bot account type + token-method auth (one-shot AuthRequest with bearer token; no challenge round-trip).
- Hot-standby HA: leader election via Postgres `pg_try_advisory_lock` + L4 load balancer pattern. `internal/ha` package with `Local` (always-leader) and `Postgres` (advisory-lock + heartbeat) implementations.
- Documented restore runbook in `docs/HA.md` covering Tier 2 topology, failover sequence, restore-from-backup, monthly restore-test discipline.
- Reconnect logic in TUI client with exponential backoff (1s → 30s capped). Essential under HA failover (10–30s blip).

**Slice 3 — Webhook subscriptions + outbound delivery.**
- REST control plane subscription CRUD
- Subscription matcher (regex/glob/mention)
- Debounce window, cooldown, budget caps
- Signed HMAC HTTP POST worker pool with retry policy
- Context-attach (last N channel messages bundled into payload)

**Slice 4 — Inbound integrations.**
- `POST /v1/inbound/{integration_id}` endpoint
- Adapter registry; Splunk, Graylog, and generic JSON-path adapters ship as reference
- Per-integration rate limit, optional HMAC signing

**Slice 5+ — Polish and growth.**
- Mention syntax + parsing
- Multi-device session policy
- Bot ghost-indicator in member lists
- More inbound adapters (Datadog, GitHub, Jenkins, k8s Events)
- Chat history (`chathistory` verb + durable storage of recent messages)
- Tier 3 active-active when single-node capacity is no longer enough

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
