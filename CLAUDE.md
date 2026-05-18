# CLAUDE.md — PRM

Working notes for anyone (human or AI) picking up this project.

## What this is

**PRM** (Private Relay Messaging) — a high-speed, auth-required chat relay built for LLM-powered bots as first-class users. Similar in shape to IRC (server, channels, identities, private messages) but uses a fresh wire protocol and modern primitives. **Not IRC-compatible** — existing IRC clients won't connect.

The standout design choice: **server-side filter pushdown for bot subscriptions.** The server runs the regex/glob/mention match and only fires a webhook (with pre-attached context) on hits, so an LLM-backed bot's token cost scales with *responses*, not *message volume*. See `DESIGN.md` for the worked economics example.

## Project state

**v1.0.0 — Slices 1 through 5 complete.** A working TLS PRM server (`prmd`) and reference TUI client (`prm`) with:

- Multi-tenant SQLite storage; Postgres backend is a stub awaiting a real Postgres to validate against
- Password auth (Argon2id, 3-frame challenge/response handshake)
- Token auth (one-shot bearer-token method for bot accounts)
- Explicit channels (slice 2: channels must be created; first JOIN no longer auto-creates)
- Channel ACLs (owner / admin / member / banned roles); public visibility = any authenticated account in tenant; private = must be in ACL
- Bot accounts as a distinct user type; API tokens issued via admin CLI (plaintext shown once)
- Channel ID cached on the connection on JOIN so handleMsg's hot path never hits storage
- TUI client with reconnect-on-disconnect (exponential backoff 1s → 30s) — important under Tier 2 HA failover
- HA leader-election skeleton (`internal/ha`): Local elector for single-instance; Postgres advisory-lock elector for hot-standby pairs. Integration test for Postgres is a documented skeleton (skipped until a real PG is available).
- Operator runbook in `docs/HA.md` covering Tier 2 hot-standby setup, failover sequence, restore-from-backup, monthly restore-test discipline.
- **Webhook subscriptions (slice 3)**: `internal/matcher` (regex/glob/mention, any_of/all_of), `internal/webhook` (HMAC-signed delivery via a pluggable Transport — HTTP / AMQP / MQTT — with retry + debounce + cooldown + daily budget caps), `internal/rest` (REST control plane on :8443 with bearer-token auth, subscription CRUD). Server's broadcast path calls `WebhookMgr.Notify` after fan-out so the LLM-token cost-savings story is real end-to-end. Per-channel ring buffer (32-deep) supplies context-attach. Subscription URL scheme picks the transport: `https://` (default POST), `amqp(s)://` (RabbitMQ publish + confirms), `mqtt(s)://` (MQTT 3.1.1 publish, QoS 1 default, envelope-wrapped HMAC sig).
- End-to-end integration test in `test/e2e/` proves the full path: chat → matcher → signed POST → verified payload with context.

- **Subscription verbs over the realtime protocol (slice 3b)**: `subscription_create` / `_list` / `_get` / `_update` / `_delete` on the JSON-line wire. Same business logic as REST via shared `internal/subops` package. End-to-end test exercises the full protocol path including HMAC verification of webhooks fired against a secret obtained over the protocol.
- **Inbound integrations (slice 4)**: `POST /v1/inbound/{integration_id}` on the REST listener. Adapter registry in `internal/inbound` with reference adapters for Splunk / Graylog / Datadog / GitHub / generic JSON-path (`internal/inbound/adapters/`). `server.Server.PublishInbound` bridges normalized events onto a channel via the same broadcast+history+notify path as chat messages. Operator runbook at [docs/INTEGRATIONS.md](docs/INTEGRATIONS.md). End-to-end test runs Splunk- and Graylog-shape POSTs through the whole stack and verifies the resulting webhooks fire with the right body + signature.
- **v1.0.0 polish (slice 5)**:
  - **Mention parser** (`internal/mention`): resolves `@username` and `@<uuid>` in chat bodies into account UUIDs scoped to the tenant. The matcher's `mention` rule kind now fires on real `@`-mentions — no regex hack.
  - **Chat history** (`internal/server/history.go`): every channel message is persisted off the hot path via a bounded-channel async writer with drop-on-full backpressure. New `chathistory` / `chathistory_ok` verbs return oldest-first with optional `before_ts` paging. Same path persists inbound integration events.
  - **Datadog + GitHub adapters**: typed inbound adapters for Datadog's Webhooks integration (configurable service tag) and GitHub events (push / pull_request / deployment_status / issues / release).
  - **Ghost-member indicator**: new `members` / `members_ok` verbs return the effective membership of a channel — live realtime connections plus any bot account with an active webhook subscription on the channel and no live connection (`is_ghost=true`). Each row carries `is_ghost` + `conn_count`.

Future slices (federation, OAuth/SSO, Tier 3 active-active, file attachments) live in [DESIGN.md](DESIGN.md#implementation-slices) under "Slice 6+ — Deferred / future." Open questions from earlier slices (mention syntax, multi-device, ghost members) all resolved in slice 5.

Headline numbers from `go test -p 1 -run TestFanoutLatency -v ./test/bench/` on Apple Silicon:

- n=10  → p50=241µs, p99=761µs
- n=50  → p50=291µs, p99=734µs
- n=100 → p50=570µs, p99=1.89ms

Sub-ms p50 fan-out target met. The benchmark skips under `-race` (numbers would be misleading); correctness under `-race` is exercised by the server package's e2e tests.

## Hard constraints — don't break without asking

- **Two binaries, one Go module.** `cmd/prmd` (server) and `cmd/prm` (TUI reference client). Both ship from the same module. No microservices.
- **Single PRM binary** — but the **production deployment is `PRM + Postgres + L4 load balancer`**, not "PRM alone." Stated up front in README so nobody is surprised. Don't slip back toward "zero external dependencies" — that was the pre-multi-tenant goal, walked back deliberately to enable multi-tenancy and HA.
- **Multi-tenant from day one.** `tenant_id` is the first dimension of every domain operation. **No storage-package function exists that lacks `tenantID` as a leading argument.** Reviewers and linters should refuse any new repository function without it. This is the single most important architectural rule — get it wrong once and you have a cross-tenant data leak.
- **PostgreSQL is the primary storage backend.** Use `github.com/jackc/pgx/v5` (no ORM). SQLite via `modernc.org/sqlite` is supported as an alternate backend for tiny / single-tenant deploys — both implement the same `storage` interface so server code is backend-agnostic. Configured at startup via `--storage postgres://...` or `--storage sqlite:./prm.db`.
- **TLS only on the wire.** No plaintext fallback, no `--allow-plaintext` flag. Localhost too.
- **No anonymous identities.** Every connection authenticates. `public` channel visibility means "any authenticated account in this tenant may join," not "no auth required."
- **Webhook delivery never blocks realtime fan-out.** The hot message path completes before any HTTP outbound is issued. Webhook firing is on a parallel worker pool.
- **JSON line framing, not binary.** Easy to debug with `nc` / `websocat`, easy to extend. Performance comes from the fan-out path (precomputed serialized frames, `Writev` on the outbound side), not from a clever binary encoding.
- **HA via Postgres advisory-lock leader election.** Slice 2 onward, two PRM processes run; whoever holds the lock serves traffic, the loser sits idle. Don't reach for Raft / etcd / Consul / Patroni-as-a-library — the advisory lock pattern is enough and stays inside Postgres.

## Layout (when code lands)

```
prm/
  README.md
  DESIGN.md
  CLAUDE.md
  go.mod
  go.sum
  cmd/
    prmd/                  # server binary
      main.go
    prm/                   # TUI reference client
      main.go
  internal/
    proto/                 # JSON frame types, marshal/unmarshal helpers, capability negotiation
    server/                # core server: connection accept, hot fan-out, channel state,
                           # history writer, members verb
    auth/                  # Argon2id password hashing, token issuance/verification, SASL flow
    channels/              # in-memory channel state, sharded locks, member list ops
    mention/               # @username / @<uuid> parser; scoped to the connection's tenant
    storage/               # storage interface + Postgres (primary) and SQLite (alt) implementations
                           # every function takes tenantID as a leading arg
      open/                # factory: storage.Open(url) -> backend
      sqlite/              # SQLite impl via modernc.org/sqlite (MaxOpenConns=1)
      postgres/            # Postgres impl (stub; awaiting real PG for slice 1/2)
    ha/                    # leader election: Local (always-leader) and Postgres
                           # (pg_try_advisory_lock + heartbeat) implementations
    tenants/               # tenant model, quotas, settings, platform-admin operations
    matcher/               # subscription match-rule compiler + evaluator (regex/glob/mention)
    webhook/               # outbound webhook delivery: HMAC signing + retry + debounce + cooldown + budget
    rest/                  # REST control plane: subscription CRUD + inbound /v1/inbound/{id} on :8443
    subops/                # shared business logic for subscription CRUD; called by both
                           # rest/ (HTTP) and server/ (PRM-protocol verbs in slice 3b)
    inbound/               # adapter registry + Event shape for inbound integrations
      adapters/            # per-source normalizers; registers via init():
                           # splunk, graylog, datadog, github, generic
    client/                # shared client TUI components
  test/
    e2e/                   # multi-process integration tests
```

This is the target. Initial code should land within this layout; resist refactoring across packages until v0 features are in.

## Conventions

- **Go style.** Standard `gofmt`, `golangci-lint` clean. Errors propagated, not logged-and-swallowed. `context.Context` first arg on anything that can block.
- **Frame types are structs with `json:"..."` tags**, generated/maintained by hand from the verb catalog in DESIGN.md. No protobuf, no codegen tools.
- **No third-party Go dependencies beyond:** `github.com/jackc/pgx/v5` (Postgres), `modernc.org/sqlite` (alt storage), `golang.org/x/crypto/argon2`, `nhooyr.io/websocket` (or `gorilla/websocket`), `github.com/charmbracelet/bubbletea` (TUI client), `github.com/google/uuid` (UUID v7), `github.com/rabbitmq/amqp091-go` (AMQP delivery transport), `github.com/eclipse/paho.mqtt.golang` (MQTT delivery transport). If you reach for anything else, ask first.
- **No `init()` functions** doing real work. Boot order is explicit in `main`.
- **No global state.** Server struct owns everything; tests construct it.
- **Tests live next to the code** (`foo_test.go`) for unit tests; cross-package integration tests live under `test/e2e/`.
- **Logging via `log/slog`** with structured fields. No `fmt.Println` in committed code.

## Performance posture

Sub-millisecond p50 fan-out from `msg` arrival to bytes-on-wire on every channel member is the bar. Mechanisms to preserve:

- **Sharded channel state.** One RWMutex per shard (`hash(channel_id) % N`). No global lock on the broadcast path.
- **Precomputed serialized broadcast frame.** Compute the wire bytes once per inbound message, write them to every member's outbound queue.
- **Per-connection write goroutine.** Each connection has a `chan []byte` outbound queue with bounded capacity. Write goroutine drains and writes with `Writev`/`bufio.Writer`. TCP_NODELAY.
- **Slow-consumer policy.** If a member's outbound queue fills, drop non-system messages (not auth/presence/system) and tag the connection as lagging. Do not block fan-out.
- **Frame allocator.** `sync.Pool` of 4 KB `[]byte` slices for the broadcast path. Avoid `encoding/json` reflection on outbound — precompute envelopes and `append` hot fields.

If you're tempted to change anything in this list, that's the conversation, not a unilateral move.

## Common pitfalls

- **Don't block fan-out on storage.** Channel ACLs are checked at JOIN time and cached in the in-memory channel state. Message delivery never touches Postgres (or SQLite). The hot path is in-memory only — that's why HA's perf overhead is zero.
- **Never write a domain query without `tenant_id` scope.** Every storage function takes `tenantID` as a leading argument. There is no `getChannelByID(id)` — only `getChannelByID(tenantID, id)`. A missing tenant scope is a cross-tenant data leak. Treat it like a security bug, not a style issue.
- **Don't try to make the standby PRM warm.** Tier 2 HA accepts a 10–30s reconnect blip on failover. Warming the standby's in-memory state would require streaming presence/membership cross-node, which is essentially the Tier 3 active-active problem in disguise. Stay cold; clients reconnect; presence rebuilds.
- **Don't put webhook delivery inline.** Even a 5ms HTTP roundtrip ruins fan-out latency if it's in the hot path. Worker pool, always.
- **Inbound integration adapters are stateless.** A `Normalize(body, headers) → Event` function. No DB lookups, no outbound calls. If a source needs enrichment, do it in the bot that subscribes, not in the adapter.
- **Don't turn PRM into a log platform.** Inbound integrations exist so PRM can *consume* events from Splunk / Graylog / Datadog / etc., not so PRM can replace them. If you find yourself adding storage, indexes, or a query language for ingested events, stop — that's a different product.
- **Don't trust client-supplied timestamps.** Server stamps every message at receive. Anything else opens replay / spoofing surface.
- **Don't make display names unique.** Two users named "alex" is fine; the `account_id` disambiguates. Resist anyone (including yourself in a future session) trying to add a uniqueness constraint.
- **Don't add IRC compatibility.** The temptation will appear ("what if we just supported NICK/USER as aliases for the auth flow?"). Decline. PRM is its own protocol; IRC bridging is a separate project if anyone wants it.

## Related projects

This is the third project in a recent group, distinct from each other:

- `~/src/wizards/` — Declan's 2D browser fighting game (his repo).
- `~/src/wizards3d/` — 3D port of the above (Tom's repo, currently paused).
- `~/src/prm/` — this project.

No shared code or runtime between them.
