# CLAUDE.md — PRM

Working notes for anyone (human or AI) picking up this project.

## What this is

**PRM** (Private Relay Messaging) — a high-speed, auth-required chat relay built for LLM-powered bots as first-class users. Similar in shape to IRC (server, channels, identities, private messages) but uses a fresh wire protocol and modern primitives. **Not IRC-compatible** — existing IRC clients won't connect.

The standout design choice: **server-side filter pushdown for bot subscriptions.** The server runs the regex/glob/mention match and only fires a webhook (with pre-attached context) on hits, so an LLM-backed bot's token cost scales with *responses*, not *message volume*. See `DESIGN.md` for the worked economics example.

## Project state

**Design phase.** As of the last edit there is no Go code, no `go.mod`, no implementation — only documentation. README is the elevator pitch; DESIGN.md is the architectural contract; this file is the working notes.

Before writing implementation code, confirm the scope decisions in DESIGN.md "Open questions" with the project owner.

## Hard constraints — don't break without asking

- **Two binaries, one Go module.** `cmd/prmd` (server) and `cmd/prm` (TUI reference client). Both ship from the same module. No microservices in v0.
- **Single binary deploys.** `prmd` brings its own SQLite (CGO-less driver, e.g. `modernc.org/sqlite`) and listens on two TLS ports: realtime + REST control plane. No external dependencies needed to operate v0.
- **TLS only on the wire.** No plaintext fallback, no `--allow-plaintext` flag. Localhost too.
- **No anonymous identities.** Every connection authenticates. `public` channel visibility means "any authenticated account may join," not "no auth required."
- **Webhook delivery never blocks realtime fan-out.** The hot message path completes before any HTTP outbound is issued. Webhook firing is on a parallel worker pool.
- **JSON line framing, not binary.** Easy to debug with `nc` / `websocat`, easy to extend. Performance comes from the fan-out path (precomputed serialized frames, `Writev` on the outbound side), not from a clever binary encoding.

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
    server/                # core server: connection accept, hot fan-out, channel state
    auth/                  # Argon2id password hashing, token issuance/verification, SASL flow
    channels/              # in-memory channel state, sharded locks, member list ops
    storage/               # SQLite schema, account/channel/ACL/subscription persistence
    rest/                  # HTTP control plane (account/channel/subscription/integration CRUD)
    webhook/               # subscription matcher, debounce buffer, signed HTTP POST worker pool
    inbound/               # inbound integration receiver: POST /v1/inbound/{id} handler + adapter registry
      adapters/            # per-source normalizers (splunk, graylog, datadog, github, generic, ...)
    client/                # shared client TUI components
  test/
    e2e/                   # multi-process integration tests
```

This is the target. Initial code should land within this layout; resist refactoring across packages until v0 features are in.

## Conventions

- **Go style.** Standard `gofmt`, `golangci-lint` clean. Errors propagated, not logged-and-swallowed. `context.Context` first arg on anything that can block.
- **Frame types are structs with `json:"..."` tags**, generated/maintained by hand from the verb catalog in DESIGN.md. No protobuf, no codegen tools.
- **No third-party Go dependencies beyond:** `modernc.org/sqlite`, `golang.org/x/crypto/argon2`, `nhooyr.io/websocket` (or `gorilla/websocket`), `github.com/charmbracelet/bubbletea` (TUI client). If you reach for anything else, ask first.
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

- **Don't block fan-out on storage.** Channel ACLs are checked at JOIN time and cached in the in-memory channel state. Message delivery never touches SQLite.
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
