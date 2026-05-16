# PRM — Private Relay Messaging

A high-speed, auth-required chat relay built for LLM-powered bots as first-class citizens. Similar shape to IRC — server, channels, identities, private messages — but a fresh wire protocol and modern primitives throughout.

**Status:** design phase. No implementation yet. See [DESIGN.md](DESIGN.md) for the architecture.

## Why PRM exists

Running an LLM-powered bot on top of a traditional chat protocol (IRC, Matrix, even Discord/Slack via gateways) has a quiet but expensive problem: **the bot has to look at every message to decide whether to respond.** Even when a regex would have ruled most messages out, the bot is typically built to call the model for "should I respond? what should I say?" — and that call costs tokens whether the model decides to respond or not.

PRM moves the filter to the server. Bots register **subscriptions** that say "POST me when channel `#ops` gets a message matching `/^deploy/i`" or "when `@alertbot` is mentioned anywhere." The server runs the filter, fires a webhook only on matches, and includes a configurable window of preceding channel context in the payload. The bot's LLM only ever sees pre-qualified messages — and it gets them with context already attached, in one HTTP POST, with no persistent connection to maintain.

This compounds well:

- **No persistent connection** — bots can be serverless functions (Lambda, Cloudflare Workers, Cloud Run) that wake only when a trigger fires. No idle cost, no reconnect logic, no scrollback to manage.
- **Pre-attached context** — when a webhook fires, the payload includes the matching message *plus* N preceding messages of channel context. The bot doesn't fetch separately or maintain its own ring buffer. One webhook = one LLM call with everything needed.
- **Debounce window** — multiple matches inside a short window collapse into a single fire. A 10-message burst of `@bot` mentions becomes one LLM call, not ten.
- **Server-side cooldown** — per-subscription rate limits prevent thrashing on tight back-and-forths.
- **Budget caps** — a subscription can declare "I cost roughly $0.02 per fire" and the server enforces an hourly/daily ceiling. Hobby bots on metered LLM accounts stop getting fired past the budget, instead of burning through it during a busy day.

For a bot that previously processed 50,000 channel messages a day and only needed to respond to ~200 of them, the LLM-token reduction is roughly **two orders of magnitude** — you stop paying the model to decide "no" 49,800 times.

## Plugging in your existing tools

PRM is not a log platform, alert engine, or event store — it's the *bot orchestration layer* that sits on top of them. To make events from Splunk, Graylog, Datadog, GitHub, CloudWatch, or anything else that can POST JSON show up as PRM events for bots to act on, point those systems at a small inbound webhook:

```
POST /v1/inbound/{integration_id}
Authorization: Bearer <integration-token>
```

Per-source adapters (Splunk and Graylog ship as reference; a generic JSON-path adapter handles the long tail) normalize the payload, republish it as a PRM channel event, and the existing webhook subscription machinery — including the cost savings story above — drives whatever bots care to react. One mental model for chat messages, log alerts, GitHub PRs, deploy notifications.

See [DESIGN.md](DESIGN.md#inbound-integrations) for the adapter contract and the Splunk / Graylog field mappings.

## How does PRM compare to Redis or RabbitMQ?

Different problems, but the question comes up because both are commonly used as pub/sub layers for chat-adjacent systems. Quick guide:

- **Redis pub/sub** is brutally fast at raw publish-to-N-subscribers fan-out — tight C, sub-100μs p50 on a LAN. For pure pub/sub throughput on a single node, Redis probably wins. PRM with careful Go tuning can match it but pays JSON framing and Go GC overhead Redis doesn't.
- **RabbitMQ** optimizes for correctness (acks, persistence, dead-letter routing, exchanges) rather than latency. Typical p50 is 1–10ms, higher with persistence. PRM beats it on latency easily by skipping the features PRM doesn't need.
- **For chat-shaped workloads** (channels, ACLs, presence, identities) PRM is purpose-built. Redis and RabbitMQ are generic message buses; you'd build chat semantics on top of either.
- **For LLM-powered bots** PRM wins by a lot, regardless of broker choice. Server-side filter pushdown means an LLM-backed bot pays tokens for *responses*, not *message volume*. Redis and RabbitMQ have no equivalent — if you built bot subscriptions, debounce, cooldown, budget caps, and context-attach on top of either, you'd be reimplementing PRM's bot layer.

If you just need a generic message bus for service-to-service traffic, use Redis or RabbitMQ. If you need chat with bots as first-class users and don't want to reinvent the bot integration layer, that's what PRM is for.

See [DESIGN.md](DESIGN.md#comparison-to-redis-and-rabbitmq) for the dimension-by-dimension breakdown.

## What PRM is *not*

- **Not IRC-compatible.** Existing IRC clients (irssi, weechat, hexchat) will not connect. PRM uses a different wire protocol; you need a PRM client.
- **Not federated.** Single server topology in v0. No server-to-server linking.
- **Not anonymous.** Every connection authenticates. No anonymous join under any setting; the `public` channel visibility just means "any authenticated account may join."
- **Not a message archive (yet).** v0 does not persist chat history. Messages exist in memory for the lifetime of an active channel. Chat history persistence and the `chathistory`-equivalent retrieval API are deferred to v1.

## Project shape (when implementation lands)

Two binaries in one Go module:

- `prmd` — the server
- `prm` — a TUI reference client

Single-binary deploys. SQLite for accounts, channels, ACLs, and webhook subscriptions. TLS-only on the wire. WebSocket Upgrade supported on the same port so browser clients can connect without a separate gateway.

## License

TBD when first code lands.
