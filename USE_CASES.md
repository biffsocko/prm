# PRM Use Cases — How Organizations Use It

PRM (Private Relay Messaging) is an auth-required, multi-tenant chat relay built around one observation: **most internal "chat" today is really an event bus that humans happen to read.** Deploy pings, alert fan-out, build failures, on-call escalations, security findings — these are events that need to land somewhere, get filtered, and trigger action. Sometimes the action is a human reading a message. Increasingly, the action is an LLM-powered bot deciding whether a human needs to be involved at all.

PRM treats that observation as the design centerpiece: humans and bots are first-class users on equal footing, the server runs the filter so bots don't burn tokens on messages they would have ignored anyway, and the same chat-and-subscription machinery handles inbound events from Splunk, Graylog, Datadog, GitHub, and anything else that can POST JSON.

This document describes the concrete situations where that shape pays off.

## TL;DR — who PRM is for

- **SRE / DevOps teams** running on-call rotations who want their alert pipeline, deploy pipeline, and triage bots in one auth-controlled channel-and-subscription system rather than a Slack channel plus three Lambdas plus a Datadog Workflow.
- **Security operations** that route SIEM alerts to LLM-assisted triage and want auditable, multi-tenant, on-prem chat with persistent history.
- **Platform engineering** teams that build internal automation bots and want server-side filter pushdown so a chatty channel doesn't cost $200/day in tokens for a bot that responds five times.
- **MSPs / consultancies** running automation for multiple client organizations who want strict tenant isolation in a single deployment.
- **Hobbyists and homelab operators** who want a single-binary, self-hosted, IRC-style server that takes bots seriously without the cost surprise of running an LLM-backed bot against a busy channel.

## What PRM is not

To set expectations cleanly:

- **Not a Slack / Teams replacement** for general workplace chat. No voice, video, file attachments, threading UI, emoji picker, or rich text editor. The TUI client is intentionally minimal.
- **Not a log platform.** PRM consumes events from Splunk, Graylog, Datadog, etc. — it doesn't replace them. There's no query engine, no time-series store, no search indexing over ingested events.
- **Not anonymous.** Every connection authenticates. "Public" channels mean "any authenticated account in this tenant" — not "no auth required."
- **Not federated** across PRM deployments. One deployment is one administrative domain.
- **Not IRC-compatible.** Existing IRC clients can't connect. PRM uses a fresh JSON-line wire protocol.

If those constraints fit, the use cases below describe what PRM does well.

---

## Use Case 1 — LLM-assisted SRE triage bot

**The before:** a Slack `#ops-alerts` channel receives 1,500 alerts a day from Datadog, Splunk, and a homegrown Prometheus alertmanager. An LLM-powered triage bot is wired up as a "listen to every message" integration. The bot calls the model for each message to decide whether the alert is actionable. Most alerts (~95%) are routine and the model returns "no action needed" — but the bot pays for every call. At ~$0.002 per call, the bot costs ~$3/day to say "no" 1,425 times and ~$0.15/day to actually triage the 75 alerts that mattered. Multiply across a few teams and the math gets uncomfortable.

**The PRM shape:** the triage bot registers a webhook subscription with a `mention` rule (`@triage-bot`) or a regex (e.g. `^\[(error|critical)\]`) and a sensible debounce window. PRM only POSTs to the bot when an alert pre-qualifies. The bot's LLM gets the alert *and* eight messages of preceding channel context, in a single signed HTTP POST, with no persistent connection to manage. Daily budget cap on the subscription stops runaway costs from a noisy day.

**What the bot's day looks like:**

```mermaid
sequenceDiagram
    participant DD as Datadog
    participant PRM as prmd
    participant Sub as Subscription matcher
    participant Bot as LLM triage bot<br/>(Lambda)
    participant Eng as On-call engineer

    DD->>PRM: POST /v1/inbound/{id} (alert)
    PRM->>PRM: republish as channel msg<br/>"[error] datadog/auth-api: CPU > 90%"
    PRM->>Sub: match?
    Note over Sub: regex `^\[(error|critical)\]`<br/>matches; debounce window open
    Sub->>Bot: signed POST<br/>(alert + 8 lines of context)
    Bot->>Bot: LLM triage<br/>(one call, one cost)
    alt Actionable
        Bot->>PRM: post diagnosis + runbook link to channel
        Bot->>Eng: page (separate path)
    else Routine
        Bot->>PRM: thread summary, no page
    end
```

**Why this helps the org:**

- **Token cost drops ~2 orders of magnitude.** The bot stops paying the LLM to decide "no" 1,425 times a day. Real numbers from a worked example in [DESIGN.md](DESIGN.md#cost-savings-model).
- **Budget caps catch runaway days.** A noisy incident that fires 5,000 alerts in an hour can't blow the bot's monthly budget. The subscription stops firing past the daily cap and resumes the next day.
- **Context is pre-attached.** The LLM sees the alert plus what was happening in the channel just before it. No separate "fetch the last N messages" call, no bot-side ring buffer, no race conditions on context.
- **Bot can be serverless.** No persistent connection means the bot can be a Lambda, Cloud Run service, or Cloudflare Worker. Pay only when work arrives.

## Use Case 2 — Deploy and release coordination

**The before:** a `#deploys` channel where a CI bot posts every build success, every deploy event, every PR merge. Engineers learn to ignore it. The signal-to-noise ratio is bad; nobody catches the failures because there are too many successes.

**The PRM shape:** GitHub webhooks point at PRM's `/v1/inbound/{id}` endpoint with the GitHub adapter. Push, pull_request, deployment_status, issues, and release events all land on the channel. A subscription with `severity=error` (driven by the GitHub adapter's mapping of `deployment_status:failure` → error) routes deploy failures to a paging bot. A separate subscription with a regex on `\bproduction\b` routes production deploys to an audit logger for compliance retention.

**Why this helps the org:**

- **One channel, two consumers.** Humans see everything in the channel; bots see only what matters to them. The same event stream serves both, without the bots having to filter client-side or the channel becoming bot-noise.
- **Audit trail is automatic.** Every event flowing through PRM lands in durable chat history (slice 5). Forensic question "when did production-svc start failing?" answers via `chathistory` against the channel, scoped by timestamp.
- **No glue Lambdas required.** The Datadog→Slack→bot→runbook pipeline collapses to Datadog→PRM→bot, with the filter inside PRM.

## Use Case 3 — Security operations / SOC

**The before:** a SIEM (Splunk, Graylog, Sentinel) generates alerts. The SOC team routes some to email, some to Slack, some to a ticketing system. An LLM is "kind of" used for tier-1 triage but it sees a sanitized copy of the alert in a Slack DM, doesn't have surrounding event context, and the team can't tell from the audit trail what the bot did or didn't act on.

**The PRM shape:** Splunk and Graylog alerts POST into PRM via their typed adapters. A `#soc-tier1` channel receives them with normalized severity tags. The triage bot has a subscription on `[error]` and `[critical]` events with 16 lines of context — the bot's LLM call sees the alert *and* the preceding alerts in the channel, often a sequence that turns "single suspicious login" into "actually part of a credential-stuffing burst." Subscription fires (every attempted webhook delivery, success or failure) are recorded in `subscription_fires` for audit. Tenants can be carved up so a Managed Security Service Provider sees only their own clients' channels.

**Why this helps the org:**

- **Bot reasoning improves with context.** "Is this attack?" answered against a window of channel context is dramatically better than against a single alert. PRM's context-attach pattern delivers it for free.
- **Auditability is concrete.** Every fire is recorded. "What did the bot see when it decided this was a false positive?" answers from `subscription_fires` plus chat history.
- **Tenant isolation is type-system-level.** No storage function exists without `tenantID` as a leading argument — a cross-tenant data leak isn't a runtime risk to test for, it's a compile error to prevent. Important for MSPs.
- **On-prem deployable.** Single Go binary plus Postgres plus a load balancer; no SaaS dependency for sensitive channels.

## Use Case 4 — Multi-tenant SaaS or MSP automation

**The before:** an MSP runs automation for 12 client organizations. Each client has its own Slack workspace, its own bot accounts, its own webhook endpoints, its own access controls. Onboarding a 13th client means standing up a new workspace, replicating the bot configs, and making sure no one accidentally cross-wires alerts.

**The PRM shape:** one PRM deployment, 12 tenants. Each tenant has its own accounts, channels, subscriptions, integrations, quotas, and rate limits — but they share the binary, the database, and the operational baseline. Tenant isolation is enforced at the storage layer; a query that forgot to scope by tenant won't compile.

**Why this helps the org:**

- **One operational footprint.** One Postgres pair, one PRM pair (active + standby), one L4 LB. Adding a client is `prmd admin create-tenant` and a few `create-channel` / `create-account` calls.
- **Per-tenant cost accounting.** Token-cost budgets are per-subscription, which is per-account, which is per-tenant. "Which client's bots cost the most this month?" is a real query.
- **Failure isolation by tenant.** A misconfigured bot in tenant A can't affect tenant B. Suspending a tenant is an admin action that stops all of their integrations and connections.

## Use Case 5 — Developer assistant bots

**The before:** an internal "ask the codebase" bot listens to a `#dev-help` channel. Engineers ask questions, the bot decides whether to respond. Most messages aren't questions for the bot (engineers also chat with each other), so the bot's LLM call rate is high relative to the number of useful responses.

**The PRM shape:** the bot subscribes with a `mention` rule on its own account (`@codebase-bot`). It only fires when explicitly addressed. The channel can carry as much human-to-human chatter as it wants; the bot's LLM cost scales with mentions, not with channel volume.

Variations on the same pattern:

- **PR summarizer bot.** Subscribes via the GitHub adapter on `pull_request:opened`; posts a one-paragraph LLM summary back into a `#prs` channel.
- **Standup bot.** Subscribes on a regex matching daily-standup patterns in a `#standups` channel; collates entries and posts a digest.
- **On-call handoff bot.** Subscribes on an `@oncall` mention; summarizes the last N hours of `#ops` traffic so the incoming engineer can read one paragraph instead of scrolling.

**Why this helps the org:**

- **Bots compose.** PRM's bots are just webhook subscriptions; adding one is a single `prmd admin issue-token` plus a POST to `/v1/subscriptions`. There's no "bot framework" to learn, no per-bot connection management.
- **Token cost is predictable.** Each subscription has a daily budget. A bot doesn't surprise you with a $400 bill.
- **Same subscription machinery for chat and integrations.** A bot that already speaks PRM webhooks for chat messages handles inbound-integration-driven events with zero code changes.

## Use Case 6 — Audit-grade channel for compliance

**The before:** a regulated team needs an auditable record of operational decisions: "who approved this prod deploy, what did the change look like, what did the on-call confirm." That trail lives across Slack DMs, ticket comments, PR descriptions, and pager logs.

**The PRM shape:** a single `#change-approval` channel with strict ACL (private; only on-call rotation + change-management leads can write). Every prod deploy event is published to this channel via the GitHub adapter. Approvers post their `+1` as chat messages. The channel's full history is persisted (slice 5 chat history) and queryable via `chathistory`. Optional: a compliance bot subscribes to the channel and writes the event stream to long-term object storage for retention.

**Why this helps the org:**

- **One channel = one audit trail.** No more correlating across systems.
- **Persistent and queryable.** Chat history survives restarts and failover; the `chathistory` verb gives auditors a paged retrieval API.
- **ACL is enforced at the server.** Private channels with explicit member lists; no "everyone with the link" exposure.
- **Tenant isolation matters here too.** A multi-business-unit company can give each BU its own tenant; auditors for BU-A see BU-A's audit channels and only those.

## Use Case 7 — Homelab / personal automation

**The before:** a homelab operator runs Home Assistant, a couple of Raspberry Pis, a Plex server, and some scripts. Notifications fan out to email, push notifications, sometimes Pushover. An LLM-backed personal assistant is appealing but cost-prohibitive on the busy events.

**The PRM shape:** PRM on a Raspberry Pi 4 (single binary, SQLite backend, no Postgres needed for small deploys). Home Assistant POSTs events to the generic adapter. A personal assistant bot subscribes with a budget cap of $0.50/day and a `mention` rule. Family members chat in a normal channel; the bot pipes up only when summoned.

**Why this helps the org:** ("org" of one)

- **Single binary, single SQLite file** — no Postgres, no LB, no Kubernetes. Runs on a Pi indefinitely.
- **Same architecture as production deployments.** Skills learned on the homelab transfer directly. The deployment shape is `PRM + Postgres + LB` when you outgrow the Pi, but the binary and the configuration shape are identical.
- **Bots without surprise bills.** Daily budget cap is enforced server-side; the bot can't decide to call the LLM 1,000 times in a runaway loop.

---

## How PRM helps a typical org — the consolidated story

Pick whichever of the use cases above is closest to your situation. The cross-cutting benefits show up in all of them:

| Benefit | Why it matters |
|---|---|
| **Server-side filter pushdown** | LLM-powered bots cost roughly the number of *responses* they produce, not the number of *messages* they could have seen. Two orders of magnitude in token reduction for typical event-channel shapes. |
| **One mental model for chat and events** | Splunk alerts, GitHub PRs, Datadog incidents, and human messages are all the same thing inside PRM: a message on a channel that subscriptions can match. Bots write against one surface. |
| **Multi-tenant from the type system** | Cross-tenant leaks don't compile. Real for MSPs, real for multi-BU enterprises, real for SaaS architects who want one PRM and many customers. |
| **HA without surprise** | Postgres advisory-lock leader election, L4 load balancer flip on health-check failure. RPO zero (synchronous replication); RTO under 60 seconds. Documented runbook. |
| **Single binary, two listeners** | `prmd` exposes realtime on `:6697` (TLS) and REST on `:8443`. No microservice mesh, no message broker dependency. |
| **Persistent chat history** | Durable async writer + `chathistory` retrieval verb. Compliance-friendly. |
| **Audit-grade subscription fires** | Every webhook delivery (success or failure) is logged in `subscription_fires`. "What did the bot see and what did it do?" is answerable. |
| **Bots as first-class identities** | Bot account type with its own auth method (one-shot bearer token, no challenge round-trip). Token issuance is an admin CLI action; revocation is immediate. |

## Anti-use-cases — when not to use PRM

- **General workplace chat with rich UX needs.** If you want threading, reactions, emoji, file uploads, voice channels, or a polished mobile client — use Slack, Teams, Discord, or Matrix. PRM is intentionally minimal.
- **A log search engine.** PRM consumes events; it doesn't index, query, or store them for ad-hoc search. Pair it with Splunk or Graylog, don't replace them.
- **Anonymous public chat.** No anonymous identities under any setting.
- **Federation across organizations.** PRM is a single administrative domain. Cross-org collaboration is not in scope.
- **Voice and video.** Not now, not later — that's a different product.

## Getting started

The README covers the local-bringup flow: clone, build, create a tenant, create accounts, create channels, issue a bot token, start the server, connect a human and a bot, create a subscription, watch the webhook fire.

- For the design rationale: [DESIGN.md](DESIGN.md)
- For the bot-author guide (creating subscriptions, verifying signatures, payload shape, retry policy, a complete minimal Python bot): [docs/WEBHOOKS.md](docs/WEBHOOKS.md)
- For the operator + integrator guide (per-source inbound setup, generic adapter, writing your own adapter): [docs/INTEGRATIONS.md](docs/INTEGRATIONS.md)
- For the HA + DR runbook: [docs/HA.md](docs/HA.md)

If a use case here matches your situation but a specific capability is missing, the design document's "Slice 6+ — Deferred / future" section is where roadmap candidates live.
