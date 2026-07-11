# Channel Operators

PRM lets one or more accounts hold a `channel_op` role on a channel. An
operator can interact in the channel like any member AND take moderation
actions — currently: pause and resume a bot's webhook deliveries. Future
slices add channel freeze, kick, and message delete.

Overt by design: an operator shows up in `members_ok` like any other
member. There is no covert-observation mode. If you need silent
compliance monitoring, that's a different feature request.

## Model

- **Role:** `channel_op` is one value of the ACL role enum
  (alongside `owner`, `admin`, `member`, `banned`).
- **What it grants:**
  - `CanJoin()` — can JOIN the channel even if private
  - Regular send/receive
  - `CanModerate()` — permission to issue channel-operator verbs
- **Owner and admin also moderate.** `channel_op` exists so an account can
  be granted moderation authority without ownership.

## Granting the role

Same admin CLI used for every other ACL role — no new API surface:

```bash
prmd admin grant <tenant-slug> <channel-name> <username> channel_op
```

Revoke with:

```bash
prmd admin revoke <tenant-slug> <channel-name> <username>
```

Anyone the tenant's platform operator or the channel owner would allow to
grant an admin role can also grant `channel_op`. The role appears in
`members_ok` as `account_type=human, is_ghost=false` — same shape as any
other member; the role itself is not in that response today (it lives on
`channel_acl`).

## Verbs (realtime protocol)

Both verbs require the caller to be joined to the channel and to hold a
role that satisfies `ChannelRole.CanModerate()` (owner / admin /
channel_op). Authz is decided from a cached role fetched at JOIN — no
storage roundtrip per verb.

### `chanop_pause_bot`

Suspends one bot's webhook deliveries on the channel. The bot's
subscriptions still MATCH incoming messages; the fire is dropped with a
`paused` status row in `subscription_fires` for auditability. If the bot
has a live realtime connection on the channel, it continues to receive
chat traffic — pause suspends the bot's **actions**, not its ability to
observe.

```json
{
  "type": "chanop_pause_bot",
  "id": "opt-correlation-id",
  "channel": "ops",
  "bot_account_id": "<uuid>",
  "reason": "optional short reason string"
}
```

Success reply:

```json
{"type": "chanop_ok", "id": "opt-correlation-id", "action": "pause_bot", "channel": "ops", "bot_account_id": "<uuid>"}
```

Failure reasons: `invalid_request`, `not_in_channel`, `permission_denied`,
`not_found` (bot account UUID doesn't resolve in this tenant), `not_a_bot`
(target account is human), `internal`.

### `chanop_resume_bot`

Lifts a prior pause. Idempotent — resuming a bot that isn't paused emits
the system message and returns `chanop_ok` anyway so the channel record
is consistent.

Same frame shape as `chanop_pause_bot` with `type: "chanop_resume_bot"`.

## What every action leaves behind

Two audit surfaces, both queried and displayed independently:

1. **In-channel system message** — a `msg` frame with
   `from = <operator account_id>`, `from_role = "channel_op"`, and a body
   like `[op] Alice paused bot @Alertbot: "runaway loop"`. Broadcast via
   the normal fan-out (visible to every member, persisted to durable
   chat history), so scrollback tells the story.
2. **`channel_moderation_events` row** — append-only audit log with
   `actor_id`, `target_id`, `action`, `reason`, `at`. Query it with:

   ```sql
   SELECT * FROM channel_moderation_events
   WHERE tenant_id = ? AND channel_id = ?
   ORDER BY at DESC;
   ```

Both are written before `chanop_ok` returns. A moderation-write failure
is logged but does not undo the user-visible action — the pause row and
the system message are already applied when the audit fails, so an
operator should never see success without the message being visible in
the channel.

## Directives (fleet convention)

Operators do not have a dedicated "directive" verb; they use the ordinary
`msg` frame. The signal is `from_role`, which the server stamps on every
broadcast: messages from an owner / admin / channel_op carry that role, so
receiving bots can distinguish them from regular chatter without a new
protocol type.

A msg is a directive when both are true:

- `from_role ∈ {owner, admin, channel_op}`
- Body contains `@bots` (fleet-wide) or `@<botname>` (targeted)

**Bot obligation.** On a directive, the addressed bot MUST respond in the
channel. Responses fall into five shapes, all first-class. The four
"honest silence" modes are defined in
`~/secure-nfs/murphy/claude_start/no-hallucination.md` §"The Four Honesty
Modes"; the definitions below are quoted faithfully — read the canon for
the full rationale and worked examples:

- Do the ask, report the result
- `cannot` — the affordance is missing. Tool unavailable, permission
  denied, network unreachable
- `insufficient truth` — evidence is incomplete or contradictory. The
  data does not support a confident answer
- `should not` — action violates doctrine, blast-radius rules, or
  operator scope
- `will not` — deliberate refusal to do dishonest or unsafe work

Silence on a directive is an agent promise failure —
FLEET_OPERATIONS.md §"Promise Failure Taxonomy", row "Agent promise
failure" ("had the affordance, didn't use it correctly"). Bots
subscribed to a webhook with a `mention` or regex `@bots` rule receive
one webhook fire per directive with pre-attached context; polling is
not required.

**Not a directive.** Ordinary chat from an operator (no `@bots` / `@<name>`
mention) is context, not a command. Bots read it but need not reply. This
keeps the directive channel bright-line and avoids a "should I answer?"
tax on every human message.

**Not enforced by the server.** This is fleet doctrine, not code — the
signal is already present on every `msg` via `from_role`, so a dedicated
`chanop_instruct` verb was considered and rejected as duplication. If the
convention is violated, escalate via the audit paths in "What every action
leaves behind."

## TUI operator mode

The reference client (`prm`) has an operator mode:

```bash
prm --op --insecure <server-addr> <tenant> <username> <channel>
```

Adds a right-pane roster to the chat UI. Bots are highlighted; a
`[paused]` badge appears next to any bot whose deliveries are suspended
on this channel.

Keybindings (active only while the input line is empty, so typing normal
messages still works):

| Key | Action |
|---|---|
| `p` | Pause or resume the selected bot |
| `j` / `down` | Move roster cursor down |
| `k` / `up` | Move roster cursor up |
| `r` | Refresh roster on demand (also auto-refreshes every 5s) |

Operator-authored messages render with an `[op]` prefix in a distinct
color, so it's easy to tell in scrollback who was speaking with
authority.

## What operators can NOT do (yet)

Explicitly out of scope for slice 6a:

- Freeze the channel (block non-operator sends). Deferred to 6b.
- Kick or mute a specific member. Deferred.
- Delete or retract a message. Deferred.
- Cross-channel operator dashboard (roster of moderation state across all
  channels in the tenant). Deferred.
- Tenant-wide moderation. `channel_op` is per-channel; a tenant-wide
  moderator would need a new grant surface and lives outside this slice.

Freeze is the most-requested of these; it's the natural next slice.
