package server

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/channels"
	"github.com/biffsocko/prm/internal/proto"
	"github.com/biffsocko/prm/internal/storage"
)

// Channel-operator verbs (slice 6a). The caller must be joined to the
// channel and hold a role that satisfies ChannelRole.CanModerate() —
// owner, admin, or channel_op. The joinedRoles cache filled on JOIN is
// the authoritative source; no per-verb storage roundtrip on the authz
// path.

func (c *Conn) handleChanopPauseBot(ctx context.Context, m proto.ChanopPauseBot) {
	target, ch, ok := c.resolveChanopTarget(ctx, m.ID, m.Channel, m.BotAccountID)
	if !ok {
		return
	}
	pause := &storage.ChannelBotPause{
		TenantID:     c.tenantID,
		ChannelID:    ch.ID,
		BotAccountID: target.ID,
		PausedBy:     c.accountID,
		Reason:       m.Reason,
	}
	if err := c.srv.store.SetChannelBotPause(ctx, pause); err != nil {
		c.log.Error("chanop pause_bot: persist failed", "err", err)
		c.sendError("internal", "persist pause failed", m.ID)
		return
	}
	if c.srv.webhooks != nil {
		c.srv.webhooks.SetBotPaused(ch.ID, target.ID, true)
	}
	c.recordChanopAudit(ctx, ch.ID, target.ID, storage.ModActionPauseBot, m.Reason)
	c.emitChanopSystemMsg(ctx, m.Channel, ch.ID,
		fmt.Sprintf("[op] %s paused bot @%s%s",
			c.displayName, target.DisplayName, formatReason(m.Reason)))
	c.sendFrame(proto.ChanopOK{
		ID:           m.ID,
		Action:       "pause_bot",
		Channel:      m.Channel,
		BotAccountID: target.ID.String(),
	})
}

func (c *Conn) handleChanopResumeBot(ctx context.Context, m proto.ChanopResumeBot) {
	target, ch, ok := c.resolveChanopTarget(ctx, m.ID, m.Channel, m.BotAccountID)
	if !ok {
		return
	}
	// Idempotent: removing a non-existent pause is not an error; the
	// caller may have raced or the pause may have been cleared already.
	if err := c.srv.store.RemoveChannelBotPause(ctx, c.tenantID, ch.ID, target.ID); err != nil && err != storage.ErrNotFound {
		c.log.Error("chanop resume_bot: remove failed", "err", err)
		c.sendError("internal", "remove pause failed", m.ID)
		return
	}
	if c.srv.webhooks != nil {
		c.srv.webhooks.SetBotPaused(ch.ID, target.ID, false)
	}
	c.recordChanopAudit(ctx, ch.ID, target.ID, storage.ModActionResumeBot, m.Reason)
	c.emitChanopSystemMsg(ctx, m.Channel, ch.ID,
		fmt.Sprintf("[op] %s resumed bot @%s%s",
			c.displayName, target.DisplayName, formatReason(m.Reason)))
	c.sendFrame(proto.ChanopOK{
		ID:           m.ID,
		Action:       "resume_bot",
		Channel:      m.Channel,
		BotAccountID: target.ID.String(),
	})
}

// resolveChanopTarget validates the common shape of a chanop verb:
//   - channel and bot_account_id are non-empty
//   - caller is joined to the channel and has a moderator role
//   - bot_account_id resolves to a bot account in the caller's tenant
//
// Returns the target account, the channel row, and ok. On !ok an error
// frame has already been sent.
func (c *Conn) resolveChanopTarget(ctx context.Context, reqID, channelName, botAccountIDStr string) (*storage.Account, *storage.Channel, bool) {
	if channelName == "" {
		c.sendError("invalid_request", "channel is required", reqID)
		return nil, nil, false
	}
	if botAccountIDStr == "" {
		c.sendError("invalid_request", "bot_account_id is required", reqID)
		return nil, nil, false
	}
	chanID, joined := c.joinedChannels[channelName]
	if !joined {
		c.sendError("not_in_channel", "join the channel before issuing operator commands", reqID)
		return nil, nil, false
	}
	role := c.joinedRoles[channelName]
	if !role.CanModerate() {
		c.sendError("permission_denied", "requires owner, admin, or channel_op role", reqID)
		return nil, nil, false
	}
	botID, err := uuid.Parse(botAccountIDStr)
	if err != nil {
		c.sendError("invalid_request", "bot_account_id must be a UUID", reqID)
		return nil, nil, false
	}
	target, err := c.srv.store.GetAccountByID(ctx, c.tenantID, botID)
	if err != nil {
		c.sendError("not_found", "no such account in this tenant", reqID)
		return nil, nil, false
	}
	if target.Type != storage.AccountBot {
		c.sendError("not_a_bot", "target account is not a bot", reqID)
		return nil, nil, false
	}
	ch, err := c.srv.store.GetChannelByID(ctx, c.tenantID, chanID)
	if err != nil {
		c.sendError("channel_not_found", "channel no longer exists", reqID)
		return nil, nil, false
	}
	return target, ch, true
}

// recordChanopAudit appends one row to the moderation-events audit log.
// Storage errors are logged, not surfaced to the client — the action has
// already succeeded (pause row + in-memory state + system message); a
// missed audit row is a soft failure worth investigating but not worth
// rolling back the user-visible action for.
func (c *Conn) recordChanopAudit(ctx context.Context, chID, targetID uuid.UUID, action storage.ModerationAction, reason string) {
	err := c.srv.store.RecordModerationEvent(ctx, &storage.ModerationEvent{
		TenantID:  c.tenantID,
		ChannelID: chID,
		ActorID:   c.accountID,
		TargetID:  targetID,
		Action:    action,
		Reason:    reason,
	})
	if err != nil {
		c.log.Warn("chanop audit write failed", "action", action, "err", err)
	}
}

// emitChanopSystemMsg broadcasts a visible in-channel notice about the
// operator action so every member (and any bot with a live connection or
// active subscription) sees what happened. Same fan-out path as a normal
// chat message — including AppendHistory and durable persist — so the
// notice appears in chathistory replies.
//
// FromRole is stamped to FromRoleChannelOp so clients can style the
// notice distinctly. The From account is the operator's own account, so
// audit-by-eye works: the visible message names who did the thing.
func (c *Conn) emitChanopSystemMsg(ctx context.Context, channelName string, chanID uuid.UUID, body string) {
	ch := c.srv.channels.Get(c.tenantID, chanID)
	if ch == nil {
		return
	}
	now := time.Now().UTC()
	frame := proto.Msg{
		Channel:  channelName,
		From:     c.accountID.String(),
		FromRole: proto.FromRoleChannelOp,
		TS:       now,
		Body:     body,
	}
	bytes, err := proto.EncodeBytes(frame)
	if err != nil {
		c.log.Error("chanop system msg encode failed", "err", err)
		return
	}
	ch.Broadcast(bytes)
	ch.AppendHistory(channels.HistoryEntry{
		From:        c.accountID,
		DisplayName: c.displayName,
		TS:          now,
		Body:        body,
	})
	if c.srv.history != nil {
		c.srv.history.enqueue(newStoredMessage(c.tenantID, chanID, c.accountID, body, now))
	}
}

func formatReason(reason string) string {
	if reason == "" {
		return ""
	}
	return ": " + strconvQuote(reason)
}

// strconvQuote wraps the reason in double quotes without pulling strconv
// into the hot path — one-line helper keeps the message format stable
// regardless of what's in the reason.
func strconvQuote(s string) string {
	// Avoid escapes for typical operator input; a quote inside the reason
	// is unusual and the audit table has the exact bytes anyway.
	return `"` + s + `"`
}

// displayFromRole maps an ACL role to the wire value used in Msg.FromRole.
// Only roles that clients render distinctly get a non-empty string; regular
// members and public-channel senders get empty (no badge). Banned users
// can't send at all, so their case is a no-op here.
func displayFromRole(r storage.ChannelRole) string {
	switch r {
	case storage.RoleOwner, storage.RoleAdmin, storage.RoleChannelOp:
		return string(r)
	default:
		return ""
	}
}
