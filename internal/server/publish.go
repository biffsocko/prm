package server

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/channels"
	"github.com/biffsocko/prm/internal/inbound"
	"github.com/biffsocko/prm/internal/proto"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/webhook"
)

// PublishInbound republishes a normalized inbound event onto the given
// channel. Does the same three things handleMsg does for a chat
// message:
//   - snapshot channel history (for webhook context-attach)
//   - broadcast the encoded Msg to chat members
//   - append to channel history (so the NEXT republished msg's webhook
//     snapshot will include this one)
//   - notify the webhook manager so matching subscriptions fire
//
// from is the account_id that "speaks" the republished event (typically
// the integration's bound bot account). It's stamped into proto.Msg.From
// for both chat broadcast and webhook payload purposes.
//
// This is the bridge between the inbound integration handler (in the
// rest package) and the realtime broadcast path. server.Server satisfies
// rest.EventPublisher via this method.
func (s *Server) PublishInbound(ctx context.Context, tenantID, channelID, fromAccountID uuid.UUID, ev inbound.Event) error {
	// Resolve channel name (needed for the wire frame's Channel field).
	// Cold-path lookup is acceptable here -- inbound integrations are
	// nowhere near the chat-broadcast hot path's frequency.
	ch, err := s.store.GetChannelByID(ctx, tenantID, channelID)
	if err != nil {
		return fmt.Errorf("publish inbound: lookup channel: %w", err)
	}

	body := FormatEventBody(ev)
	ts := ev.OccurredAt
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	// Build the broadcast frame once.
	out := proto.Msg{
		Channel: ch.Name,
		From:    fromAccountID.String(),
		TS:      ts,
		Body:    body,
	}
	frameBytes, err := proto.EncodeBytes(out)
	if err != nil {
		return fmt.Errorf("publish inbound: encode: %w", err)
	}

	// Get-or-create the in-memory channel state. The channel may have
	// no live members yet (no one's JOINed) -- that's fine, broadcast
	// is a no-op and history is recorded for whoever joins next.
	chState := s.channels.GetOrCreate(tenantID, channelID, ch.Name)

	// Snapshot history BEFORE appending this event so the snapshot
	// matches "context preceding the event" for any subscription that
	// fires on it.
	var hist []channels.HistoryEntry
	if s.webhooks != nil {
		hist = chState.RecentMessages(32)
	}

	chState.Broadcast(frameBytes)
	chState.AppendHistory(channels.HistoryEntry{
		From:        fromAccountID,
		DisplayName: ev.Source, // for now; future: resolve the integration's display name
		TS:          ts,
		Body:        body,
	})

	// Durable history persist (async; never blocks).
	if s.history != nil {
		s.history.enqueue(newStoredMessage(tenantID, channelID, fromAccountID, body, ts))
	}

	if s.webhooks != nil {
		s.webhooks.Notify(webhook.Event{
			TenantID:    tenantID,
			ChannelID:   channelID,
			ChannelName: ch.Name,
			From:        fromAccountID,
			DisplayName: ev.Source,
			Body:        body,
			TS:          ts,
			Context:     hist,
		})
	}
	return nil
}

// FormatEventBody produces the wire Body for a republished inbound
// event. The format is regex-friendly for subscription matchers:
//
//	[severity] source/service: summary
//
// Examples:
//
//	[error] splunk/auth-api: Auth API 5xx Spike (count=47)
//	[critical] graylog/db-primary: DB out of disk
//	[info] generic: Build #42 succeeded
//
// Service is omitted when empty. Severity defaults to "info" if the
// adapter didn't set one.
func FormatEventBody(ev inbound.Event) string {
	sev := ev.Severity
	if sev == "" {
		sev = inbound.SeverityInfo
	}
	src := ev.Source
	if src == "" {
		src = "inbound"
	}
	prefix := "[" + sev + "] " + src
	if ev.Service != "" {
		prefix += "/" + ev.Service
	}
	if ev.Summary == "" {
		return prefix
	}
	return prefix + ": " + ev.Summary
}

// _ enforces that *storage.Channel is reachable from this file's
// imports so a future refactor doesn't accidentally drop the
// dependency.
var _ = storage.ChannelPublic
