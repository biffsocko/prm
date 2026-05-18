package server

import (
	"context"
	"sort"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/proto"
	"github.com/biffsocko/prm/internal/storage"
)

// handleMembers responds to a Members request with the channel's
// effective membership: every live realtime connection plus every bot
// account with an active webhook subscription on the channel.
//
// Ghost detection: a row is marked IsGhost=true when an account has an
// active subscription on the channel but no live realtime connection
// at the moment of the call. Ghosts let humans see "who is effectively
// receiving from this channel" -- webhook-only bots aren't socket
// members but they ARE receiving events, and that should show up in UIs.
//
// Accounts that have BOTH a live connection AND an active subscription
// are reported as live (IsGhost=false). The IsGhost flag specifically
// answers "does this row exist solely because of a webhook
// subscription?"; a hybrid presence is a live presence.
//
// Ordering: live members first (sorted by display name, then account
// id for stability), then ghosts (same sub-ordering). UIs can rely on
// this without re-sorting.
func (c *Conn) handleMembers(ctx context.Context, m proto.Members) {
	if m.Channel == "" {
		c.sendError("invalid_request", "channel is required", m.ID)
		return
	}
	ch, err := c.srv.store.GetChannelByName(ctx, c.tenantID, m.Channel)
	if err != nil {
		c.sendError("channel_not_found", "no such channel in this tenant", m.ID)
		return
	}
	// ACL: same check as join. We're disclosing membership identities,
	// so the caller must be allowed to be in the channel themselves.
	allowed, reason := c.srv.canJoin(ctx, c.tenantID, ch, c.accountID)
	if !allowed {
		c.sendError(reason, "not permitted to view this channel's members", m.ID)
		return
	}

	// Live members from the in-memory channel registry. May be empty
	// if no one's joined yet -- that's fine; we still compute ghosts.
	type liveAggr struct {
		displayName string
		count       int
	}
	live := map[uuid.UUID]*liveAggr{}
	chState := c.srv.channels.Get(c.tenantID, ch.ID)
	if chState != nil {
		for _, mem := range chState.Members() {
			a := live[mem.AccountID()]
			if a == nil {
				a = &liveAggr{displayName: mem.DisplayName()}
				live[mem.AccountID()] = a
			}
			a.count++
		}
	}

	// Ghosts: distinct bot accounts with an active subscription on
	// this channel that AREN'T in the live set.
	subs, err := c.srv.store.ListSubscriptionsByChannel(ctx, c.tenantID, ch.ID)
	if err != nil {
		c.sendError("internal", "list subscriptions", m.ID)
		return
	}
	ghostAccountIDs := map[uuid.UUID]struct{}{}
	for _, s := range subs {
		if _, isLive := live[s.AccountID]; isLive {
			continue
		}
		ghostAccountIDs[s.AccountID] = struct{}{}
	}

	// Resolve account metadata for live + ghost rows. Live rows
	// already have a display name cached on the connection but no
	// account type; we need a storage lookup for type. Ghost rows
	// need both display name and type.
	infos := make([]proto.MemberInfo, 0, len(live)+len(ghostAccountIDs))
	for accountID, a := range live {
		acc, err := c.srv.store.GetAccountByID(ctx, c.tenantID, accountID)
		accountType := string(storage.AccountHuman)
		displayName := a.displayName
		if err == nil && acc != nil {
			accountType = string(acc.Type)
			if displayName == "" {
				displayName = acc.DisplayName
			}
		}
		infos = append(infos, proto.MemberInfo{
			AccountID:   accountID.String(),
			DisplayName: displayName,
			AccountType: accountType,
			IsGhost:     false,
			ConnCount:   a.count,
		})
	}
	for accountID := range ghostAccountIDs {
		acc, err := c.srv.store.GetAccountByID(ctx, c.tenantID, accountID)
		if err != nil || acc == nil {
			// Orphaned subscription pointing at a deleted account;
			// skip rather than emit a bogus ghost row.
			continue
		}
		infos = append(infos, proto.MemberInfo{
			AccountID:   accountID.String(),
			DisplayName: acc.DisplayName,
			AccountType: string(acc.Type),
			IsGhost:     true,
			ConnCount:   0,
		})
	}

	sortMembers(infos)
	c.sendFrame(proto.MembersOK{ID: m.ID, Channel: m.Channel, Members: infos})
}

// sortMembers orders the response: live (IsGhost=false) before ghosts,
// then by DisplayName, then by AccountID. Stable on identical fields.
func sortMembers(infos []proto.MemberInfo) {
	sort.SliceStable(infos, func(i, j int) bool {
		if infos[i].IsGhost != infos[j].IsGhost {
			return !infos[i].IsGhost
		}
		if infos[i].DisplayName != infos[j].DisplayName {
			return infos[i].DisplayName < infos[j].DisplayName
		}
		return infos[i].AccountID < infos[j].AccountID
	})
}
