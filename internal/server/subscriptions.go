package server

import (
	"context"
	"encoding/base64"
	"errors"

	"github.com/biffsocko/prm/internal/proto"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/subops"
)

// Slice 3b: subscription management over the realtime PRM protocol.
// Mirrors the REST control plane's subscription CRUD. Bot accounts can
// manage their subscriptions over the same authenticated TLS socket they
// use for chat. Auth is implicit (the connection is already authed); the
// connection's account must be a bot.

func (c *Conn) handleSubscriptionCreate(ctx context.Context, m proto.SubscriptionCreate) {
	tenant, bot, ok := c.requireBot(m.ID)
	if !ok {
		return
	}
	sub, err := subops.Create(ctx, c.srv.store, c.srv.webhooks, tenant, bot, subops.CreateInput{
		ChannelName:  m.ChannelName,
		ChannelID:    m.ChannelID,
		URL:          m.URL,
		Match:        m.Match,
		Events:       m.Events,
		ContextLines: m.ContextLines,
		DebounceMs:   m.DebounceMs,
		CooldownMs:   m.CooldownMs,
		Budget:       m.Budget,
	})
	if err != nil {
		c.sendSubopsError(err, m.ID)
		return
	}
	c.sendFrame(proto.SubscriptionOK{
		ID:           m.ID,
		Subscription: subInfo(sub, true),
	})
}

func (c *Conn) handleSubscriptionList(ctx context.Context, m proto.SubscriptionList) {
	tenant, bot, ok := c.requireBot(m.ID)
	if !ok {
		return
	}
	list, err := subops.List(ctx, c.srv.store, tenant, bot)
	if err != nil {
		c.sendSubopsError(err, m.ID)
		return
	}
	infos := make([]proto.SubscriptionInfo, 0, len(list))
	for _, sub := range list {
		infos = append(infos, subInfo(sub, false))
	}
	c.sendFrame(proto.SubscriptionListOK{ID: m.ID, Subscriptions: infos})
}

func (c *Conn) handleSubscriptionGet(ctx context.Context, m proto.SubscriptionGet) {
	tenant, bot, ok := c.requireBot(m.ID)
	if !ok {
		return
	}
	sub, err := subops.Get(ctx, c.srv.store, tenant, bot, m.SubscriptionID)
	if err != nil {
		c.sendSubopsError(err, m.ID)
		return
	}
	c.sendFrame(proto.SubscriptionOK{ID: m.ID, Subscription: subInfo(sub, false)})
}

func (c *Conn) handleSubscriptionUpdate(ctx context.Context, m proto.SubscriptionUpdate) {
	tenant, bot, ok := c.requireBot(m.ID)
	if !ok {
		return
	}
	in := subops.UpdateInput{
		URL:          m.URL,
		Match:        m.Match,
		Events:       m.Events,
		ContextLines: m.ContextLines,
		DebounceMs:   m.DebounceMs,
		CooldownMs:   m.CooldownMs,
		Budget:       m.Budget,
		Disabled:     m.Disabled,
	}
	sub, err := subops.Update(ctx, c.srv.store, c.srv.webhooks, tenant, bot, m.SubscriptionID, in)
	if err != nil {
		c.sendSubopsError(err, m.ID)
		return
	}
	c.sendFrame(proto.SubscriptionOK{ID: m.ID, Subscription: subInfo(sub, false)})
}

func (c *Conn) handleSubscriptionDelete(ctx context.Context, m proto.SubscriptionDelete) {
	tenant, bot, ok := c.requireBot(m.ID)
	if !ok {
		return
	}
	if err := subops.Delete(ctx, c.srv.store, c.srv.webhooks, tenant, bot, m.SubscriptionID); err != nil {
		c.sendSubopsError(err, m.ID)
		return
	}
	c.sendFrame(proto.SubscriptionDeleted{ID: m.ID, SubscriptionID: m.SubscriptionID})
}

// --- helpers ---

// requireBot resolves the connection's tenant + account and refuses if
// the account isn't a bot. Returns (tenant, bot, ok); on !ok an error
// frame has already been sent.
func (c *Conn) requireBot(reqID string) (*storage.Tenant, *storage.Account, bool) {
	tenant, err := c.srv.store.GetTenantByID(context.Background(), c.tenantID)
	if err != nil {
		c.sendError("internal", "tenant lookup", reqID)
		return nil, nil, false
	}
	bot, err := c.srv.store.GetAccountByID(context.Background(), c.tenantID, c.accountID)
	if err != nil {
		c.sendError("internal", "account lookup", reqID)
		return nil, nil, false
	}
	if bot.Type != storage.AccountBot {
		c.sendError("not_a_bot", "subscription management requires a bot account", reqID)
		return nil, nil, false
	}
	return tenant, bot, true
}

func (c *Conn) sendSubopsError(err error, reqID string) {
	switch {
	case errors.Is(err, subops.ErrBadRequest):
		c.sendError("bad_request", err.Error(), reqID)
	case errors.Is(err, subops.ErrNotFound):
		c.sendError("not_found", err.Error(), reqID)
	case errors.Is(err, subops.ErrNotOwner):
		c.sendError("not_owner", err.Error(), reqID)
	default:
		c.sendError("internal", err.Error(), reqID)
	}
}

func subInfo(sub *storage.Subscription, includeSecret bool) proto.SubscriptionInfo {
	info := proto.SubscriptionInfo{
		ID:           sub.ID.String(),
		TenantID:     sub.TenantID.String(),
		AccountID:    sub.AccountID.String(),
		ChannelID:    sub.ChannelID.String(),
		URL:          sub.URL,
		Match:        sub.MatchJSON,
		Events:       sub.Events,
		ContextLines: sub.ContextLines,
		DebounceMs:   sub.DebounceMs,
		CooldownMs:   sub.CooldownMs,
		Budget:       sub.BudgetJSON,
		CreatedAt:    sub.CreatedAt,
	}
	if !sub.DisabledAt.IsZero() {
		t := sub.DisabledAt
		info.DisabledAt = &t
	}
	if includeSecret {
		info.Secret = base64.RawURLEncoding.EncodeToString(sub.Secret)
	}
	return info
}

