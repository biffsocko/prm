// Package subops is the shared business logic for subscription CRUD,
// invoked from both the REST control plane (HTTP) and the realtime
// protocol handlers (slice 3b). Both paths need to do the same things:
//
//   - validate match rules
//   - resolve channel by name or id within the caller's tenant
//   - enforce cross-account isolation
//   - generate the HMAC secret on create
//   - notify the webhook manager after successful mutations
//
// Keeping this in one package keeps the two delivery paths in sync.
package subops

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/matcher"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/webhook"
)

// Sentinel errors — wrapped versions of underlying storage / validation
// failures, mapped by the calling layer to its own status-code or
// reason-code scheme.
var (
	ErrBadRequest = errors.New("subops: bad request")
	ErrNotFound   = errors.New("subops: not found")
	ErrNotOwner   = errors.New("subops: not owner")
	ErrInternal   = errors.New("subops: internal")
)

// CreateInput is what callers pass to Create. Exactly one of ChannelName
// or ChannelID must be set.
type CreateInput struct {
	ChannelName  string
	ChannelID    string
	URL          string
	Match        []byte // raw JSON match rules
	Events       []string
	ContextLines int
	DebounceMs   int
	CooldownMs   int
	Budget       []byte // raw JSON budget
}

// Create validates input, resolves the channel, generates a fresh HMAC
// secret, persists the subscription, and (if mgr != nil) registers it
// with the webhook manager. The returned subscription's Secret field is
// the plaintext for one-time display.
func Create(ctx context.Context, st storage.Store, mgr *webhook.Manager, tenant *storage.Tenant, bot *storage.Account, in CreateInput) (*storage.Subscription, error) {
	if bot.Type != storage.AccountBot {
		return nil, fmt.Errorf("%w: subscriptions are owned by bot accounts only", ErrBadRequest)
	}
	if in.URL == "" {
		return nil, fmt.Errorf("%w: url is required", ErrBadRequest)
	}
	if len(in.Match) == 0 {
		return nil, fmt.Errorf("%w: match rules are required", ErrBadRequest)
	}
	if (in.ChannelName == "") == (in.ChannelID == "") {
		return nil, fmt.Errorf("%w: exactly one of channel_name or channel_id is required", ErrBadRequest)
	}

	channelID, err := resolveChannelID(ctx, st, tenant.ID, in.ChannelName, in.ChannelID)
	if err != nil {
		return nil, err
	}

	if _, err := matcher.Compile(in.Match); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
	}

	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, fmt.Errorf("%w: secret generation: %v", ErrInternal, err)
	}

	events := in.Events
	if len(events) == 0 {
		events = []string{"message"}
	}
	budget := in.Budget
	if len(budget) == 0 {
		budget = []byte("{}")
	}

	sub := &storage.Subscription{
		AccountID:    bot.ID,
		ChannelID:    channelID,
		URL:          in.URL,
		Secret:       secret,
		MatchJSON:    in.Match,
		Events:       events,
		ContextLines: in.ContextLines,
		DebounceMs:   in.DebounceMs,
		CooldownMs:   in.CooldownMs,
		BudgetJSON:   budget,
	}
	if err := st.CreateSubscription(ctx, tenant.ID, sub); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInternal, err)
	}
	if mgr != nil {
		// Don't fail the API call on cache update failure -- the
		// subscription is persisted; next manager Reload picks it up.
		_ = mgr.AddOrUpdate(sub)
	}
	return sub, nil
}

// Get fetches one subscription, enforcing tenant + account isolation.
func Get(ctx context.Context, st storage.Store, tenant *storage.Tenant, bot *storage.Account, subscriptionID string) (*storage.Subscription, error) {
	id, err := uuid.Parse(subscriptionID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	sub, err := st.GetSubscriptionByID(ctx, tenant.ID, id)
	if err != nil {
		return nil, fmt.Errorf("%w", ErrNotFound)
	}
	if sub.AccountID != bot.ID {
		return nil, fmt.Errorf("%w", ErrNotOwner)
	}
	return sub, nil
}

// List returns all subscriptions owned by the caller's bot account in
// the caller's tenant.
func List(ctx context.Context, st storage.Store, tenant *storage.Tenant, bot *storage.Account) ([]*storage.Subscription, error) {
	list, err := st.ListSubscriptionsByAccount(ctx, tenant.ID, bot.ID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInternal, err)
	}
	return list, nil
}

// UpdateInput is the patch shape. All fields optional; nil means
// "no change."
type UpdateInput struct {
	URL          *string
	Match        []byte
	Events       []string
	ContextLines *int
	DebounceMs   *int
	CooldownMs   *int
	Budget       []byte
	Disabled     *bool
}

// Update applies a patch. Returns the post-update subscription.
func Update(ctx context.Context, st storage.Store, mgr *webhook.Manager, tenant *storage.Tenant, bot *storage.Account, subscriptionID string, in UpdateInput) (*storage.Subscription, error) {
	sub, err := Get(ctx, st, tenant, bot, subscriptionID)
	if err != nil {
		return nil, err
	}
	if in.URL != nil {
		sub.URL = *in.URL
	}
	if in.Match != nil {
		if _, err := matcher.Compile(in.Match); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
		}
		sub.MatchJSON = in.Match
	}
	if in.Events != nil {
		sub.Events = in.Events
	}
	if in.ContextLines != nil {
		sub.ContextLines = *in.ContextLines
	}
	if in.DebounceMs != nil {
		sub.DebounceMs = *in.DebounceMs
	}
	if in.CooldownMs != nil {
		sub.CooldownMs = *in.CooldownMs
	}
	if in.Budget != nil {
		sub.BudgetJSON = in.Budget
	}
	if in.Disabled != nil {
		if *in.Disabled {
			if sub.DisabledAt.IsZero() {
				sub.DisabledAt = time.Now().UTC()
			}
		} else {
			sub.DisabledAt = time.Time{}
		}
	}
	if err := st.UpdateSubscription(ctx, tenant.ID, sub); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInternal, err)
	}
	if mgr != nil {
		if sub.DisabledAt.IsZero() {
			_ = mgr.AddOrUpdate(sub)
		} else {
			mgr.Remove(sub.ID)
		}
	}
	return sub, nil
}

// Delete removes a subscription.
func Delete(ctx context.Context, st storage.Store, mgr *webhook.Manager, tenant *storage.Tenant, bot *storage.Account, subscriptionID string) error {
	sub, err := Get(ctx, st, tenant, bot, subscriptionID)
	if err != nil {
		return err
	}
	if err := st.DeleteSubscription(ctx, tenant.ID, sub.ID); err != nil {
		return fmt.Errorf("%w: %v", ErrInternal, err)
	}
	if mgr != nil {
		mgr.Remove(sub.ID)
	}
	return nil
}

// resolveChannelID accepts a name OR an id (exactly one expected) and
// returns the canonical channel UUID after a tenant-scoped lookup.
func resolveChannelID(ctx context.Context, st storage.Store, tenantID uuid.UUID, name, id string) (uuid.UUID, error) {
	if id != "" {
		chID, err := uuid.Parse(id)
		if err != nil {
			return uuid.Nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
		}
		ch, err := st.GetChannelByID(ctx, tenantID, chID)
		if err != nil {
			return uuid.Nil, fmt.Errorf("%w: channel", ErrNotFound)
		}
		return ch.ID, nil
	}
	ch, err := st.GetChannelByName(ctx, tenantID, name)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: channel", ErrNotFound)
	}
	return ch.ID, nil
}
