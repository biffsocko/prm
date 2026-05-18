package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/storage"
)

func mkTenantAccountChannel(t *testing.T) (*storage.Tenant, *storage.Account, *storage.Channel, storage.Store) {
	t.Helper()
	s := newStore(t)
	ctx := context.Background()

	ten := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(ctx, ten); err != nil {
		t.Fatal(err)
	}
	bot := &storage.Account{
		Username: "alertbot", Type: storage.AccountBot,
		PasswordHash: []byte("x"), PasswordSalt: []byte("x"), PasswordParams: "argon2id,m=65536,t=3,p=1",
	}
	if err := s.CreateAccount(ctx, ten.ID, bot); err != nil {
		t.Fatal(err)
	}
	ch := &storage.Channel{Name: "ops", OwnerID: bot.ID, Visibility: storage.ChannelPublic}
	if err := s.CreateChannel(ctx, ten.ID, ch); err != nil {
		t.Fatal(err)
	}
	return ten, bot, ch, s
}

func TestSubscriptionCRUD(t *testing.T) {
	ten, bot, ch, s := mkTenantAccountChannel(t)
	ctx := context.Background()

	matchJSON, _ := json.Marshal(map[string]any{
		"any_of": []map[string]any{{"type": "regex", "pattern": "(?i)^deploy"}},
	})
	budgetJSON, _ := json.Marshal(map[string]any{
		"daily_max_fires": 500,
	})

	sub := &storage.Subscription{
		AccountID: bot.ID, ChannelID: ch.ID,
		URL:          "https://bot.example.com/prm-webhook",
		Secret:   sha256Sum("webhook-secret"),
		MatchJSON:    matchJSON,
		Events:       []string{"message"},
		ContextLines: 8,
		DebounceMs:   750,
		CooldownMs:   5000,
		BudgetJSON:   budgetJSON,
	}
	if err := s.CreateSubscription(ctx, ten.ID, sub); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if sub.ID == uuid.Nil {
		t.Fatal("expected ID")
	}

	got, err := s.GetSubscriptionByID(ctx, ten.ID, sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.URL != sub.URL {
		t.Errorf("URL mismatch")
	}
	if got.ContextLines != 8 {
		t.Errorf("context_lines mismatch: %d", got.ContextLines)
	}
	if string(got.MatchJSON) != string(matchJSON) {
		t.Errorf("match_json round-trip failed")
	}

	// List by account
	list, err := s.ListSubscriptionsByAccount(ctx, ten.ID, bot.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 subscription, got %d", len(list))
	}

	// List by channel (active only — disabled_at = 0)
	listCh, err := s.ListSubscriptionsByChannel(ctx, ten.ID, ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(listCh) != 1 {
		t.Fatalf("expected 1 active subscription on channel, got %d", len(listCh))
	}

	// Update
	sub.DebounceMs = 1000
	sub.URL = "https://bot.example.com/v2/prm-webhook"
	if err := s.UpdateSubscription(ctx, ten.ID, sub); err != nil {
		t.Fatal(err)
	}
	got2, err := s.GetSubscriptionByID(ctx, ten.ID, sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got2.DebounceMs != 1000 {
		t.Errorf("debounce update failed")
	}
	if got2.URL != sub.URL {
		t.Errorf("url update failed")
	}

	// Disabling: set DisabledAt and re-list-by-channel
	sub.DisabledAt = time.Now().UTC()
	if err := s.UpdateSubscription(ctx, ten.ID, sub); err != nil {
		t.Fatal(err)
	}
	listCh2, err := s.ListSubscriptionsByChannel(ctx, ten.ID, ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(listCh2) != 0 {
		t.Fatalf("disabled subscription should not appear in active list; got %d", len(listCh2))
	}

	// Delete
	if err := s.DeleteSubscription(ctx, ten.ID, sub.ID); err != nil {
		t.Fatal(err)
	}
	_, err = s.GetSubscriptionByID(ctx, ten.ID, sub.ID)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	if err := s.DeleteSubscription(ctx, ten.ID, sub.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("re-delete should be ErrNotFound, got %v", err)
	}
}

func TestSubscriptionCrossTenantIsolation(t *testing.T) {
	ten, bot, ch, s := mkTenantAccountChannel(t)
	ctx := context.Background()

	// Set up a second tenant.
	t2 := &storage.Tenant{Slug: "globex", DisplayName: "Globex"}
	if err := s.CreateTenant(ctx, t2); err != nil {
		t.Fatal(err)
	}

	match, _ := json.Marshal(map[string]any{"any_of": []map[string]any{{"type": "regex", "pattern": "x"}}})
	sub := &storage.Subscription{AccountID: bot.ID, ChannelID: ch.ID, URL: "https://x", Secret: sha256Sum("s"), MatchJSON: match}
	if err := s.CreateSubscription(ctx, ten.ID, sub); err != nil {
		t.Fatal(err)
	}

	// Tenant t2 trying to read tenant ten's subscription gets ErrNotFound.
	_, err := s.GetSubscriptionByID(ctx, t2.ID, sub.ID)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cross-tenant get should be ErrNotFound, got %v", err)
	}
	// Same for update.
	err = s.UpdateSubscription(ctx, t2.ID, sub)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cross-tenant update should be ErrNotFound, got %v", err)
	}
	// Same for delete.
	err = s.DeleteSubscription(ctx, t2.ID, sub.ID)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cross-tenant delete should be ErrNotFound, got %v", err)
	}
}

func TestSubscriptionFiresCount(t *testing.T) {
	ten, bot, ch, s := mkTenantAccountChannel(t)
	ctx := context.Background()
	match, _ := json.Marshal(map[string]any{"any_of": []map[string]any{{"type": "regex", "pattern": "x"}}})
	sub := &storage.Subscription{AccountID: bot.ID, ChannelID: ch.ID, URL: "https://x", Secret: sha256Sum("s"), MatchJSON: match}
	if err := s.CreateSubscription(ctx, ten.ID, sub); err != nil {
		t.Fatal(err)
	}

	// No fires yet.
	n, err := s.CountSubscriptionFiresSince(ctx, ten.ID, sub.ID, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("expected 0 fires, got %d", n)
	}

	// Record 3 successful fires.
	for i := 0; i < 3; i++ {
		f := &storage.SubscriptionFire{
			TenantID: ten.ID, SubscriptionID: sub.ID,
			Status: "ok", Attempts: 1,
		}
		if err := s.RecordSubscriptionFire(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	// Record one failed fire (should not count toward budget).
	if err := s.RecordSubscriptionFire(ctx, &storage.SubscriptionFire{
		TenantID: ten.ID, SubscriptionID: sub.ID, Status: "failed", Attempts: 3, LastError: "timeout",
	}); err != nil {
		t.Fatal(err)
	}

	n, err = s.CountSubscriptionFiresSince(ctx, ten.ID, sub.ID, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("expected 3 successful fires (failed shouldn't count), got %d", n)
	}
}
