package sqlite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/storage"
)

func mkIntegrationFixture(t *testing.T) (*storage.Tenant, *storage.Account, *storage.Channel, storage.Store) {
	t.Helper()
	return mkTenantAccountChannel(t)
}

func TestIntegrationCRUD(t *testing.T) {
	ten, bot, ch, s := mkIntegrationFixture(t)
	ctx := context.Background()

	hash := sha256Sum("integration-token-1")
	integ := &storage.Integration{
		ChannelID:    ch.ID,
		AccountID:    bot.ID,
		Adapter:      "splunk",
		TokenHash:    hash,
		SettingsJSON: []byte(`{"service_path":"$.result.service"}`),
	}
	if err := s.CreateIntegration(ctx, ten.ID, integ); err != nil {
		t.Fatalf("CreateIntegration: %v", err)
	}
	if integ.ID == uuid.Nil {
		t.Fatal("expected ID populated")
	}

	got, err := s.GetIntegrationByID(ctx, ten.ID, integ.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Adapter != "splunk" {
		t.Errorf("adapter mismatch")
	}
	if got.TenantID != ten.ID {
		t.Errorf("tenant mismatch")
	}

	// Lookup by hash (tenant-less).
	gotByHash, err := s.GetIntegrationByTokenHash(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if gotByHash.ID != integ.ID {
		t.Errorf("hash lookup returned different integration")
	}

	// List by tenant.
	list, err := s.ListIntegrationsByTenant(ctx, ten.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1, got %d", len(list))
	}

	// Update: rebind to a new (still owned) channel + change adapter.
	ch2 := &storage.Channel{Name: "ops2", OwnerID: bot.ID, Visibility: storage.ChannelPublic}
	if err := s.CreateChannel(ctx, ten.ID, ch2); err != nil {
		t.Fatal(err)
	}
	integ.ChannelID = ch2.ID
	integ.Adapter = "graylog"
	if err := s.UpdateIntegration(ctx, ten.ID, integ); err != nil {
		t.Fatal(err)
	}
	got2, _ := s.GetIntegrationByID(ctx, ten.ID, integ.ID)
	if got2.Adapter != "graylog" || got2.ChannelID != ch2.ID {
		t.Errorf("update didn't persist")
	}

	// Disable -> hash lookup returns ErrNotFound (active-only filter).
	integ.DisabledAt = time.Now().UTC()
	if err := s.UpdateIntegration(ctx, ten.ID, integ); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetIntegrationByTokenHash(ctx, hash); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("disabled integration should not be findable by hash; got %v", err)
	}

	// Delete.
	if err := s.DeleteIntegration(ctx, ten.ID, integ.ID); err != nil {
		t.Fatal(err)
	}
	_, err = s.GetIntegrationByID(ctx, ten.ID, integ.ID)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestIntegrationCrossTenantIsolation(t *testing.T) {
	ten, bot, ch, s := mkIntegrationFixture(t)
	ctx := context.Background()

	t2 := &storage.Tenant{Slug: "globex", DisplayName: "Globex"}
	if err := s.CreateTenant(ctx, t2); err != nil {
		t.Fatal(err)
	}
	hash := sha256Sum("only-acme-integration")
	integ := &storage.Integration{
		ChannelID: ch.ID, AccountID: bot.ID, Adapter: "splunk", TokenHash: hash,
	}
	if err := s.CreateIntegration(ctx, ten.ID, integ); err != nil {
		t.Fatal(err)
	}

	// t2 cannot read it via tenant-scoped get.
	_, err := s.GetIntegrationByID(ctx, t2.ID, integ.ID)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("cross-tenant get should be ErrNotFound, got %v", err)
	}
	// Hash lookup IS tenant-less by design (the hash IS the proof of
	// tenancy). The result carries TenantID, which the caller checks.
	got, err := s.GetIntegrationByTokenHash(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if got.TenantID != ten.ID {
		t.Errorf("hash lookup returned wrong tenant")
	}
}

func TestIntegrationTokenHashGlobalUniqueness(t *testing.T) {
	ten, bot, ch, s := mkIntegrationFixture(t)
	ctx := context.Background()
	t2 := &storage.Tenant{Slug: "globex", DisplayName: "Globex"}
	if err := s.CreateTenant(ctx, t2); err != nil {
		t.Fatal(err)
	}
	hash := sha256Sum("same-everywhere")
	if err := s.CreateIntegration(ctx, ten.ID, &storage.Integration{
		ChannelID: ch.ID, AccountID: bot.ID, Adapter: "splunk", TokenHash: hash,
	}); err != nil {
		t.Fatal(err)
	}
	// Different tenant, same hash, must collide.
	err := s.CreateIntegration(ctx, t2.ID, &storage.Integration{
		ChannelID: ch.ID, AccountID: bot.ID, Adapter: "splunk", TokenHash: hash,
	})
	if !errors.Is(err, storage.ErrAlreadyExists) {
		t.Errorf("expected token-hash collision, got %v", err)
	}
}
