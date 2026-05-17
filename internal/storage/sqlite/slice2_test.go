package sqlite_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/storage"
)

// ---- channels ----

func TestChannelCRUD(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	ten := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(ctx, ten); err != nil {
		t.Fatal(err)
	}
	owner := uuid.Must(uuid.NewV7())

	ch := &storage.Channel{Name: "general", OwnerID: owner, Visibility: storage.ChannelPublic}
	if err := s.CreateChannel(ctx, ten.ID, ch); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if ch.ID == uuid.Nil {
		t.Fatal("expected ID filled in")
	}
	if ch.TenantID != ten.ID {
		t.Fatalf("tenant mismatch")
	}

	got, err := s.GetChannelByName(ctx, ten.ID, "general")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != ch.ID {
		t.Fatal("id mismatch")
	}

	gotByID, err := s.GetChannelByID(ctx, ten.ID, ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotByID.Visibility != storage.ChannelPublic {
		t.Fatalf("visibility mismatch: %q", gotByID.Visibility)
	}

	// Duplicate name in same tenant rejected
	dup := &storage.Channel{Name: "general", OwnerID: owner}
	err = s.CreateChannel(ctx, ten.ID, dup)
	if !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	// Different tenant can have the same channel name
	t2 := &storage.Tenant{Slug: "globex", DisplayName: "Globex"}
	if err := s.CreateTenant(ctx, t2); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateChannel(ctx, t2.ID, &storage.Channel{Name: "general", OwnerID: owner}); err != nil {
		t.Fatalf("same-name in other tenant should be allowed: %v", err)
	}

	// List
	list, err := s.ListChannels(ctx, ten.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 channel in acme, got %d", len(list))
	}

	// Cross-tenant lookup returns ErrNotFound
	_, err = s.GetChannelByID(ctx, t2.ID, ch.ID)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cross-tenant get should be ErrNotFound, got %v", err)
	}
}

// ---- channel ACL ----

func TestChannelACLUpsertAndLookup(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	ten := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(ctx, ten); err != nil {
		t.Fatal(err)
	}
	alice := uuid.Must(uuid.NewV7())
	bob := uuid.Must(uuid.NewV7())
	ch := &storage.Channel{Name: "general", OwnerID: alice}
	if err := s.CreateChannel(ctx, ten.ID, ch); err != nil {
		t.Fatal(err)
	}

	if err := s.SetChannelACL(ctx, ten.ID, ch.ID, alice, storage.RoleOwner, uuid.Nil); err != nil {
		t.Fatal(err)
	}
	if err := s.SetChannelACL(ctx, ten.ID, ch.ID, bob, storage.RoleMember, alice); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetChannelACL(ctx, ten.ID, ch.ID, bob)
	if err != nil {
		t.Fatal(err)
	}
	if got.Role != storage.RoleMember {
		t.Fatalf("expected member, got %q", got.Role)
	}
	if got.GrantedBy != alice {
		t.Fatalf("granted_by mismatch")
	}

	// Upsert: changing role keeps the same ACL row
	if err := s.SetChannelACL(ctx, ten.ID, ch.ID, bob, storage.RoleAdmin, alice); err != nil {
		t.Fatal(err)
	}
	got2, err := s.GetChannelACL(ctx, ten.ID, ch.ID, bob)
	if err != nil {
		t.Fatal(err)
	}
	if got2.Role != storage.RoleAdmin {
		t.Fatalf("expected admin after upsert, got %q", got2.Role)
	}

	list, err := s.ListChannelACL(ctx, ten.ID, ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 ACL entries, got %d", len(list))
	}

	// Remove
	if err := s.RemoveChannelACL(ctx, ten.ID, ch.ID, bob); err != nil {
		t.Fatal(err)
	}
	_, err = s.GetChannelACL(ctx, ten.ID, ch.ID, bob)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after remove, got %v", err)
	}
}

func TestChannelRoleCanJoin(t *testing.T) {
	cases := map[storage.ChannelRole]bool{
		storage.RoleOwner:  true,
		storage.RoleAdmin:  true,
		storage.RoleMember: true,
		storage.RoleBanned: false,
	}
	for role, want := range cases {
		if got := role.CanJoin(); got != want {
			t.Errorf("%q.CanJoin() = %v, want %v", role, got, want)
		}
	}
}

// ---- tokens ----

func TestTokenCRUDAndLookup(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	ten := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(ctx, ten); err != nil {
		t.Fatal(err)
	}
	bot := &storage.Account{
		Username: "alertbot", DisplayName: "Alert Bot", Type: storage.AccountBot,
		PasswordHash: []byte("x"), PasswordSalt: []byte("x"), PasswordParams: "argon2id,m=65536,t=3,p=1",
	}
	if err := s.CreateAccount(ctx, ten.ID, bot); err != nil {
		t.Fatal(err)
	}

	hash1 := sha256Sum("plaintext-token-1")
	tok, err := s.CreateToken(ctx, ten.ID, bot.ID, hash1, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if tok.ID == uuid.Nil {
		t.Fatal("expected token ID")
	}

	got, err := s.GetTokenByHash(ctx, hash1)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != tok.ID {
		t.Fatal("id mismatch")
	}
	if got.AccountID != bot.ID {
		t.Fatal("account id mismatch")
	}
	if got.TenantID != ten.ID {
		t.Fatal("tenant id mismatch -- the whole point is the token carries tenancy")
	}

	// LastUsed touch
	if err := s.TouchTokenLastUsed(ctx, tok.ID); err != nil {
		t.Fatal(err)
	}
	got2, _ := s.GetTokenByHash(ctx, hash1)
	if got2.LastUsedAt.IsZero() {
		t.Fatal("LastUsedAt should be set after Touch")
	}

	// List
	hash2 := sha256Sum("plaintext-token-2")
	if _, err := s.CreateToken(ctx, ten.ID, bot.ID, hash2, "secondary"); err != nil {
		t.Fatal(err)
	}
	list, err := s.ListTokens(ctx, ten.ID, bot.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 tokens, got %d", len(list))
	}

	// Revoke
	if err := s.RevokeToken(ctx, ten.ID, tok.ID); err != nil {
		t.Fatal(err)
	}
	// After revoke, GetTokenByHash should not return it (revoked filter)
	_, err = s.GetTokenByHash(ctx, hash1)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after revoke, got %v", err)
	}
	// Re-revoking returns ErrNotFound
	err = s.RevokeToken(ctx, ten.ID, tok.ID)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("re-revoke should be ErrNotFound, got %v", err)
	}
}

func TestTokenCrossTenantIsolation(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	t1 := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	t2 := &storage.Tenant{Slug: "globex", DisplayName: "Globex"}
	if err := s.CreateTenant(ctx, t1); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTenant(ctx, t2); err != nil {
		t.Fatal(err)
	}
	bot1 := &storage.Account{Username: "b", Type: storage.AccountBot, PasswordHash: []byte("x"), PasswordSalt: []byte("x"), PasswordParams: "argon2id,m=65536,t=3,p=1"}
	if err := s.CreateAccount(ctx, t1.ID, bot1); err != nil {
		t.Fatal(err)
	}
	bot2 := &storage.Account{Username: "b", Type: storage.AccountBot, PasswordHash: []byte("x"), PasswordSalt: []byte("x"), PasswordParams: "argon2id,m=65536,t=3,p=1"}
	if err := s.CreateAccount(ctx, t2.ID, bot2); err != nil {
		t.Fatal(err)
	}
	h := sha256Sum("shared-hash-not-allowed")
	if _, err := s.CreateToken(ctx, t1.ID, bot1.ID, h, ""); err != nil {
		t.Fatal(err)
	}
	// Same hash in a different tenant is disallowed by the global UNIQUE
	// constraint on hash (collision risk: SHA-256 makes it astronomically
	// unlikely, but the schema enforces it anyway).
	if _, err := s.CreateToken(ctx, t2.ID, bot2.ID, h, ""); !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists on duplicate hash, got %v", err)
	}

	// Tenant 2 admin trying to revoke tenant 1's token returns ErrNotFound
	tok, _ := s.GetTokenByHash(ctx, h)
	err := s.RevokeToken(ctx, t2.ID, tok.ID)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cross-tenant revoke should be ErrNotFound, got %v", err)
	}
}

func sha256Sum(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:]
}
