package auth_test

import (
	"context"
	"errors"
	"testing"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/storage"
)

func TestIssueAndAuthenticateToken(t *testing.T) {
	s := mkStore(t)
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

	plaintext, stored, err := auth.IssueToken(ctx, s, ten.ID, bot.ID, "primary")
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}
	if plaintext == "" {
		t.Fatal("expected plaintext token")
	}
	if stored == nil || stored.ID == [16]byte{} {
		t.Fatal("expected stored token row")
	}

	// Authenticate with the plaintext token; should succeed and resolve to
	// the bot account + tenant.
	res, retTok, err := auth.AuthenticateToken(ctx, s, plaintext)
	if err != nil {
		t.Fatalf("AuthenticateToken: %v", err)
	}
	if !res.OK {
		t.Fatalf("expected OK, got reason=%q", res.Reason)
	}
	if res.Account.ID != bot.ID {
		t.Errorf("account id mismatch")
	}
	if res.Tenant.ID != ten.ID {
		t.Errorf("tenant id mismatch")
	}
	if retTok == nil || retTok.ID != stored.ID {
		t.Errorf("expected returned token to match issued one")
	}
}

func TestAuthenticateTokenWrongTokenRejected(t *testing.T) {
	s := mkStore(t)
	ctx := context.Background()
	ten := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(ctx, ten); err != nil {
		t.Fatal(err)
	}
	bot := &storage.Account{
		Username: "b", Type: storage.AccountBot,
		PasswordHash: []byte("x"), PasswordSalt: []byte("x"), PasswordParams: "argon2id,m=65536,t=3,p=1",
	}
	if err := s.CreateAccount(ctx, ten.ID, bot); err != nil {
		t.Fatal(err)
	}
	if _, _, err := auth.IssueToken(ctx, s, ten.ID, bot.ID, ""); err != nil {
		t.Fatal(err)
	}

	res, _, err := auth.AuthenticateToken(ctx, s, "not-the-real-token")
	if err != nil {
		t.Fatal(err)
	}
	if res.OK {
		t.Fatal("expected !OK for wrong token")
	}
	if res.Reason != "invalid_credentials" {
		t.Errorf("unexpected reason: %q", res.Reason)
	}
}

func TestAuthenticateTokenRevokedRejected(t *testing.T) {
	s := mkStore(t)
	ctx := context.Background()
	ten := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(ctx, ten); err != nil {
		t.Fatal(err)
	}
	bot := &storage.Account{
		Username: "b", Type: storage.AccountBot,
		PasswordHash: []byte("x"), PasswordSalt: []byte("x"), PasswordParams: "argon2id,m=65536,t=3,p=1",
	}
	if err := s.CreateAccount(ctx, ten.ID, bot); err != nil {
		t.Fatal(err)
	}
	plaintext, stored, err := auth.IssueToken(ctx, s, ten.ID, bot.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeToken(ctx, ten.ID, stored.ID); err != nil {
		t.Fatal(err)
	}
	res, _, err := auth.AuthenticateToken(ctx, s, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if res.OK {
		t.Fatal("expected !OK after revoke")
	}
	if res.Reason != "invalid_credentials" {
		t.Errorf("unexpected reason: %q", res.Reason)
	}
}

func TestAuthenticateTokenSuspendedTenant(t *testing.T) {
	s := mkStore(t)
	ctx := context.Background()
	ten := &storage.Tenant{Slug: "acme", DisplayName: "Acme", Status: storage.TenantSuspended}
	if err := s.CreateTenant(ctx, ten); err != nil {
		t.Fatal(err)
	}
	bot := &storage.Account{
		Username: "b", Type: storage.AccountBot,
		PasswordHash: []byte("x"), PasswordSalt: []byte("x"), PasswordParams: "argon2id,m=65536,t=3,p=1",
	}
	if err := s.CreateAccount(ctx, ten.ID, bot); err != nil {
		t.Fatal(err)
	}
	plaintext, _, err := auth.IssueToken(ctx, s, ten.ID, bot.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	res, _, err := auth.AuthenticateToken(ctx, s, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if res.OK {
		t.Fatal("expected !OK for suspended tenant")
	}
	if res.Reason != "tenant_suspended" {
		t.Errorf("unexpected reason: %q", res.Reason)
	}
}

func TestAuthenticateTokenEmpty(t *testing.T) {
	s := mkStore(t)
	res, _, err := auth.AuthenticateToken(context.Background(), s, "")
	if err != nil {
		t.Fatal(err)
	}
	if res.OK {
		t.Fatal("expected !OK for empty token")
	}
	// Sanity: storage doesn't error on the empty-token case
	_, err = s.GetTenantBySlug(context.Background(), "nope")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatal("setup precondition")
	}
}
