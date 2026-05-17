package auth_test

import (
	"context"
	"errors"
	"testing"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/sqlite"
)

func mkStore(t *testing.T) storage.Store {
	t.Helper()
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mkTenantAndAccount(t *testing.T, s storage.Store, tenantSlug, username, password string) (*storage.Tenant, *storage.Account) {
	t.Helper()
	ctx := context.Background()
	ten := &storage.Tenant{Slug: tenantSlug, DisplayName: tenantSlug}
	if err := s.CreateTenant(ctx, ten); err != nil {
		t.Fatal(err)
	}
	hash, salt, params, err := auth.HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	acc := &storage.Account{
		Username:       username,
		PasswordHash:   hash,
		PasswordSalt:   salt,
		PasswordParams: params,
	}
	if err := s.CreateAccount(ctx, ten.ID, acc); err != nil {
		t.Fatal(err)
	}
	return ten, acc
}

func TestHashAndVerifyPassword(t *testing.T) {
	hash, salt, params, err := auth.HashPassword("hunter2")
	if err != nil {
		t.Fatal(err)
	}
	acc := &storage.Account{PasswordHash: hash, PasswordSalt: salt, PasswordParams: params}
	if err := auth.VerifyPassword(acc, "hunter2"); err != nil {
		t.Errorf("VerifyPassword (correct): %v", err)
	}
	if err := auth.VerifyPassword(acc, "wrong"); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Errorf("VerifyPassword (wrong): expected ErrUnauthenticated, got %v", err)
	}
}

func TestFullPasswordHandshake(t *testing.T) {
	s := mkStore(t)
	ten, acc := mkTenantAndAccount(t, s, "acme", "alex", "hunter2")
	ctx := context.Background()

	// Server side: begin
	chal, gotTenant, err := auth.BeginPasswordAuth(ctx, s, "acme", "alex")
	if err != nil {
		t.Fatalf("BeginPasswordAuth: %v", err)
	}
	if gotTenant.ID != ten.ID {
		t.Errorf("tenant id mismatch")
	}
	if chal.AccountID != acc.ID {
		t.Errorf("account id mismatch")
	}

	// Client side: compute proof using the salt + params from the challenge
	proof, err := auth.ComputeClientProof("hunter2", chal.Salt, chal.Params)
	if err != nil {
		t.Fatalf("ComputeClientProof: %v", err)
	}

	// Server side: complete
	res, err := auth.CompletePasswordAuth(ctx, s, chal, auth.EncodeBase64(proof))
	if err != nil {
		t.Fatalf("CompletePasswordAuth: %v", err)
	}
	if !res.OK {
		t.Fatalf("expected OK, got reason=%q", res.Reason)
	}
	if res.Account.ID != acc.ID {
		t.Errorf("account id mismatch")
	}
	if res.Tenant.ID != ten.ID {
		t.Errorf("tenant id mismatch")
	}
}

func TestPasswordHandshakeRejectsWrongPassword(t *testing.T) {
	s := mkStore(t)
	mkTenantAndAccount(t, s, "acme", "alex", "hunter2")
	ctx := context.Background()
	chal, _, err := auth.BeginPasswordAuth(ctx, s, "acme", "alex")
	if err != nil {
		t.Fatalf("BeginPasswordAuth: %v", err)
	}
	wrong, _ := auth.ComputeClientProof("wrong", chal.Salt, chal.Params)
	res, err := auth.CompletePasswordAuth(ctx, s, chal, auth.EncodeBase64(wrong))
	if err != nil {
		t.Fatalf("CompletePasswordAuth: %v", err)
	}
	if res.OK {
		t.Fatal("expected !OK")
	}
	if res.Reason != "invalid_credentials" {
		t.Errorf("unexpected reason: %q", res.Reason)
	}
}

func TestBeginRejectsUnknownTenant(t *testing.T) {
	s := mkStore(t)
	_, err := s.GetTenantBySlug(context.Background(), "nope")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatal("setup precondition")
	}
	_, _, err = auth.BeginPasswordAuth(context.Background(), s, "nope", "anyone")
	if !errors.Is(err, auth.ErrTenantNotFound) {
		t.Fatalf("expected ErrTenantNotFound, got %v", err)
	}
}

func TestBeginRejectsUnknownAccountWithUnauthenticated(t *testing.T) {
	s := mkStore(t)
	mkTenantAndAccount(t, s, "acme", "alex", "hunter2")
	// Look up a nonexistent username -- server returns ErrUnauthenticated, not
	// a "user not found" leak.
	_, _, err := auth.BeginPasswordAuth(context.Background(), s, "acme", "nobody")
	if !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("expected ErrUnauthenticated, got %v", err)
	}
}

func TestBeginRejectsSuspendedTenant(t *testing.T) {
	s := mkStore(t)
	ctx := context.Background()
	ten := &storage.Tenant{Slug: "acme", DisplayName: "acme", Status: storage.TenantSuspended}
	if err := s.CreateTenant(ctx, ten); err != nil {
		t.Fatal(err)
	}
	_, _, err := auth.BeginPasswordAuth(ctx, s, "acme", "alex")
	if !errors.Is(err, auth.ErrTenantSuspended) {
		t.Fatalf("expected ErrTenantSuspended, got %v", err)
	}
}
