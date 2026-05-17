package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/sqlite"
)

func newStore(t *testing.T) *sqlite.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := sqlite.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestTenantCRUD(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	in := &storage.Tenant{Slug: "acme", DisplayName: "Acme Corp"}
	if err := s.CreateTenant(ctx, in); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	if in.ID == uuid.Nil {
		t.Fatal("expected ID to be filled in")
	}
	if in.Status != storage.TenantActive {
		t.Errorf("expected Status=active, got %q", in.Status)
	}

	got, err := s.GetTenantBySlug(ctx, "acme")
	if err != nil {
		t.Fatalf("GetTenantBySlug: %v", err)
	}
	if got.ID != in.ID {
		t.Errorf("id mismatch: got %v want %v", got.ID, in.ID)
	}
	if got.DisplayName != "Acme Corp" {
		t.Errorf("display_name mismatch: got %q", got.DisplayName)
	}

	gotByID, err := s.GetTenantByID(ctx, in.ID)
	if err != nil {
		t.Fatalf("GetTenantByID: %v", err)
	}
	if gotByID.Slug != "acme" {
		t.Errorf("slug mismatch: got %q", gotByID.Slug)
	}

	// Duplicate slug rejected
	dup := &storage.Tenant{Slug: "acme", DisplayName: "Other"}
	err = s.CreateTenant(ctx, dup)
	if !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	// Unknown slug returns ErrNotFound
	_, err = s.GetTenantBySlug(ctx, "nope")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// List
	list, err := s.ListTenants(ctx)
	if err != nil {
		t.Fatalf("ListTenants: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 tenant, got %d", len(list))
	}
}

func TestAccountCRUD(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	tenant := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(ctx, tenant); err != nil {
		t.Fatal(err)
	}

	a := &storage.Account{
		Username:       "alex",
		DisplayName:    "Alex",
		Type:           storage.AccountHuman,
		PasswordHash:   []byte("hash-bytes"),
		PasswordSalt:   []byte("salt-bytes"),
		PasswordParams: "argon2id,m=65536,t=3,p=1",
	}
	if err := s.CreateAccount(ctx, tenant.ID, a); err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	if a.ID == uuid.Nil {
		t.Fatal("expected account ID to be filled in")
	}
	if a.TenantID != tenant.ID {
		t.Fatalf("tenant id mismatch: got %v want %v", a.TenantID, tenant.ID)
	}

	got, err := s.GetAccountByUsername(ctx, tenant.ID, "alex")
	if err != nil {
		t.Fatalf("GetAccountByUsername: %v", err)
	}
	if got.ID != a.ID {
		t.Errorf("id mismatch")
	}
	if string(got.PasswordHash) != "hash-bytes" {
		t.Errorf("hash mismatch")
	}

	// Duplicate username in same tenant rejected
	dup := *a
	dup.ID = uuid.Nil
	err = s.CreateAccount(ctx, tenant.ID, &dup)
	if !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestCrossTenantIsolation(t *testing.T) {
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

	// Same username in both tenants is allowed
	mkAccount := func(name string) *storage.Account {
		return &storage.Account{
			Username:       name,
			Type:           storage.AccountHuman,
			PasswordHash:   []byte("h"),
			PasswordSalt:   []byte("s"),
			PasswordParams: "argon2id,m=65536,t=3,p=1",
		}
	}
	a1 := mkAccount("alex")
	a2 := mkAccount("alex")
	if err := s.CreateAccount(ctx, t1.ID, a1); err != nil {
		t.Fatalf("CreateAccount t1: %v", err)
	}
	if err := s.CreateAccount(ctx, t2.ID, a2); err != nil {
		t.Fatalf("CreateAccount t2: %v", err)
	}
	if a1.ID == a2.ID {
		t.Fatal("expected different account IDs for same username across tenants")
	}

	// Looking up t1's account from t2 returns ErrNotFound
	_, err := s.GetAccountByID(ctx, t2.ID, a1.ID)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cross-tenant lookup should be ErrNotFound, got %v", err)
	}

	// And looking up by username in the wrong tenant gets the right tenant's account, not the wrong one
	gotT1, err := s.GetAccountByUsername(ctx, t1.ID, "alex")
	if err != nil {
		t.Fatal(err)
	}
	if gotT1.ID != a1.ID {
		t.Fatal("got wrong tenant's account")
	}
	gotT2, err := s.GetAccountByUsername(ctx, t2.ID, "alex")
	if err != nil {
		t.Fatal(err)
	}
	if gotT2.ID != a2.ID {
		t.Fatal("got wrong tenant's account")
	}
}

func TestCreateAccountRejectsMismatchedTenantID(t *testing.T) {
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
	a := &storage.Account{
		TenantID:       t1.ID, // claim t1
		Username:       "alex",
		PasswordHash:   []byte("h"),
		PasswordSalt:   []byte("s"),
		PasswordParams: "argon2id,m=65536,t=3,p=1",
	}
	err := s.CreateAccount(ctx, t2.ID, a) // but pass t2
	if !errors.Is(err, storage.ErrInvalid) {
		t.Fatalf("expected ErrInvalid, got %v", err)
	}
}
