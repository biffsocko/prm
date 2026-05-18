package mention_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/mention"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/sqlite"
)

func mkStore(t *testing.T) storage.Store {
	t.Helper()
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

func mkAccount(t *testing.T, s storage.Store, tenantID uuid.UUID, username string) *storage.Account {
	t.Helper()
	acc := &storage.Account{
		Username:     username,
		PasswordHash: []byte("x"), PasswordSalt: []byte("x"), PasswordParams: "argon2id,m=65536,t=3,p=1",
	}
	if err := s.CreateAccount(context.Background(), tenantID, acc); err != nil {
		t.Fatal(err)
	}
	return acc
}

func TestResolveNoMentions(t *testing.T) {
	s := mkStore(t)
	if got := mention.Resolve(context.Background(), s, uuid.New(), "hello world"); got != nil {
		t.Errorf("expected nil for no-@ body, got %v", got)
	}
}

func TestResolveSingleUsername(t *testing.T) {
	s := mkStore(t)
	ten := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(context.Background(), ten); err != nil {
		t.Fatal(err)
	}
	alice := mkAccount(t, s, ten.ID, "alice")

	got := mention.Resolve(context.Background(), s, ten.ID, "hey @alice can you look at this")
	if len(got) != 1 || got[0] != alice.ID {
		t.Errorf("got %v, want [%v]", got, alice.ID)
	}
}

func TestResolveAtStartOfMessage(t *testing.T) {
	s := mkStore(t)
	ten := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	_ = s.CreateTenant(context.Background(), ten)
	alice := mkAccount(t, s, ten.ID, "alice")

	got := mention.Resolve(context.Background(), s, ten.ID, "@alice look")
	if len(got) != 1 || got[0] != alice.ID {
		t.Errorf("at-start mention failed: %v", got)
	}
}

func TestResolveMultipleAndDedup(t *testing.T) {
	s := mkStore(t)
	ten := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	_ = s.CreateTenant(context.Background(), ten)
	alice := mkAccount(t, s, ten.ID, "alice")
	bob := mkAccount(t, s, ten.ID, "bob")

	got := mention.Resolve(context.Background(), s, ten.ID, "@alice @bob @alice ping")
	if len(got) != 2 {
		t.Fatalf("expected 2 deduped, got %d (%v)", len(got), got)
	}
	idSet := map[uuid.UUID]bool{got[0]: true, got[1]: true}
	if !idSet[alice.ID] || !idSet[bob.ID] {
		t.Errorf("expected both alice and bob mentioned: %v", got)
	}
}

func TestResolveUnknownUsernameIgnored(t *testing.T) {
	s := mkStore(t)
	ten := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	_ = s.CreateTenant(context.Background(), ten)
	alice := mkAccount(t, s, ten.ID, "alice")

	got := mention.Resolve(context.Background(), s, ten.ID, "@alice @nobody @ghost")
	if len(got) != 1 || got[0] != alice.ID {
		t.Errorf("expected only alice; got %v", got)
	}
}

func TestResolveUUIDForm(t *testing.T) {
	s := mkStore(t)
	id := uuid.MustParse("019e3bbf-b303-72af-9fee-dd0eb745dc78")
	got := mention.Resolve(context.Background(), s, uuid.New(), "ping @"+id.String()+" thanks")
	if len(got) != 1 || got[0] != id {
		t.Errorf("expected uuid-form mention to pass through; got %v", got)
	}
}

func TestResolveCrossTenantNotResolved(t *testing.T) {
	s := mkStore(t)
	t1 := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	t2 := &storage.Tenant{Slug: "globex", DisplayName: "Globex"}
	_ = s.CreateTenant(context.Background(), t1)
	_ = s.CreateTenant(context.Background(), t2)
	// Same username in both tenants.
	mkAccount(t, s, t1.ID, "alice")
	t2Alice := mkAccount(t, s, t2.ID, "alice")

	got := mention.Resolve(context.Background(), s, t2.ID, "@alice please")
	if len(got) != 1 || got[0] != t2Alice.ID {
		t.Errorf("expected resolution to be tenant-scoped; got %v want [%v]", got, t2Alice.ID)
	}
}

func TestResolveBoundaryHandling(t *testing.T) {
	s := mkStore(t)
	ten := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	_ = s.CreateTenant(context.Background(), ten)
	alice := mkAccount(t, s, ten.ID, "alice")

	// "email@alice.com" is NOT a mention -- there's no boundary before @.
	got := mention.Resolve(context.Background(), s, ten.ID, "send email@alice.com")
	if len(got) != 0 {
		t.Errorf("email-style @ should not be a mention; got %v", got)
	}

	// "(@alice)" IS a mention.
	got2 := mention.Resolve(context.Background(), s, ten.ID, "ping (@alice)")
	if len(got2) != 1 || got2[0] != alice.ID {
		t.Errorf("punctuation-wrapped mention: got %v", got2)
	}
}
