package server_test

import (
	"context"
	"testing"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/proto"
	"github.com/biffsocko/prm/internal/storage"
)

// TestServerTokenAuthSucceeds: a bot account with an issued token can
// authenticate via the one-shot token method (no challenge round-trip).
func TestServerTokenAuthSucceeds(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	// Create a bot account in the existing acme tenant.
	hash, salt, params, _ := auth.HashPassword("x")
	bot := &storage.Account{
		Username: "alertbot", DisplayName: "Alert Bot", Type: storage.AccountBot,
		PasswordHash: hash, PasswordSalt: salt, PasswordParams: params,
	}
	if err := f.store.CreateAccount(ctx, f.tenant.ID, bot); err != nil {
		t.Fatal(err)
	}
	plaintext, _, err := auth.IssueToken(ctx, f.store, f.tenant.ID, bot.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}

	c := f.dial(t)
	c.send(t, proto.Hello{CapVersion: "0.1"})
	c.recvType(t, proto.TypeWelcome)
	c.send(t, proto.AuthRequest{Method: proto.AuthMethodToken, Token: plaintext})
	got := c.recvType(t, proto.TypeAuthOK).(proto.AuthOK)

	if got.AccountID != bot.ID.String() {
		t.Errorf("account id mismatch: got %q want %q", got.AccountID, bot.ID.String())
	}
	if got.TenantID != f.tenant.ID.String() {
		t.Errorf("tenant id mismatch")
	}
	if got.AccountType != string(storage.AccountBot) {
		t.Errorf("expected account_type=bot, got %q", got.AccountType)
	}
}

// TestServerTokenAuthRejectsBadToken: a wrong token returns AuthErr with
// reason "invalid_credentials".
func TestServerTokenAuthRejectsBadToken(t *testing.T) {
	f := newFixture(t)
	c := f.dial(t)
	c.send(t, proto.Hello{CapVersion: "0.1"})
	c.recvType(t, proto.TypeWelcome)
	c.send(t, proto.AuthRequest{Method: proto.AuthMethodToken, Token: "definitely-not-a-real-token"})
	got := c.recvType(t, proto.TypeAuthErr).(proto.AuthErr)
	if got.Reason != "invalid_credentials" {
		t.Errorf("expected invalid_credentials, got %q", got.Reason)
	}
}

// TestServerTokenAuthEnablesBotJoin: after token auth, the bot can join
// a public channel just like a password-authed account.
func TestServerTokenAuthEnablesBotJoin(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	hash, salt, params, _ := auth.HashPassword("x")
	bot := &storage.Account{
		Username: "alertbot", Type: storage.AccountBot,
		PasswordHash: hash, PasswordSalt: salt, PasswordParams: params,
	}
	if err := f.store.CreateAccount(ctx, f.tenant.ID, bot); err != nil {
		t.Fatal(err)
	}
	plaintext, _, err := auth.IssueToken(ctx, f.store, f.tenant.ID, bot.ID, "")
	if err != nil {
		t.Fatal(err)
	}

	c := f.dial(t)
	c.send(t, proto.Hello{CapVersion: "0.1"})
	c.recvType(t, proto.TypeWelcome)
	c.send(t, proto.AuthRequest{Method: proto.AuthMethodToken, Token: plaintext})
	c.recvType(t, proto.TypeAuthOK)

	c.send(t, proto.Join{Channel: "general"}) // #general is public in fixture
	pres := c.recvType(t, proto.TypePresence).(proto.Presence)
	if pres.Kind != proto.PresenceJoin {
		t.Errorf("expected presence join, got %q", pres.Kind)
	}
}
