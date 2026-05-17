package server_test

import (
	"context"
	"testing"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/proto"
	"github.com/biffsocko/prm/internal/storage"
)

// TestServerJoinUnknownChannel: explicit channel creation is required
// (slice 2 change). JOINing a channel that doesn't exist returns Error.
func TestServerJoinUnknownChannel(t *testing.T) {
	f := newFixture(t)
	c := f.dial(t)
	f.login(t, c)
	c.send(t, proto.Join{Channel: "no-such-channel"})
	got := c.recvType(t, proto.TypeError).(proto.Error)
	if got.Reason != "channel_not_found" {
		t.Errorf("expected channel_not_found, got %q", got.Reason)
	}
}

// TestServerJoinPrivateChannelRequiresACL: a private channel that the
// joining account isn't listed in is refused.
func TestServerJoinPrivateChannelRequiresACL(t *testing.T) {
	f := newFixture(t)
	// Create a private channel that "alex" is NOT in the ACL of.
	ctx := context.Background()
	otherOwner := f.user.ID // any UUID is fine for owner; alex is not in the ACL
	_ = otherOwner
	if err := f.store.CreateChannel(ctx, f.tenant.ID, &storage.Channel{
		Name: "private-room", OwnerID: f.user.ID, Visibility: storage.ChannelPrivate,
	}); err != nil {
		t.Fatal(err)
	}
	// Note: deliberately NOT setting the channel ACL for alex.

	c := f.dial(t)
	f.login(t, c)
	c.send(t, proto.Join{Channel: "private-room"})
	got := c.recvType(t, proto.TypeError).(proto.Error)
	if got.Reason != "not_in_acl" {
		t.Errorf("expected not_in_acl, got %q", got.Reason)
	}
}

// TestServerJoinPrivateChannelWithACLSucceeds: granting the joiner a
// member role lets them in.
func TestServerJoinPrivateChannelWithACLSucceeds(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	ch := &storage.Channel{Name: "private-room", OwnerID: f.user.ID, Visibility: storage.ChannelPrivate}
	if err := f.store.CreateChannel(ctx, f.tenant.ID, ch); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SetChannelACL(ctx, f.tenant.ID, ch.ID, f.user.ID, storage.RoleMember, f.user.ID); err != nil {
		t.Fatal(err)
	}

	c := f.dial(t)
	f.login(t, c)
	c.send(t, proto.Join{Channel: "private-room"})
	// Should receive Presence on join (own membership broadcast).
	pres := c.recvType(t, proto.TypePresence).(proto.Presence)
	if pres.Kind != proto.PresenceJoin {
		t.Errorf("expected presence join, got kind %q", pres.Kind)
	}
}

// TestServerJoinBannedAccountRefused: a banned role can't join even on a
// public channel? Actually public is "any authenticated account in tenant"
// per current canJoin, so banned only matters on private. Verify that
// behavior explicitly here so a future tightening of public semantics
// doesn't silently change it.
func TestServerJoinBannedRoleRefusedOnPrivateChannel(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	ch := &storage.Channel{Name: "vip", OwnerID: f.user.ID, Visibility: storage.ChannelPrivate}
	if err := f.store.CreateChannel(ctx, f.tenant.ID, ch); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SetChannelACL(ctx, f.tenant.ID, ch.ID, f.user.ID, storage.RoleBanned, f.user.ID); err != nil {
		t.Fatal(err)
	}

	c := f.dial(t)
	f.login(t, c)
	c.send(t, proto.Join{Channel: "vip"})
	got := c.recvType(t, proto.TypeError).(proto.Error)
	if got.Reason != "permission_denied" {
		t.Errorf("expected permission_denied, got %q", got.Reason)
	}
}

// TestServerJoinSecondTenantSameChannelName: two tenants can each have a
// channel called "general" without interference. The slice-1 test verified
// data-plane isolation; this verifies control-plane (per-tenant channel
// lookups) isolation.
func TestServerJoinSecondTenantSameChannelName(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	// Create a second tenant + account + private channel called "general".
	t2 := &storage.Tenant{Slug: "globex", DisplayName: "Globex"}
	if err := f.store.CreateTenant(ctx, t2); err != nil {
		t.Fatal(err)
	}
	hash, salt, params, _ := auth.HashPassword("p")
	bob := &storage.Account{
		Username: "bob", PasswordHash: hash, PasswordSalt: salt, PasswordParams: params,
	}
	if err := f.store.CreateAccount(ctx, t2.ID, bob); err != nil {
		t.Fatal(err)
	}
	// t2's "general" is PRIVATE and only bob has access.
	ch := &storage.Channel{Name: "general", OwnerID: bob.ID, Visibility: storage.ChannelPrivate}
	if err := f.store.CreateChannel(ctx, t2.ID, ch); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SetChannelACL(ctx, t2.ID, ch.ID, bob.ID, storage.RoleOwner, bob.ID); err != nil {
		t.Fatal(err)
	}

	// alex (tenant acme, where "general" is public) joins fine.
	alex := f.dial(t)
	f.login(t, alex)
	alex.send(t, proto.Join{Channel: "general"})
	if got := alex.recvType(t, proto.TypePresence).(proto.Presence); got.Kind != proto.PresenceJoin {
		t.Errorf("alex join: unexpected presence")
	}

	// bob (tenant globex, private but in ACL) also joins fine.
	bobC := f.dial(t)
	// Inline bob login since the f.login helper assumes the f.user account.
	bobC.send(t, proto.Hello{CapVersion: "0.1"})
	bobC.recvType(t, proto.TypeWelcome)
	bobC.send(t, proto.AuthRequest{Method: proto.AuthMethodPassword, Tenant: "globex", Username: "bob"})
	chal := bobC.recvType(t, proto.TypeAuthChallenge).(proto.AuthChallenge)
	saltB, _ := auth.DecodeBase64(chal.Salt)
	proof, _ := auth.ComputeClientProof("p", saltB, chal.Params)
	bobC.send(t, proto.AuthResponse{Proof: auth.EncodeBase64(proof)})
	bobC.recvType(t, proto.TypeAuthOK)
	bobC.send(t, proto.Join{Channel: "general"})
	if got := bobC.recvType(t, proto.TypePresence).(proto.Presence); got.Kind != proto.PresenceJoin {
		t.Errorf("bob join: unexpected presence")
	}
}
