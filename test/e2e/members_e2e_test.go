package e2e_test

import (
	"context"
	"crypto/tls"
	"sync"
	"testing"
	"time"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/proto"
	"github.com/biffsocko/prm/internal/server"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/sqlite"
)

// TestEndToEndMembersGhostBot proves the members verb returns ghost
// rows for bot accounts whose only presence on a channel is an active
// webhook subscription. Setup:
//
//   - tenant acme, channel "ops"
//   - human "alice" joins via the realtime socket (live member)
//   - bot "alertbot" has an active subscription on "ops" but never
//     connects to the realtime socket
//
// Expect MembersOK to return exactly 2 members: alice (IsGhost=false,
// ConnCount=1) and alertbot (IsGhost=true, ConnCount=0). Order: live
// before ghost.
func TestEndToEndMembersGhostBot(t *testing.T) {
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tenant := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(ctx, tenant); err != nil {
		t.Fatal(err)
	}

	ahash, asalt, aparams, _ := auth.HashPassword("hunter2")
	alice := &storage.Account{
		Username: "alice", DisplayName: "Alice",
		PasswordHash: ahash, PasswordSalt: asalt, PasswordParams: aparams,
	}
	if err := s.CreateAccount(ctx, tenant.ID, alice); err != nil {
		t.Fatal(err)
	}
	bhash, bsalt, bparams, _ := auth.HashPassword("x")
	bot := &storage.Account{
		Username: "alertbot", DisplayName: "Alert Bot", Type: storage.AccountBot,
		PasswordHash: bhash, PasswordSalt: bsalt, PasswordParams: bparams,
	}
	if err := s.CreateAccount(ctx, tenant.ID, bot); err != nil {
		t.Fatal(err)
	}
	ch := &storage.Channel{Name: "ops", OwnerID: alice.ID, Visibility: storage.ChannelPublic}
	if err := s.CreateChannel(ctx, tenant.ID, ch); err != nil {
		t.Fatal(err)
	}
	// Bot has an active subscription on "ops" -- this is what makes
	// it a ghost member.
	sub := &storage.Subscription{
		AccountID: bot.ID, ChannelID: ch.ID,
		URL:       "https://bot.example.invalid/hook",
		Secret:    []byte("test-secret"),
		MatchJSON: []byte(`{"any_of":[{"type":"regex","pattern":".*"}]}`),
	}
	if err := s.CreateSubscription(ctx, tenant.ID, sub); err != nil {
		t.Fatal(err)
	}

	tlsCfg, err := server.DevTLSConfig("localhost")
	if err != nil {
		t.Fatal(err)
	}
	addr := pickFreeAddr(t)
	rt, err := server.New(server.Config{
		Addr: addr, TLSConfig: tlsCfg, Store: s,
		Name: "prmd-e2e", Version: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = rt.Serve(runCtx) }()
	if err := waitDialable(addr, 3*time.Second); err != nil {
		cancel()
		wg.Wait()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); wg.Wait() })

	clientTLS := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost"}
	c, err := tls.Dial("tcp", addr, clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	dec := proto.NewDecoder(c)
	_ = proto.Encode(c, proto.Hello{CapVersion: "0.1"})
	_, _ = dec.Decode()
	_ = proto.Encode(c, proto.AuthRequest{Method: proto.AuthMethodPassword, Tenant: "acme", Username: "alice"})
	chalF, _ := dec.Decode()
	chal := chalF.(proto.AuthChallenge)
	saltBytes, _ := auth.DecodeBase64(chal.Salt)
	proof, _ := auth.ComputeClientProof("hunter2", saltBytes, chal.Params)
	_ = proto.Encode(c, proto.AuthResponse{Proof: auth.EncodeBase64(proof)})
	_, _ = dec.Decode() // auth_ok
	_ = proto.Encode(c, proto.Join{Channel: "ops"})
	_, _ = dec.Decode() // own presence

	_ = proto.Encode(c, proto.Members{Channel: "ops"})
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer c.SetReadDeadline(time.Time{})

	var resp proto.MembersOK
	for {
		f, err := dec.Decode()
		if err != nil {
			t.Fatal(err)
		}
		ok, isOK := f.(proto.MembersOK)
		if !isOK {
			continue
		}
		resp = ok
		break
	}
	if resp.Channel != "ops" {
		t.Errorf("channel: %q", resp.Channel)
	}
	if len(resp.Members) != 2 {
		t.Fatalf("expected 2 members (1 live + 1 ghost), got %d: %+v", len(resp.Members), resp.Members)
	}
	// Live first.
	if resp.Members[0].IsGhost {
		t.Errorf("first member should be live, got ghost: %+v", resp.Members[0])
	}
	if resp.Members[0].AccountID != alice.ID.String() {
		t.Errorf("first should be alice, got %q (display=%q)", resp.Members[0].AccountID, resp.Members[0].DisplayName)
	}
	if resp.Members[0].AccountType != string(storage.AccountHuman) {
		t.Errorf("alice account_type: %q", resp.Members[0].AccountType)
	}
	if resp.Members[0].ConnCount != 1 {
		t.Errorf("alice conn count: %d", resp.Members[0].ConnCount)
	}
	// Ghost second.
	if !resp.Members[1].IsGhost {
		t.Errorf("second member should be ghost: %+v", resp.Members[1])
	}
	if resp.Members[1].AccountID != bot.ID.String() {
		t.Errorf("ghost should be bot, got %q", resp.Members[1].AccountID)
	}
	if resp.Members[1].AccountType != string(storage.AccountBot) {
		t.Errorf("bot account_type: %q", resp.Members[1].AccountType)
	}
	if resp.Members[1].ConnCount != 0 {
		t.Errorf("ghost conn count must be 0; got %d", resp.Members[1].ConnCount)
	}
}

// TestEndToEndMembersBotLiveSupersesesGhost proves that when a bot
// account has BOTH a live realtime connection AND an active webhook
// subscription on the same channel, it is reported ONCE as live
// (IsGhost=false). The "ghost" flag specifically marks rows whose
// only basis is a subscription.
func TestEndToEndMembersBotLiveSupersesesGhost(t *testing.T) {
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tenant := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	bhash, bsalt, bparams, _ := auth.HashPassword("botpw")
	bot := &storage.Account{
		Username: "alertbot", DisplayName: "Alert Bot", Type: storage.AccountBot,
		PasswordHash: bhash, PasswordSalt: bsalt, PasswordParams: bparams,
	}
	if err := s.CreateAccount(ctx, tenant.ID, bot); err != nil {
		t.Fatal(err)
	}
	ch := &storage.Channel{Name: "ops", OwnerID: bot.ID, Visibility: storage.ChannelPublic}
	if err := s.CreateChannel(ctx, tenant.ID, ch); err != nil {
		t.Fatal(err)
	}
	// Subscription exists on "ops" for the bot.
	if err := s.CreateSubscription(ctx, tenant.ID, &storage.Subscription{
		AccountID: bot.ID, ChannelID: ch.ID,
		URL: "https://bot.example.invalid/hook", Secret: []byte("x"),
		MatchJSON: []byte(`{"any_of":[{"type":"regex","pattern":".*"}]}`),
	}); err != nil {
		t.Fatal(err)
	}

	tlsCfg, _ := server.DevTLSConfig("localhost")
	addr := pickFreeAddr(t)
	rt, _ := server.New(server.Config{
		Addr: addr, TLSConfig: tlsCfg, Store: s,
		Name: "prmd-e2e", Version: "test",
	})
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = rt.Serve(runCtx) }()
	if err := waitDialable(addr, 3*time.Second); err != nil {
		cancel()
		wg.Wait()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); wg.Wait() })

	// Bot logs in via password auth, joins "ops", then asks for members.
	clientTLS := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost"}
	c, err := tls.Dial("tcp", addr, clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	dec := proto.NewDecoder(c)
	_ = proto.Encode(c, proto.Hello{CapVersion: "0.1"})
	_, _ = dec.Decode()
	_ = proto.Encode(c, proto.AuthRequest{Method: proto.AuthMethodPassword, Tenant: "acme", Username: "alertbot"})
	chalF, _ := dec.Decode()
	chal := chalF.(proto.AuthChallenge)
	saltBytes, _ := auth.DecodeBase64(chal.Salt)
	proof, _ := auth.ComputeClientProof("botpw", saltBytes, chal.Params)
	_ = proto.Encode(c, proto.AuthResponse{Proof: auth.EncodeBase64(proof)})
	_, _ = dec.Decode() // auth_ok
	_ = proto.Encode(c, proto.Join{Channel: "ops"})
	_, _ = dec.Decode() // own presence

	_ = proto.Encode(c, proto.Members{Channel: "ops"})
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer c.SetReadDeadline(time.Time{})

	var resp proto.MembersOK
	for {
		f, err := dec.Decode()
		if err != nil {
			t.Fatal(err)
		}
		ok, isOK := f.(proto.MembersOK)
		if !isOK {
			continue
		}
		resp = ok
		break
	}
	if len(resp.Members) != 1 {
		t.Fatalf("bot should appear ONCE (live, not ghost); got %d rows: %+v", len(resp.Members), resp.Members)
	}
	if resp.Members[0].IsGhost {
		t.Errorf("live + subscribed must NOT be ghost: %+v", resp.Members[0])
	}
	if resp.Members[0].ConnCount != 1 {
		t.Errorf("conn count: %d", resp.Members[0].ConnCount)
	}
	if resp.Members[0].AccountType != string(storage.AccountBot) {
		t.Errorf("account_type: %q", resp.Members[0].AccountType)
	}
}
