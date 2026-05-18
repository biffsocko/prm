package e2e_test

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/proto"
	"github.com/biffsocko/prm/internal/server"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/sqlite"
	"github.com/biffsocko/prm/internal/webhook"
)

// TestEndToEndSubscriptionsOverProtocol verifies that a bot can manage
// subscriptions over the realtime PRM protocol (slice 3b) without
// touching REST -- and that a subscription created via the protocol
// fires webhooks just like one created via REST.
func TestEndToEndSubscriptionsOverProtocol(t *testing.T) {
	// Stand up: store + tenant + bot + token + channel + fake bot HTTP
	// receiver + webhook manager + realtime server. No REST control
	// plane this time -- everything goes over the protocol.
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
	hash, salt, params, _ := auth.HashPassword("x")
	bot := &storage.Account{
		Username: "alertbot", Type: storage.AccountBot,
		PasswordHash: hash, PasswordSalt: salt, PasswordParams: params,
	}
	if err := s.CreateAccount(ctx, tenant.ID, bot); err != nil {
		t.Fatal(err)
	}
	botToken, _, err := auth.IssueToken(ctx, s, tenant.ID, bot.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	ch := &storage.Channel{Name: "ops", OwnerID: bot.ID, Visibility: storage.ChannelPublic}
	if err := s.CreateChannel(ctx, tenant.ID, ch); err != nil {
		t.Fatal(err)
	}

	// Fake receiver.
	var hitsMu sync.Mutex
	var hits []*recordedHit
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		hitsMu.Lock()
		hits = append(hits, &recordedHit{Body: body, Signature: r.Header.Get("PRM-Signature")})
		hitsMu.Unlock()
		w.WriteHeader(200)
	}))
	t.Cleanup(receiver.Close)

	// Webhook manager.
	mgr := webhook.NewManager(s, webhook.Config{}, nil)
	tens, _ := s.ListTenants(ctx)
	if err := mgr.Reload(ctx, tens); err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(runCtx)
	t.Cleanup(mgr.Stop)

	// Realtime server.
	tlsCfg, err := server.DevTLSConfig("localhost")
	if err != nil {
		t.Fatal(err)
	}
	addr := pickFreeAddr(t)
	rt, err := server.New(server.Config{
		Addr: addr, TLSConfig: tlsCfg, Store: s,
		Name: "prmd-e2e", Version: "test", WebhookMgr: mgr,
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = rt.Serve(runCtx) }()
	if err := waitDialable(addr, 3*time.Second); err != nil {
		cancel()
		wg.Wait()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); wg.Wait() })

	// Bot dials in, token-auths, creates a subscription via the protocol.
	clientTLS := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost"}
	c, err := tls.Dial("tcp", addr, clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	dec := proto.NewDecoder(c)

	mustSend := func(f proto.Frame) {
		if err := proto.Encode(c, f); err != nil {
			t.Fatalf("send %s: %v", f.FrameType(), err)
		}
	}
	mustRecv := func(want string) proto.Frame {
		t.Helper()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		defer c.SetReadDeadline(time.Time{})
		for {
			f, err := dec.Decode()
			if err != nil {
				t.Fatalf("decode (want %s): %v", want, err)
			}
			if f.FrameType() == want {
				return f
			}
			if p, ok := f.(proto.Ping); ok {
				_ = proto.Encode(c, proto.Pong{Token: p.Token})
				continue
			}
			t.Fatalf("got %s, want %s", f.FrameType(), want)
		}
	}

	mustSend(proto.Hello{CapVersion: "0.1"})
	mustRecv(proto.TypeWelcome)
	mustSend(proto.AuthRequest{Method: proto.AuthMethodToken, Token: botToken})
	mustRecv(proto.TypeAuthOK)

	// subscription_create over the protocol.
	createReq := proto.SubscriptionCreate{
		ID:           "create-1",
		ChannelName:  "ops",
		URL:          receiver.URL,
		Match:        json.RawMessage(`{"any_of":[{"type":"regex","pattern":"(?i)^deploy"}]}`),
		ContextLines: 2,
	}
	mustSend(createReq)
	okF := mustRecv(proto.TypeSubscriptionOK).(proto.SubscriptionOK)
	if okF.ID != "create-1" {
		t.Errorf("correlation id mismatch: got %q want create-1", okF.ID)
	}
	if okF.Subscription.Secret == "" {
		t.Fatal("expected plaintext secret on create response")
	}
	secret, err := base64.RawURLEncoding.DecodeString(okF.Subscription.Secret)
	if err != nil {
		t.Fatal(err)
	}
	subID := okF.Subscription.ID

	// subscription_list -> should see the one we just created.
	mustSend(proto.SubscriptionList{ID: "list-1"})
	listF := mustRecv(proto.TypeSubscriptionListOK).(proto.SubscriptionListOK)
	if len(listF.Subscriptions) != 1 {
		t.Errorf("expected 1 subscription in list, got %d", len(listF.Subscriptions))
	}
	if listF.Subscriptions[0].Secret != "" {
		t.Errorf("list should not include plaintext secret")
	}

	// subscription_get -> matches.
	mustSend(proto.SubscriptionGet{ID: "get-1", SubscriptionID: subID})
	getF := mustRecv(proto.TypeSubscriptionOK).(proto.SubscriptionOK)
	if getF.Subscription.ID != subID {
		t.Errorf("get id mismatch")
	}
	if getF.Subscription.Secret != "" {
		t.Errorf("get should not include plaintext secret")
	}

	// Now have a human-style password account send a matching msg and
	// verify the webhook fires using the secret we got over the protocol.
	hhash, hsalt, hparams, _ := auth.HashPassword("hunter2")
	human := &storage.Account{
		Username: "alex",
		PasswordHash: hhash, PasswordSalt: hsalt, PasswordParams: hparams,
	}
	if err := s.CreateAccount(ctx, tenant.ID, human); err != nil {
		t.Fatal(err)
	}
	hc, err := tls.Dial("tcp", addr, clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer hc.Close()
	hdec := proto.NewDecoder(hc)
	_ = proto.Encode(hc, proto.Hello{CapVersion: "0.1"})
	_, _ = hdec.Decode()
	_ = proto.Encode(hc, proto.AuthRequest{Method: proto.AuthMethodPassword, Tenant: "acme", Username: "alex"})
	chalF, _ := hdec.Decode()
	chal := chalF.(proto.AuthChallenge)
	saltBytes, _ := auth.DecodeBase64(chal.Salt)
	proof, _ := auth.ComputeClientProof("hunter2", saltBytes, chal.Params)
	_ = proto.Encode(hc, proto.AuthResponse{Proof: auth.EncodeBase64(proof)})
	_, _ = hdec.Decode()
	_ = proto.Encode(hc, proto.Join{Channel: "ops"})
	_, _ = hdec.Decode() // presence
	_ = proto.Encode(hc, proto.Msg{Channel: "ops", Body: "deploy now"})
	_, _ = hdec.Decode() // own msg echo

	// Webhook should fire within ~1s.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		hitsMu.Lock()
		n := len(hits)
		hitsMu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	hitsMu.Lock()
	n := len(hits)
	hitsMu.Unlock()
	if n != 1 {
		t.Fatalf("expected 1 webhook hit, got %d", n)
	}
	hitsMu.Lock()
	hit := hits[0]
	hitsMu.Unlock()
	if !webhook.Verify(hit.Signature, hit.Body, secret) {
		t.Errorf("signature did not verify (secret obtained via protocol)")
	}
	var p webhook.Payload
	if err := json.Unmarshal(hit.Body, &p); err != nil {
		t.Fatal(err)
	}
	if p.Matches[0].Body != "deploy now" {
		t.Errorf("match body: got %q want %q", p.Matches[0].Body, "deploy now")
	}

	// subscription_update over the protocol: disable it; another match should NOT fire.
	disabled := true
	mustSend(proto.SubscriptionUpdate{ID: "update-1", SubscriptionID: subID, Disabled: &disabled})
	updF := mustRecv(proto.TypeSubscriptionOK).(proto.SubscriptionOK)
	if updF.Subscription.DisabledAt == nil {
		t.Errorf("expected disabled_at to be set after update")
	}
	// Brief pause so the manager picks up the Remove before next msg.
	time.Sleep(50 * time.Millisecond)

	_ = proto.Encode(hc, proto.Msg{Channel: "ops", Body: "deploy again"})
	_, _ = hdec.Decode() // own msg echo
	time.Sleep(300 * time.Millisecond)
	hitsMu.Lock()
	n2 := len(hits)
	hitsMu.Unlock()
	if n2 != 1 {
		t.Errorf("disabled subscription should NOT fire; total hits=%d", n2)
	}

	// subscription_delete over the protocol; subsequent get -> not_found.
	mustSend(proto.SubscriptionDelete{ID: "del-1", SubscriptionID: subID})
	delF := mustRecv(proto.TypeSubscriptionDeleted).(proto.SubscriptionDeleted)
	if delF.SubscriptionID != subID {
		t.Errorf("delete response id mismatch")
	}
	mustSend(proto.SubscriptionGet{ID: "get-2", SubscriptionID: subID})
	errF := mustRecv(proto.TypeError).(proto.Error)
	if errF.Reason != "not_found" {
		t.Errorf("expected not_found after delete, got %q", errF.Reason)
	}
}

// TestEndToEndProtocolSubscriptionRequiresBot verifies that a
// password-authed human account can't manage subscriptions over the
// protocol (rejected with not_a_bot).
func TestEndToEndProtocolSubscriptionRequiresBot(t *testing.T) {
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
	_ = s.CreateTenant(ctx, tenant)
	hash, salt, params, _ := auth.HashPassword("hunter2")
	human := &storage.Account{Username: "alex", PasswordHash: hash, PasswordSalt: salt, PasswordParams: params}
	_ = s.CreateAccount(ctx, tenant.ID, human)
	_ = s.CreateChannel(ctx, tenant.ID, &storage.Channel{Name: "ops", OwnerID: human.ID, Visibility: storage.ChannelPublic})

	tlsCfg, _ := server.DevTLSConfig("localhost")
	addr := pickFreeAddr(t)
	rt, _ := server.New(server.Config{Addr: addr, TLSConfig: tlsCfg, Store: s})
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
	c, _ := tls.Dial("tcp", addr, clientTLS)
	defer c.Close()
	dec := proto.NewDecoder(c)
	_ = proto.Encode(c, proto.Hello{CapVersion: "0.1"})
	_, _ = dec.Decode()
	_ = proto.Encode(c, proto.AuthRequest{Method: proto.AuthMethodPassword, Tenant: "acme", Username: "alex"})
	chalF, _ := dec.Decode()
	chal := chalF.(proto.AuthChallenge)
	saltBytes, _ := auth.DecodeBase64(chal.Salt)
	proof, _ := auth.ComputeClientProof("hunter2", saltBytes, chal.Params)
	_ = proto.Encode(c, proto.AuthResponse{Proof: auth.EncodeBase64(proof)})
	_, _ = dec.Decode()

	// Human-authed account tries to create a subscription -> not_a_bot.
	_ = proto.Encode(c, proto.SubscriptionCreate{
		ID:          "x",
		ChannelName: "ops",
		URL:         "https://example.com",
		Match:       json.RawMessage(`{"any_of":[{"type":"regex","pattern":"x"}]}`),
	})
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer c.SetReadDeadline(time.Time{})
	f, err := dec.Decode()
	if err != nil {
		t.Fatal(err)
	}
	errFrame, ok := f.(proto.Error)
	if !ok {
		t.Fatalf("expected error frame, got %s", f.FrameType())
	}
	if errFrame.Reason != "not_a_bot" {
		t.Errorf("expected not_a_bot, got %q", errFrame.Reason)
	}
}
