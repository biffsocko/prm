package e2e_test

import (
	"context"
	"crypto/tls"
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

// TestEndToEndChannelOpPauseBot exercises slice 6a: a human operator with
// RoleChannelOp pauses a bot's webhook deliveries in a channel, matching
// chat messages arrive but the bot's subscription does NOT fire; the
// operator resumes and the next matching message DOES fire.
//
// Verifies:
//  1. chanop_pause_bot succeeds and returns ChanopOK
//  2. A moderation event is recorded
//  3. A visible in-channel system msg with FromRole=channel_op is emitted
//  4. Subsequent matching chat message → no webhook fire (pause honored)
//  5. chanop_resume_bot succeeds; the pause is cleared
//  6. Next matching chat message → webhook fires
//  7. A non-operator human account cannot pause a bot (permission_denied)
func TestEndToEndChannelOpPauseBot(t *testing.T) {
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Tenant + accounts.
	tenant := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	// Operator (human with RoleChannelOp on the channel).
	opHash, opSalt, opParams, _ := auth.HashPassword("op-pw")
	opAcct := &storage.Account{
		Username: "opal", DisplayName: "Opal",
		PasswordHash: opHash, PasswordSalt: opSalt, PasswordParams: opParams,
	}
	if err := s.CreateAccount(ctx, tenant.ID, opAcct); err != nil {
		t.Fatal(err)
	}
	// A second human WITHOUT operator privileges — used to prove the
	// permission_denied case.
	regHash, regSalt, regParams, _ := auth.HashPassword("reg-pw")
	regAcct := &storage.Account{
		Username: "regina", DisplayName: "Regina",
		PasswordHash: regHash, PasswordSalt: regSalt, PasswordParams: regParams,
	}
	if err := s.CreateAccount(ctx, tenant.ID, regAcct); err != nil {
		t.Fatal(err)
	}
	// Bot with an API token + a webhook subscription.
	bhash, bsalt, bparams, _ := auth.HashPassword("x")
	bot := &storage.Account{
		Username: "alertbot", DisplayName: "Alert Bot", Type: storage.AccountBot,
		PasswordHash: bhash, PasswordSalt: bsalt, PasswordParams: bparams,
	}
	if err := s.CreateAccount(ctx, tenant.ID, bot); err != nil {
		t.Fatal(err)
	}

	// Public channel — anyone can JOIN. Operator gets RoleChannelOp;
	// regular human intentionally has no ACL entry (public visibility
	// lets them join anyway) so the moderation check has to reject them.
	ch := &storage.Channel{Name: "ops", OwnerID: bot.ID, Visibility: storage.ChannelPublic}
	if err := s.CreateChannel(ctx, tenant.ID, ch); err != nil {
		t.Fatal(err)
	}
	if err := s.SetChannelACL(ctx, tenant.ID, ch.ID, opAcct.ID, storage.RoleChannelOp, opAcct.ID); err != nil {
		t.Fatal(err)
	}

	// Fake webhook receiver — counts hits so we can assert pause behavior.
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

	// Subscription that matches messages beginning "deploy".
	secret := []byte("test-secret-32-bytes-abcdefghij!!")
	sub := &storage.Subscription{
		AccountID: bot.ID,
		ChannelID: ch.ID,
		URL:       receiver.URL,
		Secret:    secret,
		MatchJSON: []byte(`{"any_of":[{"type":"regex","pattern":"(?i)^deploy"}]}`),
	}
	if err := s.CreateSubscription(ctx, tenant.ID, sub); err != nil {
		t.Fatal(err)
	}

	// Webhook manager + realtime server.
	mgr := webhook.NewManager(s, webhook.Config{}, nil)
	tens, _ := s.ListTenants(ctx)
	if err := mgr.Reload(ctx, tens); err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(runCtx)
	t.Cleanup(mgr.Stop)

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

	clientTLS := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost"}

	// --- helper: sign in + join #ops as `username` with `password`. ---
	loginAndJoin := func(username, password string) (*tls.Conn, *proto.Decoder) {
		c, err := tls.Dial("tcp", addr, clientTLS)
		if err != nil {
			t.Fatal(err)
		}
		dec := proto.NewDecoder(c)
		_ = proto.Encode(c, proto.Hello{CapVersion: "0.1"})
		_, _ = dec.Decode() // welcome
		_ = proto.Encode(c, proto.AuthRequest{Method: proto.AuthMethodPassword, Tenant: "acme", Username: username})
		chalF, _ := dec.Decode()
		chal := chalF.(proto.AuthChallenge)
		salt, _ := auth.DecodeBase64(chal.Salt)
		proof, _ := auth.ComputeClientProof(password, salt, chal.Params)
		_ = proto.Encode(c, proto.AuthResponse{Proof: auth.EncodeBase64(proof)})
		_, _ = dec.Decode() // authok
		_ = proto.Encode(c, proto.Join{Channel: "ops"})
		_, _ = dec.Decode() // own presence
		return c, dec
	}

	// Operator connects.
	opConn, opDec := loginAndJoin("opal", "op-pw")
	defer opConn.Close()

	// Regular member connects (no channel_op role).
	regConn, regDec := loginAndJoin("regina", "reg-pw")
	defer regConn.Close()

	// Drain any presence frames the operator sees from regina joining.
	_ = opConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, _ = opDec.Decode()
	_ = opConn.SetReadDeadline(time.Time{})

	// --- 1. Baseline: regina sends "deploy" -> webhook fires. ---
	_ = proto.Encode(regConn, proto.Msg{Channel: "ops", Body: "deploy alpha"})
	// regina echoes; op sees the message too.
	drainOne(t, regDec)
	drainOne(t, opDec)

	if !waitForHits(&hitsMu, &hits, 1, 3*time.Second) {
		t.Fatalf("expected 1 webhook hit before pause; got %d", hitCount(&hitsMu, &hits))
	}

	// --- 2. Operator pauses the bot on this channel. ---
	pauseReq := proto.ChanopPauseBot{
		ID:           "p1",
		Channel:      "ops",
		BotAccountID: bot.ID.String(),
		Reason:       "runaway loop",
	}
	_ = proto.Encode(opConn, pauseReq)

	// Read frames until we get ChanopOK; along the way we should see the
	// in-channel system msg the server emits.
	sawSystemMsg := false
	_ = opConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		f, err := opDec.Decode()
		if err != nil {
			t.Fatalf("decode after pause: %v", err)
		}
		if okF, ok := f.(proto.ChanopOK); ok {
			if okF.Action != "pause_bot" {
				t.Errorf("chanop_ok action: got %q want pause_bot", okF.Action)
			}
			if okF.ID != "p1" {
				t.Errorf("correlation id mismatch: got %q want p1", okF.ID)
			}
			break
		}
		if msgF, ok := f.(proto.Msg); ok {
			if msgF.FromRole == proto.FromRoleChannelOp {
				sawSystemMsg = true
			}
		}
	}
	_ = opConn.SetReadDeadline(time.Time{})
	if !sawSystemMsg {
		// The system message may have been broadcast before the
		// ChanopOK reply; check the regina side too.
		_ = regConn.SetReadDeadline(time.Now().Add(1 * time.Second))
		for {
			f, err := regDec.Decode()
			if err != nil {
				break
			}
			if msgF, ok := f.(proto.Msg); ok && msgF.FromRole == proto.FromRoleChannelOp {
				sawSystemMsg = true
				break
			}
		}
		_ = regConn.SetReadDeadline(time.Time{})
	}
	if !sawSystemMsg {
		t.Errorf("expected an in-channel system msg with FromRole=channel_op")
	}

	// Moderation-events audit row should exist.
	events, err := s.ListModerationEvents(ctx, tenant.ID, ch.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Fatal("expected at least one moderation event after pause")
	}
	if events[0].Action != storage.ModActionPauseBot {
		t.Errorf("first event action: got %q want %q", events[0].Action, storage.ModActionPauseBot)
	}
	if events[0].ActorID != opAcct.ID {
		t.Errorf("first event actor: got %s want %s", events[0].ActorID, opAcct.ID)
	}
	if events[0].TargetID != bot.ID {
		t.Errorf("first event target: got %s want %s", events[0].TargetID, bot.ID)
	}

	// Manager should report the bot as paused.
	if !mgr.IsBotPaused(ch.ID, bot.ID) {
		t.Fatal("expected manager to report bot as paused")
	}

	// --- 3. While paused, matching msg → NO new webhook hit. ---
	hitsBefore := hitCount(&hitsMu, &hits)
	_ = proto.Encode(regConn, proto.Msg{Channel: "ops", Body: "deploy beta"})
	time.Sleep(300 * time.Millisecond)
	if hitCount(&hitsMu, &hits) != hitsBefore {
		t.Errorf("webhook fired while paused: hits went %d -> %d",
			hitsBefore, hitCount(&hitsMu, &hits))
	}

	// --- 4. Regina (not an operator) tries to pause → permission_denied. ---
	_ = proto.Encode(regConn, proto.ChanopPauseBot{
		ID:           "regp1",
		Channel:      "ops",
		BotAccountID: bot.ID.String(),
	})
	_ = regConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		f, err := regDec.Decode()
		if err != nil {
			t.Fatalf("decode after regina pause attempt: %v", err)
		}
		if errF, ok := f.(proto.Error); ok {
			if errF.Reason != "permission_denied" {
				t.Errorf("regina got %q, want permission_denied", errF.Reason)
			}
			break
		}
		if _, ok := f.(proto.Msg); ok {
			continue // ignore system msg echoes
		}
	}
	_ = regConn.SetReadDeadline(time.Time{})

	// --- 5. Operator resumes → next match fires. ---
	_ = proto.Encode(opConn, proto.ChanopResumeBot{
		ID:           "r1",
		Channel:      "ops",
		BotAccountID: bot.ID.String(),
	})
	_ = opConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		f, err := opDec.Decode()
		if err != nil {
			t.Fatalf("decode after resume: %v", err)
		}
		if okF, ok := f.(proto.ChanopOK); ok {
			if okF.Action != "resume_bot" {
				t.Errorf("chanop_ok action: got %q want resume_bot", okF.Action)
			}
			break
		}
	}
	_ = opConn.SetReadDeadline(time.Time{})
	if mgr.IsBotPaused(ch.ID, bot.ID) {
		t.Fatal("expected manager to report bot as NOT paused after resume")
	}

	hitsBefore = hitCount(&hitsMu, &hits)
	_ = proto.Encode(regConn, proto.Msg{Channel: "ops", Body: "deploy gamma"})
	if !waitForHits(&hitsMu, &hits, hitsBefore+1, 3*time.Second) {
		t.Fatalf("expected new webhook hit after resume; hits: %d -> %d",
			hitsBefore, hitCount(&hitsMu, &hits))
	}

	// A resume moderation event should now sit at the front (newest first).
	events, err = s.ListModerationEvents(ctx, tenant.ID, ch.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 2 {
		t.Fatalf("expected at least 2 moderation events (pause + resume); got %d", len(events))
	}
	if events[0].Action != storage.ModActionResumeBot {
		t.Errorf("newest event action: got %q want %q", events[0].Action, storage.ModActionResumeBot)
	}
}

// --- small local helpers ---

func drainOne(t *testing.T, dec *proto.Decoder) {
	t.Helper()
	_, _ = dec.Decode()
}

func hitCount(mu *sync.Mutex, hits *[]*recordedHit) int {
	mu.Lock()
	defer mu.Unlock()
	return len(*hits)
}

func waitForHits(mu *sync.Mutex, hits *[]*recordedHit, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if hitCount(mu, hits) >= want {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return hitCount(mu, hits) >= want
}

var _ = json.Unmarshal // keep encoding/json imported for future assertions
