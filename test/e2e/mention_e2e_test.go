package e2e_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

// TestEndToEndMentionRuleFiresOnAtMention proves slice 5's mention
// parser: a bot subscribes with the "mention" rule kind (not a regex
// hack), a human types "@alertbot please look", and the bot's
// webhook fires.
func TestEndToEndMentionRuleFiresOnAtMention(t *testing.T) {
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
	// Human "alice".
	ahash, asalt, aparams, _ := auth.HashPassword("hunter2")
	alice := &storage.Account{
		Username: "alice", PasswordHash: ahash, PasswordSalt: asalt, PasswordParams: aparams,
	}
	if err := s.CreateAccount(ctx, tenant.ID, alice); err != nil {
		t.Fatal(err)
	}
	// Bot "alertbot".
	bhash, bsalt, bparams, _ := auth.HashPassword("x")
	bot := &storage.Account{
		Username: "alertbot", Type: storage.AccountBot,
		PasswordHash: bhash, PasswordSalt: bsalt, PasswordParams: bparams,
	}
	if err := s.CreateAccount(ctx, tenant.ID, bot); err != nil {
		t.Fatal(err)
	}
	ch := &storage.Channel{Name: "ops", OwnerID: bot.ID, Visibility: storage.ChannelPublic}
	if err := s.CreateChannel(ctx, tenant.ID, ch); err != nil {
		t.Fatal(err)
	}

	// Bot webhook receiver.
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

	mgr := webhook.NewManager(s, webhook.Config{}, nil)
	// Create subscription directly via storage with the "mention" rule
	// targeting the bot's own account_id -- no regex.
	mentionMatch := fmt.Sprintf(`{"any_of":[{"type":"mention","account_id":"%s"}]}`, bot.ID)
	sub := &storage.Subscription{
		AccountID: bot.ID, ChannelID: ch.ID,
		URL:       receiver.URL,
		Secret:    []byte("test-secret"),
		MatchJSON: []byte(mentionMatch),
	}
	if err := s.CreateSubscription(ctx, tenant.ID, sub); err != nil {
		t.Fatal(err)
	}
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
	_, _ = dec.Decode()
	_ = proto.Encode(c, proto.Join{Channel: "ops"})
	_, _ = dec.Decode() // own presence

	// Non-matching: no mention.
	_ = proto.Encode(c, proto.Msg{Channel: "ops", Body: "just chatting"})
	_, _ = dec.Decode() // echo
	time.Sleep(200 * time.Millisecond)
	hitsMu.Lock()
	n := len(hits)
	hitsMu.Unlock()
	if n != 0 {
		t.Errorf("non-mention msg should not fire; got %d hits", n)
	}

	// Matching: @-mention by username.
	_ = proto.Encode(c, proto.Msg{Channel: "ops", Body: "@alertbot please look at this"})
	_, _ = dec.Decode() // echo

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		hitsMu.Lock()
		n = len(hits)
		hitsMu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	hitsMu.Lock()
	defer hitsMu.Unlock()
	if len(hits) != 1 {
		t.Fatalf("expected 1 mention-triggered webhook, got %d", len(hits))
	}
	var p webhook.Payload
	if err := json.Unmarshal(hits[0].Body, &p); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(p.Matches[0].Body, "@alertbot") {
		t.Errorf("expected matched body to contain the mention: %q", p.Matches[0].Body)
	}
}
