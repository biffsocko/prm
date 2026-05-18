package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	_ "github.com/biffsocko/prm/internal/inbound/adapters" // register splunk/graylog/generic
	"github.com/biffsocko/prm/internal/rest"
	"github.com/biffsocko/prm/internal/server"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/sqlite"
	"github.com/biffsocko/prm/internal/webhook"
)

// TestEndToEndInboundSplunkAlertFiresWebhook proves the full inbound
// integration path:
//
//	Splunk POST -> /v1/inbound/{id} -> adapter normalize ->
//	  channel republish -> matching subscription's webhook fires
//
// One in-process prmd, one HTTP fixture acting as Splunk, one HTTP
// fixture acting as the LLM bot's webhook receiver.
func TestEndToEndInboundSplunkAlertFiresWebhook(t *testing.T) {
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Tenant + bot + channel.
	tenant := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	hash, salt, params, _ := auth.HashPassword("x")
	bot := &storage.Account{
		Username: "alertbot", DisplayName: "Alert Bot", Type: storage.AccountBot,
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

	// Splunk integration: server generates the token; we precompute the
	// same plaintext so the test can POST as Splunk would.
	integrationToken := "test-splunk-token-deadbeef-12345"
	integHash := sha256.Sum256([]byte(integrationToken))
	integ := &storage.Integration{
		ChannelID:    ch.ID,
		AccountID:    bot.ID,
		Adapter:      "splunk",
		TokenHash:    integHash[:],
		SettingsJSON: []byte(`{}`),
	}
	if err := s.CreateIntegration(ctx, tenant.ID, integ); err != nil {
		t.Fatal(err)
	}

	// Fake bot webhook receiver.
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

	tlsCfg, err := server.DevTLSConfig("localhost")
	if err != nil {
		t.Fatal(err)
	}

	// Realtime server (we need it because rest's EventPublisher is
	// server.Server.PublishInbound).
	rtAddr := pickFreeAddr(t)
	rt, err := server.New(server.Config{
		Addr: rtAddr, TLSConfig: tlsCfg, Store: s,
		Name: "prmd-e2e", Version: "test", WebhookMgr: mgr,
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = rt.Serve(runCtx) }()
	if err := waitDialable(rtAddr, 3*time.Second); err != nil {
		cancel()
		wg.Wait()
		t.Fatal(err)
	}

	// REST listener with EventPublisher wired.
	restAddr := pickFreeAddr(t)
	rs, err := rest.New(rest.Config{
		Addr: restAddr, TLSConfig: tlsCfg, Store: s,
		WebhookMgr: mgr, EventPublisher: rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(1)
	go func() { defer wg.Done(); _ = rs.Serve(runCtx) }()
	httpClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
		Timeout:   5 * time.Second,
	}
	if err := waitHTTP(httpClient, "https://"+restAddr+"/healthz", 3*time.Second); err != nil {
		cancel()
		wg.Wait()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); wg.Wait() })

	// Bot creates a subscription on #ops via REST -- regex matches
	// "deploy" anywhere; since inbound events get formatted into a
	// body like "[error] splunk/auth-api: ...", we'll match on the
	// "[error]" severity tag instead.
	subBody := `{
		"channel_name": "ops",
		"url": "` + receiver.URL + `",
		"match": {"any_of":[{"type":"regex","pattern":"\\[(error|critical)\\]"}]},
		"context_lines": 0
	}`
	req, _ := http.NewRequest("POST", "https://"+restAddr+"/v1/subscriptions", bytes.NewReader([]byte(subBody)))
	req.Header.Set("Authorization", "Bearer "+botToken)
	req.Header.Set("Content-Type", "application/json")
	subResp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer subResp.Body.Close()
	if subResp.StatusCode != 201 {
		body, _ := io.ReadAll(subResp.Body)
		t.Fatalf("create subscription: %d %s", subResp.StatusCode, body)
	}
	var subBodyParsed map[string]any
	_ = json.NewDecoder(subResp.Body).Decode(&subBodyParsed)
	secretB64, _ := subBodyParsed["secret"].(string)
	secret, err := base64.RawURLEncoding.DecodeString(secretB64)
	if err != nil {
		t.Fatal(err)
	}

	// "Splunk" POSTs an alert to /v1/inbound/{id}.
	splunkPayload := `{
		"sid": "scheduler__admin__abc",
		"search_name": "Auth API 5xx Spike",
		"app": "search",
		"owner": "admin",
		"results_link": "https://splunk.example.com/r/x",
		"result": {"status_code": "503", "service": "auth-api", "count": "47"}
	}`
	inboundURL := "https://" + restAddr + "/v1/inbound/" + integ.ID.String()
	req2, _ := http.NewRequest("POST", inboundURL, bytes.NewReader([]byte(splunkPayload)))
	req2.Header.Set("Authorization", "Bearer "+integrationToken)
	req2.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 202 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("inbound POST: status=%d body=%s", resp.StatusCode, body)
	}

	// Wait for the webhook fire.
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
	defer hitsMu.Unlock()
	if len(hits) != 1 {
		t.Fatalf("expected 1 webhook hit from inbound flow, got %d", len(hits))
	}
	hit := hits[0]
	if !webhook.Verify(hit.Signature, hit.Body, secret) {
		t.Errorf("signature did not verify on inbound-driven webhook")
	}
	var p webhook.Payload
	if err := json.Unmarshal(hit.Body, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(p.Matches))
	}
	body := p.Matches[0].Body
	if !contains(body, "[error]") {
		t.Errorf("expected severity tag in body: %q", body)
	}
	if !contains(body, "splunk/auth-api") {
		t.Errorf("expected source/service in body: %q", body)
	}
	if !contains(body, "Auth API 5xx Spike") {
		t.Errorf("expected search_name in body: %q", body)
	}
	if !contains(body, "count=47") {
		t.Errorf("expected count in body: %q", body)
	}
}

// TestEndToEndInboundRejectsBadToken: wrong bearer token -> 401.
func TestEndToEndInboundRejectsBadToken(t *testing.T) {
	s, _ := sqlite.Open(":memory:")
	t.Cleanup(func() { _ = s.Close() })
	_ = s.Migrate(context.Background())
	tenant := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	_ = s.CreateTenant(context.Background(), tenant)
	hash, salt, params, _ := auth.HashPassword("x")
	bot := &storage.Account{Username: "bot", Type: storage.AccountBot,
		PasswordHash: hash, PasswordSalt: salt, PasswordParams: params}
	_ = s.CreateAccount(context.Background(), tenant.ID, bot)
	ch := &storage.Channel{Name: "ops", OwnerID: bot.ID, Visibility: storage.ChannelPublic}
	_ = s.CreateChannel(context.Background(), tenant.ID, ch)
	// Real integration so the path id is plausible.
	realHash := sha256.Sum256([]byte("real-token"))
	integ := &storage.Integration{
		ChannelID: ch.ID, AccountID: bot.ID, Adapter: "splunk", TokenHash: realHash[:],
	}
	_ = s.CreateIntegration(context.Background(), tenant.ID, integ)

	tlsCfg, _ := server.DevTLSConfig("localhost")
	rtAddr := pickFreeAddr(t)
	rt, _ := server.New(server.Config{Addr: rtAddr, TLSConfig: tlsCfg, Store: s, Name: "x", Version: "x"})
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = rt.Serve(runCtx) }()
	if err := waitDialable(rtAddr, 3*time.Second); err != nil {
		cancel()
		wg.Wait()
		t.Fatal(err)
	}
	restAddr := pickFreeAddr(t)
	rs, _ := rest.New(rest.Config{Addr: restAddr, TLSConfig: tlsCfg, Store: s, EventPublisher: rt})
	wg.Add(1)
	go func() { defer wg.Done(); _ = rs.Serve(runCtx) }()
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, Timeout: 5 * time.Second}
	if err := waitHTTP(client, "https://"+restAddr+"/healthz", 3*time.Second); err != nil {
		cancel()
		wg.Wait()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); wg.Wait() })

	inboundURL := "https://" + restAddr + "/v1/inbound/" + integ.ID.String()
	req, _ := http.NewRequest("POST", inboundURL, bytes.NewReader([]byte(`{"search_name":"x"}`)))
	req.Header.Set("Authorization", "Bearer wrong-token")
	resp, _ := client.Do(req)
	if resp.StatusCode != 401 {
		t.Errorf("expected 401 for bad token, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
