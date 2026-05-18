package rest_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/rest"
	"github.com/biffsocko/prm/internal/server"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/sqlite"
)

// restFixture spins up a real REST server on a random port + an in-memory
// SQLite + a tenant/bot/channel/token. The token is returned for use in
// Authorization headers.
type restFixture struct {
	t      *testing.T
	srv    *rest.Server
	addr   string
	store  storage.Store
	tenant *storage.Tenant
	bot    *storage.Account
	ch     *storage.Channel
	token  string
	client *http.Client
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func newRESTFixture(t *testing.T) *restFixture {
	t.Helper()
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ten := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(ctx, ten); err != nil {
		t.Fatal(err)
	}
	hash, salt, params, _ := auth.HashPassword("x")
	bot := &storage.Account{
		Username: "alertbot", Type: storage.AccountBot,
		PasswordHash: hash, PasswordSalt: salt, PasswordParams: params,
	}
	if err := s.CreateAccount(ctx, ten.ID, bot); err != nil {
		t.Fatal(err)
	}
	ch := &storage.Channel{Name: "ops", OwnerID: bot.ID, Visibility: storage.ChannelPublic}
	if err := s.CreateChannel(ctx, ten.ID, ch); err != nil {
		t.Fatal(err)
	}
	plaintext, _, err := auth.IssueToken(ctx, s, ten.ID, bot.ID, "test")
	if err != nil {
		t.Fatal(err)
	}

	addr := pickFreeAddr(t)
	tlsCfg, err := server.DevTLSConfig("localhost")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := rest.New(rest.Config{
		Addr: addr, TLSConfig: tlsCfg, Store: s,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx2, cancel := context.WithCancel(context.Background())
	f := &restFixture{t: t, srv: srv, addr: addr, store: s, tenant: ten, bot: bot, ch: ch, token: plaintext, cancel: cancel}
	f.client = &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
		Timeout:   5 * time.Second,
	}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		_ = srv.Serve(ctx2)
	}()
	if err := waitForHealth(f, 2*time.Second); err != nil {
		cancel()
		f.wg.Wait()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); f.wg.Wait() })
	return f
}

func pickFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := l.Addr().String()
	_ = l.Close()
	return a
}

func waitForHealth(f *restFixture, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := f.client.Get("https://" + f.addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("rest server did not become healthy")
}

func (f *restFixture) do(t *testing.T, method, path, body string) (*http.Response, []byte) {
	t.Helper()
	var b io.Reader
	if body != "" {
		b = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, "https://"+f.addr+path, b)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if f.token != "" {
		req.Header.Set("Authorization", "Bearer "+f.token)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return resp, rb
}

// ---- tests ----

func TestRESTHealthz(t *testing.T) {
	f := newRESTFixture(t)
	resp, body := f.do(t, "GET", "/healthz", "")
	if resp.StatusCode != 200 {
		t.Errorf("healthz: status=%d body=%s", resp.StatusCode, body)
	}
}

func TestRESTAuthRejectsMissingToken(t *testing.T) {
	f := newRESTFixture(t)
	oldToken := f.token
	f.token = ""
	resp, _ := f.do(t, "GET", "/v1/subscriptions", "")
	if resp.StatusCode != 401 {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
	f.token = oldToken
}

func TestRESTAuthRejectsBadToken(t *testing.T) {
	f := newRESTFixture(t)
	f.token = "definitely-not-a-real-token"
	resp, _ := f.do(t, "GET", "/v1/subscriptions", "")
	if resp.StatusCode != 401 {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestRESTCreateSubscriptionRoundTrip(t *testing.T) {
	f := newRESTFixture(t)
	body := fmt.Sprintf(`{
		"channel_name": "ops",
		"url": "https://my-bot.example.com/webhook",
		"match": {"any_of":[{"type":"regex","pattern":"^deploy"}]},
		"context_lines": 8,
		"debounce_ms": 500
	}`)
	resp, rb := f.do(t, "POST", "/v1/subscriptions", body)
	if resp.StatusCode != 201 {
		t.Fatalf("create: status=%d body=%s", resp.StatusCode, rb)
	}
	var created map[string]any
	if err := json.Unmarshal(rb, &created); err != nil {
		t.Fatal(err)
	}
	if created["secret"] == nil || created["secret"] == "" {
		t.Errorf("expected plaintext secret returned on create; got %v", created["secret"])
	}
	id, _ := created["id"].(string)
	if id == "" {
		t.Fatal("missing id")
	}

	// GET it back; secret should NOT be in the response this time.
	resp2, rb2 := f.do(t, "GET", "/v1/subscriptions/"+id, "")
	if resp2.StatusCode != 200 {
		t.Fatalf("get: status=%d body=%s", resp2.StatusCode, rb2)
	}
	if bytes.Contains(rb2, []byte(`"secret"`)) {
		t.Errorf("GET should not include secret in response: %s", rb2)
	}

	// LIST
	resp3, rb3 := f.do(t, "GET", "/v1/subscriptions", "")
	if resp3.StatusCode != 200 {
		t.Fatalf("list: %d %s", resp3.StatusCode, rb3)
	}
	var list map[string]any
	if err := json.Unmarshal(rb3, &list); err != nil {
		t.Fatal(err)
	}
	subs, _ := list["subscriptions"].([]any)
	if len(subs) != 1 {
		t.Errorf("expected 1 subscription, got %d", len(subs))
	}
}

func TestRESTCreateSubscriptionRejectsBadMatch(t *testing.T) {
	f := newRESTFixture(t)
	body := `{
		"channel_name": "ops",
		"url": "https://x",
		"match": {"any_of":[{"type":"regex","pattern":"["}]}
	}`
	resp, rb := f.do(t, "POST", "/v1/subscriptions", body)
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 for unparseable regex, got %d body=%s", resp.StatusCode, rb)
	}
}

func TestRESTCreateSubscriptionRejectsUnknownChannel(t *testing.T) {
	f := newRESTFixture(t)
	body := `{
		"channel_name": "no-such-channel",
		"url": "https://x",
		"match": {"any_of":[{"type":"regex","pattern":"x"}]}
	}`
	resp, _ := f.do(t, "POST", "/v1/subscriptions", body)
	if resp.StatusCode != 404 {
		t.Errorf("expected 404 for unknown channel, got %d", resp.StatusCode)
	}
}

func TestRESTUpdateAndDisable(t *testing.T) {
	f := newRESTFixture(t)
	createBody := `{
		"channel_name": "ops",
		"url": "https://a",
		"match": {"any_of":[{"type":"regex","pattern":"x"}]}
	}`
	resp, rb := f.do(t, "POST", "/v1/subscriptions", createBody)
	if resp.StatusCode != 201 {
		t.Fatalf("create: %d %s", resp.StatusCode, rb)
	}
	var created map[string]any
	_ = json.Unmarshal(rb, &created)
	id := created["id"].(string)

	// PATCH: change URL + disable
	patch := `{"url":"https://b","disabled":true}`
	resp2, rb2 := f.do(t, "PATCH", "/v1/subscriptions/"+id, patch)
	if resp2.StatusCode != 200 {
		t.Fatalf("patch: %d %s", resp2.StatusCode, rb2)
	}
	var updated map[string]any
	_ = json.Unmarshal(rb2, &updated)
	if updated["url"] != "https://b" {
		t.Errorf("url not updated: %v", updated["url"])
	}
	if updated["disabled_at"] == nil {
		t.Errorf("expected disabled_at to be set")
	}

	// DELETE
	resp3, _ := f.do(t, "DELETE", "/v1/subscriptions/"+id, "")
	if resp3.StatusCode != 204 {
		t.Errorf("delete: %d", resp3.StatusCode)
	}
	resp4, _ := f.do(t, "GET", "/v1/subscriptions/"+id, "")
	if resp4.StatusCode != 404 {
		t.Errorf("get after delete: %d", resp4.StatusCode)
	}
}

func TestRESTCannotReadAnotherAccountsSubscription(t *testing.T) {
	f := newRESTFixture(t)
	// Create as bot1 (f.bot).
	body := `{"channel_name":"ops","url":"https://x","match":{"any_of":[{"type":"regex","pattern":"x"}]}}`
	resp, rb := f.do(t, "POST", "/v1/subscriptions", body)
	if resp.StatusCode != 201 {
		t.Fatalf("create: %d %s", resp.StatusCode, rb)
	}
	var created map[string]any
	_ = json.Unmarshal(rb, &created)
	id := created["id"].(string)

	// Create a second bot in the same tenant with its own token.
	ctx := context.Background()
	hash, salt, params, _ := auth.HashPassword("x")
	bot2 := &storage.Account{
		Username: "bot2", Type: storage.AccountBot,
		PasswordHash: hash, PasswordSalt: salt, PasswordParams: params,
	}
	if err := f.store.CreateAccount(ctx, f.tenant.ID, bot2); err != nil {
		t.Fatal(err)
	}
	plaintext2, _, err := auth.IssueToken(ctx, f.store, f.tenant.ID, bot2.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	oldToken := f.token
	f.token = plaintext2
	defer func() { f.token = oldToken }()

	// bot2 tries to fetch bot1's subscription -> 403 not_owner.
	resp2, _ := f.do(t, "GET", "/v1/subscriptions/"+id, "")
	if resp2.StatusCode != 403 {
		t.Errorf("expected 403 for cross-account access, got %d", resp2.StatusCode)
	}
	// And bot2's list should be empty (its own subs only).
	resp3, rb3 := f.do(t, "GET", "/v1/subscriptions", "")
	if resp3.StatusCode != 200 {
		t.Fatalf("list: %d", resp3.StatusCode)
	}
	var list map[string]any
	_ = json.Unmarshal(rb3, &list)
	subs, _ := list["subscriptions"].([]any)
	if len(subs) != 0 {
		t.Errorf("expected 0 subscriptions for bot2, got %d", len(subs))
	}
}

func TestRESTCrossTenantTokensCantReachOtherTenant(t *testing.T) {
	f := newRESTFixture(t)
	// Create a subscription in tenant acme via bot1.
	body := `{"channel_name":"ops","url":"https://x","match":{"any_of":[{"type":"regex","pattern":"x"}]}}`
	resp, rb := f.do(t, "POST", "/v1/subscriptions", body)
	if resp.StatusCode != 201 {
		t.Fatalf("create: %d %s", resp.StatusCode, rb)
	}
	var created map[string]any
	_ = json.Unmarshal(rb, &created)
	id := created["id"].(string)

	// Set up a second tenant with its own bot + token. Same channel name.
	ctx := context.Background()
	t2 := &storage.Tenant{Slug: "globex", DisplayName: "Globex"}
	if err := f.store.CreateTenant(ctx, t2); err != nil {
		t.Fatal(err)
	}
	hash, salt, params, _ := auth.HashPassword("x")
	bot2 := &storage.Account{Username: "bot", Type: storage.AccountBot,
		PasswordHash: hash, PasswordSalt: salt, PasswordParams: params}
	if err := f.store.CreateAccount(ctx, t2.ID, bot2); err != nil {
		t.Fatal(err)
	}
	plaintext2, _, err := auth.IssueToken(ctx, f.store, t2.ID, bot2.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	oldToken := f.token
	f.token = plaintext2
	defer func() { f.token = oldToken }()

	// tenant t2's token trying to read tenant acme's subscription -> 404
	// (not 403; we don't disclose that the id exists in another tenant).
	resp2, _ := f.do(t, "GET", "/v1/subscriptions/"+id, "")
	if resp2.StatusCode != 404 {
		t.Errorf("expected 404 (tenant isolation), got %d", resp2.StatusCode)
	}
}
