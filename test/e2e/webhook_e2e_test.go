// Package e2e holds integration tests that exercise the full PRM stack
// in-process: realtime TLS server + REST control plane + webhook
// manager + a fake HTTP receiver acting as the bot's webhook endpoint.
package e2e_test

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/biffsocko/prm/internal/auth"
	"github.com/biffsocko/prm/internal/proto"
	"github.com/biffsocko/prm/internal/rest"
	"github.com/biffsocko/prm/internal/server"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/sqlite"
	"github.com/biffsocko/prm/internal/webhook"
)

// fullStack stands up the entire PRM server (realtime + REST + webhook
// manager + fake bot HTTP receiver) for an integration test.
type fullStack struct {
	t           *testing.T
	rtAddr      string
	restAddr    string
	store       storage.Store
	tenant      *storage.Tenant
	humanAcct   *storage.Account
	humanPwd    string
	botAcct     *storage.Account
	botToken    string
	channel     *storage.Channel
	receiver    *httptest.Server
	receiverHits []*recordedHit
	hitsMu      sync.Mutex
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	httpClient  *http.Client
}

type recordedHit struct {
	Body      []byte
	Signature string
}

func newStack(t *testing.T) *fullStack {
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
	tenant := &storage.Tenant{Slug: "acme", DisplayName: "Acme"}
	if err := s.CreateTenant(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	// Human account.
	hash, salt, params, _ := auth.HashPassword("hunter2")
	human := &storage.Account{
		Username: "alex", DisplayName: "Alex",
		PasswordHash: hash, PasswordSalt: salt, PasswordParams: params,
	}
	if err := s.CreateAccount(ctx, tenant.ID, human); err != nil {
		t.Fatal(err)
	}
	// Bot account + token.
	bhash, bsalt, bparams, _ := auth.HashPassword("x")
	bot := &storage.Account{
		Username: "alertbot", DisplayName: "Alert Bot", Type: storage.AccountBot,
		PasswordHash: bhash, PasswordSalt: bsalt, PasswordParams: bparams,
	}
	if err := s.CreateAccount(ctx, tenant.ID, bot); err != nil {
		t.Fatal(err)
	}
	botToken, _, err := auth.IssueToken(ctx, s, tenant.ID, bot.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	// Public channel.
	ch := &storage.Channel{Name: "ops", OwnerID: bot.ID, Visibility: storage.ChannelPublic}
	if err := s.CreateChannel(ctx, tenant.ID, ch); err != nil {
		t.Fatal(err)
	}

	stack := &fullStack{
		t: t, store: s, tenant: tenant, humanAcct: human, humanPwd: "hunter2",
		botAcct: bot, botToken: botToken, channel: ch,
	}
	stack.httpClient = &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
		Timeout:   5 * time.Second,
	}

	// Fake bot HTTP receiver.
	stack.receiver = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stack.hitsMu.Lock()
		stack.receiverHits = append(stack.receiverHits, &recordedHit{
			Body: body, Signature: r.Header.Get("PRM-Signature"),
		})
		stack.hitsMu.Unlock()
		w.WriteHeader(200)
	}))
	t.Cleanup(stack.receiver.Close)

	// Webhook manager.
	mgr := webhook.NewManager(s, webhook.Config{}, nil)
	tens, _ := s.ListTenants(ctx)
	if err := mgr.Reload(ctx, tens); err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	stack.cancel = cancel
	mgr.Start(runCtx)
	t.Cleanup(mgr.Stop)

	// Real TLS for both listeners; one cert for localhost is fine.
	tlsCfg, err := server.DevTLSConfig("localhost")
	if err != nil {
		t.Fatal(err)
	}

	// Realtime server.
	stack.rtAddr = pickFreeAddr(t)
	rt, err := server.New(server.Config{
		Addr: stack.rtAddr, TLSConfig: tlsCfg, Store: s,
		Name: "prmd-e2e", Version: "test", WebhookMgr: mgr,
	})
	if err != nil {
		t.Fatal(err)
	}
	stack.wg.Add(1)
	go func() {
		defer stack.wg.Done()
		_ = rt.Serve(runCtx)
	}()

	// REST control plane.
	stack.restAddr = pickFreeAddr(t)
	rs, err := rest.New(rest.Config{
		Addr: stack.restAddr, TLSConfig: tlsCfg, Store: s, WebhookMgr: mgr,
	})
	if err != nil {
		t.Fatal(err)
	}
	stack.wg.Add(1)
	go func() {
		defer stack.wg.Done()
		_ = rs.Serve(runCtx)
	}()

	if err := waitDialable(stack.rtAddr, 3*time.Second); err != nil {
		cancel()
		stack.wg.Wait()
		t.Fatal(err)
	}
	if err := waitHTTP(stack.httpClient, "https://"+stack.restAddr+"/healthz", 3*time.Second); err != nil {
		cancel()
		stack.wg.Wait()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		stack.wg.Wait()
	})
	return stack
}

func pickFreeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := l.Addr().String()
	_ = l.Close()
	return a
}

func waitDialable(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	cfg := &tls.Config{InsecureSkipVerify: true}
	for time.Now().Before(deadline) {
		c, err := tls.Dial("tcp", addr, cfg)
		if err == nil {
			_ = c.Close()
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("not dialable: %s", addr)
}

func waitHTTP(c *http.Client, url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := c.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("not healthy: %s", url)
}

func (s *fullStack) hits() []*recordedHit {
	s.hitsMu.Lock()
	defer s.hitsMu.Unlock()
	out := make([]*recordedHit, len(s.receiverHits))
	copy(out, s.receiverHits)
	return out
}

func (s *fullStack) waitForHits(want int, timeout time.Duration) bool {
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		if len(s.hits()) >= want {
			return true
		}
		time.Sleep(15 * time.Millisecond)
	}
	return len(s.hits()) >= want
}

func (s *fullStack) doREST(t *testing.T, method, path, body string) (*http.Response, []byte) {
	t.Helper()
	var br io.Reader
	if body != "" {
		br = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, "https://"+s.restAddr+path, br)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+s.botToken)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return resp, rb
}

// ---- the actual e2e test ----

func TestEndToEndWebhookFiresOnMatchingMsg(t *testing.T) {
	stack := newStack(t)

	// 1. Bot creates a subscription on #ops via REST.
	createBody := fmt.Sprintf(`{
		"channel_name": "ops",
		"url": %q,
		"match": {"any_of": [{"type":"regex","pattern":"(?i)^deploy"}]},
		"context_lines": 3
	}`, stack.receiver.URL)
	resp, rb := stack.doREST(t, "POST", "/v1/subscriptions", createBody)
	if resp.StatusCode != 201 {
		t.Fatalf("create subscription: status=%d body=%s", resp.StatusCode, rb)
	}
	var created map[string]any
	if err := json.Unmarshal(rb, &created); err != nil {
		t.Fatal(err)
	}
	secretB64, _ := created["secret"].(string)
	if secretB64 == "" {
		t.Fatal("expected plaintext secret in create response")
	}
	// Decode the secret so we can verify signatures on hits. REST returns
	// base64-url (matching the encoding used in rest.toResponse).
	secret, err := base64.RawURLEncoding.DecodeString(secretB64)
	if err != nil {
		t.Fatalf("decode secret: %v", err)
	}

	// 2. Human user connects via realtime, sends some chat then a "deploy" msg.
	tlsCfg := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost"}
	c, err := tls.Dial("tcp", stack.rtAddr, tlsCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	dec := proto.NewDecoder(c)

	// hello + welcome
	_ = proto.Encode(c, proto.Hello{CapVersion: "0.1"})
	if _, err := dec.Decode(); err != nil {
		t.Fatal(err)
	}
	// password auth
	_ = proto.Encode(c, proto.AuthRequest{Method: proto.AuthMethodPassword,
		Tenant: stack.tenant.Slug, Username: stack.humanAcct.Username})
	chalF, err := dec.Decode()
	if err != nil {
		t.Fatal(err)
	}
	chal := chalF.(proto.AuthChallenge)
	saltBytes, _ := auth.DecodeBase64(chal.Salt)
	proof, _ := auth.ComputeClientProof(stack.humanPwd, saltBytes, chal.Params)
	_ = proto.Encode(c, proto.AuthResponse{Proof: auth.EncodeBase64(proof)})
	if _, err := dec.Decode(); err != nil {
		t.Fatal(err)
	}
	// join
	_ = proto.Encode(c, proto.Join{Channel: "ops"})
	if _, err := dec.Decode(); err != nil { // own presence
		t.Fatal(err)
	}

	// Non-matching chatter (becomes webhook context once the trigger fires).
	_ = proto.Encode(c, proto.Msg{Channel: "ops", Body: "good morning"})
	if _, err := dec.Decode(); err != nil {
		t.Fatal(err)
	}
	_ = proto.Encode(c, proto.Msg{Channel: "ops", Body: "anyone here?"})
	if _, err := dec.Decode(); err != nil {
		t.Fatal(err)
	}

	// The matching message.
	_ = proto.Encode(c, proto.Msg{Channel: "ops", Body: "deploy production"})
	if _, err := dec.Decode(); err != nil {
		t.Fatal(err)
	}

	// 3. Expect the webhook fired exactly once with the right body + signature.
	if !stack.waitForHits(1, 3*time.Second) {
		t.Fatalf("expected 1 webhook hit, got %d", len(stack.hits()))
	}
	hits := stack.hits()
	hit := hits[0]
	if !webhook.Verify(hit.Signature, hit.Body, secret) {
		t.Errorf("signature did not verify")
	}
	var p webhook.Payload
	if err := json.Unmarshal(hit.Body, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(p.Matches))
	}
	if p.Matches[0].Body != "deploy production" {
		t.Errorf("match body: got %q want %q", p.Matches[0].Body, "deploy production")
	}
	// context_lines=3 means up to 3 preceding non-match messages.
	if len(p.Context) != 2 {
		t.Errorf("expected 2 context lines (the two non-match msgs), got %d", len(p.Context))
	}
	if len(p.Context) >= 2 {
		if p.Context[0].Body != "good morning" {
			t.Errorf("context[0]: got %q", p.Context[0].Body)
		}
		if p.Context[1].Body != "anyone here?" {
			t.Errorf("context[1]: got %q", p.Context[1].Body)
		}
	}
	if p.ChannelName != "ops" {
		t.Errorf("channel_name mismatch: %q", p.ChannelName)
	}

	// 4. A non-matching message should NOT add a hit.
	_ = proto.Encode(c, proto.Msg{Channel: "ops", Body: "just chatting again"})
	if _, err := dec.Decode(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := len(stack.hits()); got != 1 {
		t.Errorf("non-match should not fire webhook; total hits=%d", got)
	}
}
