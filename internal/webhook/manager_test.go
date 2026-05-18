package webhook_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/channels"
	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/sqlite"
	"github.com/biffsocko/prm/internal/webhook"
)

// fixture stands up the full webhook stack against a real sqlite + a
// httptest server that records hits.
type fixture struct {
	t        *testing.T
	store    storage.Store
	tenant   *storage.Tenant
	bot      *storage.Account
	channel  *storage.Channel
	mgr      *webhook.Manager
	receiver *httptest.Server
	hits     []*recordedHit
	hitsMu   sync.Mutex
	secret   []byte
}

type recordedHit struct {
	Body      []byte
	Signature string
	Status    int
}

func newFixture(t *testing.T) *fixture {
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
	bot := &storage.Account{Username: "bot", Type: storage.AccountBot,
		PasswordHash: []byte("x"), PasswordSalt: []byte("x"), PasswordParams: "argon2id,m=65536,t=3,p=1"}
	if err := s.CreateAccount(ctx, ten.ID, bot); err != nil {
		t.Fatal(err)
	}
	ch := &storage.Channel{Name: "ops", OwnerID: bot.ID, Visibility: storage.ChannelPublic}
	if err := s.CreateChannel(ctx, ten.ID, ch); err != nil {
		t.Fatal(err)
	}

	f := &fixture{t: t, store: s, tenant: ten, bot: bot, channel: ch, secret: []byte("test-secret")}
	f.receiver = httptest.NewServer(http.HandlerFunc(f.handleHit))
	t.Cleanup(f.receiver.Close)

	mgr := webhook.NewManager(s, webhook.Config{Workers: 4, QueueDepth: 64}, nil)
	mgr.Start(context.Background())
	t.Cleanup(mgr.Stop)
	f.mgr = mgr
	return f
}

func (f *fixture) handleHit(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.hitsMu.Lock()
	f.hits = append(f.hits, &recordedHit{
		Body:      body,
		Signature: r.Header.Get("PRM-Signature"),
		Status:    200,
	})
	f.hitsMu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (f *fixture) hitCount() int {
	f.hitsMu.Lock()
	defer f.hitsMu.Unlock()
	return len(f.hits)
}

func (f *fixture) hit(i int) *recordedHit {
	f.hitsMu.Lock()
	defer f.hitsMu.Unlock()
	return f.hits[i]
}

func (f *fixture) addSub(t *testing.T, matchJSON string, debounceMs, cooldownMs, ctxLines int) *storage.Subscription {
	t.Helper()
	sub := &storage.Subscription{
		AccountID: f.bot.ID, ChannelID: f.channel.ID,
		URL: f.receiver.URL, Secret: f.secret,
		MatchJSON:    []byte(matchJSON),
		Events:       []string{"message"},
		ContextLines: ctxLines,
		DebounceMs:   debounceMs,
		CooldownMs:   cooldownMs,
	}
	if err := f.store.CreateSubscription(context.Background(), f.tenant.ID, sub); err != nil {
		t.Fatal(err)
	}
	if err := f.mgr.AddOrUpdate(sub); err != nil {
		t.Fatal(err)
	}
	return sub
}

func (f *fixture) notify(body string, ts time.Time) {
	f.mgr.Notify(webhook.Event{
		TenantID: f.tenant.ID, ChannelID: f.channel.ID, ChannelName: f.channel.Name,
		From: f.bot.ID, DisplayName: "Bot", Body: body, TS: ts,
	})
}

func waitForHits(f *fixture, want int, deadline time.Duration) bool {
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if f.hitCount() >= want {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return f.hitCount() >= want
}

func TestWebhookFiresOnRegexMatch(t *testing.T) {
	f := newFixture(t)
	f.addSub(t, `{"any_of":[{"type":"regex","pattern":"(?i)^deploy"}]}`, 0, 0, 0)

	f.notify("deploy production now", time.Now())
	if !waitForHits(f, 1, 2*time.Second) {
		t.Fatalf("expected 1 webhook hit, got %d", f.hitCount())
	}

	hit := f.hit(0)
	if !webhook.Verify(hit.Signature, hit.Body, f.secret) {
		t.Errorf("signature verification failed")
	}

	var p webhook.Payload
	if err := json.Unmarshal(hit.Body, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Matches) != 1 {
		t.Errorf("expected 1 match, got %d", len(p.Matches))
	}
	if p.Matches[0].Body != "deploy production now" {
		t.Errorf("body mismatch: %q", p.Matches[0].Body)
	}
}

func TestWebhookDoesNotFireOnNonMatch(t *testing.T) {
	f := newFixture(t)
	f.addSub(t, `{"any_of":[{"type":"regex","pattern":"^deploy"}]}`, 0, 0, 0)
	f.notify("just chatting", time.Now())
	// Give the pool time to maybe-fire-wrongly.
	time.Sleep(100 * time.Millisecond)
	if f.hitCount() != 0 {
		t.Errorf("expected 0 hits, got %d", f.hitCount())
	}
}

func TestWebhookCooldownDropsRapidMatches(t *testing.T) {
	f := newFixture(t)
	f.addSub(t, `{"any_of":[{"type":"regex","pattern":"x"}]}`, 0, 500, 0)

	// Two rapid matches in a 500ms cooldown -> only first fires.
	f.notify("x first", time.Now())
	if !waitForHits(f, 1, 2*time.Second) {
		t.Fatalf("expected 1 hit, got %d", f.hitCount())
	}
	f.notify("x second", time.Now())
	time.Sleep(200 * time.Millisecond)
	if f.hitCount() != 1 {
		t.Errorf("cooldown should drop second match; got %d hits", f.hitCount())
	}
}

func TestWebhookDebounceCollapsesMultipleMatchesIntoOneFire(t *testing.T) {
	f := newFixture(t)
	f.addSub(t, `{"any_of":[{"type":"regex","pattern":"x"}]}`, 200, 0, 0)

	// Three matches within the 200ms debounce window -> one fire with 3 matches.
	f.notify("x1", time.Now())
	time.Sleep(50 * time.Millisecond)
	f.notify("x2", time.Now())
	time.Sleep(50 * time.Millisecond)
	f.notify("x3", time.Now())

	if !waitForHits(f, 1, 2*time.Second) {
		t.Fatalf("expected 1 batched hit, got %d", f.hitCount())
	}
	// Make sure a second fire doesn't sneak in.
	time.Sleep(100 * time.Millisecond)
	if f.hitCount() != 1 {
		t.Errorf("debounce should batch; got %d hits", f.hitCount())
	}
	var p webhook.Payload
	if err := json.Unmarshal(f.hit(0).Body, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Matches) != 3 {
		t.Errorf("expected 3 batched matches, got %d", len(p.Matches))
	}
}

func TestWebhookContextLinesIncludedInPayload(t *testing.T) {
	f := newFixture(t)
	f.addSub(t, `{"any_of":[{"type":"regex","pattern":"trigger"}]}`, 0, 0, 5)

	// Build a fake event with context history.
	ctxHist := []channels.HistoryEntry{
		{From: uuid.Must(uuid.NewV7()), Body: "a", TS: time.Now()},
		{From: uuid.Must(uuid.NewV7()), Body: "b", TS: time.Now()},
		{From: uuid.Must(uuid.NewV7()), Body: "c", TS: time.Now()},
	}
	f.mgr.Notify(webhook.Event{
		TenantID: f.tenant.ID, ChannelID: f.channel.ID, ChannelName: f.channel.Name,
		From: f.bot.ID, Body: "trigger this", TS: time.Now(),
		Context: ctxHist,
	})
	if !waitForHits(f, 1, 2*time.Second) {
		t.Fatalf("expected 1 hit, got %d", f.hitCount())
	}
	var p webhook.Payload
	if err := json.Unmarshal(f.hit(0).Body, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Context) != 3 {
		t.Errorf("expected 3 context lines (capped by available history), got %d", len(p.Context))
	}
}

func TestWebhookRetriesOn5xx(t *testing.T) {
	f := newFixture(t)
	// Replace the receiver with one that fails twice then succeeds.
	var calls int32
	failingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(failingServer.Close)

	sub := &storage.Subscription{
		AccountID: f.bot.ID, ChannelID: f.channel.ID,
		URL: failingServer.URL, Secret: f.secret,
		MatchJSON: []byte(`{"any_of":[{"type":"regex","pattern":"x"}]}`),
	}
	if err := f.store.CreateSubscription(context.Background(), f.tenant.ID, sub); err != nil {
		t.Fatal(err)
	}
	if err := f.mgr.AddOrUpdate(sub); err != nil {
		t.Fatal(err)
	}

	f.notify("x trigger", time.Now())
	// Three attempts total; allow a few seconds for the backoff.
	end := time.Now().Add(5 * time.Second)
	for time.Now().Before(end) && atomic.LoadInt32(&calls) < 3 {
		time.Sleep(20 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&calls); got < 3 {
		t.Errorf("expected at least 3 attempts (2 failures + 1 success), got %d", got)
	}
}

func TestWebhookDropsOn4xxWithoutRetry(t *testing.T) {
	f := newFixture(t)
	var calls int32
	rejectingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(rejectingServer.Close)

	sub := &storage.Subscription{
		AccountID: f.bot.ID, ChannelID: f.channel.ID,
		URL: rejectingServer.URL, Secret: f.secret,
		MatchJSON: []byte(`{"any_of":[{"type":"regex","pattern":"x"}]}`),
	}
	if err := f.store.CreateSubscription(context.Background(), f.tenant.ID, sub); err != nil {
		t.Fatal(err)
	}
	if err := f.mgr.AddOrUpdate(sub); err != nil {
		t.Fatal(err)
	}

	f.notify("x", time.Now())
	time.Sleep(500 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("expected exactly 1 attempt on 4xx (no retry), got %d", got)
	}
}
