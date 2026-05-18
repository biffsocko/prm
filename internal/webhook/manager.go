package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/channels"
	"github.com/biffsocko/prm/internal/matcher"
	"github.com/biffsocko/prm/internal/storage"
)

// Budget is the per-subscription rate-limit envelope. Stored as JSON in
// the subscription row (storage.Subscription.BudgetJSON).
type Budget struct {
	DailyMaxFires           int     `json:"daily_max_fires,omitempty"`
	EstimatedCostPerFireUSD float64 `json:"estimated_cost_per_fire_usd,omitempty"`
}

// Manager owns the in-memory cache of compiled subscriptions + the
// worker pool + the per-subscription runtime state (debounce timers,
// cooldown, drop counters).
type Manager struct {
	store storage.Store
	log   *slog.Logger
	pool  *workerPool

	// HTTP client used for webhook POSTs. Per-subscription timeout is
	// applied at request time via context. Reused across requests.
	httpClient *http.Client

	mu  sync.RWMutex
	// subs maps subscription_id -> live cached entry. Built from storage
	// at startup via Reload; mutated by Manager.OnSubscriptionChanged when
	// the REST control plane (or admin CLI) creates / updates / deletes.
	subs map[uuid.UUID]*Subscription
	// byChannel maps channel_id -> set of subscription_ids active on that
	// channel. Lets the broadcast hot path get matching subs without
	// scanning the whole map.
	byChannel map[uuid.UUID]map[uuid.UUID]struct{}
}

// Subscription is the live, compiled, mutable runtime form of a
// storage.Subscription. The Matcher and Budget are parsed once at load
// time; the per-sub runtime state (lastFire, debounceTimer, etc.) lives
// here and is protected by the embedded sync.Mutex.
type Subscription struct {
	ID         uuid.UUID
	TenantID   uuid.UUID
	AccountID  uuid.UUID
	ChannelID  uuid.UUID
	URL        string
	Secret     []byte // plaintext HMAC key
	Matcher    *matcher.Matcher
	Events     []string
	CtxLines   int
	DebounceMs int
	CooldownMs int
	Budget     Budget

	mu              sync.Mutex
	debounceTimer   *time.Timer
	pendingMatches  []Message // accumulated during debounce window
	pendingContext  []Message // captured at first match in the window
	pendingChanName string
	lastFire        time.Time
	consec4xx       int
}

// Config tunes the worker pool.
type Config struct {
	// Workers is the number of goroutines doing HTTP POSTs concurrently.
	// Default: 16.
	Workers int
	// QueueDepth is the pending-task buffer between Dispatch and the
	// worker pool. Default: 1024. Exceeded -> drop with metric.
	QueueDepth int
	// HTTPTimeout is the per-attempt HTTP timeout (no headers vs. body).
	// Default: 5 * time.Second.
	HTTPTimeout time.Duration
	// MaxRetries on 5xx / timeouts. Default: 3.
	MaxRetries int
	// AutoDisable4xx is how many consecutive 4xx responses before the
	// subscription is auto-disabled. Default: 5.
	AutoDisable4xx int
}

func (c *Config) applyDefaults() {
	if c.Workers == 0 {
		c.Workers = 16
	}
	if c.QueueDepth == 0 {
		c.QueueDepth = 1024
	}
	if c.HTTPTimeout == 0 {
		c.HTTPTimeout = 5 * time.Second
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 3
	}
	if c.AutoDisable4xx == 0 {
		c.AutoDisable4xx = 5
	}
}

// NewManager constructs a Manager. Call Reload(ctx) to load existing
// subscriptions from storage, then Start to spin up the worker pool.
func NewManager(store storage.Store, cfg Config, logger *slog.Logger) *Manager {
	cfg.applyDefaults()
	if logger == nil {
		logger = slog.Default()
	}
	m := &Manager{
		store:     store,
		log:       logger,
		subs:      make(map[uuid.UUID]*Subscription),
		byChannel: make(map[uuid.UUID]map[uuid.UUID]struct{}),
		httpClient: &http.Client{
			Timeout: cfg.HTTPTimeout,
		},
	}
	m.pool = newWorkerPool(cfg, m)
	return m
}

// Reload scans every active subscription in storage and rebuilds the
// in-memory cache. Called at startup and on demand if storage drifts
// from the in-memory state. Tenant-by-tenant would be cheaper at scale;
// slice 3 does a full scan and revisits for slice 4+.
func (m *Manager) Reload(ctx context.Context, tenants []*storage.Tenant) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subs = make(map[uuid.UUID]*Subscription)
	m.byChannel = make(map[uuid.UUID]map[uuid.UUID]struct{})

	for _, t := range tenants {
		channels, err := m.store.ListChannels(ctx, t.ID)
		if err != nil {
			return fmt.Errorf("reload: list channels in %s: %w", t.Slug, err)
		}
		for _, ch := range channels {
			subs, err := m.store.ListSubscriptionsByChannel(ctx, t.ID, ch.ID)
			if err != nil {
				return fmt.Errorf("reload: list subs in %s/#%s: %w", t.Slug, ch.Name, err)
			}
			for _, sub := range subs {
				live, err := compileSubscription(sub)
				if err != nil {
					m.log.Warn("reload: skipping subscription with bad rules",
						"subscription_id", sub.ID, "err", err)
					continue
				}
				m.subs[sub.ID] = live
				m.indexByChannel(live)
			}
		}
	}
	m.log.Info("webhook manager reloaded", "subscriptions", len(m.subs))
	return nil
}

func (m *Manager) indexByChannel(s *Subscription) {
	set, ok := m.byChannel[s.ChannelID]
	if !ok {
		set = make(map[uuid.UUID]struct{}, 2)
		m.byChannel[s.ChannelID] = set
	}
	set[s.ID] = struct{}{}
}

// AddOrUpdate puts a new/updated subscription into the in-memory cache.
// Called by the REST control plane after a successful create/update so
// the next message broadcast can see the change without a full Reload.
func (m *Manager) AddOrUpdate(sub *storage.Subscription) error {
	live, err := compileSubscription(sub)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Cancel pending debounce timer on the old version, if any.
	if old, ok := m.subs[sub.ID]; ok {
		old.mu.Lock()
		if old.debounceTimer != nil {
			old.debounceTimer.Stop()
		}
		old.mu.Unlock()
		// Drop from old channel index if channel changed.
		if old.ChannelID != live.ChannelID {
			if set, ok := m.byChannel[old.ChannelID]; ok {
				delete(set, old.ID)
				if len(set) == 0 {
					delete(m.byChannel, old.ChannelID)
				}
			}
		}
	}
	m.subs[sub.ID] = live
	m.indexByChannel(live)
	return nil
}

// Remove drops a subscription from the in-memory cache. Idempotent.
func (m *Manager) Remove(id uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sub, ok := m.subs[id]
	if !ok {
		return
	}
	sub.mu.Lock()
	if sub.debounceTimer != nil {
		sub.debounceTimer.Stop()
	}
	sub.mu.Unlock()
	delete(m.subs, id)
	if set, ok := m.byChannel[sub.ChannelID]; ok {
		delete(set, id)
		if len(set) == 0 {
			delete(m.byChannel, sub.ChannelID)
		}
	}
}

// Start spins up the worker pool. Returns immediately. Call Stop to
// drain on shutdown.
func (m *Manager) Start(ctx context.Context) {
	m.pool.Start(ctx)
}

// Stop drains the worker pool. Waits for in-flight tasks to complete (or
// the parent ctx to cancel, whichever comes first).
func (m *Manager) Stop() {
	m.pool.Stop()
}

// Notify is the hot-path entry point. Called once per channel broadcast
// (after the broadcast has fanned out to chat members). Iterates active
// subscriptions on the channel, evaluates each matcher, and triggers
// debounce / cooldown / budget logic.
//
// Returns immediately even when subscriptions fire — actual HTTP is on
// the worker pool. Designed to be safe to call from the message-fan-out
// path.
func (m *Manager) Notify(ev Event) {
	m.mu.RLock()
	set, ok := m.byChannel[ev.ChannelID]
	if !ok || len(set) == 0 {
		m.mu.RUnlock()
		return
	}
	// Snapshot the subscription pointers so we don't hold the manager
	// lock while interacting with subscription-level state.
	subs := make([]*Subscription, 0, len(set))
	for id := range set {
		if s, ok := m.subs[id]; ok {
			subs = append(subs, s)
		}
	}
	m.mu.RUnlock()

	matcherEvent := matcher.Event{
		Body:          ev.Body,
		FromAccountID: ev.From,
		Mentions:      ev.Mentions,
	}
	for _, s := range subs {
		if !s.Matcher.Match(matcherEvent) {
			continue
		}
		m.handleMatch(s, ev)
	}
}

// Event is what the server hands to Notify. Caller is responsible for
// providing the context history (typically Channel.RecentMessages(N)
// where N >= ContextLines on any subscription) and the @-mention list.
type Event struct {
	TenantID    uuid.UUID
	ChannelID   uuid.UUID
	ChannelName string
	From        uuid.UUID
	DisplayName string
	Body        string
	TS          time.Time
	Mentions    []uuid.UUID
	// Context is the channel history snapshot taken at the moment of
	// fan-out. Stored once and pulled by all matching subscriptions
	// according to their ContextLines setting.
	Context []channels.HistoryEntry
}

// handleMatch implements the per-subscription gating: cooldown -> budget
// -> debounce -> fire.
//
// Locking discipline: this function holds s.mu through the gating
// decisions, builds a fireTask if we should fire NOW, releases the lock,
// then enqueues the task on the worker pool. queueFire (the deferred
// path) replays the same drain-while-locked / enqueue-after-unlock
// pattern.
func (m *Manager) handleMatch(s *Subscription, ev Event) {
	var taskToEnqueue *fireTask

	s.mu.Lock()
	// Cooldown: drop if we fired too recently.
	if s.CooldownMs > 0 && !s.lastFire.IsZero() {
		if time.Since(s.lastFire) < time.Duration(s.CooldownMs)*time.Millisecond {
			s.mu.Unlock()
			return
		}
	}

	// Budget: count today's successful fires; bail if over.
	// (Storage call is fast for SQLite; for Postgres it's a sub-ms query.
	// We hold s.mu but NOT m.mu, so this doesn't block other subscriptions.)
	if s.Budget.DailyMaxFires > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		todayStart := time.Now().UTC().Truncate(24 * time.Hour)
		n, err := m.store.CountSubscriptionFiresSince(ctx, s.TenantID, s.ID, todayStart)
		cancel()
		if err == nil && n >= s.Budget.DailyMaxFires {
			s.mu.Unlock()
			_ = m.store.RecordSubscriptionFire(context.Background(), &storage.SubscriptionFire{
				TenantID: s.TenantID, SubscriptionID: s.ID,
				Status: "budget_exhausted", Attempts: 0,
			})
			return
		}
	}

	// Accumulate this match.
	msg := Message{
		From:        ev.From.String(),
		DisplayName: ev.DisplayName,
		TS:          ev.TS,
		Body:        ev.Body,
	}
	s.pendingMatches = append(s.pendingMatches, msg)
	s.pendingChanName = ev.ChannelName

	// Capture context on the FIRST match of a window so later matches
	// in the same window share the same surrounding context.
	if s.pendingContext == nil && s.CtxLines > 0 && len(ev.Context) > 0 {
		ctxMsgs := make([]Message, 0, s.CtxLines)
		take := s.CtxLines
		if take > len(ev.Context) {
			take = len(ev.Context)
		}
		for _, h := range ev.Context[len(ev.Context)-take:] {
			ctxMsgs = append(ctxMsgs, MessageFromHistory(h))
		}
		s.pendingContext = ctxMsgs
	}

	if s.debounceTimer != nil {
		// Active debounce window; the timer will fire on schedule.
		s.mu.Unlock()
		return
	}

	delay := time.Duration(s.DebounceMs) * time.Millisecond
	if delay > 0 {
		s.debounceTimer = time.AfterFunc(delay, func() { m.flushDebounced(s) })
		s.mu.Unlock()
		return
	}

	// No debounce: build the task in-line and enqueue after the unlock.
	taskToEnqueue = drainPendingLocked(s)
	s.mu.Unlock()

	if taskToEnqueue != nil {
		m.pool.enqueue(taskToEnqueue)
	}
}

// drainPendingLocked builds a fireTask from s.pending* and resets them.
// Must be called with s.mu HELD. Returns nil if there's nothing pending.
func drainPendingLocked(s *Subscription) *fireTask {
	if len(s.pendingMatches) == 0 {
		return nil
	}
	task := &fireTask{
		sub:         s,
		channelName: s.pendingChanName,
		matches:     s.pendingMatches,
		context:     s.pendingContext,
	}
	s.pendingMatches = nil
	s.pendingContext = nil
	s.pendingChanName = ""
	s.lastFire = time.Now()
	return task
}

func (m *Manager) flushDebounced(s *Subscription) {
	s.mu.Lock()
	s.debounceTimer = nil
	task := drainPendingLocked(s)
	s.mu.Unlock()
	if task != nil {
		m.pool.enqueue(task)
	}
}

// --- subscription compile ---

func compileSubscription(sub *storage.Subscription) (*Subscription, error) {
	mat, err := matcher.Compile(sub.MatchJSON)
	if err != nil {
		return nil, fmt.Errorf("compile match rules: %w", err)
	}
	var budget Budget
	if len(sub.BudgetJSON) > 0 {
		if err := json.Unmarshal(sub.BudgetJSON, &budget); err != nil {
			return nil, fmt.Errorf("parse budget: %w", err)
		}
	}
	if len(sub.Secret) == 0 {
		return nil, errors.New("subscription has empty secret")
	}
	return &Subscription{
		ID:         sub.ID,
		TenantID:   sub.TenantID,
		AccountID:  sub.AccountID,
		ChannelID:  sub.ChannelID,
		URL:        sub.URL,
		Secret:     sub.Secret,
		Matcher:    mat,
		Events:     sub.Events,
		CtxLines:   sub.ContextLines,
		DebounceMs: sub.DebounceMs,
		CooldownMs: sub.CooldownMs,
		Budget:     budget,
	}, nil
}
