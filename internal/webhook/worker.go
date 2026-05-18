package webhook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/biffsocko/prm/internal/storage"
)

// fireTask is one unit of work for the worker pool: deliver this batch
// of matches (with attached context) to this subscription's URL with
// retries.
type fireTask struct {
	sub         *Subscription
	channelName string
	matches     []Message
	context     []Message
	attempt     int
}

// workerPool is a fixed-size goroutine pool consuming fireTasks from a
// bounded channel. Spilling the channel drops the task and increments a
// metric (logged for now; surfaced via metrics endpoint later).
type workerPool struct {
	cfg Config
	mgr *Manager

	tasks   chan *fireTask
	wg      sync.WaitGroup
	dropped int64 // atomic
	started atomic.Bool
	stop    chan struct{}
}

func newWorkerPool(cfg Config, mgr *Manager) *workerPool {
	return &workerPool{
		cfg:   cfg,
		mgr:   mgr,
		tasks: make(chan *fireTask, cfg.QueueDepth),
		stop:  make(chan struct{}),
	}
}

func (p *workerPool) Start(ctx context.Context) {
	if !p.started.CompareAndSwap(false, true) {
		return
	}
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.run(ctx)
	}
}

func (p *workerPool) Stop() {
	select {
	case <-p.stop:
		return
	default:
	}
	close(p.stop)
	// Don't close tasks chan -- producers may still be calling enqueue
	// from concurrent message broadcasts. Workers exit on stop signal.
	p.wg.Wait()
}

func (p *workerPool) enqueue(task *fireTask) {
	select {
	case <-p.stop:
		// Shutting down; drop silently.
		return
	default:
	}
	select {
	case p.tasks <- task:
	default:
		atomic.AddInt64(&p.dropped, 1)
		p.mgr.log.Warn("webhook task dropped (queue full)",
			"subscription_id", task.sub.ID,
			"dropped_total", atomic.LoadInt64(&p.dropped))
	}
}

func (p *workerPool) run(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-p.stop:
			return
		case <-ctx.Done():
			return
		case task := <-p.tasks:
			p.deliver(ctx, task)
		}
	}
}

// deliver performs the HMAC-signed POST with retry policy:
//   - 2xx -> record OK fire, done
//   - 5xx / timeout / network error -> exponential backoff, up to MaxRetries
//   - 4xx -> drop without retry; increment consec4xx; auto-disable at threshold
func (p *workerPool) deliver(ctx context.Context, task *fireTask) {
	s := task.sub
	payload := &Payload{
		EventID:        NewEventID(),
		SubscriptionID: s.ID.String(),
		TenantID:       s.TenantID.String(),
		ChannelID:      s.ChannelID.String(),
		ChannelName:    task.channelName,
		TS:             time.Now().UTC(),
		Matches:        task.matches,
		Context:        task.context,
	}
	body, err := Marshal(payload)
	if err != nil {
		p.mgr.log.Error("payload marshal failed",
			"subscription_id", s.ID, "err", err)
		p.recordFire(s, "failed", 0, err.Error())
		return
	}

	var lastErr error
	for attempt := 1; attempt <= p.cfg.MaxRetries; attempt++ {
		statusCode, err := p.postOnce(ctx, s, body)
		if err == nil && statusCode >= 200 && statusCode < 300 {
			s.mu.Lock()
			s.consec4xx = 0
			s.mu.Unlock()
			p.recordFire(s, "ok", attempt, "")
			return
		}
		// 4xx: don't retry. Bump consec4xx; auto-disable at threshold.
		if err == nil && statusCode >= 400 && statusCode < 500 {
			s.mu.Lock()
			s.consec4xx++
			disable := s.consec4xx >= p.cfg.AutoDisable4xx
			s.mu.Unlock()
			p.recordFire(s, "dropped_4xx", attempt, fmt.Sprintf("HTTP %d", statusCode))
			if disable {
				p.autoDisable(s, fmt.Sprintf("%d consecutive 4xx responses", p.cfg.AutoDisable4xx))
			}
			return
		}
		// 5xx / network / timeout: retry with backoff.
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("HTTP %d", statusCode)
		}
		if attempt < p.cfg.MaxRetries {
			delay := time.Duration(1<<uint(attempt-1)) * 250 * time.Millisecond // 250ms, 500ms, 1s, ...
			if delay > 5*time.Second {
				delay = 5 * time.Second
			}
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				p.recordFire(s, "failed", attempt, "shutdown")
				return
			}
		}
	}
	p.recordFire(s, "failed", p.cfg.MaxRetries,
		fmt.Sprintf("last error: %v", lastErr))
}

// postOnce performs a single HTTP attempt; returns (statusCode, transportError).
// transportError is non-nil only when the request couldn't reach the server
// or timed out -- HTTP responses (even 5xx) come back as nil error +
// statusCode for the caller's retry logic to decide.
func (p *workerPool) postOnce(ctx context.Context, s *Subscription, body []byte) (int, error) {
	sig := Sign(body, s.Secret, time.Now().Unix())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("PRM-Signature", sig.Header())
	req.Header.Set("User-Agent", "prmd-webhook/0.1")

	resp, err := p.mgr.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	// Read+discard so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func (p *workerPool) recordFire(s *Subscription, status string, attempts int, lastErr string) {
	fire := &storage.SubscriptionFire{
		TenantID:       s.TenantID,
		SubscriptionID: s.ID,
		Status:         status,
		Attempts:       attempts,
		LastError:      lastErr,
	}
	// Use a short timeout; we don't want a slow DB to back up workers.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.mgr.store.RecordSubscriptionFire(ctx, fire); err != nil {
		p.mgr.log.Warn("record fire failed", "err", err)
	}
}

// autoDisable marks the subscription disabled in storage and drops it
// from the manager cache. Owners can re-enable via the REST control
// plane.
func (p *workerPool) autoDisable(s *Subscription, reason string) {
	p.mgr.log.Warn("auto-disabling subscription",
		"subscription_id", s.ID, "reason", reason)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Pull from storage, set DisabledAt, push back.
	sub, err := p.mgr.store.GetSubscriptionByID(ctx, s.TenantID, s.ID)
	if err != nil {
		// Subscription already gone; just drop from cache.
		p.mgr.Remove(s.ID)
		return
	}
	if sub.DisabledAt.IsZero() {
		sub.DisabledAt = time.Now().UTC()
		if err := p.mgr.store.UpdateSubscription(ctx, s.TenantID, sub); err != nil && !errors.Is(err, storage.ErrNotFound) {
			p.mgr.log.Warn("auto-disable update failed", "err", err)
		}
	}
	p.mgr.Remove(s.ID)
}
