package webhook

import (
	"context"
	"errors"
	"fmt"
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

// deliver performs the HMAC-signed publish via the transport selected
// by the subscription URL's scheme. Retry semantics are shared across
// transports:
//   - DeliveryOK        -> record + stop retrying
//   - DeliveryTransient -> exponential backoff, up to MaxRetries
//   - DeliveryPermanent -> drop without retry; bump consec4xx;
//                          auto-disable at the configured threshold.
//
// "consec4xx" is a historical name carried forward from the HTTP-only
// era; it now counts permanent failures from any transport
// (HTTP 4xx, AMQP "no route", MQTT auth refused, etc.) -- same
// behavior, more transports.
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

	scheme := schemeOf(s.URL)
	tr, ok := p.mgr.transports.for_(scheme)
	if !ok {
		p.mgr.log.Error("no transport for subscription scheme",
			"subscription_id", s.ID, "scheme", scheme)
		p.recordFire(s, "failed", 0, fmt.Sprintf("no transport for scheme %q", scheme))
		return
	}
	target := Target{URL: s.URL, SubscriptionID: s.ID.String()}

	var lastDetail string
	for attempt := 1; attempt <= p.cfg.MaxRetries; attempt++ {
		sig := Sign(body, s.Secret, time.Now().Unix())
		res := tr.Send(ctx, target, body, sig)
		switch res.Kind {
		case DeliveryOK:
			s.mu.Lock()
			s.consec4xx = 0
			s.mu.Unlock()
			p.recordFire(s, "ok", attempt, res.StatusDetail)
			return
		case DeliveryPermanent:
			s.mu.Lock()
			s.consec4xx++
			disable := s.consec4xx >= p.cfg.AutoDisable4xx
			s.mu.Unlock()
			detail := res.StatusDetail
			if res.Err != nil {
				detail = fmt.Sprintf("%s: %v", detail, res.Err)
			}
			p.recordFire(s, "dropped_4xx", attempt, detail)
			if disable {
				p.autoDisable(s, fmt.Sprintf("%d consecutive permanent failures", p.cfg.AutoDisable4xx))
			}
			return
		case DeliveryTransient:
			lastDetail = res.StatusDetail
			if res.Err != nil {
				lastDetail = fmt.Sprintf("%s: %v", lastDetail, res.Err)
			}
			if attempt < p.cfg.MaxRetries {
				delay := time.Duration(1<<uint(attempt-1)) * 250 * time.Millisecond
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
	}
	p.recordFire(s, "failed", p.cfg.MaxRetries, "last error: "+lastDetail)
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
