package ha

import (
	"context"
	"sync"
)

// Local is an Elector that always considers itself the leader. Use for
// single-instance deployments (SQLite, dev, homelab) where there's no
// standby to elect against.
//
// Acquire returns a context that cancels only when Release is called or
// the parent context cancels.
type Local struct {
	mu     sync.Mutex
	cancel context.CancelFunc
}

// NewLocal constructs a Local elector.
func NewLocal() *Local { return &Local{} }

func (l *Local) Acquire(parent context.Context) (context.Context, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cancel != nil {
		// Already held; return the existing context.
		ctx, cancel := context.WithCancel(parent)
		old := l.cancel
		l.cancel = func() {
			old()
			cancel()
		}
		return ctx, nil
	}
	ctx, cancel := context.WithCancel(parent)
	l.cancel = cancel
	return ctx, nil
}

func (l *Local) Release() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cancel != nil {
		l.cancel()
		l.cancel = nil
	}
	return nil
}
