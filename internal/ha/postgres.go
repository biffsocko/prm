package ha

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Postgres is an Elector backed by a Postgres advisory lock. Per the
// design:
//
//   - One dedicated connection (NOT from the application's pool) holds
//     the lock for the entire leader tenure. Postgres releases an
//     advisory lock when its session closes, so the connection IS the
//     lease.
//   - Acquire polls pg_try_advisory_lock(lockKey) every
//     AcquireRetryInterval until it succeeds or the parent ctx cancels.
//   - Once acquired, a goroutine heartbeats SELECT 1 over the same
//     connection every HealthCheckInterval. On heartbeat failure, the
//     returned context cancels with ErrLockLost.
//   - Release closes the dedicated connection (which drops the lock
//     server-side) and cancels the leader context.
//
// The advisory lock key is a single 64-bit integer chosen at construction
// time. Different deployments should use the same key; different lock
// scopes (e.g., per-tenant) would use different keys, but slice 2 has a
// single global leader.
//
// Status: this file compiles cleanly; full integration tests require a
// running Postgres reachable via PRM_HA_PG_URL. See postgres_test.go
// for the skipping test.
type Postgres struct {
	// connFn returns a fresh *sql.Conn dedicated to lock holding.
	// Caller-supplied so tests can inject; in production this comes from
	// the storage's *sql.DB.Conn(ctx).
	connFn func(context.Context) (*sql.Conn, error)
	key    int64

	mu     sync.Mutex
	held   *sql.Conn          // the lock-holding connection
	cancel context.CancelFunc // cancels the leader context
}

// NewPostgres constructs a Postgres elector.
//
// connFn must return a fresh connection NOT pooled with application
// queries -- advisory locks are session-scoped, so any other query on
// the same connection's session would interact with the lock state.
// In production this is typically built from a small dedicated
// *sql.DB pointed at the same Postgres.
func NewPostgres(connFn func(context.Context) (*sql.Conn, error), lockKey int64) *Postgres {
	return &Postgres{connFn: connFn, key: lockKey}
}

func (p *Postgres) Acquire(parent context.Context) (context.Context, error) {
	conn, err := p.connFn(parent)
	if err != nil {
		return nil, fmt.Errorf("ha postgres: acquire dedicated conn: %w", err)
	}

	for {
		select {
		case <-parent.Done():
			_ = conn.Close()
			return nil, parent.Err()
		default:
		}
		var got bool
		if err := conn.QueryRowContext(parent, `SELECT pg_try_advisory_lock($1)`, p.key).Scan(&got); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("ha postgres: try_advisory_lock: %w", err)
		}
		if got {
			break
		}
		// Wait and retry.
		select {
		case <-parent.Done():
			_ = conn.Close()
			return nil, parent.Err()
		case <-time.After(AcquireRetryInterval):
		}
	}

	// We hold the lock. Stash the conn + start the heartbeat goroutine.
	ctx, cancel := context.WithCancelCause(parent)
	p.mu.Lock()
	p.held = conn
	p.cancel = func() { cancel(ErrLockLost) }
	p.mu.Unlock()

	go p.heartbeat(parent, ctx, conn)

	return ctx, nil
}

func (p *Postgres) heartbeat(parent context.Context, leader context.Context, conn *sql.Conn) {
	t := time.NewTicker(HealthCheckInterval)
	defer t.Stop()
	for {
		select {
		case <-leader.Done():
			return
		case <-parent.Done():
			p.Release() //nolint:errcheck // shutdown path
			return
		case <-t.C:
			if err := conn.PingContext(parent); err != nil {
				// Connection is dead; advisory lock has been released
				// server-side already.
				p.mu.Lock()
				if p.cancel != nil {
					p.cancel()
				}
				p.mu.Unlock()
				return
			}
		}
	}
}

func (p *Postgres) Release() error {
	p.mu.Lock()
	conn := p.held
	cancel := p.cancel
	p.held = nil
	p.cancel = nil
	p.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if conn == nil {
		return nil
	}
	// Best-effort explicit unlock; closing the conn would release it
	// anyway, but pg_advisory_unlock makes intent explicit and lets us
	// detect already-released locks.
	var ok bool
	if err := conn.QueryRowContext(context.Background(), `SELECT pg_advisory_unlock($1)`, p.key).Scan(&ok); err != nil && !errors.Is(err, sql.ErrConnDone) {
		// Connection issues here are non-fatal -- we're shutting down.
		_ = conn.Close()
		return nil
	}
	return conn.Close()
}
