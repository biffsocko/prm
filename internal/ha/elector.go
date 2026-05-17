// Package ha implements the leader election used by the slice 2 hot-standby
// HA topology. Two prmd processes run pointed at the same Postgres
// primary; whichever holds the advisory lock serves traffic. The other
// sits idle waiting on its own lock attempt. On failure the standby
// acquires, an L4 load balancer flips, and clients reconnect.
//
// This package contains:
//
//   - Elector interface: Acquire blocks until the lock is held, then
//     returns a context that cancels if the lock is lost. Release gives
//     it up voluntarily.
//   - Local: a single-instance Elector that always considers itself the
//     leader. Use this for SQLite deployments and tests.
//   - Postgres: an Elector that takes a pg_try_advisory_lock on a
//     well-known key and heartbeats with SELECT 1 over a dedicated
//     connection. Requires a real Postgres to exercise; the connection-
//     dependent code path is covered by an integration test that skips
//     when PRM_HA_PG_URL is unset.
package ha

import (
	"context"
	"errors"
	"time"
)

// Elector is the leader-election interface.
//
// Lifecycle:
//
//	ctx, err := el.Acquire(parent) // blocks until leader
//	defer el.Release()
//	// while ctx.Err() == nil, this process is the leader. Use this ctx
//	// for any operation that should be cancelled on leader loss.
//	<-ctx.Done()  // leader role lost; failover or shutdown
type Elector interface {
	// Acquire blocks until this process holds the leader lock. Returns
	// a context that cancels if the lock is lost (e.g., the underlying
	// Postgres connection drops, or the lock is forcibly released).
	// The parent context is honored; cancelling it returns
	// context.Canceled before the lock is acquired.
	Acquire(ctx context.Context) (context.Context, error)

	// Release voluntarily gives up the lock. Idempotent; safe to call
	// from cleanup paths even if Acquire was never called.
	Release() error
}

// ErrLockLost is returned from a leader context's Err() (via context
// cancellation cause) when the leader lock is lost.
var ErrLockLost = errors.New("ha: leader lock lost")

// HealthCheckInterval is how often the Postgres elector heartbeats the
// connection that holds the advisory lock. Lower values shorten the
// detection window for a dead connection but cost an extra round-trip
// per interval.
const HealthCheckInterval = 2 * time.Second

// AcquireRetryInterval is how often the Postgres elector retries
// pg_try_advisory_lock while waiting for the current leader to release.
const AcquireRetryInterval = 1 * time.Second
