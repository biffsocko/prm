// Package postgres is the PostgreSQL-backed implementation of storage.Store.
// This is the documented primary backend for production deployments — for
// multi-tenancy at scale, streaming-replication HA (Tier 2 of the redundancy
// plan), and operationally-mature schema migrations.
//
// Status: stub. The interface is implemented but each method returns
// ErrNotImplemented. Full implementation needs a running Postgres to
// exercise against and is queued for the next pass. The SQLite backend is
// fully functional for slice 1 local development.
package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/storage"
)

// ErrNotImplemented is returned by every method on the stub Store until the
// real implementation lands.
var ErrNotImplemented = errors.New("postgres backend: not implemented yet (use --storage sqlite:... for now)")

// Store is the Postgres implementation of storage.Store (stub).
type Store struct{}

// Open prepares a connection pool to the given Postgres DSN. Currently a
// stub that returns ErrNotImplemented when any method is called; the
// connection itself is not yet established.
func Open(dsn string) (*Store, error) {
	_ = dsn // kept for the future-real impl
	return &Store{}, nil
}

func (*Store) Close() error                                                  { return nil }
func (*Store) Migrate(ctx context.Context) error                             { return ErrNotImplemented }
func (*Store) CreateTenant(ctx context.Context, t *storage.Tenant) error     { return ErrNotImplemented }
func (*Store) GetTenantByID(context.Context, uuid.UUID) (*storage.Tenant, error) {
	return nil, ErrNotImplemented
}
func (*Store) GetTenantBySlug(context.Context, string) (*storage.Tenant, error) {
	return nil, ErrNotImplemented
}
func (*Store) ListTenants(context.Context) ([]*storage.Tenant, error) {
	return nil, ErrNotImplemented
}
func (*Store) CreateAccount(context.Context, uuid.UUID, *storage.Account) error {
	return ErrNotImplemented
}
func (*Store) GetAccountByID(context.Context, uuid.UUID, uuid.UUID) (*storage.Account, error) {
	return nil, ErrNotImplemented
}
func (*Store) GetAccountByUsername(context.Context, uuid.UUID, string) (*storage.Account, error) {
	return nil, ErrNotImplemented
}
func (*Store) CreateChannel(context.Context, uuid.UUID, *storage.Channel) error { return ErrNotImplemented }
func (*Store) GetChannelByID(context.Context, uuid.UUID, uuid.UUID) (*storage.Channel, error) {
	return nil, ErrNotImplemented
}
func (*Store) GetChannelByName(context.Context, uuid.UUID, string) (*storage.Channel, error) {
	return nil, ErrNotImplemented
}
func (*Store) ListChannels(context.Context, uuid.UUID) ([]*storage.Channel, error) {
	return nil, ErrNotImplemented
}
func (*Store) SetChannelACL(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, storage.ChannelRole, uuid.UUID) error {
	return ErrNotImplemented
}
func (*Store) GetChannelACL(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) (*storage.ChannelACLEntry, error) {
	return nil, ErrNotImplemented
}
func (*Store) ListChannelACL(context.Context, uuid.UUID, uuid.UUID) ([]*storage.ChannelACLEntry, error) {
	return nil, ErrNotImplemented
}
func (*Store) RemoveChannelACL(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) error {
	return ErrNotImplemented
}
func (*Store) CreateToken(context.Context, uuid.UUID, uuid.UUID, []byte, string) (*storage.Token, error) {
	return nil, ErrNotImplemented
}
func (*Store) GetTokenByHash(context.Context, []byte) (*storage.Token, error) {
	return nil, ErrNotImplemented
}
func (*Store) ListTokens(context.Context, uuid.UUID, uuid.UUID) ([]*storage.Token, error) {
	return nil, ErrNotImplemented
}
func (*Store) RevokeToken(context.Context, uuid.UUID, uuid.UUID) error { return ErrNotImplemented }
func (*Store) TouchTokenLastUsed(context.Context, uuid.UUID) error      { return ErrNotImplemented }
func (*Store) CreateSubscription(context.Context, uuid.UUID, *storage.Subscription) error {
	return ErrNotImplemented
}
func (*Store) GetSubscriptionByID(context.Context, uuid.UUID, uuid.UUID) (*storage.Subscription, error) {
	return nil, ErrNotImplemented
}
func (*Store) ListSubscriptionsByAccount(context.Context, uuid.UUID, uuid.UUID) ([]*storage.Subscription, error) {
	return nil, ErrNotImplemented
}
func (*Store) ListSubscriptionsByChannel(context.Context, uuid.UUID, uuid.UUID) ([]*storage.Subscription, error) {
	return nil, ErrNotImplemented
}
func (*Store) UpdateSubscription(context.Context, uuid.UUID, *storage.Subscription) error {
	return ErrNotImplemented
}
func (*Store) DeleteSubscription(context.Context, uuid.UUID, uuid.UUID) error {
	return ErrNotImplemented
}
func (*Store) RecordSubscriptionFire(context.Context, *storage.SubscriptionFire) error {
	return ErrNotImplemented
}
func (*Store) CountSubscriptionFiresSince(context.Context, uuid.UUID, uuid.UUID, time.Time) (int, error) {
	return 0, ErrNotImplemented
}
