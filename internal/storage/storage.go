// Package storage defines the durable-state interface for PRM and the
// model types that flow through it.
//
// Every domain function takes a tenantID as a leading argument. This is the
// single most important architectural rule in PRM: there is no
// GetAccountByID(id), only GetAccountByID(tenantID, id). A missing tenant
// scope is a cross-tenant data leak. Treat it as a security bug.
//
// Concrete backends live in subpackages: storage/sqlite and storage/postgres.
package storage

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Sentinel errors. Backends translate driver-specific errors into these so
// callers can switch on a stable error vocabulary.
var (
	ErrNotFound      = errors.New("storage: not found")
	ErrAlreadyExists = errors.New("storage: already exists")
	ErrInvalid       = errors.New("storage: invalid argument")
)

// Tenant is an organization / workspace. It is the top-level isolation
// boundary. Every other model belongs to exactly one Tenant.
type Tenant struct {
	ID          uuid.UUID
	Slug        string // URL-safe; used in auth and integration URLs
	DisplayName string
	Settings    map[string]any // quotas, rate limits, per-tenant feature flags
	Status      TenantStatus
	CreatedAt   time.Time
}

// TenantStatus controls whether the tenant accepts new connections.
type TenantStatus string

const (
	TenantActive    TenantStatus = "active"
	TenantSuspended TenantStatus = "suspended"
)

// AccountType distinguishes humans from bots.
type AccountType string

const (
	AccountHuman AccountType = "human"
	AccountBot   AccountType = "bot"
)

// Account is a user (or bot) within a tenant. account.Username is the
// login handle (used in AuthRequest); account.DisplayName is what other
// users see and is editable. Two accounts in different tenants can share a
// username; uniqueness is per-tenant.
type Account struct {
	ID             uuid.UUID
	TenantID       uuid.UUID
	Username       string
	DisplayName    string
	Type           AccountType
	PasswordHash   []byte // Argon2id output bytes
	PasswordSalt   []byte // per-account salt
	PasswordParams string // "argon2id,m=65536,t=3,p=1"
	RecoveryEmail  string // optional
	CreatedAt      time.Time
}

// Store is the durable-state interface. Implementations live in
// storage/sqlite and storage/postgres.
//
// Hard rule: every domain query takes tenantID as a leading argument.
// Tenant-management functions are the only exception (they operate on the
// tenant table itself).
type Store interface {
	// Lifecycle
	Migrate(ctx context.Context) error
	Close() error

	// Tenants — the one place that doesn't take a tenantID arg, because
	// tenants are the boundary.
	CreateTenant(ctx context.Context, t *Tenant) error
	GetTenantByID(ctx context.Context, id uuid.UUID) (*Tenant, error)
	GetTenantBySlug(ctx context.Context, slug string) (*Tenant, error)
	ListTenants(ctx context.Context) ([]*Tenant, error)

	// Accounts
	CreateAccount(ctx context.Context, tenantID uuid.UUID, a *Account) error
	GetAccountByID(ctx context.Context, tenantID, id uuid.UUID) (*Account, error)
	GetAccountByUsername(ctx context.Context, tenantID uuid.UUID, username string) (*Account, error)
}
