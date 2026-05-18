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

// ChannelVisibility controls who can join.
type ChannelVisibility string

const (
	// ChannelPrivate requires an explicit ACL entry to JOIN.
	ChannelPrivate ChannelVisibility = "private"
	// ChannelPublic lets any authenticated account in the tenant JOIN.
	ChannelPublic ChannelVisibility = "public"
)

// Channel is a persisted chat channel within a tenant. Channel.Name is
// human-readable and unique per tenant. Channel.ID is opaque and stable.
type Channel struct {
	ID         uuid.UUID
	TenantID   uuid.UUID
	Name       string
	OwnerID    uuid.UUID
	Visibility ChannelVisibility
	CreatedAt  time.Time
}

// ChannelRole controls what an account can do in a channel.
type ChannelRole string

const (
	RoleOwner  ChannelRole = "owner"
	RoleAdmin  ChannelRole = "admin"
	RoleMember ChannelRole = "member"
	RoleBanned ChannelRole = "banned"
)

// CanJoin reports whether the role permits joining the channel.
func (r ChannelRole) CanJoin() bool {
	return r == RoleOwner || r == RoleAdmin || r == RoleMember
}

// ChannelACLEntry is one row of a channel's access control list.
type ChannelACLEntry struct {
	TenantID  uuid.UUID
	ChannelID uuid.UUID
	AccountID uuid.UUID
	Role      ChannelRole
	GrantedAt time.Time
	GrantedBy uuid.UUID // account who issued the grant; zero for owner self-grant on create
}

// Subscription is a webhook subscription owned by a bot account. The
// match rules and budget are stored as JSON; the package matcher
// (internal/matcher) compiles them.
//
// Note on Secret: this is the plaintext HMAC signing secret, not a
// hash. The server needs the plaintext to sign outbound webhook POSTs;
// the bot's webhook receiver needs it to verify. It's generated at
// subscription creation and returned to the caller exactly once; the
// caller is responsible for stashing it in their bot's config. Stored
// at rest in the DB — if you need confidentiality beyond filesystem /
// DB ACL, encrypt the column at the storage layer (out of scope for
// slice 3).
type Subscription struct {
	ID           uuid.UUID
	TenantID     uuid.UUID
	AccountID    uuid.UUID // owner (a bot account)
	ChannelID    uuid.UUID
	URL          string
	Secret       []byte   // plaintext HMAC signing secret (see note above)
	MatchJSON    []byte   // serialized match rules; opaque to storage
	Events       []string // e.g. ["message"]
	ContextLines int
	DebounceMs   int
	CooldownMs   int
	BudgetJSON   []byte // serialized Budget; opaque to storage
	DisabledAt   time.Time
	CreatedAt    time.Time
}

// SubscriptionFire is one webhook attempt — for budget accounting and
// failure auditing. One row per fire (not per attempt) — Attempts counts
// retries.
type SubscriptionFire struct {
	ID             uuid.UUID
	TenantID       uuid.UUID
	SubscriptionID uuid.UUID
	FiredAt        time.Time
	Status         string // "ok" | "retrying" | "failed" | "dropped_4xx" | "budget_exhausted"
	Attempts       int
	LastError      string
}

// Token is an API token issued to a bot account. The plaintext token is
// shown to the user exactly once at issuance; only the SHA-256 hash is
// stored. Lookup is by hash (the server hashes the bearer token on auth
// and looks it up).
type Token struct {
	ID         uuid.UUID
	TenantID   uuid.UUID
	AccountID  uuid.UUID
	Hash       []byte    // SHA-256 of the plaintext token
	Label      string    // optional, human-readable
	CreatedAt  time.Time
	LastUsedAt time.Time // updated opportunistically; not transactional
	RevokedAt  time.Time // zero = active
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

	// Channels
	CreateChannel(ctx context.Context, tenantID uuid.UUID, c *Channel) error
	GetChannelByID(ctx context.Context, tenantID, id uuid.UUID) (*Channel, error)
	GetChannelByName(ctx context.Context, tenantID uuid.UUID, name string) (*Channel, error)
	ListChannels(ctx context.Context, tenantID uuid.UUID) ([]*Channel, error)

	// Channel ACLs
	SetChannelACL(ctx context.Context, tenantID, channelID, accountID uuid.UUID, role ChannelRole, grantedBy uuid.UUID) error
	GetChannelACL(ctx context.Context, tenantID, channelID, accountID uuid.UUID) (*ChannelACLEntry, error)
	ListChannelACL(ctx context.Context, tenantID, channelID uuid.UUID) ([]*ChannelACLEntry, error)
	RemoveChannelACL(ctx context.Context, tenantID, channelID, accountID uuid.UUID) error

	// Tokens (bot API tokens)
	CreateToken(ctx context.Context, tenantID, accountID uuid.UUID, hash []byte, label string) (*Token, error)
	GetTokenByHash(ctx context.Context, hash []byte) (*Token, error)
	ListTokens(ctx context.Context, tenantID, accountID uuid.UUID) ([]*Token, error)
	RevokeToken(ctx context.Context, tenantID, tokenID uuid.UUID) error
	TouchTokenLastUsed(ctx context.Context, tokenID uuid.UUID) error

	// Subscriptions (webhook subscriptions)
	CreateSubscription(ctx context.Context, tenantID uuid.UUID, sub *Subscription) error
	GetSubscriptionByID(ctx context.Context, tenantID, id uuid.UUID) (*Subscription, error)
	ListSubscriptionsByAccount(ctx context.Context, tenantID, accountID uuid.UUID) ([]*Subscription, error)
	ListSubscriptionsByChannel(ctx context.Context, tenantID, channelID uuid.UUID) ([]*Subscription, error)
	UpdateSubscription(ctx context.Context, tenantID uuid.UUID, sub *Subscription) error
	DeleteSubscription(ctx context.Context, tenantID, id uuid.UUID) error

	// Subscription fires (for budget accounting + audit)
	RecordSubscriptionFire(ctx context.Context, fire *SubscriptionFire) error
	CountSubscriptionFiresSince(ctx context.Context, tenantID, subID uuid.UUID, since time.Time) (int, error)
}
