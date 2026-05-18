// Package sqlite is the SQLite-backed implementation of storage.Store.
// Suitable for tiny / single-tenant / homelab deployments and as the
// default backend for local development. Production deployments should use
// the postgres backend.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite" // CGO-less SQLite driver

	"github.com/biffsocko/prm/internal/storage"
)

// Store is the SQLite implementation of storage.Store.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) a SQLite database at the given DSN.
// The DSN is the path (or file:... URL) accepted by modernc.org/sqlite.
// The caller should call Migrate(ctx) before using the store.
//
// Connection-pool size is pinned to 1. SQLite serializes writes anyway,
// and for in-memory databases (":memory:") database/sql will otherwise
// give each pooled connection its own private database -- the first
// connection sees the schema, others see nothing. One connection is also
// simpler to reason about under WAL mode for our scale; if you need
// higher concurrency, use the postgres backend.
func Open(dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn+"?_pragma=journal_mode(WAL)&_pragma=foreign_keys(on)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite ping: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.db.Close() }

// Migrate runs the schema migrations to bring the database up to date.
// Idempotent.
func (s *Store) Migrate(ctx context.Context) error {
	for i, stmt := range migrations {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlite migration %d: %w", i, err)
		}
	}
	return nil
}

// migrations are applied in order on every Migrate() call. Each statement
// must be idempotent (use IF NOT EXISTS).
var migrations = []string{
	`CREATE TABLE IF NOT EXISTS tenants (
		id          TEXT PRIMARY KEY,
		slug        TEXT NOT NULL UNIQUE,
		display_name TEXT NOT NULL,
		settings    TEXT NOT NULL DEFAULT '{}',
		status      TEXT NOT NULL DEFAULT 'active',
		created_at  INTEGER NOT NULL
	) STRICT`,
	`CREATE INDEX IF NOT EXISTS tenants_slug_idx ON tenants(slug)`,
	`CREATE TABLE IF NOT EXISTS accounts (
		id              TEXT PRIMARY KEY,
		tenant_id       TEXT NOT NULL,
		username        TEXT NOT NULL,
		display_name    TEXT NOT NULL,
		type            TEXT NOT NULL,
		password_hash   BLOB NOT NULL,
		password_salt   BLOB NOT NULL,
		password_params TEXT NOT NULL,
		recovery_email  TEXT NOT NULL DEFAULT '',
		created_at      INTEGER NOT NULL,
		UNIQUE(tenant_id, username),
		FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
	) STRICT`,
	`CREATE INDEX IF NOT EXISTS accounts_tenant_username_idx ON accounts(tenant_id, username)`,
	`CREATE TABLE IF NOT EXISTS channels (
		id          TEXT PRIMARY KEY,
		tenant_id   TEXT NOT NULL,
		name        TEXT NOT NULL,
		owner_id    TEXT NOT NULL,
		visibility  TEXT NOT NULL DEFAULT 'private',
		created_at  INTEGER NOT NULL,
		UNIQUE(tenant_id, name),
		FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
	) STRICT`,
	`CREATE INDEX IF NOT EXISTS channels_tenant_name_idx ON channels(tenant_id, name)`,
	`CREATE TABLE IF NOT EXISTS channel_acl (
		tenant_id   TEXT NOT NULL,
		channel_id  TEXT NOT NULL,
		account_id  TEXT NOT NULL,
		role        TEXT NOT NULL,
		granted_at  INTEGER NOT NULL,
		granted_by  TEXT NOT NULL DEFAULT '',
		PRIMARY KEY(tenant_id, channel_id, account_id),
		FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
	) STRICT`,
	`CREATE TABLE IF NOT EXISTS tokens (
		id           TEXT PRIMARY KEY,
		tenant_id    TEXT NOT NULL,
		account_id   TEXT NOT NULL,
		hash         BLOB NOT NULL UNIQUE,
		label        TEXT NOT NULL DEFAULT '',
		created_at   INTEGER NOT NULL,
		last_used_at INTEGER NOT NULL DEFAULT 0,
		revoked_at   INTEGER NOT NULL DEFAULT 0,
		FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
	) STRICT`,
	`CREATE INDEX IF NOT EXISTS tokens_hash_idx ON tokens(hash)`,
	`CREATE INDEX IF NOT EXISTS tokens_account_idx ON tokens(tenant_id, account_id)`,
	`CREATE TABLE IF NOT EXISTS subscriptions (
		id            TEXT PRIMARY KEY,
		tenant_id     TEXT NOT NULL,
		account_id    TEXT NOT NULL,
		channel_id    TEXT NOT NULL,
		url           TEXT NOT NULL,
		secret        BLOB NOT NULL,
		match_json    BLOB NOT NULL,
		events_json   TEXT NOT NULL DEFAULT '["message"]',
		context_lines INTEGER NOT NULL DEFAULT 0,
		debounce_ms   INTEGER NOT NULL DEFAULT 0,
		cooldown_ms   INTEGER NOT NULL DEFAULT 0,
		budget_json   BLOB NOT NULL DEFAULT '{}',
		disabled_at   INTEGER NOT NULL DEFAULT 0,
		created_at    INTEGER NOT NULL,
		FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
	) STRICT`,
	`CREATE INDEX IF NOT EXISTS subscriptions_tenant_channel_idx ON subscriptions(tenant_id, channel_id) WHERE disabled_at = 0`,
	`CREATE INDEX IF NOT EXISTS subscriptions_tenant_account_idx ON subscriptions(tenant_id, account_id)`,
	`CREATE TABLE IF NOT EXISTS subscription_fires (
		id              TEXT PRIMARY KEY,
		tenant_id       TEXT NOT NULL,
		subscription_id TEXT NOT NULL,
		fired_at        INTEGER NOT NULL,
		status          TEXT NOT NULL,
		attempts        INTEGER NOT NULL DEFAULT 0,
		last_error      TEXT NOT NULL DEFAULT '',
		FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
	) STRICT`,
	`CREATE INDEX IF NOT EXISTS subscription_fires_subid_fired_idx ON subscription_fires(tenant_id, subscription_id, fired_at)`,
	`CREATE TABLE IF NOT EXISTS integrations (
		id            TEXT PRIMARY KEY,
		tenant_id     TEXT NOT NULL,
		channel_id    TEXT NOT NULL,
		account_id    TEXT NOT NULL,
		adapter       TEXT NOT NULL,
		token_hash    BLOB NOT NULL UNIQUE,
		settings_json BLOB NOT NULL DEFAULT '{}',
		disabled_at   INTEGER NOT NULL DEFAULT 0,
		created_at    INTEGER NOT NULL,
		FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
	) STRICT`,
	`CREATE INDEX IF NOT EXISTS integrations_token_idx ON integrations(token_hash)`,
	`CREATE INDEX IF NOT EXISTS integrations_tenant_idx ON integrations(tenant_id)`,
}

// CreateTenant inserts a new tenant. If t.ID is zero, a UUID v7 is generated.
// If t.CreatedAt is zero, the current time is used. The Status field defaults
// to TenantActive.
func (s *Store) CreateTenant(ctx context.Context, t *storage.Tenant) error {
	if t.ID == uuid.Nil {
		var err error
		t.ID, err = uuid.NewV7()
		if err != nil {
			return fmt.Errorf("sqlite: generate tenant id: %w", err)
		}
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now().UTC()
	}
	if t.Status == "" {
		t.Status = storage.TenantActive
	}
	if t.Slug == "" || t.DisplayName == "" {
		return fmt.Errorf("%w: slug and display_name are required", storage.ErrInvalid)
	}
	settingsJSON, err := marshalSettings(t.Settings)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tenants (id, slug, display_name, settings, status, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		t.ID.String(), t.Slug, t.DisplayName, settingsJSON, string(t.Status), t.CreatedAt.UnixMicro(),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: tenant slug %q", storage.ErrAlreadyExists, t.Slug)
		}
		return fmt.Errorf("sqlite create tenant: %w", err)
	}
	return nil
}

// GetTenantByID returns the tenant with the given ID or ErrNotFound.
func (s *Store) GetTenantByID(ctx context.Context, id uuid.UUID) (*storage.Tenant, error) {
	return s.scanTenant(s.db.QueryRowContext(ctx,
		`SELECT id, slug, display_name, settings, status, created_at FROM tenants WHERE id = ?`,
		id.String()))
}

// GetTenantBySlug returns the tenant with the given slug or ErrNotFound.
func (s *Store) GetTenantBySlug(ctx context.Context, slug string) (*storage.Tenant, error) {
	return s.scanTenant(s.db.QueryRowContext(ctx,
		`SELECT id, slug, display_name, settings, status, created_at FROM tenants WHERE slug = ?`,
		slug))
}

// ListTenants returns all tenants. Intended for admin use; do not call from
// per-request paths.
func (s *Store) ListTenants(ctx context.Context) ([]*storage.Tenant, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, slug, display_name, settings, status, created_at FROM tenants ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("sqlite list tenants: %w", err)
	}
	defer rows.Close()
	var out []*storage.Tenant
	for rows.Next() {
		t, err := s.scanTenantRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// CreateAccount inserts a new account in the given tenant. tenantID is
// passed explicitly even if a.TenantID is set; mismatched IDs return
// ErrInvalid (catches a class of cross-tenant bugs at the API boundary).
func (s *Store) CreateAccount(ctx context.Context, tenantID uuid.UUID, a *storage.Account) error {
	if tenantID == uuid.Nil {
		return fmt.Errorf("%w: tenantID required", storage.ErrInvalid)
	}
	if a.TenantID != uuid.Nil && a.TenantID != tenantID {
		return fmt.Errorf("%w: account.TenantID does not match argument tenantID", storage.ErrInvalid)
	}
	a.TenantID = tenantID
	if a.ID == uuid.Nil {
		var err error
		a.ID, err = uuid.NewV7()
		if err != nil {
			return fmt.Errorf("sqlite: generate account id: %w", err)
		}
	}
	if a.CreatedAt.IsZero() {
		a.CreatedAt = time.Now().UTC()
	}
	if a.Username == "" {
		return fmt.Errorf("%w: username is required", storage.ErrInvalid)
	}
	if a.DisplayName == "" {
		a.DisplayName = a.Username
	}
	if a.Type == "" {
		a.Type = storage.AccountHuman
	}
	if len(a.PasswordHash) == 0 || len(a.PasswordSalt) == 0 || a.PasswordParams == "" {
		return fmt.Errorf("%w: password fields are required", storage.ErrInvalid)
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO accounts (id, tenant_id, username, display_name, type, password_hash, password_salt, password_params, recovery_email, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.ID.String(), tenantID.String(), a.Username, a.DisplayName, string(a.Type),
		a.PasswordHash, a.PasswordSalt, a.PasswordParams, a.RecoveryEmail, a.CreatedAt.UnixMicro(),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: account %q in tenant %s", storage.ErrAlreadyExists, a.Username, tenantID)
		}
		return fmt.Errorf("sqlite create account: %w", err)
	}
	return nil
}

// GetAccountByID returns the account with the given ID within the tenant
// or ErrNotFound. An account from a different tenant returns ErrNotFound;
// this is the multi-tenant isolation guarantee at the storage layer.
func (s *Store) GetAccountByID(ctx context.Context, tenantID, id uuid.UUID) (*storage.Account, error) {
	return s.scanAccount(s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, username, display_name, type, password_hash, password_salt, password_params, recovery_email, created_at
		 FROM accounts WHERE tenant_id = ? AND id = ?`,
		tenantID.String(), id.String()))
}

// GetAccountByUsername returns the account with the given username within
// the tenant or ErrNotFound.
func (s *Store) GetAccountByUsername(ctx context.Context, tenantID uuid.UUID, username string) (*storage.Account, error) {
	return s.scanAccount(s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, username, display_name, type, password_hash, password_salt, password_params, recovery_email, created_at
		 FROM accounts WHERE tenant_id = ? AND username = ?`,
		tenantID.String(), username))
}

// --- channels ---

func (s *Store) CreateChannel(ctx context.Context, tenantID uuid.UUID, c *storage.Channel) error {
	if tenantID == uuid.Nil {
		return fmt.Errorf("%w: tenantID required", storage.ErrInvalid)
	}
	if c.TenantID != uuid.Nil && c.TenantID != tenantID {
		return fmt.Errorf("%w: channel.TenantID does not match argument tenantID", storage.ErrInvalid)
	}
	c.TenantID = tenantID
	if c.ID == uuid.Nil {
		var err error
		c.ID, err = uuid.NewV7()
		if err != nil {
			return fmt.Errorf("sqlite: generate channel id: %w", err)
		}
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	if c.Name == "" {
		return fmt.Errorf("%w: channel name is required", storage.ErrInvalid)
	}
	if c.OwnerID == uuid.Nil {
		return fmt.Errorf("%w: channel owner_id is required", storage.ErrInvalid)
	}
	if c.Visibility == "" {
		c.Visibility = storage.ChannelPrivate
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO channels (id, tenant_id, name, owner_id, visibility, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		c.ID.String(), tenantID.String(), c.Name, c.OwnerID.String(), string(c.Visibility), c.CreatedAt.UnixMicro(),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: channel %q in tenant %s", storage.ErrAlreadyExists, c.Name, tenantID)
		}
		return fmt.Errorf("sqlite create channel: %w", err)
	}
	return nil
}

func (s *Store) GetChannelByID(ctx context.Context, tenantID, id uuid.UUID) (*storage.Channel, error) {
	return s.scanChannel(s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, name, owner_id, visibility, created_at FROM channels WHERE tenant_id = ? AND id = ?`,
		tenantID.String(), id.String()))
}

func (s *Store) GetChannelByName(ctx context.Context, tenantID uuid.UUID, name string) (*storage.Channel, error) {
	return s.scanChannel(s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, name, owner_id, visibility, created_at FROM channels WHERE tenant_id = ? AND name = ?`,
		tenantID.String(), name))
}

func (s *Store) ListChannels(ctx context.Context, tenantID uuid.UUID) ([]*storage.Channel, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, name, owner_id, visibility, created_at FROM channels WHERE tenant_id = ? ORDER BY name`,
		tenantID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite list channels: %w", err)
	}
	defer rows.Close()
	var out []*storage.Channel
	for rows.Next() {
		c, err := s.scanChannel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// --- channel ACL ---

func (s *Store) SetChannelACL(ctx context.Context, tenantID, channelID, accountID uuid.UUID, role storage.ChannelRole, grantedBy uuid.UUID) error {
	if tenantID == uuid.Nil || channelID == uuid.Nil || accountID == uuid.Nil {
		return fmt.Errorf("%w: tenantID, channelID, accountID required", storage.ErrInvalid)
	}
	if role == "" {
		return fmt.Errorf("%w: role required", storage.ErrInvalid)
	}
	grantedByStr := ""
	if grantedBy != uuid.Nil {
		grantedByStr = grantedBy.String()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO channel_acl (tenant_id, channel_id, account_id, role, granted_at, granted_by)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(tenant_id, channel_id, account_id) DO UPDATE SET role = excluded.role, granted_at = excluded.granted_at, granted_by = excluded.granted_by`,
		tenantID.String(), channelID.String(), accountID.String(), string(role), time.Now().UTC().UnixMicro(), grantedByStr,
	)
	if err != nil {
		return fmt.Errorf("sqlite set channel acl: %w", err)
	}
	return nil
}

func (s *Store) GetChannelACL(ctx context.Context, tenantID, channelID, accountID uuid.UUID) (*storage.ChannelACLEntry, error) {
	return s.scanChannelACL(s.db.QueryRowContext(ctx,
		`SELECT tenant_id, channel_id, account_id, role, granted_at, granted_by FROM channel_acl WHERE tenant_id = ? AND channel_id = ? AND account_id = ?`,
		tenantID.String(), channelID.String(), accountID.String()))
}

func (s *Store) ListChannelACL(ctx context.Context, tenantID, channelID uuid.UUID) ([]*storage.ChannelACLEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT tenant_id, channel_id, account_id, role, granted_at, granted_by FROM channel_acl WHERE tenant_id = ? AND channel_id = ? ORDER BY granted_at`,
		tenantID.String(), channelID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite list channel acl: %w", err)
	}
	defer rows.Close()
	var out []*storage.ChannelACLEntry
	for rows.Next() {
		e, err := s.scanChannelACL(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) RemoveChannelACL(ctx context.Context, tenantID, channelID, accountID uuid.UUID) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM channel_acl WHERE tenant_id = ? AND channel_id = ? AND account_id = ?`,
		tenantID.String(), channelID.String(), accountID.String())
	if err != nil {
		return fmt.Errorf("sqlite remove channel acl: %w", err)
	}
	return nil
}

// --- tokens ---

func (s *Store) CreateToken(ctx context.Context, tenantID, accountID uuid.UUID, hash []byte, label string) (*storage.Token, error) {
	if tenantID == uuid.Nil || accountID == uuid.Nil {
		return nil, fmt.Errorf("%w: tenantID, accountID required", storage.ErrInvalid)
	}
	if len(hash) == 0 {
		return nil, fmt.Errorf("%w: hash required", storage.ErrInvalid)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("sqlite: generate token id: %w", err)
	}
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tokens (id, tenant_id, account_id, hash, label, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		id.String(), tenantID.String(), accountID.String(), hash, label, now.UnixMicro(),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, fmt.Errorf("%w: token hash collision", storage.ErrAlreadyExists)
		}
		return nil, fmt.Errorf("sqlite create token: %w", err)
	}
	return &storage.Token{
		ID: id, TenantID: tenantID, AccountID: accountID,
		Hash: hash, Label: label, CreatedAt: now,
	}, nil
}

// GetTokenByHash intentionally has no tenantID arg — the token IS proof of
// tenancy. The result carries the tenant_id, which the caller then uses
// for all subsequent scoped queries. Returns ErrNotFound if the token
// doesn't exist or has been revoked.
func (s *Store) GetTokenByHash(ctx context.Context, hash []byte) (*storage.Token, error) {
	return s.scanToken(s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, account_id, hash, label, created_at, last_used_at, revoked_at
		 FROM tokens WHERE hash = ? AND revoked_at = 0`, hash))
}

func (s *Store) ListTokens(ctx context.Context, tenantID, accountID uuid.UUID) ([]*storage.Token, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, account_id, hash, label, created_at, last_used_at, revoked_at
		 FROM tokens WHERE tenant_id = ? AND account_id = ? ORDER BY created_at`,
		tenantID.String(), accountID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite list tokens: %w", err)
	}
	defer rows.Close()
	var out []*storage.Token
	for rows.Next() {
		t, err := s.scanToken(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) RevokeToken(ctx context.Context, tenantID, tokenID uuid.UUID) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE tokens SET revoked_at = ? WHERE tenant_id = ? AND id = ? AND revoked_at = 0`,
		time.Now().UTC().UnixMicro(), tenantID.String(), tokenID.String())
	if err != nil {
		return fmt.Errorf("sqlite revoke token: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return storage.ErrNotFound
	}
	return nil
}

func (s *Store) TouchTokenLastUsed(ctx context.Context, tokenID uuid.UUID) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE tokens SET last_used_at = ? WHERE id = ?`,
		time.Now().UTC().UnixMicro(), tokenID.String())
	if err != nil {
		return fmt.Errorf("sqlite touch token: %w", err)
	}
	return nil
}

// --- subscriptions ---

func (s *Store) CreateSubscription(ctx context.Context, tenantID uuid.UUID, sub *storage.Subscription) error {
	if tenantID == uuid.Nil {
		return fmt.Errorf("%w: tenantID required", storage.ErrInvalid)
	}
	if sub.TenantID != uuid.Nil && sub.TenantID != tenantID {
		return fmt.Errorf("%w: subscription.TenantID does not match argument tenantID", storage.ErrInvalid)
	}
	sub.TenantID = tenantID
	if sub.ID == uuid.Nil {
		var err error
		sub.ID, err = uuid.NewV7()
		if err != nil {
			return fmt.Errorf("sqlite: generate subscription id: %w", err)
		}
	}
	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = time.Now().UTC()
	}
	if sub.URL == "" {
		return fmt.Errorf("%w: subscription URL required", storage.ErrInvalid)
	}
	if len(sub.MatchJSON) == 0 {
		return fmt.Errorf("%w: subscription match rules required", storage.ErrInvalid)
	}
	if len(sub.Events) == 0 {
		sub.Events = []string{"message"}
	}
	eventsJSON, err := json.Marshal(sub.Events)
	if err != nil {
		return fmt.Errorf("sqlite: marshal events: %w", err)
	}
	budgetJSON := sub.BudgetJSON
	if len(budgetJSON) == 0 {
		budgetJSON = []byte("{}")
	}
	disabledAt := int64(0)
	if !sub.DisabledAt.IsZero() {
		disabledAt = sub.DisabledAt.UnixMicro()
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO subscriptions (id, tenant_id, account_id, channel_id, url, secret, match_json, events_json, context_lines, debounce_ms, cooldown_ms, budget_json, disabled_at, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		sub.ID.String(), tenantID.String(), sub.AccountID.String(), sub.ChannelID.String(),
		sub.URL, sub.Secret, sub.MatchJSON, string(eventsJSON),
		sub.ContextLines, sub.DebounceMs, sub.CooldownMs, budgetJSON,
		disabledAt, sub.CreatedAt.UnixMicro(),
	)
	if err != nil {
		return fmt.Errorf("sqlite create subscription: %w", err)
	}
	return nil
}

func (s *Store) GetSubscriptionByID(ctx context.Context, tenantID, id uuid.UUID) (*storage.Subscription, error) {
	return s.scanSubscription(s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, account_id, channel_id, url, secret, match_json, events_json, context_lines, debounce_ms, cooldown_ms, budget_json, disabled_at, created_at
		 FROM subscriptions WHERE tenant_id = ? AND id = ?`,
		tenantID.String(), id.String()))
}

func (s *Store) ListSubscriptionsByAccount(ctx context.Context, tenantID, accountID uuid.UUID) ([]*storage.Subscription, error) {
	return s.querySubscriptions(ctx,
		`SELECT id, tenant_id, account_id, channel_id, url, secret, match_json, events_json, context_lines, debounce_ms, cooldown_ms, budget_json, disabled_at, created_at
		 FROM subscriptions WHERE tenant_id = ? AND account_id = ? ORDER BY created_at`,
		tenantID.String(), accountID.String())
}

func (s *Store) ListSubscriptionsByChannel(ctx context.Context, tenantID, channelID uuid.UUID) ([]*storage.Subscription, error) {
	return s.querySubscriptions(ctx,
		`SELECT id, tenant_id, account_id, channel_id, url, secret, match_json, events_json, context_lines, debounce_ms, cooldown_ms, budget_json, disabled_at, created_at
		 FROM subscriptions WHERE tenant_id = ? AND channel_id = ? AND disabled_at = 0 ORDER BY created_at`,
		tenantID.String(), channelID.String())
}

func (s *Store) UpdateSubscription(ctx context.Context, tenantID uuid.UUID, sub *storage.Subscription) error {
	if tenantID == uuid.Nil || sub.ID == uuid.Nil {
		return fmt.Errorf("%w: tenantID, subscription.ID required", storage.ErrInvalid)
	}
	eventsJSON, err := json.Marshal(sub.Events)
	if err != nil {
		return fmt.Errorf("sqlite: marshal events: %w", err)
	}
	budgetJSON := sub.BudgetJSON
	if len(budgetJSON) == 0 {
		budgetJSON = []byte("{}")
	}
	disabledAt := int64(0)
	if !sub.DisabledAt.IsZero() {
		disabledAt = sub.DisabledAt.UnixMicro()
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE subscriptions SET url = ?, match_json = ?, events_json = ?, context_lines = ?, debounce_ms = ?, cooldown_ms = ?, budget_json = ?, disabled_at = ?
		 WHERE tenant_id = ? AND id = ?`,
		sub.URL, sub.MatchJSON, string(eventsJSON), sub.ContextLines, sub.DebounceMs, sub.CooldownMs, budgetJSON, disabledAt,
		tenantID.String(), sub.ID.String())
	if err != nil {
		return fmt.Errorf("sqlite update subscription: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return storage.ErrNotFound
	}
	return nil
}

func (s *Store) DeleteSubscription(ctx context.Context, tenantID, id uuid.UUID) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM subscriptions WHERE tenant_id = ? AND id = ?`,
		tenantID.String(), id.String())
	if err != nil {
		return fmt.Errorf("sqlite delete subscription: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return storage.ErrNotFound
	}
	return nil
}

func (s *Store) RecordSubscriptionFire(ctx context.Context, f *storage.SubscriptionFire) error {
	if f.ID == uuid.Nil {
		var err error
		f.ID, err = uuid.NewV7()
		if err != nil {
			return fmt.Errorf("sqlite: generate fire id: %w", err)
		}
	}
	if f.FiredAt.IsZero() {
		f.FiredAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO subscription_fires (id, tenant_id, subscription_id, fired_at, status, attempts, last_error) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		f.ID.String(), f.TenantID.String(), f.SubscriptionID.String(),
		f.FiredAt.UnixMicro(), f.Status, f.Attempts, f.LastError)
	if err != nil {
		return fmt.Errorf("sqlite record fire: %w", err)
	}
	return nil
}

func (s *Store) CountSubscriptionFiresSince(ctx context.Context, tenantID, subID uuid.UUID, since time.Time) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM subscription_fires WHERE tenant_id = ? AND subscription_id = ? AND fired_at >= ? AND status IN ('ok','retrying')`,
		tenantID.String(), subID.String(), since.UnixMicro()).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("sqlite count fires: %w", err)
	}
	return n, nil
}

func (s *Store) querySubscriptions(ctx context.Context, query string, args ...any) ([]*storage.Subscription, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite query subscriptions: %w", err)
	}
	defer rows.Close()
	var out []*storage.Subscription
	for rows.Next() {
		sub, err := s.scanSubscription(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

func (s *Store) scanSubscription(r scanner) (*storage.Subscription, error) {
	var (
		id, tenantID, accountID, channelID, url, eventsJSON string
		secret, matchJSON, budgetJSON                   []byte
		contextLines, debounceMs, cooldownMs                int
		disabledAt, createdAt                               int64
	)
	if err := r.Scan(&id, &tenantID, &accountID, &channelID, &url, &secret, &matchJSON, &eventsJSON, &contextLines, &debounceMs, &cooldownMs, &budgetJSON, &disabledAt, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("sqlite scan subscription: %w", err)
	}
	subID, _ := uuid.Parse(id)
	tid, _ := uuid.Parse(tenantID)
	aid, _ := uuid.Parse(accountID)
	cid, _ := uuid.Parse(channelID)
	var events []string
	_ = json.Unmarshal([]byte(eventsJSON), &events)
	sub := &storage.Subscription{
		ID: subID, TenantID: tid, AccountID: aid, ChannelID: cid,
		URL: url, Secret: secret, MatchJSON: matchJSON,
		Events: events, ContextLines: contextLines,
		DebounceMs: debounceMs, CooldownMs: cooldownMs,
		BudgetJSON: budgetJSON,
		CreatedAt:  time.UnixMicro(createdAt).UTC(),
	}
	if disabledAt > 0 {
		sub.DisabledAt = time.UnixMicro(disabledAt).UTC()
	}
	return sub, nil
}

// --- integrations ---

func (s *Store) CreateIntegration(ctx context.Context, tenantID uuid.UUID, integ *storage.Integration) error {
	if tenantID == uuid.Nil {
		return fmt.Errorf("%w: tenantID required", storage.ErrInvalid)
	}
	if integ.TenantID != uuid.Nil && integ.TenantID != tenantID {
		return fmt.Errorf("%w: integration.TenantID does not match argument tenantID", storage.ErrInvalid)
	}
	integ.TenantID = tenantID
	if integ.ID == uuid.Nil {
		var err error
		integ.ID, err = uuid.NewV7()
		if err != nil {
			return fmt.Errorf("sqlite: generate integration id: %w", err)
		}
	}
	if integ.CreatedAt.IsZero() {
		integ.CreatedAt = time.Now().UTC()
	}
	if integ.Adapter == "" {
		return fmt.Errorf("%w: adapter required", storage.ErrInvalid)
	}
	if len(integ.TokenHash) == 0 {
		return fmt.Errorf("%w: token_hash required", storage.ErrInvalid)
	}
	if integ.ChannelID == uuid.Nil || integ.AccountID == uuid.Nil {
		return fmt.Errorf("%w: channel_id and account_id required", storage.ErrInvalid)
	}
	settings := integ.SettingsJSON
	if len(settings) == 0 {
		settings = []byte("{}")
	}
	disabledAt := int64(0)
	if !integ.DisabledAt.IsZero() {
		disabledAt = integ.DisabledAt.UnixMicro()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO integrations (id, tenant_id, channel_id, account_id, adapter, token_hash, settings_json, disabled_at, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		integ.ID.String(), tenantID.String(), integ.ChannelID.String(), integ.AccountID.String(),
		integ.Adapter, integ.TokenHash, settings, disabledAt, integ.CreatedAt.UnixMicro(),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: integration token hash collision", storage.ErrAlreadyExists)
		}
		return fmt.Errorf("sqlite create integration: %w", err)
	}
	return nil
}

func (s *Store) GetIntegrationByID(ctx context.Context, tenantID, id uuid.UUID) (*storage.Integration, error) {
	return s.scanIntegration(s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, channel_id, account_id, adapter, token_hash, settings_json, disabled_at, created_at
		 FROM integrations WHERE tenant_id = ? AND id = ?`,
		tenantID.String(), id.String()))
}

// GetIntegrationByTokenHash is intentionally tenant-less. The token IS
// the proof of tenancy -- the caller hashed an inbound bearer token and
// is asking "which integration is this?". The result carries tenant_id.
// Returns ErrNotFound if not found or disabled.
func (s *Store) GetIntegrationByTokenHash(ctx context.Context, hash []byte) (*storage.Integration, error) {
	return s.scanIntegration(s.db.QueryRowContext(ctx,
		`SELECT id, tenant_id, channel_id, account_id, adapter, token_hash, settings_json, disabled_at, created_at
		 FROM integrations WHERE token_hash = ? AND disabled_at = 0`, hash))
}

func (s *Store) ListIntegrationsByTenant(ctx context.Context, tenantID uuid.UUID) ([]*storage.Integration, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, tenant_id, channel_id, account_id, adapter, token_hash, settings_json, disabled_at, created_at
		 FROM integrations WHERE tenant_id = ? ORDER BY created_at`,
		tenantID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite list integrations: %w", err)
	}
	defer rows.Close()
	var out []*storage.Integration
	for rows.Next() {
		i, err := s.scanIntegration(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

func (s *Store) UpdateIntegration(ctx context.Context, tenantID uuid.UUID, integ *storage.Integration) error {
	if tenantID == uuid.Nil || integ.ID == uuid.Nil {
		return fmt.Errorf("%w: tenantID and integration.ID required", storage.ErrInvalid)
	}
	settings := integ.SettingsJSON
	if len(settings) == 0 {
		settings = []byte("{}")
	}
	disabledAt := int64(0)
	if !integ.DisabledAt.IsZero() {
		disabledAt = integ.DisabledAt.UnixMicro()
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE integrations SET channel_id = ?, account_id = ?, adapter = ?, settings_json = ?, disabled_at = ?
		 WHERE tenant_id = ? AND id = ?`,
		integ.ChannelID.String(), integ.AccountID.String(), integ.Adapter, settings, disabledAt,
		tenantID.String(), integ.ID.String())
	if err != nil {
		return fmt.Errorf("sqlite update integration: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return storage.ErrNotFound
	}
	return nil
}

func (s *Store) DeleteIntegration(ctx context.Context, tenantID, id uuid.UUID) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM integrations WHERE tenant_id = ? AND id = ?`,
		tenantID.String(), id.String())
	if err != nil {
		return fmt.Errorf("sqlite delete integration: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return storage.ErrNotFound
	}
	return nil
}

func (s *Store) scanIntegration(r scanner) (*storage.Integration, error) {
	var (
		id, tenantID, channelID, accountID, adapter string
		tokenHash, settingsJSON                     []byte
		disabledAt, createdAt                       int64
	)
	if err := r.Scan(&id, &tenantID, &channelID, &accountID, &adapter, &tokenHash, &settingsJSON, &disabledAt, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("sqlite scan integration: %w", err)
	}
	iid, _ := uuid.Parse(id)
	tid, _ := uuid.Parse(tenantID)
	cid, _ := uuid.Parse(channelID)
	aid, _ := uuid.Parse(accountID)
	out := &storage.Integration{
		ID: iid, TenantID: tid, ChannelID: cid, AccountID: aid,
		Adapter: adapter, TokenHash: tokenHash, SettingsJSON: settingsJSON,
		CreatedAt: time.UnixMicro(createdAt).UTC(),
	}
	if disabledAt > 0 {
		out.DisabledAt = time.UnixMicro(disabledAt).UTC()
	}
	return out, nil
}

// --- helpers ---

type scanner interface {
	Scan(dest ...any) error
}

func (s *Store) scanTenant(r scanner) (*storage.Tenant, error) {
	var (
		id, slug, displayName, settingsJSON, status string
		createdAtMicros                             int64
	)
	if err := r.Scan(&id, &slug, &displayName, &settingsJSON, &status, &createdAtMicros); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("sqlite scan tenant: %w", err)
	}
	tenantID, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("sqlite scan tenant: parse id: %w", err)
	}
	settings, err := unmarshalSettings(settingsJSON)
	if err != nil {
		return nil, err
	}
	return &storage.Tenant{
		ID:          tenantID,
		Slug:        slug,
		DisplayName: displayName,
		Settings:    settings,
		Status:      storage.TenantStatus(status),
		CreatedAt:   time.UnixMicro(createdAtMicros).UTC(),
	}, nil
}

func (s *Store) scanTenantRow(r interface{ Scan(...any) error }) (*storage.Tenant, error) {
	return s.scanTenant(r)
}

func (s *Store) scanAccount(r scanner) (*storage.Account, error) {
	var (
		id, tenantID, username, displayName, accountType, params, email string
		hash, salt                                                       []byte
		createdAtMicros                                                  int64
	)
	if err := r.Scan(&id, &tenantID, &username, &displayName, &accountType, &hash, &salt, &params, &email, &createdAtMicros); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("sqlite scan account: %w", err)
	}
	accID, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("sqlite scan account: parse id: %w", err)
	}
	tenID, err := uuid.Parse(tenantID)
	if err != nil {
		return nil, fmt.Errorf("sqlite scan account: parse tenant id: %w", err)
	}
	return &storage.Account{
		ID:             accID,
		TenantID:       tenID,
		Username:       username,
		DisplayName:    displayName,
		Type:           storage.AccountType(accountType),
		PasswordHash:   hash,
		PasswordSalt:   salt,
		PasswordParams: params,
		RecoveryEmail:  email,
		CreatedAt:      time.UnixMicro(createdAtMicros).UTC(),
	}, nil
}

func (s *Store) scanChannel(r scanner) (*storage.Channel, error) {
	var (
		id, tenantID, name, ownerID, visibility string
		createdAtMicros                          int64
	)
	if err := r.Scan(&id, &tenantID, &name, &ownerID, &visibility, &createdAtMicros); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("sqlite scan channel: %w", err)
	}
	cid, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("sqlite scan channel: parse id: %w", err)
	}
	tid, err := uuid.Parse(tenantID)
	if err != nil {
		return nil, fmt.Errorf("sqlite scan channel: parse tenant id: %w", err)
	}
	oid, err := uuid.Parse(ownerID)
	if err != nil {
		return nil, fmt.Errorf("sqlite scan channel: parse owner id: %w", err)
	}
	return &storage.Channel{
		ID: cid, TenantID: tid, Name: name, OwnerID: oid,
		Visibility: storage.ChannelVisibility(visibility),
		CreatedAt:  time.UnixMicro(createdAtMicros).UTC(),
	}, nil
}

func (s *Store) scanChannelACL(r scanner) (*storage.ChannelACLEntry, error) {
	var (
		tenantID, channelID, accountID, role, grantedBy string
		grantedAtMicros                                  int64
	)
	if err := r.Scan(&tenantID, &channelID, &accountID, &role, &grantedAtMicros, &grantedBy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("sqlite scan channel_acl: %w", err)
	}
	tid, _ := uuid.Parse(tenantID)
	cid, _ := uuid.Parse(channelID)
	aid, _ := uuid.Parse(accountID)
	var gb uuid.UUID
	if grantedBy != "" {
		gb, _ = uuid.Parse(grantedBy)
	}
	return &storage.ChannelACLEntry{
		TenantID: tid, ChannelID: cid, AccountID: aid,
		Role:      storage.ChannelRole(role),
		GrantedAt: time.UnixMicro(grantedAtMicros).UTC(),
		GrantedBy: gb,
	}, nil
}

func (s *Store) scanToken(r scanner) (*storage.Token, error) {
	var (
		id, tenantID, accountID, label string
		hash                            []byte
		createdAt, lastUsed, revoked    int64
	)
	if err := r.Scan(&id, &tenantID, &accountID, &hash, &label, &createdAt, &lastUsed, &revoked); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("sqlite scan token: %w", err)
	}
	tokID, _ := uuid.Parse(id)
	tid, _ := uuid.Parse(tenantID)
	aid, _ := uuid.Parse(accountID)
	tok := &storage.Token{
		ID: tokID, TenantID: tid, AccountID: aid,
		Hash: hash, Label: label,
		CreatedAt: time.UnixMicro(createdAt).UTC(),
	}
	if lastUsed > 0 {
		tok.LastUsedAt = time.UnixMicro(lastUsed).UTC()
	}
	if revoked > 0 {
		tok.RevokedAt = time.UnixMicro(revoked).UTC()
	}
	return tok, nil
}

func marshalSettings(s map[string]any) (string, error) {
	if s == nil {
		return "{}", nil
	}
	b, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("sqlite: marshal settings: %w", err)
	}
	return string(b), nil
}

func unmarshalSettings(s string) (map[string]any, error) {
	if s == "" {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, fmt.Errorf("sqlite: unmarshal settings: %w", err)
	}
	if out == nil {
		out = map[string]any{}
	}
	return out, nil
}

// isUniqueViolation detects modernc.org/sqlite's unique-constraint error
// without depending on its internal error types.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return containsAny(msg,
		"UNIQUE constraint failed",
		"constraint failed: UNIQUE",
	)
}

func containsAny(s string, needles ...string) bool {
	for _, n := range needles {
		if len(s) >= len(n) {
			for i := 0; i <= len(s)-len(n); i++ {
				if s[i:i+len(n)] == n {
					return true
				}
			}
		}
	}
	return false
}
