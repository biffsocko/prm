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
func Open(dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn+"?_pragma=journal_mode(WAL)&_pragma=foreign_keys(on)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
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
