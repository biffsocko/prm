// Package tenants is a small read-mostly cache over storage.Store for
// tenant lookups, plus convenience helpers for platform-admin operations
// (create tenant, suspend, list).
//
// Lookups by slug happen on every authenticated connection; caching them
// avoids hammering the database on the connection path. The cache TTL is
// short by default (60 seconds) so that admin changes propagate quickly
// without an explicit invalidation channel.
package tenants

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/biffsocko/prm/internal/storage"
)

// DefaultTTL is the default cache TTL for tenant lookups.
const DefaultTTL = 60 * time.Second

// Service is a read-mostly cache + tenant operations.
type Service struct {
	store storage.Store
	ttl   time.Duration

	mu       sync.RWMutex
	bySlug   map[string]cacheEntry
	byID     map[uuid.UUID]cacheEntry
}

type cacheEntry struct {
	tenant *storage.Tenant
	expiry time.Time
}

// New constructs a Service backed by the given store.
func New(store storage.Store) *Service {
	return &Service{
		store:  store,
		ttl:    DefaultTTL,
		bySlug: make(map[string]cacheEntry),
		byID:   make(map[uuid.UUID]cacheEntry),
	}
}

// WithTTL sets a custom cache TTL. Mainly for tests.
func (s *Service) WithTTL(ttl time.Duration) *Service {
	s.ttl = ttl
	return s
}

// BySlug returns the tenant with the given slug, hitting the cache when
// possible. Returns storage.ErrNotFound when the tenant doesn't exist.
func (s *Service) BySlug(ctx context.Context, slug string) (*storage.Tenant, error) {
	s.mu.RLock()
	if e, ok := s.bySlug[slug]; ok && time.Now().Before(e.expiry) {
		s.mu.RUnlock()
		return e.tenant, nil
	}
	s.mu.RUnlock()

	t, err := s.store.GetTenantBySlug(ctx, slug)
	if err != nil {
		return nil, err
	}
	s.put(t)
	return t, nil
}

// ByID returns the tenant with the given ID, hitting the cache when possible.
func (s *Service) ByID(ctx context.Context, id uuid.UUID) (*storage.Tenant, error) {
	s.mu.RLock()
	if e, ok := s.byID[id]; ok && time.Now().Before(e.expiry) {
		s.mu.RUnlock()
		return e.tenant, nil
	}
	s.mu.RUnlock()

	t, err := s.store.GetTenantByID(ctx, id)
	if err != nil {
		return nil, err
	}
	s.put(t)
	return t, nil
}

// Create inserts a new tenant and primes the cache.
func (s *Service) Create(ctx context.Context, t *storage.Tenant) error {
	if err := s.store.CreateTenant(ctx, t); err != nil {
		return err
	}
	s.put(t)
	return nil
}

// List returns all tenants. Bypasses the cache (intended for admin use).
func (s *Service) List(ctx context.Context) ([]*storage.Tenant, error) {
	return s.store.ListTenants(ctx)
}

// Invalidate drops the cached entries for a tenant. Call after a mutation
// from outside this Service (e.g., direct store edit). For mutations
// performed via Create() this is unnecessary.
func (s *Service) Invalidate(slug string, id uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.bySlug, slug)
	delete(s.byID, id)
}

func (s *Service) put(t *storage.Tenant) {
	if t == nil {
		return
	}
	exp := time.Now().Add(s.ttl)
	s.mu.Lock()
	s.bySlug[t.Slug] = cacheEntry{tenant: t, expiry: exp}
	s.byID[t.ID] = cacheEntry{tenant: t, expiry: exp}
	s.mu.Unlock()
}
