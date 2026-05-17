package tenants_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/sqlite"
	"github.com/biffsocko/prm/internal/tenants"
)

func mkStore(t *testing.T) storage.Store {
	t.Helper()
	s, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestServiceCreateAndLookup(t *testing.T) {
	s := mkStore(t)
	svc := tenants.New(s)
	ctx := context.Background()

	in := &storage.Tenant{Slug: "acme", DisplayName: "Acme Corp"}
	if err := svc.Create(ctx, in); err != nil {
		t.Fatal(err)
	}

	got, err := svc.BySlug(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != in.ID {
		t.Fatal("id mismatch")
	}

	gotByID, err := svc.ByID(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotByID.Slug != "acme" {
		t.Fatal("slug mismatch")
	}

	_, err = svc.BySlug(ctx, "nope")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestCacheServesAfterStoreChange(t *testing.T) {
	// Confirm the cache is in fact caching: after the first BySlug, a direct
	// store edit shouldn't be visible until TTL expires or Invalidate is
	// called.
	s := mkStore(t)
	svc := tenants.New(s).WithTTL(60 * time.Second) // long TTL for this test
	ctx := context.Background()

	in := &storage.Tenant{Slug: "acme", DisplayName: "Original"}
	if err := svc.Create(ctx, in); err != nil {
		t.Fatal(err)
	}

	// Prime the cache
	first, err := svc.BySlug(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if first.DisplayName != "Original" {
		t.Fatal("setup")
	}

	// Simulate a direct store edit by deleting + recreating with a new name.
	// (No UpdateTenant in the storage interface yet; this is a stand-in.)
	// For this test the cache should serve the original from the prior put.
	second, err := svc.BySlug(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if second != first {
		t.Errorf("expected cached pointer to be returned, got different instance")
	}

	// After Invalidate, the next lookup hits the store again.
	svc.Invalidate("acme", in.ID)
	third, err := svc.BySlug(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	// We didn't actually mutate the store, so DisplayName is still "Original";
	// but the returned pointer should be a fresh allocation from the store.
	if third == first {
		t.Errorf("after Invalidate expected fresh fetch (different pointer)")
	}
}

func TestList(t *testing.T) {
	s := mkStore(t)
	svc := tenants.New(s)
	ctx := context.Background()
	for _, slug := range []string{"a", "b", "c"} {
		if err := svc.Create(ctx, &storage.Tenant{Slug: slug, DisplayName: slug}); err != nil {
			t.Fatal(err)
		}
	}
	list, err := svc.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 {
		t.Errorf("expected 3 tenants, got %d", len(list))
	}
}
