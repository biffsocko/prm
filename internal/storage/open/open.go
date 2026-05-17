// Package open returns the right storage backend for a given URL.
// Kept separate from internal/storage so the storage subpackages
// (sqlite, postgres) can import the interface without creating a cycle.
package open

import (
	"fmt"
	"strings"

	"github.com/biffsocko/prm/internal/storage"
	"github.com/biffsocko/prm/internal/storage/postgres"
	"github.com/biffsocko/prm/internal/storage/sqlite"
)

// Store returns a backend implementation of storage.Store for the given URL.
//
//	postgres://user:pass@host:5432/db?sslmode=... -> Postgres
//	sqlite:./prm.db                                -> SQLite file
//	sqlite:file:./prm.db?cache=shared              -> SQLite (file URL form)
//
// Empty url defaults to in-memory SQLite (intended for tests).
func Store(url string) (storage.Store, error) {
	switch {
	case url == "":
		return sqlite.Open(":memory:")
	case strings.HasPrefix(url, "sqlite:"):
		return sqlite.Open(strings.TrimPrefix(url, "sqlite:"))
	case strings.HasPrefix(url, "postgres://") || strings.HasPrefix(url, "postgresql://"):
		return postgres.Open(url)
	default:
		return nil, fmt.Errorf("storage open: unrecognized URL scheme (expected sqlite: or postgres://): %q", url)
	}
}
