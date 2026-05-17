package ha_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "modernc.org/sqlite" // unused-import shim for clarity; not used here

	"github.com/biffsocko/prm/internal/ha"
)

// TestPostgresElectorWithRealDB exercises the Postgres elector against a
// running Postgres. Skipped unless PRM_HA_PG_URL is set, e.g.:
//
//	PRM_HA_PG_URL="postgres://prm:prm@localhost:5432/prmtest?sslmode=disable" \
//	  go test ./internal/ha/...
//
// The test:
//   - Opens two electors against the same key on the same Postgres.
//   - First Acquire wins. Second blocks.
//   - First Releases. Second Acquires.
//   - Validates the leader-context cancellation contract on Release.
func TestPostgresElectorWithRealDB(t *testing.T) {
	dsn := os.Getenv("PRM_HA_PG_URL")
	if dsn == "" {
		t.Skip("set PRM_HA_PG_URL to run the Postgres HA integration test")
	}

	// NB: We don't have pgx loaded in this test (it's optional for the
	// project as a whole). When you wire this up, swap in pgx via:
	//   _ "github.com/jackc/pgx/v5/stdlib"
	//   db, err := sql.Open("pgx", dsn)
	// For now, this test is a documented skeleton.
	t.Skip("integration test skeleton -- needs pgx driver registered; see comment")

	const lockKey int64 = 0x70726D5F6861 // "prm_ha"

	db, err := sql.Open("pgx", dsn) //nolint:staticcheck // driver registration left to caller
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	connFn := func(ctx context.Context) (*sql.Conn, error) {
		return db.Conn(ctx)
	}

	el1 := ha.NewPostgres(connFn, lockKey)
	el2 := ha.NewPostgres(connFn, lockKey)

	leader1, err := el1.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if leader1.Err() != nil {
		t.Fatal("expected leader1 alive")
	}

	// el2.Acquire should block. Run it with a short timeout.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = el2.Acquire(ctx2)
	cancel2()
	if err == nil {
		t.Fatal("expected el2 to be blocked while el1 holds lock")
	}

	// Release el1; el2 should now succeed.
	if err := el1.Release(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-leader1.Done():
	case <-time.After(time.Second):
		t.Fatal("leader1 ctx should cancel on Release")
	}

	leader2, err := el2.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer el2.Release()
	if leader2.Err() != nil {
		t.Fatal("expected leader2 alive after el1 release")
	}
}
