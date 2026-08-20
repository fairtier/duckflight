//go:build duckdb_arrow

package engine

import (
	"context"
	"testing"

	"github.com/fairtier/duckflight/internal/config"
)

// TestReleaseDoesNotLeakTransaction is the regression test for the connection
// lifecycle bug this design exists to prevent: a client that opens a
// transaction and goes away must not hand the next borrower a connection that
// is still inside it. If it did, the next BEGIN would be skipped as redundant
// and the two clients would share one transaction — the second one's COMMIT
// committing the first one's uncommitted writes.
func TestReleaseDoesNotLeakTransaction(t *testing.T) {
	ctx := context.Background()
	eng, err := NewEngine(&config.Config{MemoryLimit: "256MB", MaxThreads: 2, PoolSize: 1})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer func() { _ = eng.Close() }()

	if err := eng.ExecSQL(ctx, "seed", "CREATE TABLE leak_probe (id INTEGER)"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ac, err := eng.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	// Exactly what a client's raw `BEGIN` does, classification included.
	if intent, _ := ac.ClassifyStatement(ctx, "BEGIN TRANSACTION"); intent != TxnIntentBegin {
		t.Fatalf("expected TxnIntentBegin, got %v", intent)
	}
	if _, err := ac.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if _, err := ac.ExecContext(ctx, "INSERT INTO leak_probe VALUES (1)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	// The client vanishes here, mid-transaction.
	eng.Pool.Release(ac)

	next, err := eng.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}
	defer eng.Pool.Release(next)

	inTxn, err := next.InExplicitTransaction(ctx)
	if err != nil {
		t.Fatalf("InExplicitTransaction: %v", err)
	}
	if inTxn {
		t.Fatal("connection was pooled while still inside a transaction")
	}

	// The abandoned INSERT must have been rolled back, not left pending for
	// the next client to commit.
	rdr, err := next.Arrow.QueryContext(ctx, "SELECT count(*) FROM leak_probe")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	defer rdr.Release()
	if !rdr.Next() {
		t.Fatal("no count result")
	}
	rec := rdr.RecordBatch()
	if got := rec.Column(0).ValueStr(0); got != "0" {
		t.Fatalf("uncommitted row survived the connection handoff: count = %s", got)
	}
}

// TestReleaseRecyclesConnectionLocalState covers the other half: temp tables
// and setting overrides must not follow a connection to its next borrower.
func TestReleaseRecyclesConnectionLocalState(t *testing.T) {
	ctx := context.Background()
	eng, err := NewEngine(&config.Config{MemoryLimit: "256MB", MaxThreads: 2, PoolSize: 1})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer func() { _ = eng.Close() }()

	ac, err := eng.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	ac.ClassifyStatement(ctx, "CREATE TEMP TABLE scratch (id INTEGER)")
	if _, err := ac.ExecContext(ctx, "CREATE TEMP TABLE scratch (id INTEGER)"); err != nil {
		t.Fatalf("CREATE TEMP TABLE: %v", err)
	}
	eng.Pool.Release(ac)

	next, err := eng.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}
	defer eng.Pool.Release(next)

	if _, err := next.ExecContext(ctx, "SELECT * FROM scratch"); err == nil {
		t.Fatal("temp table leaked to the next borrower of the connection")
	}
}

// TestReleaseKeepsCleanConnection guards the fast path: a plain SELECT must
// not cost a connection teardown and re-boot on every release.
func TestReleaseKeepsCleanConnection(t *testing.T) {
	ctx := context.Background()
	eng, err := NewEngine(&config.Config{MemoryLimit: "256MB", MaxThreads: 2, PoolSize: 1})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer func() { _ = eng.Close() }()

	ac, err := eng.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	ac.ClassifyStatement(ctx, "SELECT 1")
	if ac.IsDirty() {
		t.Fatal("a SELECT must not mark the connection dirty")
	}
	eng.Pool.Release(ac)

	same, err := eng.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}
	defer eng.Pool.Release(same)
	if same != ac {
		t.Fatal("clean connection was recycled instead of reused")
	}
}
