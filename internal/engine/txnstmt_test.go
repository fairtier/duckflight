//go:build duckdb_arrow

package engine

import (
	"context"
	"testing"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/fairtier/duckflight/internal/config"
)

// TestIsTransactionStatement verifies that DuckDB's parser correctly classifies
// transaction-control statements (used by [server.isRedundantTxnCollision] to
// avoid substring matching on SQL or error messages).
func TestIsTransactionStatement(t *testing.T) {
	eng, err := NewEngine(&config.Config{MemoryLimit: "256MB", MaxThreads: 1, PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()

	ac, err := eng.Pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Pool.Release(ac)

	cases := []struct {
		query string
		want  bool
	}{
		{"BEGIN", true},
		{"BEGIN TRANSACTION", true},
		{"COMMIT", true},
		{"ROLLBACK", true},
		{"ABORT", true},
		{"START TRANSACTION", true},
		{"SELECT 1", false},
		{"CREATE TABLE x (id INT)", false},
		{"", false},
		{"NOT VALID SQL", false},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			got := ac.IsTransactionStatement(context.Background(), tc.query)
			if got != tc.want {
				t.Errorf("IsTransactionStatement(%q) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}

// TestClassifyStatementDoesNotExecute: classifying multi-statement SQL must not
// run any of it, and must assume it leaves state behind, since DuckDB can only
// describe statements it was willing to prepare one at a time.
func TestClassifyStatementDoesNotExecute(t *testing.T) {
	ctx := context.Background()
	eng, err := NewEngine(&config.Config{MemoryLimit: "256MB", MaxThreads: 1, PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	ac, err := eng.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Pool.Release(ac)

	if _, err := ac.ExecContext(ctx, "CREATE TABLE classify_t (i INT)"); err != nil {
		t.Fatal(err)
	}
	ac.dirty.Store(false)

	for _, query := range []string{
		"INSERT INTO classify_t VALUES (1); SELECT 1",
		"INSERT INTO classify_t VALUES (1); COMMIT",
		"BEGIN; INSERT INTO classify_t VALUES (1)",
	} {
		ac.dirty.Store(false)
		intent, stmtType := ac.ClassifyStatement(ctx, query)
		if intent != TxnIntentNone {
			t.Errorf("%q: intent = %v, want none: a script's transaction control runs as written", query, intent)
		}
		if stmtType != duckdb.STATEMENT_TYPE_MULTI {
			t.Errorf("%q: type = %v, want MULTI", query, stmtType)
		}
		if !ac.IsDirty() {
			t.Errorf("%q: not marked dirty", query)
		}
	}

	rdr, err := ac.Arrow.QueryContext(ctx, "SELECT i FROM classify_t")
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Release()
	for rdr.Next() {
		if n := rdr.RecordBatch().NumRows(); n != 0 {
			t.Errorf("classification executed the statements: %d rows", n)
		}
	}
	if inTxn, err := ac.InExplicitTransaction(ctx); err != nil || inTxn {
		t.Errorf("classification opened a transaction: %v, %v", inTxn, err)
	}

	// Single statements keep their real classification, and don't get dirty for
	// a semicolon or a literal.
	ac.dirty.Store(false)
	for _, query := range []string{"SELECT 1;", "INSERT INTO classify_t VALUES (1); -- done", "SELECT ';'"} {
		if _, stmtType := ac.ClassifyStatement(ctx, query); stmtType == duckdb.STATEMENT_TYPE_MULTI {
			t.Errorf("%q: classified as MULTI", query)
		}
		if ac.IsDirty() {
			t.Errorf("%q: marked dirty", query)
		}
	}
}
