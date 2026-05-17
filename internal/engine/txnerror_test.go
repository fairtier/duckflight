//go:build duckdb_arrow

package engine

import (
	"context"
	"errors"
	"testing"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/fairtier/duckflight/internal/config"
)

// TestArrowQueryErrorIsTyped verifies the error returned by Arrow.QueryContext
// for a redundant BEGIN unwraps to *duckdb.Error with ErrorTypeTransaction.
// This is the contract isRedundantTxnCollision relies on.
func TestArrowQueryErrorIsTyped(t *testing.T) {
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

	// Put DuckDB in a txn via raw SQL.
	if _, err := ac.ExecContext(context.Background(), "BEGIN"); err != nil {
		t.Fatalf("setup BEGIN: %v", err)
	}
	defer func() { _, _ = ac.ExecContext(context.Background(), "ROLLBACK") }()

	// Now a second BEGIN via Arrow path should error with ErrorTypeTransaction.
	rdr, err := ac.Arrow.QueryContext(context.Background(), "BEGIN")
	if rdr != nil {
		rdr.Release()
	}
	if err == nil {
		t.Fatalf("expected error from redundant BEGIN via Arrow path, got nil")
	}
	t.Logf("err type=%T msg=%q", err, err.Error())

	var derr *duckdb.Error
	if !errors.As(err, &derr) {
		t.Fatalf("error does not unwrap to *duckdb.Error: %v (type %T)", err, err)
	}
	if derr.Type != duckdb.ErrorTypeTransaction {
		t.Errorf("derr.Type = %v, want ErrorTypeTransaction; msg=%q", derr.Type, derr.Msg)
	}
}
