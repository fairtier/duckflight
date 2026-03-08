//go:build duckdb_arrow

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/prochac/duckflight/internal/config"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	cfg := &config.Config{
		MemoryLimit: "256MB",
		MaxThreads:  2,
		PoolSize:    2,
	}
	eng, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	return eng
}

func TestNewEngine(t *testing.T) {
	eng := newTestEngine(t)
	if eng.connector == nil {
		t.Fatal("connector is nil")
	}
	if eng.Pool == nil {
		t.Fatal("pool is nil")
	}
}

func TestExecSQL(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()

	if err := eng.ExecSQL(ctx, "CREATE TABLE test_exec_sql (id INTEGER)"); err != nil {
		t.Fatalf("ExecSQL CREATE: %v", err)
	}
	if err := eng.ExecSQL(ctx, "INSERT INTO test_exec_sql VALUES (1), (2), (3)"); err != nil {
		t.Fatalf("ExecSQL INSERT: %v", err)
	}
}

func TestPoolAcquireRelease(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()

	ac, err := eng.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if ac.Arrow == nil {
		t.Fatal("Arrow handle is nil")
	}

	// Query via Arrow
	rdr, err := ac.Arrow.QueryContext(ctx, "SELECT 42 AS answer")
	if err != nil {
		eng.Pool.Release(ac)
		t.Fatalf("QueryContext: %v", err)
	}
	defer rdr.Release()

	if !rdr.Next() {
		eng.Pool.Release(ac)
		t.Fatal("expected at least one record batch")
	}
	rec := rdr.Record()
	if rec.NumRows() != 1 {
		eng.Pool.Release(ac)
		t.Fatalf("expected 1 row, got %d", rec.NumRows())
	}
	if rec.Schema().Field(0).Name != "answer" {
		eng.Pool.Release(ac)
		t.Fatalf("expected field 'answer', got %q", rec.Schema().Field(0).Name)
	}

	eng.Pool.Release(ac)
}

func TestPoolAcquireTimeout(t *testing.T) {
	cfg := &config.Config{
		MemoryLimit: "256MB",
		MaxThreads:  2,
		PoolSize:    1, // only 1 connection
	}
	eng, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer eng.Close()

	ctx := context.Background()
	ac, err := eng.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// Try to acquire with a short timeout — should fail
	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	_, err = eng.Pool.Acquire(shortCtx)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	eng.Pool.Release(ac)
}

func TestPoolClose(t *testing.T) {
	cfg := &config.Config{
		MemoryLimit: "256MB",
		MaxThreads:  2,
		PoolSize:    2,
	}
	eng, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	// Just verify Close doesn't panic
	eng.Close()
}
