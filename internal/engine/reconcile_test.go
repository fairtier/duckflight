//go:build duckdb_arrow

package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// queryCount returns SELECT count(*) of the given table via a pooled conn.
func queryCount(t *testing.T, eng *Engine, table string) (string, error) {
	t.Helper()
	ctx := context.Background()
	ac, err := eng.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer eng.Pool.Release(ac)

	rdr, err := ac.Arrow.QueryContext(ctx, "SELECT count(*) FROM "+table)
	if err != nil {
		return "", err
	}
	defer rdr.Release()
	if !rdr.Next() {
		t.Fatal("no count result")
	}
	return rdr.RecordBatch().Column(0).ValueStr(0), nil
}

// waitForCount polls until the table reports want rows or the deadline hits.
func waitForCount(t *testing.T, eng *Engine, table, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got string
	var err error
	for time.Now().Before(deadline) {
		got, err = queryCount(t, eng, table)
		if err == nil && got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("table %s never reached count %s (last: %q, err: %v)", table, want, got, err)
}

// TestReconcilerAppliesAndWatches covers the whole contract: a missing file is
// a non-event, the initial apply runs synchronously and executes a
// multi-statement file, a content change is re-applied without restart, and
// broken SQL is tolerated (logged, engine keeps serving).
func TestReconcilerAppliesAndWatches(t *testing.T) {
	eng := newTestEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := filepath.Join(t.TempDir(), "reconcile.sql")
	r := NewReconciler(eng, path)
	r.interval = 20 * time.Millisecond

	// Missing file at startup is a normal state.
	r.Start(ctx)

	// Multi-statement content: the reconciler hands the whole file to one
	// exec, so this doubles as a regression test for the driver's
	// multi-statement support.
	writeFile(t, path, "CREATE TABLE recon_probe (id INTEGER);\nINSERT INTO recon_probe VALUES (1), (2);")
	waitForCount(t, eng, "recon_probe", "2")

	// Content change → re-applied.
	writeFile(t, path, "INSERT INTO recon_probe VALUES (3);")
	waitForCount(t, eng, "recon_probe", "3")

	// Broken SQL must not kill anything; the engine keeps serving.
	writeFile(t, path, "THIS IS NOT SQL;")
	time.Sleep(100 * time.Millisecond)
	if got, err := queryCount(t, eng, "recon_probe"); err != nil || got != "3" {
		t.Fatalf("engine degraded after broken reconcile SQL: count=%q err=%v", got, err)
	}

	// Recovery: valid content after broken content is applied again.
	writeFile(t, path, "INSERT INTO recon_probe VALUES (4);")
	waitForCount(t, eng, "recon_probe", "4")
}

// TestReconcilerSecretVisibleAcrossPool verifies the property federation
// relies on: a secret created via reconcile SQL on a temporary connection is
// visible to every pooled connection (DuckDB temporary secrets are
// instance-wide).
func TestReconcilerSecretVisibleAcrossPool(t *testing.T) {
	eng := newTestEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := filepath.Join(t.TempDir(), "reconcile.sql")
	writeFile(t, path, "CREATE OR REPLACE SECRET recon_secret (TYPE http, EXTRA_HTTP_HEADERS MAP {'x-probe': 'v1'});")
	r := NewReconciler(eng, path)
	r.interval = 20 * time.Millisecond
	r.Start(ctx)

	waitForSecret := func(want string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			ac, err := eng.Pool.Acquire(ctx)
			if err != nil {
				t.Fatalf("Acquire: %v", err)
			}
			rdr, err := ac.Arrow.QueryContext(ctx,
				"SELECT count(*) FROM duckdb_secrets() WHERE name = 'recon_secret'")
			ok := false
			if err == nil {
				if rdr.Next() && rdr.RecordBatch().Column(0).ValueStr(0) == want {
					ok = true
				}
				rdr.Release()
			}
			eng.Pool.Release(ac)
			if ok {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("secret recon_secret never reached count %s on pooled connections", want)
	}
	waitForSecret("1")
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
