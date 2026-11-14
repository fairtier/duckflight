//go:build duckdb_arrow

package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fairtier/duckflight/internal/config"
)

func newReadOnlyTestEngine(t *testing.T, ro *config.ReadOnlyConfig, tempDir string) *Engine {
	t.Helper()
	eng, err := NewReadOnlyEngine(&config.Config{
		MemoryLimit:   "256MB",
		MaxThreads:    2,
		PoolSize:      3,
		TempDirectory: tempDir,
		ReadOnly:      ro,
	})
	if err != nil {
		t.Fatalf("NewReadOnlyEngine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func queryOne(ctx context.Context, ac *ArrowConn, q string) (string, error) {
	rdr, err := ac.Arrow.QueryContext(ctx, q)
	if err != nil {
		return "", err
	}
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.RecordBatch()
		if rec.NumRows() > 0 {
			return strings.Clone(rec.Column(0).ValueStr(0)), nil
		}
	}
	return "", rdr.Err()
}

// TestReadOnlyEngineIsHardened checks every pooled connection — not only the
// first, which is the one that ran the lockdown — sees the instance confined.
func TestReadOnlyEngineIsHardened(t *testing.T) {
	ctx := context.Background()
	allowed := t.TempDir()
	if err := os.WriteFile(filepath.Join(allowed, "ok.csv"), []byte("a\n1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	eng := newReadOnlyTestEngine(t, &config.ReadOnlyConfig{
		MemoryLimit:        "128MB",
		AllowedDirectories: []string{allowed + "/"},
	}, "")

	var conns []*ArrowConn
	for range eng.Pool.Cap() {
		ac, err := eng.Pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, ac)
	}
	defer func() {
		for _, ac := range conns {
			eng.Pool.Release(ac)
		}
	}()

	for i, ac := range conns {
		for setting, want := range map[string]string{
			"enable_external_access": "false",
			"lock_configuration":     "true",
			"memory_limit":           "122.0 MiB", // 128MB as DuckDB reports it
		} {
			got, err := queryOne(ctx, ac, "SELECT current_setting('"+setting+"')::VARCHAR")
			if err != nil {
				t.Fatalf("conn %d: %s: %v", i, setting, err)
			}
			if got != want {
				t.Errorf("conn %d: %s = %q, want %q", i, setting, got, want)
			}
		}

		refused := []string{
			"SELECT * FROM read_text('/proc/self/environ')",
			"SELECT * FROM read_text('/etc/hostname')",
			"SELECT * FROM read_csv('http://127.0.0.1:1/x.csv')",
			"SELECT * FROM read_csv('" + allowed + "/../escape.csv')",
			"COPY (SELECT 1) TO '/tmp/duckflight-readonly-leak.csv'",
			"INSTALL spatial",
			"LOAD spatial",
			"SET enable_external_access = true",
			"SET lock_configuration = false",
			"RESET memory_limit",
			"SET memory_limit = '1GB'",
			"PRAGMA threads = 8",
			"SELECT * FROM duckdb_secrets(redact := false)",
		}
		for _, q := range refused {
			if _, err := ac.ExecContext(ctx, q); err == nil {
				t.Errorf("conn %d: %q succeeded on the read-only instance", i, q)
			}
		}

		got, err := queryOne(ctx, ac, "SELECT a::VARCHAR FROM read_csv('"+filepath.Join(allowed, "ok.csv")+"')")
		if err != nil || got != "1" {
			t.Errorf("conn %d: read inside allowed_directories: %q, %v", i, got, err)
		}
	}
}

// TestReadOnlyEngineSpills pins that disabling external access does not
// disable spilling: DuckDB allows its own temp_directory implicitly. (The
// alternative hardening, disabled_filesystems='LocalFileSystem', breaks
// exactly this, which is why it is not used.)
func TestReadOnlyEngineSpills(t *testing.T) {
	if testing.Short() {
		t.Skip("spills ~100MB")
	}
	ctx := context.Background()
	tmp := t.TempDir()
	eng := newReadOnlyTestEngine(t, &config.ReadOnlyConfig{MemoryLimit: "64MB"}, tmp)

	ac, err := eng.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Pool.Release(ac)

	got, err := queryOne(ctx, ac, "SELECT current_setting('temp_directory')")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(tmp, "read-only"); got != want {
		t.Fatalf("temp_directory = %q, want %q (its own subdirectory)", got, want)
	}
	got, err = queryOne(ctx, ac,
		"SELECT count(*)::VARCHAR FROM (SELECT md5(r::VARCHAR) m, r FROM range(3000000) t(r) ORDER BY m)")
	if err != nil {
		t.Fatalf("larger-than-memory sort on the read-only instance: %v", err)
	}
	if got != "3000000" {
		t.Fatalf("count = %s", got)
	}
}

// TestReadOnlyEngineReplacementConnections covers connections booted after
// the lockdown — pool replacements and ExecSQL's — which must not re-run the
// boot SETs (they would fail against the lock) yet still come up usable.
func TestReadOnlyEngineReplacementConnections(t *testing.T) {
	ctx := context.Background()
	eng := newReadOnlyTestEngine(t, &config.ReadOnlyConfig{}, "")

	for range 3 {
		ac, err := eng.Pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		eng.Pool.Discard(ac) // forces a freshly booted replacement
	}
	if got := eng.Pool.Len(); got != eng.Pool.Cap() {
		t.Fatalf("pool shrank to %d/%d: replacement boot failed", got, eng.Pool.Cap())
	}
	// ExecSQL opens its own connection: what reconcile SQL runs through.
	if err := eng.ExecSQL(ctx, "macro", "CREATE OR REPLACE MACRO answer() AS 42"); err != nil {
		t.Fatalf("ExecSQL on the read-only instance: %v", err)
	}
	// A secret type whose extension was not loaded before the lockdown cannot
	// be created afterwards: autoloading httpfs is an extension load. (With
	// Iceberg configured, boot loads httpfs, so S3 secrets rotate fine.)
	if err := eng.ExecSQL(ctx, "secret", "CREATE OR REPLACE SECRET s (TYPE s3, KEY_ID 'k', SECRET 'v')"); err == nil {
		t.Fatal("S3 secret without httpfs loaded succeeded after lockdown")
	}
	err := eng.ExecSQL(ctx, "load", "LOAD spatial")
	if err == nil || !strings.Contains(err.Error(), "load") {
		t.Fatalf("LOAD through ExecSQL after lockdown: %v", err)
	}
}

func TestHardenSQLOrder(t *testing.T) {
	got := hardenSQL([]string{"s3://warehouse/", "it's"})
	var labels []string
	for _, s := range got {
		labels = append(labels, s.label)
	}
	want := "set allowed_directories,disable external access,lock configuration"
	if strings.Join(labels, ",") != want {
		t.Fatalf("order = %v, want %s", labels, want)
	}
	if got[0].sql != "SET allowed_directories = ['s3://warehouse/', 'it''s']" {
		t.Fatalf("allowed_directories SQL = %s", got[0].sql)
	}
	if len(hardenSQL(nil)) != 2 {
		t.Fatal("no allowlist must still disable external access and lock")
	}
}

func TestReadOnlyAllowedDirectoriesDefault(t *testing.T) {
	cases := []struct {
		cfg  config.Config
		want []string
	}{
		{config.Config{ReadOnly: &config.ReadOnlyConfig{}}, nil},
		{config.Config{IcebergEndpoint: "http://c", ReadOnly: &config.ReadOnlyConfig{}}, []string{"s3://"}},
		{config.Config{IcebergEndpoint: "http://c", ReadOnly: &config.ReadOnlyConfig{AllowedDirectories: []string{}}}, []string{}},
		{config.Config{IcebergEndpoint: "http://c", ReadOnly: &config.ReadOnlyConfig{AllowedDirectories: []string{"s3://wh/"}}}, []string{"s3://wh/"}},
	}
	for i, tc := range cases {
		got := readOnlyAllowedDirectories(&tc.cfg)
		if strings.Join(got, ",") != strings.Join(tc.want, ",") || (got == nil) != (tc.want == nil) {
			t.Errorf("case %d: got %#v, want %#v", i, got, tc.want)
		}
	}
}
