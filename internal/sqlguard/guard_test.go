//go:build duckdb_arrow

package sqlguard

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func newGuard(t *testing.T) *Guard {
	t.Helper()
	g, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(g.Close)
	return g
}

type verdict int

const (
	allow verdict = iota
	deny
	parseErr
)

func (v verdict) String() string {
	return [...]string{"allow", "deny", "parse error"}[v]
}

func classify(err error) verdict {
	var v *Violation
	var p *ParseError
	switch {
	case err == nil:
		return allow
	case errors.As(err, &v):
		return deny
	case errors.As(err, &p):
		return parseErr
	}
	return -1
}

func TestCheckReadOnly(t *testing.T) {
	g := newGuard(t)

	cases := []struct {
		name  string
		sql   string
		want  verdict
		match string // substring of the violation reason, when denied
	}{
		// --- allowed reads
		{"select", "SELECT 1", allow, ""},
		{"select from lake table", "SELECT id, name FROM lake.ns.tbl WHERE id > 3 ORDER BY 1 LIMIT 10", allow, ""},
		{"cte select", "WITH x AS (SELECT 1 AS a) SELECT * FROM x", allow, ""},
		{"recursive cte", "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) SELECT * FROM r", allow, ""},
		{"from-first shorthand", "FROM lake.ns.tbl", allow, ""},
		{"from-first with select", "FROM lake.ns.tbl SELECT id", allow, ""},
		{"values", "VALUES (1), (2)", allow, ""},
		{"union", "SELECT 1 UNION ALL SELECT 2", allow, ""},
		{"describe", "DESCRIBE lake.ns.tbl", allow, ""},
		{"describe query", "DESCRIBE SELECT 1", allow, ""},
		{"show tables", "SHOW TABLES", allow, ""},
		{"show all tables", "SHOW ALL TABLES", allow, ""},
		{"summarize", "SUMMARIZE lake.ns.tbl", allow, ""},
		{"explain select", "EXPLAIN SELECT * FROM lake.ns.tbl", allow, ""},
		{"explain after comment", "/* why */ EXPLAIN -- plan\n SELECT 1", allow, ""},
		{"leading comments", "-- a comment\n/* another /* nested */ one */ SELECT 1", allow, ""},
		{"trailing line comment", "SELECT 1 -- done", allow, ""},
		{"multiple selects", "SELECT 1; SELECT 2;", allow, ""},
		{"parameters", "SELECT * FROM lake.ns.tbl WHERE id = ? AND name = $2", allow, ""},
		{"range table function", "SELECT * FROM range(10)", allow, ""},
		{"catalog introspection", "SELECT * FROM duckdb_tables() JOIN information_schema.columns USING (table_name)", allow, ""},
		{"iceberg snapshots", "SELECT * FROM iceberg_snapshots(lake.ns.tbl)", allow, ""},
		{"window and aggregates", "SELECT sum(x) OVER (PARTITION BY y), count(*) FROM t GROUP BY ALL", allow, ""},
		{"string mentioning a path", "SELECT '/proc/self/environ' AS p", allow, ""},
		{"current_setting", "SELECT current_setting('threads')", allow, ""},
		{"standard pivot in from", "SELECT * FROM t PIVOT (sum(b) FOR a IN (1, 2))", allow, ""},
		{"unpivot statement", "UNPIVOT t ON a, b INTO NAME k VALUE v", allow, ""},

		// --- statement types the parser does not call SELECT
		{"insert", "INSERT INTO t VALUES (1)", deny, "only SELECT"},
		{"cte + insert", "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x", deny, "only SELECT"},
		{"cte + delete", "WITH x AS (SELECT 1) DELETE FROM t WHERE a IN (SELECT * FROM x)", deny, "only SELECT"},
		{"update", "UPDATE t SET a = 1", deny, "only SELECT"},
		{"merge", "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE", deny, "only SELECT"},
		{"create table as", "CREATE TABLE t AS SELECT 1", deny, "only SELECT"},
		{"create temp table", "CREATE TEMP TABLE t AS SELECT 1", deny, "only SELECT"},
		{"create view", "CREATE VIEW v AS SELECT 1", deny, "only SELECT"},
		{"create macro", "CREATE MACRO m(x) AS x", deny, "only SELECT"},
		{"create secret", "CREATE SECRET s (TYPE s3, KEY_ID 'a', SECRET 'b')", deny, "only SELECT"},
		{"drop", "DROP TABLE lake.ns.tbl", deny, "only SELECT"},
		{"alter", "ALTER TABLE t ADD COLUMN b INT", deny, "only SELECT"},
		{"copy to", "COPY t TO 's3://bucket/out.parquet'", deny, "only SELECT"},
		{"copy select to", "COPY (SELECT 1) TO 's3://bucket/out.parquet'", deny, "only SELECT"},
		{"copy from", "COPY t FROM '/etc/passwd'", deny, "only SELECT"},
		{"export database", "EXPORT DATABASE '/tmp/x'", deny, "only SELECT"},
		{"attach", "ATTACH ':memory:' AS m", deny, "only SELECT"},
		{"detach", "DETACH lake", deny, "only SELECT"},
		{"use", "USE memory", deny, "only SELECT"},
		{"set", "SET threads = 1", deny, "only SELECT"},
		{"reset", "RESET threads", deny, "only SELECT"},
		{"pragma", "PRAGMA show_tables", deny, "only SELECT"},
		{"pragma assignment", "PRAGMA threads=4", deny, "only SELECT"},
		{"call", "CALL pragma_version()", deny, "only SELECT"},
		{"install", "INSTALL spatial", deny, "only SELECT"},
		{"load", "LOAD spatial", deny, "only SELECT"},
		{"begin", "BEGIN TRANSACTION", deny, "only SELECT"},
		{"checkpoint", "CHECKPOINT", deny, "only SELECT"},
		{"vacuum", "VACUUM", deny, "only SELECT"},
		{"analyze", "ANALYZE", deny, "only SELECT"},
		{"prepare", "PREPARE p AS SELECT 1", deny, "only SELECT"},
		{"pivot statement", "PIVOT t ON a USING sum(b)", deny, "only SELECT"},

		// --- EXPLAIN
		{"explain analyze of a write", "EXPLAIN ANALYZE INSERT INTO t VALUES (1)", deny, "EXPLAIN ANALYZE"},
		{"explain analyze of a select", "EXPLAIN ANALYZE SELECT 1", deny, "EXPLAIN ANALYZE"},
		{"explain analyse spelling", "explain analyse select 1", deny, "EXPLAIN ANALYZE"}, //nolint:misspell // DuckDB keyword
		{"explain analyze behind a comment", "EXPLAIN /* x */ ANALYZE SELECT 1", deny, "EXPLAIN ANALYZE"},
		{"explain options", "EXPLAIN (ANALYZE) SELECT 1", deny, "options"},
		{"explain of a write", "EXPLAIN INSERT INTO t VALUES (1)", deny, "only SELECT"},
		{"explain then a second statement", "EXPLAIN SELECT 1; SELECT 2", deny, "only statement"},
		{"explain then a write", "EXPLAIN SELECT 1; DROP TABLE t", deny, "only SELECT"},

		// --- multi-statement scripts
		{"select then insert", "SELECT 1; INSERT INTO t VALUES (1)", deny, "only SELECT"},
		{"insert then select", "INSERT INTO t VALUES (1); SELECT 1", deny, "only SELECT"},
		{"select then load", "SELECT 1; LOAD httpfs", deny, "only SELECT"},
		{"select then attach", "SELECT 1;ATTACH 'x.db'", deny, "only SELECT"},

		// --- the function hole: file, URL and SQL-in-a-string access
		{"read_text environ", "SELECT * FROM read_text('/proc/self/environ')", deny, "read_text"},
		{"read_text nested in cte", "WITH e AS (SELECT * FROM read_text('/proc/self/environ')) SELECT * FROM e", deny, "read_text"},
		{"read_text in scalar subquery", "SELECT (SELECT content FROM read_text('/proc/self/environ'))", deny, "read_text"},
		{"read_text lateral", "SELECT * FROM range(1) r, LATERAL (SELECT * FROM read_blob('/etc/passwd')) x", deny, "read_blob"},
		{"read_csv url", "SELECT * FROM read_csv('https://example.com/x.csv')", deny, "read_csv"},
		{"read_csv_auto", "SELECT * FROM read_csv_auto('/etc/passwd')", deny, "read_csv_auto"},
		{"read_parquet s3", "SELECT * FROM read_parquet('s3://bucket/**/*.parquet')", deny, "read_parquet"},
		{"read_json", "SELECT * FROM read_json('/x.json')", deny, "read_json"},
		{"glob", "SELECT * FROM glob('s3://bucket/**')", deny, "glob"},
		{"parquet_metadata", "SELECT * FROM parquet_metadata('s3://b/x.parquet')", deny, "parquet_metadata"},
		{"iceberg_scan path", "SELECT * FROM iceberg_scan('s3://b/t')", deny, "iceberg_scan"},
		{"qualified table function", "SELECT * FROM system.main.read_text('/etc/passwd')", deny, "read_text"},
		{"upper-case table function", "SELECT * FROM READ_TEXT('/etc/passwd')", deny, "read_text"}, // DuckDB lower-cases the name
		{"query()", "SELECT * FROM query('SELECT * FROM read_text(''/etc/passwd'')')", deny, "query"},
		{"query_table()", "SELECT * FROM query_table('t')", deny, "query_table"},
		{"duckdb_secrets", "SELECT * FROM duckdb_secrets()", deny, "duckdb_secrets"},
		{"duckdb_settings", "SELECT * FROM duckdb_settings()", deny, "duckdb_settings"},
		{"getenv", "SELECT getenv('S3_SECRET_KEY')", deny, "getenv"},
		{"getenv in where", "SELECT 1 WHERE getenv('HOME') IS NOT NULL", deny, "getenv"},
		{"nextval", "SELECT nextval('seq')", deny, "nextval"},
		{"setseed", "SELECT setseed(0.5)", deny, "setseed"},
		{"which_secret", "SELECT which_secret('s3://b/x', 's3')", deny, "which_secret"},
		{"replacement scan path", "SELECT * FROM '/proc/self/environ'", deny, "files or URLs"},
		{"replacement scan s3", "SELECT * FROM 's3://bucket/x.parquet'", deny, "files or URLs"},
		{"replacement scan csv", "FROM 'data.csv'", deny, "files or URLs"},
		{"replacement scan in join", "SELECT * FROM t JOIN 'https://x/y.parquet' USING (id)", deny, "files or URLs"},

		// --- not SQL at all
		{"empty", "", deny, "empty"},
		{"only a comment", "-- nothing", deny, "empty"},
		{"syntax error", "SELEC 1", parseErr, ""},
		{"unterminated comment", "/* SELECT 1", parseErr, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := g.CheckReadOnly(context.Background(), tc.sql)
			if got := classify(err); got != tc.want {
				t.Fatalf("CheckReadOnly(%q) = %v (%v), want %v", tc.sql, got, err, tc.want)
			}
			if tc.match != "" && !strings.Contains(err.Error(), tc.match) {
				t.Fatalf("CheckReadOnly(%q) error %q does not mention %q", tc.sql, err, tc.match)
			}
		})
	}
}

func TestExtensionStatements(t *testing.T) {
	g := newGuard(t)

	cases := []struct {
		sql     string
		want    bool
		wantErr bool
	}{
		{"LOAD httpfs", true, false},
		{"INSTALL spatial", true, false},
		{"FORCE INSTALL spatial FROM 'https://example.com'", true, false},
		{"UPDATE EXTENSIONS", true, false},
		{"SELECT 1; LOAD httpfs", true, false},
		{"SELECT 1; INSTALL spatial; SELECT 2", true, false},
		{"CREATE TABLE t (i INT); LOAD 'spatial'", true, false},
		{"select * from lake.ns.missing; load httpfs", true, false},
		{"SELECT 1", false, false},
		{"SELECT 1; SELECT 2", false, false},
		{"INSERT INTO t VALUES (1); SELECT * FROM t", false, false},
		{"SELECT 'LOAD httpfs'", false, false},
		{"-- LOAD httpfs\nSELECT 1", false, false},
		{"", false, false},
		{"SELEC 1; LOAD httpfs", false, true},
	}
	for _, tc := range cases {
		got, err := g.ExtensionStatements(tc.sql)
		if (err != nil) != tc.wantErr {
			t.Fatalf("ExtensionStatements(%q) error = %v, wantErr %v", tc.sql, err, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("ExtensionStatements(%q) = %v, want %v", tc.sql, got, tc.want)
		}
	}
}

// TestGuardHasNoSideEffects pins that classifying never reaches the outside
// world, even for statements whose bind step would: the guard's own instance
// has external access disabled.
func TestGuardHasNoSideEffects(t *testing.T) {
	g := newGuard(t)
	dir := t.TempDir()
	// A script whose LOAD would only be detected after preparing a COPY: the
	// COPY must not have written anything.
	if _, err := g.ExtensionStatements("COPY (SELECT 1) TO '" + dir + "/leak.csv'; LOAD httpfs"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.ExtensionStatements("CREATE TABLE gone AS SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if err := g.CheckReadOnly(context.Background(), "SELECT * FROM gone"); err != nil {
		t.Fatalf("plain select refused: %v", err)
	}
	var n int
	if err := g.astDB.QueryRowContext(context.Background(), "SELECT count(*) FROM duckdb_tables()").Scan(&n); err != nil || n != 0 {
		t.Fatalf("guard instance gained tables: n=%d err=%v", n, err)
	}
	if _, err := g.astDB.ExecContext(context.Background(), "SET enable_external_access = true"); err == nil {
		t.Fatal("guard instance configuration is not locked")
	}
}

func TestGuardConcurrent(t *testing.T) {
	g := newGuard(t)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				if i%2 == 0 {
					if err := g.CheckReadOnly(context.Background(), "SELECT * FROM range(3)"); err != nil {
						t.Error(err)
						return
					}
				} else if ok, err := g.ExtensionStatements("SELECT 1; LOAD x"); err != nil || !ok {
					t.Errorf("ExtensionStatements = %v, %v", ok, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestClosedGuard(t *testing.T) {
	g, err := New()
	if err != nil {
		t.Fatal(err)
	}
	g.Close()
	g.Close()
	if err := g.CheckReadOnly(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("closed guard allowed a statement")
	}
	if _, err := g.ExtensionStatements("SELECT 1"); err == nil {
		t.Fatal("closed guard classified a statement")
	}
}
