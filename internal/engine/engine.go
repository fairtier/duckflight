//go:build duckdb_arrow

package engine

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/fairtier/duckflight/internal/config"
)

// Engine owns the DuckDB connector, runs boot SQL, and provides
// an Arrow connection pool for query execution.
type Engine struct {
	connector *duckdb.Connector
	Pool      *ArrowPool
	WriteMu   sync.Mutex
	cfg       *config.Config
}

// quoteLiteral renders s as a single-quoted SQL string literal, doubling any
// embedded quote. DuckDB's standard single-quoted literals have no backslash
// escapes, so doubling is sufficient. Without it a config value containing a
// quote would terminate the literal and inject SQL that runs on every new
// connection.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// bootStatement is one boot SQL statement plus a log-safe label. The label is
// used in error messages instead of the statement text, because the CREATE
// SECRET statements embed live Iceberg/S3 credentials and boot errors are
// logged to stderr and shipped to the OTLP collector.
type bootStatement struct {
	label string
	sql   string
}

// NewEngine creates a new in-memory DuckDB engine with the given config.
func NewEngine(cfg *config.Config) (*Engine, error) {
	boot, bestEffort := bootPlan(cfg, false)
	connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		// Boot SQL runs for every connection the pool ever opens, not only at
		// startup, so a fatal statement that starts failing mid-life does not
		// crash the server: it silently shrinks the pool one replacement at a
		// time until nothing answers. That is why bestEffort exists.
		if err := runBoot(execer, boot); err != nil {
			return err
		}
		runBestEffort(execer, bestEffort)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB connector: %w", err)
	}
	return newEngine(connector, cfg, cfg.PoolSize, "")
}

func newEngine(connector *duckdb.Connector, cfg *config.Config, poolSize int, label string) (*Engine, error) {
	pool, err := NewLabeledArrowPool(connector, poolSize, label)
	if err != nil {
		_ = connector.Close()
		return nil, fmt.Errorf("failed to create arrow pool: %w", err)
	}
	return &Engine{connector: connector, Pool: pool, cfg: cfg}, nil
}

// bootPlan builds the boot SQL for an engine. bestEffort statements run after
// boot and log a failure instead of failing it.
//
// readOnly builds the plan for the read-only instance, which differs in what
// it must have in place *before* [hardenSQL] cuts it off from the outside:
// every extension Iceberg reads need is loaded explicitly (nothing can be
// autoloaded afterwards), the catalog is attached READ_ONLY, and nothing tries
// to create the default namespace.
func bootPlan(cfg *config.Config, readOnly bool) (boot, bestEffort []bootStatement) {
	memoryLimit, tempDir := cfg.MemoryLimit, cfg.TempDirectory
	if readOnly {
		if cfg.ReadOnly.MemoryLimit != "" {
			memoryLimit = cfg.ReadOnly.MemoryLimit
		}
		if tempDir != "" {
			// Two DuckDB instances must not share a spill directory: each
			// owns and cleans up the files it creates there.
			tempDir = filepath.Join(tempDir, "read-only")
		}
	}

	boot = []bootStatement{
		{"set autoinstall_known_extensions", fmt.Sprintf("SET autoinstall_known_extensions = %t", !staticExtensions)},
		{"set autoload_known_extensions", "SET autoload_known_extensions = true"},
		{"set memory_limit", "SET memory_limit = " + quoteLiteral(memoryLimit)},
		{"set threads", fmt.Sprintf("SET threads = %d", cfg.MaxThreads)},
	}

	if cfg.ExtensionDir != "" {
		boot = append(boot, bootStatement{
			"set extension_directory", "SET extension_directory = " + quoteLiteral(cfg.ExtensionDir),
		})
	}

	if tempDir != "" {
		boot = append(boot, bootStatement{
			"set temp_directory", "SET temp_directory = " + quoteLiteral(tempDir),
		})
	}

	if cfg.IcebergEndpoint == "" {
		return boot, nil
	}

	extensions := []string{"iceberg"}
	if readOnly {
		// The main instance autoloads these on first use. The read-only
		// instance cannot: once external access is off, loading an
		// extension file is refused, and the first Iceberg scan would fail
		// on the missing S3 filesystem or Avro manifest reader.
		extensions = append(extensions, "httpfs", "avro")
	}
	for _, ext := range extensions {
		if !staticExtensions {
			boot = append(boot, bootStatement{"install " + ext, "INSTALL " + ext})
		}
		boot = append(boot, bootStatement{"load " + ext, "LOAD " + ext})
	}

	// Catalog auth (OAuth2) — optional
	if cfg.IcebergClientID != "" {
		boot = append(boot, bootStatement{"create iceberg secret", fmt.Sprintf(
			`CREATE SECRET IF NOT EXISTS iceberg_secret (
				TYPE iceberg,
				CLIENT_ID %s,
				CLIENT_SECRET %s,
				OAUTH2_SERVER_URI %s
			)`, quoteLiteral(cfg.IcebergClientID), quoteLiteral(cfg.IcebergClientSecret), quoteLiteral(cfg.IcebergOAuth2URI))})
	}

	// S3 data layer credentials — optional
	if cfg.S3Endpoint != "" {
		urlStyle := cfg.S3URLStyle
		if urlStyle == "" {
			urlStyle = "path"
		}
		boot = append(boot, bootStatement{"create s3 secret", fmt.Sprintf(
			`CREATE OR REPLACE SECRET s3_secret (
				TYPE s3,
				KEY_ID %s,
				SECRET %s,
				ENDPOINT %s,
				REGION %s,
				URL_STYLE %s,
				USE_SSL false
			)`, quoteLiteral(cfg.S3AccessKey), quoteLiteral(cfg.S3SecretKey), quoteLiteral(cfg.S3Endpoint),
			quoteLiteral(cfg.S3Region), quoteLiteral(urlStyle))})
	}

	// ATTACH iceberg catalog
	attachSQL := fmt.Sprintf("ATTACH IF NOT EXISTS %s AS lake (TYPE iceberg, ENDPOINT %s",
		quoteLiteral(cfg.IcebergWarehouse), quoteLiteral(cfg.IcebergEndpoint))
	if cfg.IcebergClientID != "" {
		attachSQL += ", SECRET 'iceberg_secret'"
	} else {
		attachSQL += ", AUTHORIZATION_TYPE 'none'"
	}
	// When the catalog doesn't vend credentials, DuckDB must use its own S3 secret.
	if cfg.S3Endpoint != "" {
		attachSQL += ", ACCESS_DELEGATION_MODE 'none'"
	}
	if readOnly {
		// DuckDB then refuses INSERT/UPDATE/DELETE/CREATE/DROP/ALTER against
		// the lake itself, whatever reaches the connection.
		attachSQL += ", READ_ONLY"
	}
	attachSQL += ")"
	boot = append(boot, bootStatement{"attach iceberg catalog", attachSQL})

	// The namespace USE points at must exist in the catalog, and nothing
	// else creates it. DuckDB-Iceberg accepts USE of a namespace it has
	// never seen without asking the catalog, but the first table lookup
	// that misses in it (any unqualified typo) makes it ask, and a
	// "no such namespace" answer is cached instance-wide for the life of
	// the process — after which USE fails on every new connection.
	// Best-effort because a read-only catalog client may not create
	// namespaces; that deployment has to create `default` itself. The
	// read-only instance never tries: its lake is attached READ_ONLY, and
	// the main instance, booted first, has already created it.
	if !readOnly {
		bestEffort = append(bestEffort,
			bootStatement{"create schema lake.default", "CREATE SCHEMA IF NOT EXISTS lake.\"default\""})
	}
	// Make the Iceberg catalog the default so users can omit the
	// "lake.default." prefix. A connection that cannot USE it still
	// serves fully qualified queries.
	bestEffort = append(bestEffort, useLakeDefault)
	return boot, bestEffort
}

var useLakeDefault = bootStatement{"use lake.default", "USE lake.\"default\""}

func runBoot(execer driver.ExecerContext, stmts []bootStatement) error {
	for _, stmt := range stmts {
		if _, err := execer.ExecContext(context.Background(), stmt.sql, nil); err != nil {
			// Deliberately reports the label, not stmt.sql: the secret
			// statements carry credentials in cleartext.
			return fmt.Errorf("boot SQL failed (%s): %w", stmt.label, err)
		}
	}
	return nil
}

func runBestEffort(execer driver.ExecerContext, stmts []bootStatement) {
	for _, stmt := range stmts {
		if _, err := execer.ExecContext(context.Background(), stmt.sql, nil); err != nil {
			slog.Error("best-effort boot SQL failed; continuing",
				slog.String("statement", stmt.label), slog.String("error", err.Error()))
		}
	}
}

// Connector returns the underlying DuckDB connector.
func (e *Engine) Connector() *duckdb.Connector {
	return e.connector
}

// ExecSQL executes SQL using a temporary connection from the connector.
// DuckDB's temporary (non-PERSISTENT) secrets and loaded extensions are
// instance-wide, so statements run here are visible to every pooled
// connection immediately.
//
// The error deliberately reports label rather than the statement text: callers
// pass credential-bearing SQL (CREATE SECRET …) and errors are logged and
// shipped to the OTLP collector — same rationale as [bootStatement].
func (e *Engine) ExecSQL(ctx context.Context, label, sql string) error {
	conn, err := e.connector.Connect(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close() }()

	execer, ok := conn.(driver.ExecerContext)
	if !ok {
		return fmt.Errorf("connection does not implement ExecerContext")
	}

	if _, err := execer.ExecContext(ctx, sql, nil); err != nil {
		return fmt.Errorf("exec (%s): %w", label, err)
	}
	return nil
}

// Close shuts down the pool and the connector.
func (e *Engine) Close() error {
	e.Pool.Close()
	return e.connector.Close()
}
