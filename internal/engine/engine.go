//go:build duckdb_arrow

package engine

import (
	"context"
	"database/sql/driver"
	"fmt"
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
	connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		bootSQL := []bootStatement{
			{"set autoinstall_known_extensions", fmt.Sprintf("SET autoinstall_known_extensions = %t", !staticExtensions)},
			{"set autoload_known_extensions", "SET autoload_known_extensions = true"},
			{"set memory_limit", "SET memory_limit = " + quoteLiteral(cfg.MemoryLimit)},
			{"set threads", fmt.Sprintf("SET threads = %d", cfg.MaxThreads)},
		}

		if cfg.ExtensionDir != "" {
			bootSQL = append(bootSQL, bootStatement{
				"set extension_directory", "SET extension_directory = " + quoteLiteral(cfg.ExtensionDir),
			})
		}

		if cfg.TempDirectory != "" {
			bootSQL = append(bootSQL, bootStatement{
				"set temp_directory", "SET temp_directory = " + quoteLiteral(cfg.TempDirectory),
			})
		}

		if cfg.IcebergEndpoint != "" {
			if !staticExtensions {
				bootSQL = append(bootSQL, bootStatement{"install iceberg", "INSTALL iceberg"})
			}
			bootSQL = append(bootSQL, bootStatement{"load iceberg", "LOAD iceberg"})

			// Catalog auth (OAuth2) — optional
			if cfg.IcebergClientID != "" {
				bootSQL = append(bootSQL, bootStatement{"create iceberg secret", fmt.Sprintf(
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
				bootSQL = append(bootSQL, bootStatement{"create s3 secret", fmt.Sprintf(
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
			attachSQL += ")"
			bootSQL = append(bootSQL, bootStatement{"attach iceberg catalog", attachSQL})

			// Make the Iceberg catalog the default so users can omit the "lake.default." prefix.
			bootSQL = append(bootSQL, bootStatement{"use lake.default", "USE lake.\"default\""})
		}

		for _, stmt := range bootSQL {
			if _, err := execer.ExecContext(context.Background(), stmt.sql, nil); err != nil {
				// Deliberately reports the label, not stmt.sql: the secret
				// statements carry credentials in cleartext.
				return fmt.Errorf("boot SQL failed (%s): %w", stmt.label, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB connector: %w", err)
	}

	pool, err := NewArrowPool(connector, cfg.PoolSize)
	if err != nil {
		_ = connector.Close()
		return nil, fmt.Errorf("failed to create arrow pool: %w", err)
	}

	return &Engine{connector: connector, Pool: pool, cfg: cfg}, nil
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
