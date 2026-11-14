//go:build duckdb_arrow

package engine

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/fairtier/duckflight/internal/config"
)

// readOnlyDSN is applied when the read-only instance is opened, before any
// boot SQL runs. Persistent secrets would be read from the server user's home
// directory; the read-only instance gets exactly the secrets boot SQL creates
// and nothing else. Community extensions are never needed there.
// ReadOnlyPoolLabel is the pool=… metric attribute of the read-only
// instance's pool and session manager.
const ReadOnlyPoolLabel = "read_only"

const readOnlyDSN = "?allow_persistent_secrets=false&allow_community_extensions=false"

// NewReadOnlyEngine creates the separate in-memory DuckDB instance that serves
// read-only sessions. cfg.ReadOnly must be non-nil.
//
// It is a second instance, not a second pool on the main one, because every
// setting that confines DuckDB is instance-wide (and one-way):
// enable_external_access, allowed_directories and lock_configuration cannot
// be applied per connection, and applying them to the main instance would
// break its writers. The instance boots like the main one — extensions,
// secrets, the Iceberg catalog (attached READ_ONLY) — and then, once, before
// the first connection is handed out:
//
//	SET allowed_directories = [...]     -- warehouse prefix(es) only
//	SET enable_external_access = false  -- no other file, URL or extension load
//	SET lock_configuration = true       -- no SET/RESET/PRAGMA can undo any of it
//
// Every later connection on the instance only runs USE lake."default": the
// settings above are global and locked, and re-running the boot SET
// statements would fail against the lock.
//
// What still works after that, verified against Lakekeeper + S3 in
// test/iceberg_integration_test.go: Iceberg reads of tables existing at boot
// and created afterwards (the REST catalog's HTTP traffic is not subject to
// the file-system allowlist), iceberg_snapshots(), and spilling to
// temp_directory (DuckDB allows its own temp directory implicitly).
func NewReadOnlyEngine(cfg *config.Config) (*Engine, error) {
	if cfg.ReadOnly == nil {
		return nil, fmt.Errorf("read-only engine requested without read-only config")
	}
	boot, bestEffort := bootPlan(cfg, true)
	harden := hardenSQL(readOnlyAllowedDirectories(cfg))
	var perConn []bootStatement
	if cfg.IcebergEndpoint != "" {
		perConn = []bootStatement{useLakeDefault}
	}

	var (
		mu       sync.Mutex
		hardened bool
	)
	connector, err := duckdb.NewConnector(readOnlyDSN, func(execer driver.ExecerContext) error {
		mu.Lock()
		defer mu.Unlock()
		if hardened {
			runBestEffort(execer, perConn)
			return nil
		}
		if err := runBoot(execer, boot); err != nil {
			return err
		}
		runBestEffort(execer, bestEffort)
		if err := runBoot(execer, harden); err != nil {
			return err
		}
		hardened = true
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create read-only DuckDB connector: %w", err)
	}
	poolSize := cfg.ReadOnly.PoolSize
	if poolSize <= 0 {
		poolSize = cfg.PoolSize
	}
	return newEngine(connector, cfg, poolSize, ReadOnlyPoolLabel)
}

// readOnlyAllowedDirectories resolves the configured allowlist, defaulting to
// every S3 path when an Iceberg catalog is configured: the catalog names the
// table locations, and they are not known before the first scan.
func readOnlyAllowedDirectories(cfg *config.Config) []string {
	if cfg.ReadOnly.AllowedDirectories != nil {
		return cfg.ReadOnly.AllowedDirectories
	}
	if cfg.IcebergEndpoint != "" {
		return []string{"s3://"}
	}
	return nil
}

// hardenSQL is the one-way lockdown of the read-only instance. Order matters:
// allowed_directories can only be set while external access is still on, and
// lock_configuration must come last because it also locks itself.
func hardenSQL(allowed []string) []bootStatement {
	var out []bootStatement
	if len(allowed) > 0 {
		quoted := make([]string, len(allowed))
		for i, d := range allowed {
			quoted[i] = quoteLiteral(d)
		}
		out = append(out, bootStatement{
			"set allowed_directories", "SET allowed_directories = [" + strings.Join(quoted, ", ") + "]",
		})
	}
	return append(out,
		bootStatement{"disable external access", "SET enable_external_access = false"},
		bootStatement{"lock configuration", "SET lock_configuration = true"},
	)
}
