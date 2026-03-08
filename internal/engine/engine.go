//go:build duckdb_arrow

package engine

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/prochac/duckflight/internal/config"
)

// Engine owns the DuckDB connector, runs boot SQL, and provides
// an Arrow connection pool for query execution.
type Engine struct {
	connector *duckdb.Connector
	Pool      *ArrowPool
	WriteMu   sync.Mutex
	cfg       *config.Config
}

// NewEngine creates a new in-memory DuckDB engine with the given config.
func NewEngine(cfg *config.Config) (*Engine, error) {
	connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		bootSQL := []string{
			"SET autoinstall_known_extensions = true",
			"SET autoload_known_extensions = true",
			fmt.Sprintf("SET memory_limit = '%s'", cfg.MemoryLimit),
			fmt.Sprintf("SET threads = %d", cfg.MaxThreads),
		}

		for _, sql := range bootSQL {
			if _, err := execer.ExecContext(context.Background(), sql, nil); err != nil {
				return fmt.Errorf("boot SQL failed (%s): %w", sql, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB connector: %w", err)
	}

	pool, err := NewArrowPool(connector, cfg.PoolSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow pool: %w", err)
	}

	return &Engine{connector: connector, Pool: pool, cfg: cfg}, nil
}

// Connector returns the underlying DuckDB connector.
func (e *Engine) Connector() *duckdb.Connector {
	return e.connector
}

// ExecSQL executes a SQL statement using a temporary connection from the connector.
func (e *Engine) ExecSQL(ctx context.Context, sql string) error {
	conn, err := e.connector.Connect(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	execer, ok := conn.(driver.ExecerContext)
	if !ok {
		return fmt.Errorf("connection does not implement ExecerContext")
	}

	if _, err := execer.ExecContext(ctx, sql, nil); err != nil {
		return fmt.Errorf("exec %q: %w", sql, err)
	}
	return nil
}

// Close shuts down the pool and the connector.
func (e *Engine) Close() error {
	e.Pool.Close()
	return e.connector.Close()
}
