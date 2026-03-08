//go:build duckdb_arrow

package server

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prochac/duckflight/internal/config"
	"github.com/prochac/duckflight/internal/engine"
)

// Config mirrors the test's expected configuration surface.
type Config struct {
	MemoryLimit  string
	MaxThreads   int
	QueryTimeout string
	PoolSize     int
}

// DuckFlightSQLServer implements the FlightSQL server interface backed by DuckDB.
type DuckFlightSQLServer struct {
	flightsql.BaseServer

	engine           *engine.Engine
	mu               sync.RWMutex
	preparedStmts    sync.Map // handle string -> preparedStatement
	openTransactions sync.Map // handle string -> *engine.ArrowConn
}

// globalEngine is used by the SeedSQL helper for test setup.
var globalEngine *engine.Engine

// New creates a DuckFlightSQLServer with an in-memory DuckDB engine.
func New(cfg Config) (*DuckFlightSQLServer, error) {
	eng, err := engine.NewEngine(&config.Config{
		MemoryLimit:  cfg.MemoryLimit,
		MaxThreads:   cfg.MaxThreads,
		QueryTimeout: cfg.QueryTimeout,
		PoolSize:     cfg.PoolSize,
	})
	if err != nil {
		return nil, err
	}

	srv := &DuckFlightSQLServer{
		engine: eng,
	}
	srv.Alloc = memory.DefaultAllocator

	globalEngine = eng
	return srv, nil
}

// Engine returns the underlying engine for direct access.
func (s *DuckFlightSQLServer) Engine() *engine.Engine {
	return s.engine
}

// SeedSQL executes arbitrary SQL on the global engine for test setup.
func SeedSQL(ctx context.Context, sql string) error {
	return globalEngine.ExecSQL(ctx, sql)
}
