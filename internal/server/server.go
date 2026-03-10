//go:build duckdb_arrow

package server

import (
	"context"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prochac/duckflight/internal/config"
	"github.com/prochac/duckflight/internal/engine"
)

// Config mirrors the test's expected configuration surface.
type Config struct {
	MemoryLimit    string
	MaxThreads     int
	QueryTimeout   string
	PoolSize       int
	MaxResultBytes int64

	// Iceberg catalog
	IcebergEndpoint     string
	IcebergWarehouse    string
	IcebergClientID     string
	IcebergClientSecret string
	IcebergOAuth2URI    string

	// S3-compatible storage credentials
	S3Endpoint string
	S3AccessKey string
	S3SecretKey string
	S3Region    string
	S3URLStyle  string
}

// DuckFlightSQLServer implements the FlightSQL server interface backed by DuckDB.
type DuckFlightSQLServer struct {
	flightsql.BaseServer

	engine           *engine.Engine
	preparedStmts    sync.Map // handle string -> preparedStatement
	openTransactions sync.Map // handle string -> *engine.ArrowConn
	queryTimeout     time.Duration
	maxResultBytes   int64
}

// globalEngine is used by the SeedSQL helper for test setup.
var globalEngine *engine.Engine

// New creates a DuckFlightSQLServer with an in-memory DuckDB engine.
func New(cfg Config) (*DuckFlightSQLServer, error) {
	eng, err := engine.NewEngine(&config.Config{
		MemoryLimit:         cfg.MemoryLimit,
		MaxThreads:          cfg.MaxThreads,
		QueryTimeout:        cfg.QueryTimeout,
		PoolSize:            cfg.PoolSize,
		IcebergEndpoint:     cfg.IcebergEndpoint,
		IcebergWarehouse:    cfg.IcebergWarehouse,
		IcebergClientID:     cfg.IcebergClientID,
		IcebergClientSecret: cfg.IcebergClientSecret,
		IcebergOAuth2URI:    cfg.IcebergOAuth2URI,
		S3Endpoint:          cfg.S3Endpoint,
		S3AccessKey:         cfg.S3AccessKey,
		S3SecretKey:         cfg.S3SecretKey,
		S3Region:            cfg.S3Region,
		S3URLStyle:          cfg.S3URLStyle,
	})
	if err != nil {
		return nil, err
	}

	var timeout time.Duration
	if cfg.QueryTimeout != "" {
		timeout, err = time.ParseDuration(cfg.QueryTimeout)
		if err != nil {
			return nil, err
		}
	}

	srv := &DuckFlightSQLServer{
		engine:         eng,
		queryTimeout:   timeout,
		maxResultBytes: cfg.MaxResultBytes,
	}
	srv.Alloc = memory.DefaultAllocator
	_ = srv.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerTransaction, int32(flightsql.SqlTransactionTransaction))

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
