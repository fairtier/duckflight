//go:build duckdb_arrow

package server

import (
	"context"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
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
	registerSqlInfo(srv)

	globalEngine = eng
	return srv, nil
}

func registerSqlInfo(srv *DuckFlightSQLServer) {
	reg := func(id flightsql.SqlInfo, val interface{}) {
		_ = srv.RegisterSqlInfo(id, val)
	}

	// Server identity
	reg(flightsql.SqlInfoFlightSqlServerName, "DuckFlight")
	reg(flightsql.SqlInfoFlightSqlServerVersion, "0.1.0")
	reg(flightsql.SqlInfoFlightSqlServerArrowVersion, arrow.PkgVersion)
	reg(flightsql.SqlInfoFlightSqlServerReadOnly, false)
	reg(flightsql.SqlInfoFlightSqlServerSql, true)
	reg(flightsql.SqlInfoFlightSqlServerSubstrait, false)

	// Transaction support
	reg(flightsql.SqlInfoFlightSqlServerTransaction, int32(flightsql.SqlTransactionTransaction))
	reg(flightsql.SqlInfoTransactionsSupported, true)

	// SQL capabilities
	reg(flightsql.SqlInfoDDLCatalog, false)
	reg(flightsql.SqlInfoDDLSchema, false)
	reg(flightsql.SqlInfoDDLTable, true)
	reg(flightsql.SqlInfoIdentifierCase, int64(flightsql.SqlCaseSensitivityCaseInsensitive))
	reg(flightsql.SqlInfoIdentifierQuoteChar, `"`)
	reg(flightsql.SqlInfoQuotedIdentifierCase, int64(flightsql.SqlCaseSensitivityCaseInsensitive))
	reg(flightsql.SqlInfoAllTablesAreASelectable, true)
	reg(flightsql.SqlInfoNullOrdering, int64(flightsql.SqlNullOrderingSortAtEnd))
	reg(flightsql.SqlInfoSearchStringEscape, `\`)
	reg(flightsql.SqlInfoExtraNameChars, "")
	reg(flightsql.SqlInfoSupportsColumnAliasing, true)
	reg(flightsql.SqlInfoNullPlusNullIsNull, true)
	reg(flightsql.SqlInfoSupportsTableCorrelationNames, true)
	reg(flightsql.SqlInfoSupportsExpressionsInOrderBy, true)
	reg(flightsql.SqlInfoSupportsOrderByUnrelated, true)
	reg(flightsql.SqlInfoSupportedGroupBy, int64(2)) // SQL_GROUP_BY_BEYOND_SELECT bitmask
	reg(flightsql.SqlInfoSupportsLikeEscapeClause, true)

	reg(flightsql.SqlInfoKeywords, fetchKeywords(srv.engine))
	reg(flightsql.SqlInfoNumericFunctions, []string{})
	reg(flightsql.SqlInfoStringFunctions, []string{})
	reg(flightsql.SqlInfoSystemFunctions, []string{})
	reg(flightsql.SqlInfoDateTimeFunctions, []string{})

	// Cancellation & timeout
	reg(flightsql.SqlInfoFlightSqlServerCancel, false)
	reg(flightsql.SqlInfoFlightSqlServerStatementTimeout, int32(0))
	reg(flightsql.SqlInfoFlightSqlServerTransactionTimeout, int32(0))

	// Bulk ingestion
	reg(flightsql.SqlInfoFlightSqlServerBulkIngestion, false)
	reg(flightsql.SqlInfoFlightSqlServerIngestTransactionsSupported, false)
}

// fetchKeywords queries DuckDB for its reserved keywords list.
func fetchKeywords(eng *engine.Engine) []string {
	ac, err := eng.Pool.Acquire(context.Background())
	if err != nil {
		return []string{}
	}
	defer eng.Pool.Release(ac)

	rdr, err := ac.Arrow.QueryContext(context.Background(), "SELECT keyword_name FROM duckdb_keywords() ORDER BY keyword_name")
	if err != nil {
		return []string{}
	}
	defer rdr.Release()

	var keywords []string
	for rdr.Next() {
		rec := rdr.RecordBatch()
		col := rec.Column(0).(*array.String)
		for i := 0; i < col.Len(); i++ {
			keywords = append(keywords, col.Value(i))
		}
	}
	if rdr.Err() != nil {
		return []string{}
	}
	return keywords
}

// CloseSession is called by clients on disconnect. No-op since we don't track sessions.
func (s *DuckFlightSQLServer) CloseSession(_ context.Context, _ *flight.CloseSessionRequest) (*flight.CloseSessionResult, error) {
	return &flight.CloseSessionResult{Status: flight.CloseSessionResultClosed}, nil
}

// Engine returns the underlying engine for direct access.
func (s *DuckFlightSQLServer) Engine() *engine.Engine {
	return s.engine
}

// SeedSQL executes arbitrary SQL on the global engine for test setup.
func SeedSQL(ctx context.Context, sql string) error {
	return globalEngine.ExecSQL(ctx, sql)
}
