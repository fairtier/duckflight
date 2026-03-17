//go:build duckdb_arrow

package server

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prochac/duckflight/internal/config"
	"github.com/prochac/duckflight/internal/engine"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// DuckFlightSQLServer implements the FlightSQL server interface backed by DuckDB.
type DuckFlightSQLServer struct {
	flightsql.BaseServer

	engine           *engine.Engine
	preparedStmts    sync.Map // handle string -> preparedStatement
	openTransactions sync.Map // handle string -> *txnState
	queryTimeout     time.Duration
	maxResultBytes   int64
	tracker          *queryTracker
	resourceTTL      time.Duration
	stopCleanup      context.CancelFunc
	tracer           trace.Tracer
}

// globalEngine is used by the SeedSQL helper for test setup.
var globalEngine *engine.Engine

// New creates a DuckFlightSQLServer with an in-memory DuckDB engine.
func New(cfg *config.Config) (*DuckFlightSQLServer, error) {
	eng, err := engine.NewEngine(cfg)
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

	trackerTTL := 30 * time.Minute
	if timeout > 0 {
		trackerTTL = 2 * timeout
	}

	cleanupCtx, stopCleanup := context.WithCancel(context.Background())

	resourceTTL := 30 * time.Minute
	if timeout > 0 {
		resourceTTL = 2 * timeout
	}

	srv := &DuckFlightSQLServer{
		engine:         eng,
		queryTimeout:   timeout,
		maxResultBytes: cfg.MaxResultBytes,
		tracker:        &queryTracker{ttl: trackerTTL},
		resourceTTL:    resourceTTL,
		stopCleanup:    stopCleanup,
		tracer:         otel.Tracer("duckflight"),
	}
	srv.Alloc = memory.DefaultAllocator
	srv.tracker.StartCleanup(cleanupCtx)
	srv.startResourceCleanup(cleanupCtx)
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
	reg(flightsql.SqlInfoDDLCatalog, true)
	reg(flightsql.SqlInfoDDLSchema, true)
	reg(flightsql.SqlInfoDDLTable, true)
	reg(flightsql.SqlInfoIdentifierCase, int64(flightsql.SqlCaseSensitivityCaseInsensitive))
	reg(flightsql.SqlInfoIdentifierQuoteChar, `"`)
	reg(flightsql.SqlInfoQuotedIdentifierCase, int64(flightsql.SqlCaseSensitivityCaseInsensitive))
	reg(flightsql.SqlInfoAllTablesAreASelectable, true)
	reg(flightsql.SqlInfoNullOrdering, int64(flightsql.SqlNullOrderingSortAtEnd))
	reg(flightsql.SqlInfoSearchStringEscape, `\`)
	reg(flightsql.SqlInfoExtraNameChars, "")
	reg(flightsql.SqlInfoCatalogTerm, "database")
	reg(flightsql.SqlInfoSchemaTerm, "schema")
	reg(flightsql.SqlInfoCatalogAtStart, true)
	reg(flightsql.SqlInfoSupportsColumnAliasing, true)
	reg(flightsql.SqlInfoNullPlusNullIsNull, true)
	reg(flightsql.SqlInfoSupportsTableCorrelationNames, true)
	reg(flightsql.SqlInfoSupportsExpressionsInOrderBy, true)
	reg(flightsql.SqlInfoSupportsOrderByUnrelated, true)
	reg(flightsql.SqlInfoSupportedGroupBy, int64(2)) // SQL_GROUP_BY_BEYOND_SELECT bitmask
	reg(flightsql.SqlInfoSupportsLikeEscapeClause, true)

	reg(flightsql.SqlInfoSupportsConvert, buildConvertMap())

	reg(flightsql.SqlInfoKeywords, fetchKeywords(srv.engine))
	reg(flightsql.SqlInfoNumericFunctions, []string{})
	reg(flightsql.SqlInfoStringFunctions, []string{})
	reg(flightsql.SqlInfoSystemFunctions, []string{})
	reg(flightsql.SqlInfoDateTimeFunctions, []string{})

	// Cancellation & timeout
	reg(flightsql.SqlInfoFlightSqlServerCancel, true)
	reg(flightsql.SqlInfoFlightSqlServerStatementTimeout, int32(0))
	reg(flightsql.SqlInfoFlightSqlServerTransactionTimeout, int32(0))

	// Bulk ingestion
	reg(flightsql.SqlInfoFlightSqlServerBulkIngestion, true)
	reg(flightsql.SqlInfoFlightSqlServerIngestTransactionsSupported, true)
}

// buildConvertMap returns a SqlInfoSupportsConvert map describing which SQL
// type casts DuckDB supports. Keys are "convert from" types, values list the
// types each can be cast to.
func buildConvertMap() map[int32][]int32 {
	c := func(ids ...flightsql.SqlSupportsConvert) []int32 {
		out := make([]int32, len(ids))
		for i, id := range ids {
			out[i] = int32(id)
		}
		return out
	}

	numeric := c(
		flightsql.SqlConvertBigInt,
		flightsql.SqlConvertInteger,
		flightsql.SqlConvertSmallInt,
		flightsql.SqlConvertTinyInt,
		flightsql.SqlConvertFloat,
		flightsql.SqlConvertReal,
		flightsql.SqlConvertDecimal,
		flightsql.SqlConvertNumeric,
	)
	str := c(
		flightsql.SqlConvertChar,
		flightsql.SqlConvertVarchar,
		flightsql.SqlConvertLongVarchar,
	)
	bin := c(
		flightsql.SqlConvertBinary,
		flightsql.SqlConvertVarbinary,
		flightsql.SqlConvertLongVarbinary,
	)
	bit := c(flightsql.SqlConvertBit)

	// DuckDB numeric types → other numerics, strings, boolean
	numericTargets := slices.Concat(numeric, str, bit)
	// DuckDB varchar → nearly everything
	stringTargets := slices.Concat(numeric, str, bin, bit,
		c(flightsql.SqlConvertDate, flightsql.SqlConvertTime, flightsql.SqlConvertTimestamp,
			flightsql.SqlConvertIntervalDayTime, flightsql.SqlConvertIntervalYearMonth))

	m := make(map[int32][]int32, 20)

	for _, n := range numeric {
		m[n] = numericTargets
	}
	for _, s := range str {
		m[s] = stringTargets
	}
	for _, b := range bin {
		m[b] = slices.Concat(bin, str)
	}
	m[int32(flightsql.SqlConvertBit)] = slices.Concat(numeric, str)
	m[int32(flightsql.SqlConvertDate)] = slices.Concat(str,
		c(flightsql.SqlConvertTimestamp))
	m[int32(flightsql.SqlConvertTime)] = str
	m[int32(flightsql.SqlConvertTimestamp)] = slices.Concat(str,
		c(flightsql.SqlConvertDate, flightsql.SqlConvertTime))
	m[int32(flightsql.SqlConvertIntervalDayTime)] = str
	m[int32(flightsql.SqlConvertIntervalYearMonth)] = str

	return m
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

// startResourceCleanup runs a background goroutine that reaps stale prepared
// statements and abandoned transactions that clients failed to close.
func (s *DuckFlightSQLServer) startResourceCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				s.preparedStmts.Range(func(key, value any) bool {
					ps := value.(preparedStatement)
					if now.Sub(ps.createdAt) > s.resourceTTL {
						s.preparedStmts.Delete(key)
						slog.Warn("reaped stale prepared statement",
							"handle", key, "age", now.Sub(ps.createdAt))
					}
					return true
				})
				s.openTransactions.Range(func(key, value any) bool {
					ts := value.(*txnState)
					if now.Sub(ts.createdAt) > s.resourceTTL {
						s.openTransactions.Delete(key)
						_ = ts.tx.Rollback()
						s.engine.Pool.Release(ts.conn)
						slog.Warn("reaped stale transaction",
							"handle", key, "age", now.Sub(ts.createdAt))
					}
					return true
				})
			}
		}
	}()
}

// Engine returns the underlying engine for direct access.
func (s *DuckFlightSQLServer) Engine() *engine.Engine {
	return s.engine
}

// OpenTransactionCount returns the number of currently open transactions.
func (s *DuckFlightSQLServer) OpenTransactionCount() int {
	n := 0
	s.openTransactions.Range(func(_, _ any) bool { n++; return true })
	return n
}

// PreparedStatementCount returns the number of currently open prepared statements.
func (s *DuckFlightSQLServer) PreparedStatementCount() int {
	n := 0
	s.preparedStmts.Range(func(_, _ any) bool { n++; return true })
	return n
}

// ActiveQueryCount returns the number of queries currently being executed
// (i.e. picked up by DoGet and not yet completed).
func (s *DuckFlightSQLServer) ActiveQueryCount() int {
	n := 0
	s.tracker.queries.Range(func(_, val any) bool {
		qs := val.(*queryState)
		qs.mu.Lock()
		running := qs.cancel != nil
		qs.mu.Unlock()
		if !running {
			return true
		}
		select {
		case <-qs.done:
		default:
			n++
		}
		return true
	})
	return n
}

// SeedSQL executes arbitrary SQL on the global engine for test setup.
func SeedSQL(ctx context.Context, sql string) error {
	return globalEngine.ExecSQL(ctx, sql)
}
