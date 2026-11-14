//go:build duckdb_arrow

package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fairtier/duckflight/internal/auth"
	"github.com/fairtier/duckflight/internal/config"
	"github.com/fairtier/duckflight/internal/engine"
	"github.com/fairtier/duckflight/internal/otelutil"
	"github.com/fairtier/duckflight/internal/session"
	"github.com/fairtier/duckflight/internal/sqlguard"
	"go.opentelemetry.io/otel/trace"
)

const (
	// minResourceTTL is the floor for how long an idle session, transaction or
	// prepared statement is kept alive.
	minResourceTTL = 10 * time.Minute
	// minTicketTTL is the floor for how long a statement handle stays
	// resolvable. It is deliberately much shorter than minResourceTTL: a
	// handle only has to outlive a DoGet retry, and every GetFlightInfo
	// creates one, so tying it to the resource lifetime would grow the tracker
	// for ten minutes' worth of traffic.
	minTicketTTL = time.Minute
)

// DuckFlightSQLServer implements the FlightSQL server interface backed by DuckDB.
type DuckFlightSQLServer struct {
	flightsql.BaseServer

	// rw is the main DuckDB instance; ro the hardened read-only one, nil
	// unless read-only sessions are enabled. See [plane].
	rw               *plane
	ro               *plane
	preparedStmts    sync.Map // handle string -> *preparedStatement
	openTransactions sync.Map // handle string -> *txnState
	queryTimeout     time.Duration
	maxResultBytes   int64
	tracker          *queryTracker
	resourceTTL      time.Duration
	stopCleanup      context.CancelFunc
	closeOnce        sync.Once
	tracer           trace.Tracer
	gauges           *serverGauges
	// rejectClientExtensions refuses client-issued INSTALL/LOAD statements;
	// extensions are then managed exclusively through boot/reconcile SQL.
	rejectClientExtensions bool
	// guard classifies client SQL on its own isolated DuckDB instance. Only
	// opened when a policy needs it; nil otherwise.
	guard *sqlguard.Guard
	// roInfo holds the SqlInfo answered to read-only callers, which differs
	// from the main registration in what the session can do (read-only, no
	// DDL, no bulk ingestion). See [DuckFlightSQLServer.DoGetSqlInfo].
	roInfo flightsql.BaseServer
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

	// Idle lifetime for sessions, open transactions and prepared statements.
	// It scales with the query timeout so a long-running-query deployment
	// doesn't collect resources out from under itself, but never drops below
	// the floor: reaping a client's session after a minute of think time
	// destroys its temp tables and invalidates its open transactions.
	resourceTTL := max(2*timeout, minResourceTTL)

	// Statement handles are short-lived by comparison: they only need to
	// outlive a DoGet retry, not a client's whole idle window.
	ticketTTL := max(2*timeout, minTicketTTL)

	var guard *sqlguard.Guard
	if cfg.RejectClientExtensions || cfg.ReadOnly != nil {
		if guard, err = sqlguard.New(); err != nil {
			_ = eng.Close()
			return nil, err
		}
	}

	// Booted after the main instance, which creates lake."default" for the
	// read-only instance's USE to find.
	var roEng *engine.Engine
	if cfg.ReadOnly != nil {
		if roEng, err = engine.NewReadOnlyEngine(cfg); err != nil {
			guard.Close()
			_ = eng.Close()
			return nil, fmt.Errorf("read-only engine: %w", err)
		}
	}

	cleanupCtx, stopCleanup := context.WithCancel(context.Background())

	srv := &DuckFlightSQLServer{
		rw:                     &plane{engine: eng, sessions: session.NewManager(eng.Pool, resourceTTL)},
		queryTimeout:           timeout,
		maxResultBytes:         cfg.MaxResultBytes,
		tracker:                &queryTracker{ttl: ticketTTL},
		resourceTTL:            resourceTTL,
		stopCleanup:            stopCleanup,
		tracer:                 otelutil.Tracer(),
		rejectClientExtensions: cfg.RejectClientExtensions,
		guard:                  guard,
	}
	if roEng != nil {
		srv.ro = &plane{engine: roEng, sessions: session.NewManager(roEng.Pool, resourceTTL), readOnly: true}
	}
	srv.gauges = registerServerGauges(srv)
	srv.Alloc = memory.DefaultAllocator
	srv.roInfo.Alloc = memory.DefaultAllocator
	srv.tracker.StartCleanup(cleanupCtx)
	srv.startResourceCleanup(cleanupCtx)
	for _, p := range srv.planes() {
		p.sessions.StartReaper(cleanupCtx)
		if cfg.ReconcileSQLPath != "" {
			// Secrets are per instance, so rotation must reach both. On the
			// read-only instance, statements that need external access
			// (INSTALL/LOAD, ATTACH of a file) fail after its lockdown and are
			// logged by label like any other reconcile error.
			engine.NewReconciler(p.engine, cfg.ReconcileSQLPath).Start(cleanupCtx)
		}
	}
	registerSqlInfo(srv)

	globalEngine = eng
	return srv, nil
}

// planes returns the server's planes, the main one first.
func (s *DuckFlightSQLServer) planes() []*plane {
	if s.ro == nil {
		return []*plane{s.rw}
	}
	return []*plane{s.rw, s.ro}
}

// DoGetSqlInfo answers from the read-only registration for a read-only
// caller, so SqlInfoFlightSqlServerReadOnly (and DDL/ingestion support)
// describe the session the caller actually has.
func (s *DuckFlightSQLServer) DoGetSqlInfo(ctx context.Context, cmd flightsql.GetSqlInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if auth.ReadOnlyFromContext(ctx) {
		return s.roInfo.DoGetSqlInfo(ctx, cmd)
	}
	return s.BaseServer.DoGetSqlInfo(ctx, cmd)
}

func registerSqlInfo(srv *DuckFlightSQLServer) {
	readOnlyOverrides := map[flightsql.SqlInfo]any{
		flightsql.SqlInfoFlightSqlServerReadOnly:                    true,
		flightsql.SqlInfoDDLCatalog:                                 false,
		flightsql.SqlInfoDDLSchema:                                  false,
		flightsql.SqlInfoDDLTable:                                   false,
		flightsql.SqlInfoFlightSqlServerBulkIngestion:               false,
		flightsql.SqlInfoFlightSqlServerIngestTransactionsSupported: false,
	}
	reg := func(id flightsql.SqlInfo, val any) {
		_ = srv.RegisterSqlInfo(id, val)
		if ro, ok := readOnlyOverrides[id]; ok {
			val = ro
		}
		_ = srv.roInfo.RegisterSqlInfo(id, val)
	}

	// Server identity. The version string is prefixed with "duckdb " because some
	// clients (e.g. gizmosql SQLAlchemy dialect) gate behavior on the underlying
	// SQL engine vendor reported via SqlInfoFlightSqlServerVersion.
	reg(flightsql.SqlInfoFlightSqlServerName, "DuckFlight")
	reg(flightsql.SqlInfoFlightSqlServerVersion, "duckdb "+fetchDuckDBVersion(srv.rw.engine))
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

	reg(flightsql.SqlInfoKeywords, fetchKeywords(srv.rw.engine))
	reg(flightsql.SqlInfoNumericFunctions, []string{})
	reg(flightsql.SqlInfoStringFunctions, []string{})
	reg(flightsql.SqlInfoSystemFunctions, []string{})
	reg(flightsql.SqlInfoDateTimeFunctions, []string{})

	// Cancellation & timeout. Both are advertised in seconds; 0 means "no
	// limit", so reporting 0 while QUERY_TIMEOUT is set would promise clients
	// their statements never expire and then hand them DeadlineExceeded.
	reg(flightsql.SqlInfoFlightSqlServerCancel, true)
	reg(flightsql.SqlInfoFlightSqlServerStatementTimeout, timeoutSeconds(srv.queryTimeout))
	reg(flightsql.SqlInfoFlightSqlServerTransactionTimeout, timeoutSeconds(srv.resourceTTL))

	// Bulk ingestion
	reg(flightsql.SqlInfoFlightSqlServerBulkIngestion, true)
	reg(flightsql.SqlInfoFlightSqlServerIngestTransactionsSupported, true)
}

// timeoutSeconds renders a duration for the SqlInfo timeout fields, which are
// expressed in whole seconds with 0 meaning "no timeout". A sub-second timeout
// rounds up to 1 rather than down to "unlimited".
func timeoutSeconds(d time.Duration) int32 {
	if d <= 0 {
		return 0
	}
	secs := (d + time.Second - 1) / time.Second
	return int32(secs)
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

// fetchDuckDBVersion returns the DuckDB library version, e.g. "v1.2.0".
// Falls back to "unknown" if the query fails.
func fetchDuckDBVersion(eng *engine.Engine) string {
	ac, err := eng.Pool.Acquire(context.Background())
	if err != nil {
		return "unknown"
	}
	defer eng.Pool.Release(ac)

	rdr, err := ac.Arrow.QueryContext(context.Background(), "SELECT version()")
	if err != nil {
		return "unknown"
	}
	defer rdr.Release()

	if !rdr.Next() {
		return "unknown"
	}
	rec := rdr.RecordBatch()
	if rec.NumCols() == 0 || rec.NumRows() == 0 {
		return "unknown"
	}
	col, ok := rec.Column(0).(*array.String)
	if !ok || col.Len() == 0 || col.IsNull(0) {
		return "unknown"
	}
	// Clone: col.Value aliases the CGo-allocated Arrow buffer freed by rdr.Release.
	return strings.Clone(col.Value(0))
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
			// Clone: col.Value aliases the CGo-allocated Arrow buffer freed by rdr.Release.
			keywords = append(keywords, strings.Clone(col.Value(i)))
		}
	}
	if rdr.Err() != nil {
		return []string{}
	}
	return keywords
}

// CloseSession releases the DuckDB connection pinned to the caller's session
// (if any) back to the pool. Idempotent — calling on an unknown sid is a no-op.
func (s *DuckFlightSQLServer) CloseSession(ctx context.Context, _ *flight.CloseSessionRequest) (*flight.CloseSessionResult, error) {
	// A read-only request on a server without read-only sessions has no
	// session to close; that is not an error here.
	if sid := auth.SessionIDFromContext(ctx); sid != "" {
		if p, err := s.planeFor(ctx); err == nil {
			p.sessions.Close(sid)
		}
	}
	return &flight.CloseSessionResult{Status: flight.CloseSessionResultClosed}, nil
}

// startResourceCleanup runs a background goroutine that reaps prepared
// statements and transactions that clients abandoned without closing.
func (s *DuckFlightSQLServer) startResourceCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.reapResources(time.Now())
			}
		}
	}()
}

// reapResources collects prepared statements and transactions that have gone
// untouched for longer than resourceTTL. Idleness, not age, is what makes a
// resource collectable: a client working steadily inside a long transaction
// has not abandoned anything.
func (s *DuckFlightSQLServer) reapResources(now time.Time) {
	s.preparedStmts.Range(func(key, value any) bool {
		ps := value.(*preparedStatement)
		if idle := ps.idleFor(now); idle > s.resourceTTL {
			s.preparedStmts.Delete(key)
			slog.Warn("reaped stale prepared statement", "handle", key, "idle", idle)
		}
		return true
	})

	s.openTransactions.Range(func(key, value any) bool {
		ts := value.(*txnState)
		if ts.idleFor(now) <= s.resourceTTL {
			return true
		}
		// TryLock, so an in-flight DoGet still streaming on this connection is
		// never torn out from under it — rolling back and repooling the
		// connection mid-stream would hand a live DuckDB connection to a
		// second borrower.
		if !ts.mu.TryLock() {
			return true
		}
		idle := ts.idleFor(now)
		if idle <= s.resourceTTL {
			ts.mu.Unlock()
			return true
		}
		// LoadAndDelete decides ownership: if EndTransaction already claimed
		// this handle it is finalizing the transaction and releasing the
		// connection, and doing it here too would put the same connection in
		// the pool twice.
		if _, loaded := s.openTransactions.LoadAndDelete(key); !loaded {
			ts.mu.Unlock()
			return true
		}
		conn := ts.conn
		ts.conn = nil
		ts.mu.Unlock()

		// Session-bound txns share the session's pinned conn, which the
		// session manager owns. Only an anonymous txn's pool conn is ours.
		if conn != nil {
			// Release rolls back whatever the client left open and recycles
			// the connection rather than pooling its leftover state.
			ts.plane.engine.Pool.Release(conn)
		}
		txnCountAdd(context.Background(), "reaped")
		slog.Warn("reaped stale transaction", "handle", key, "idle", idle)
		return true
	})
}

// Close stops the background reapers, drops every session, and shuts down the
// DuckDB engine. Safe to call more than once. Callers should stop serving
// gRPC first so no request is using a connection when it closes.
func (s *DuckFlightSQLServer) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.gauges.stop()
		s.stopCleanup()
		var errs []error
		for _, p := range s.planes() {
			p.sessions.CloseAll()
			errs = append(errs, p.engine.Close())
		}
		err = errors.Join(errs...)
		if s.guard != nil {
			s.guard.Close()
		}
	})
	return err
}

// Engine returns the underlying engine for direct access.
func (s *DuckFlightSQLServer) Engine() *engine.Engine {
	return s.rw.engine
}

// ReadOnlyEngine returns the read-only instance's engine, or nil when
// read-only sessions are disabled. For tests that probe the instance below
// the statement policy.
func (s *DuckFlightSQLServer) ReadOnlyEngine() *engine.Engine {
	if s.ro == nil {
		return nil
	}
	return s.ro.engine
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
	return globalEngine.ExecSQL(ctx, "seed sql", sql)
}
