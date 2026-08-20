//go:build duckdb_arrow

package engine

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"go.opentelemetry.io/otel/trace"
)

// ArrowConn wraps a raw DuckDB connection and its Arrow interface.
type ArrowConn struct {
	conn  driver.Conn
	Arrow *duckdb.Arrow

	// dirty records that a statement which mutates connection-local state
	// (SET, PRAGMA, ATTACH, CREATE TEMP …) ran on this connection, so the
	// pool must not hand it to another client. See [ArrowConn.MarkDirty].
	dirty atomic.Bool
}

// MarkDirty flags the connection as carrying client-visible connection-local
// state. A dirty connection is destroyed and replaced instead of being reused
// when it is returned to the pool.
func (ac *ArrowConn) MarkDirty() { ac.dirty.Store(true) }

// IsDirty reports whether [ArrowConn.MarkDirty] was called on this connection.
func (ac *ArrowConn) IsDirty() bool { return ac.dirty.Load() }

// ExecContext executes a SQL statement that does not return rows.
func (ac *ArrowConn) ExecContext(ctx context.Context, query string, args ...any) (int64, error) {
	execer, ok := ac.conn.(driver.ExecerContext)
	if !ok {
		return 0, errNoExecer
	}
	var named []driver.NamedValue
	for i, a := range args {
		named = append(named, driver.NamedValue{Ordinal: i + 1, Value: a})
	}
	result, err := execer.ExecContext(ctx, query, named)
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return n, nil
}

// BeginTx starts a transaction on the connection.
func (ac *ArrowConn) BeginTx(ctx context.Context) (driver.Tx, error) {
	beginner, ok := ac.conn.(driver.ConnBeginTx)
	if !ok {
		return nil, errNoBeginner
	}
	return beginner.BeginTx(ctx, driver.TxOptions{})
}

// TxnIntent classifies a transaction-control statement's direction —
// whether it starts a new transaction or ends an existing one. Returned by
// [ArrowConn.ClassifyTxnStatement] alongside the parser-level confirmation
// that the statement is in fact transaction-control.
type TxnIntent int

const (
	TxnIntentNone  TxnIntent = iota // not a transaction-control statement
	TxnIntentBegin                  // BEGIN / START TRANSACTION
	TxnIntentEnd                    // COMMIT / ROLLBACK / ABORT / END
)

// statementType asks DuckDB's parser to classify query. Returns
// STATEMENT_TYPE_INVALID when the statement cannot be prepared (syntax error,
// unknown table, …); callers treat that as "no classification available".
func (ac *ArrowConn) statementType(ctx context.Context, query string) duckdb.StmtType {
	preparer, ok := ac.conn.(driver.ConnPrepareContext)
	if !ok {
		return duckdb.STATEMENT_TYPE_INVALID
	}
	stmt, err := preparer.PrepareContext(ctx, query)
	if err != nil {
		return duckdb.STATEMENT_TYPE_INVALID
	}
	defer func() { _ = stmt.Close() }()

	duckStmt, ok := stmt.(*duckdb.Stmt)
	if !ok {
		return duckdb.STATEMENT_TYPE_INVALID
	}
	t, err := duckStmt.StatementType()
	if err != nil {
		return duckdb.STATEMENT_TYPE_INVALID
	}
	return t
}

// mutatesConnState reports whether a statement of type t leaves state behind
// on the connection that a subsequent borrower of the same connection would
// observe — settings (SET/PRAGMA), attached databases, temporary objects, or
// an open transaction. Such connections are recycled rather than pooled.
//
// CREATE/DROP/ALTER are included because DuckDB's statement type does not
// distinguish `CREATE TEMP TABLE` from `CREATE TABLE`; recycling on schema DDL
// is cheap relative to how rarely analytics clients issue it.
func mutatesConnState(t duckdb.StmtType) bool {
	switch t {
	case duckdb.STATEMENT_TYPE_SELECT,
		duckdb.STATEMENT_TYPE_INSERT,
		duckdb.STATEMENT_TYPE_UPDATE,
		duckdb.STATEMENT_TYPE_DELETE,
		duckdb.STATEMENT_TYPE_EXPLAIN,
		duckdb.STATEMENT_TYPE_ANALYZE,
		duckdb.STATEMENT_TYPE_RELATION,
		duckdb.STATEMENT_TYPE_LOGICAL_PLAN:
		return false
	case duckdb.STATEMENT_TYPE_INVALID:
		// Unparseable: it cannot have run, so it cannot have dirtied anything.
		return false
	default:
		return true
	}
}

// ClassifyStatement parses query with DuckDB and, as a side effect, marks the
// connection dirty when the statement would leave connection-local state
// behind. It returns the transaction intent so callers can decide whether a
// redundant BEGIN/COMMIT should be skipped, and DuckDB's own statement type
// so callers can enforce statement-level policy (the server uses it to reject
// client-issued INSTALL/LOAD — both surface as STATEMENT_TYPE_LOAD).
//
// The "begin vs end" decision uses the first SQL keyword, which is safe
// because DuckDB's parser has already validated the statement is in the
// TRANSACTION category — within that category only BEGIN/START open a
// transaction and COMMIT/ROLLBACK/ABORT/END close one.
func (ac *ArrowConn) ClassifyStatement(ctx context.Context, query string) (TxnIntent, duckdb.StmtType) {
	t := ac.statementType(ctx, query)
	if mutatesConnState(t) {
		ac.MarkDirty()
	}
	if t != duckdb.STATEMENT_TYPE_TRANSACTION {
		return TxnIntentNone, t
	}

	fields := strings.Fields(query)
	if len(fields) == 0 {
		return TxnIntentNone, t
	}
	switch strings.ToUpper(fields[0]) {
	case "BEGIN", "START":
		return TxnIntentBegin, t
	case "COMMIT", "ROLLBACK", "ABORT", "END":
		return TxnIntentEnd, t
	}
	return TxnIntentNone, t
}

// ClassifyTxnStatement reports the transaction intent of query without
// touching the connection's dirty flag. Prefer [ArrowConn.ClassifyStatement]
// on paths that go on to execute the statement.
func (ac *ArrowConn) ClassifyTxnStatement(ctx context.Context, query string) TxnIntent {
	if ac.statementType(ctx, query) != duckdb.STATEMENT_TYPE_TRANSACTION {
		return TxnIntentNone
	}
	fields := strings.Fields(query)
	if len(fields) == 0 {
		return TxnIntentNone
	}
	switch strings.ToUpper(fields[0]) {
	case "BEGIN", "START":
		return TxnIntentBegin
	case "COMMIT", "ROLLBACK", "ABORT", "END":
		return TxnIntentEnd
	}
	return TxnIntentNone
}

// IsTransactionStatement reports whether query is a transaction-control
// statement (BEGIN, COMMIT, ROLLBACK, …) as classified by DuckDB's own
// parser. Thin shorthand over [ArrowConn.ClassifyTxnStatement].
func (ac *ArrowConn) IsTransactionStatement(ctx context.Context, query string) bool {
	return ac.ClassifyTxnStatement(ctx, query) != TxnIntentNone
}

// InExplicitTransaction reports whether the connection is currently inside an
// explicit DuckDB transaction (the kind started by BEGIN/START TRANSACTION).
//
// Detection trick: “current_transaction_id()“ is stable across all
// statements within a single explicit transaction, but increments for every
// statement in autocommit mode (because each statement is its own implicit
// transaction). Two consecutive probes that return the same id mean we're
// inside an explicit transaction; differing ids mean autocommit.
//
// Used by [server.DuckFlightSQLServer.BeginTransaction] to avoid issuing a
// BEGIN that would collide with a transaction another client layer already
// opened (such collisions abort DuckDB's txn even when the BEGIN error is
// otherwise swallowed). Avoids substring matching of error messages or
// stateful tracking that could drift from DuckDB's view of the world.
func (ac *ArrowConn) InExplicitTransaction(ctx context.Context) (bool, error) {
	const q = "SELECT current_transaction_id()"
	id1, err := scanTxnID(ctx, ac, q)
	if err != nil {
		return false, err
	}
	id2, err := scanTxnID(ctx, ac, q)
	if err != nil {
		return false, err
	}
	return id1 == id2, nil
}

func scanTxnID(ctx context.Context, ac *ArrowConn, q string) (int64, error) {
	queryer, ok := ac.conn.(driver.QueryerContext)
	if !ok {
		return 0, errNoQueryer
	}
	rows, err := queryer.QueryContext(ctx, q, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	dest := []driver.Value{nil}
	if err := rows.Next(dest); err != nil {
		return 0, err
	}
	switch v := dest[0].(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	default:
		return 0, errUnexpectedTxnIDType
	}
}

var (
	errNoQueryer           = errors.New("connection does not implement driver.QueryerContext")
	errNoExecer            = errors.New("connection does not implement driver.ExecerContext")
	errNoBeginner          = errors.New("connection does not implement driver.ConnBeginTx")
	errUnexpectedTxnIDType = errors.New("unexpected current_transaction_id() type")
	// ErrPoolClosed is returned by Acquire once the pool has been closed.
	ErrPoolClosed = errors.New("connection pool is closed")
)

// ArrowPool is a bounded pool of Arrow-enabled DuckDB connections.
//
// Connections are sanitized on release: any transaction the client left open
// is rolled back, and a connection that accumulated connection-local state is
// destroyed and replaced with a freshly booted one. Without that, a client
// that disconnects mid-transaction would hand the next borrower a connection
// sitting inside its transaction — the next BEGIN gets skipped as redundant
// and the two clients silently share one transaction.
type ArrowPool struct {
	pool chan *ArrowConn
	// newConn boots a replacement connection (running the connector's boot
	// SQL, so replacements start from the same state as the originals).
	newConn func() (*ArrowConn, error)
	metrics *poolMetrics

	mu     sync.Mutex
	closed bool
	// live tracks every connection the pool owns, including ones currently
	// checked out, so Close can shut all of them down.
	live map[*ArrowConn]struct{}
}

// NewArrowPool creates a pool of size Arrow connections from the given connector.
func NewArrowPool(connector *duckdb.Connector, size int) (*ArrowPool, error) {
	if size < 1 {
		return nil, fmt.Errorf("pool size must be at least 1, got %d", size)
	}
	p := &ArrowPool{
		pool: make(chan *ArrowConn, size),
		live: make(map[*ArrowConn]struct{}, size),
	}
	// Before the boot loop below: a failure there calls Close, which stops the
	// metrics registration.
	p.metrics = newPoolMetrics(p)
	p.newConn = func() (*ArrowConn, error) {
		conn, err := connector.Connect(context.Background())
		if err != nil {
			return nil, err
		}
		ar, err := duckdb.NewArrowFromConn(conn)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		return &ArrowConn{conn: conn, Arrow: ar}, nil
	}

	for i := 0; i < size; i++ {
		ac, err := p.newConn()
		if err != nil {
			p.Close()
			return nil, err
		}
		p.track(ac)
		p.pool <- ac
	}
	return p, nil
}

func (p *ArrowPool) track(ac *ArrowConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.live[ac] = struct{}{}
}

// Acquire blocks until an ArrowConn is available or ctx is canceled.
func (p *ArrowPool) Acquire(ctx context.Context) (*ArrowConn, error) {
	start := time.Now()
	// The wait is measured on every path, failed ones included: an acquire that
	// ends in a deadline is precisely the saturation signal worth keeping.
	defer func() { p.metrics.acquireDuration.Record(ctx, time.Since(start).Seconds()) }()

	select {
	case ac, ok := <-p.pool:
		if !ok {
			return nil, ErrPoolClosed
		}
		return ac, nil
	default:
	}

	// Nothing idle. An event rather than a span: the wait is one moment in the
	// caller's span, and it says why that span is about to look slow.
	trace.SpanFromContext(ctx).AddEvent("pool.exhausted")

	select {
	case ac, ok := <-p.pool:
		if !ok {
			return nil, ErrPoolClosed
		}
		return ac, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TryAcquire returns an idle connection without blocking, or (nil, false) if
// the pool is empty or closed.
func (p *ArrowPool) TryAcquire() (*ArrowConn, bool) {
	select {
	case ac, ok := <-p.pool:
		if !ok {
			return nil, false
		}
		return ac, true
	default:
		return nil, false
	}
}

// Release returns an ArrowConn to the pool, recycling it first if it carries
// client-visible state. Callers must not touch ac after calling Release.
//
// A connection is only pooled again when it is provably clean: every statement
// the server runs on a client's behalf is classified first (see
// [ArrowConn.ClassifyStatement]), and anything that can leave state behind —
// BEGIN, SET, PRAGMA, ATTACH, DDL — marks the connection dirty. A dirty
// connection is destroyed rather than reused. Without that, a client that
// disconnects mid-transaction hands the next borrower a connection sitting
// inside its transaction: the next BEGIN gets skipped as redundant and the two
// clients silently share one transaction, with the second one's COMMIT
// committing the first one's writes.
func (p *ArrowPool) Release(ac *ArrowConn) {
	if ac == nil {
		return
	}
	if ac.IsDirty() {
		p.discard(ac, recycleDirty)
		return
	}
	p.put(ac)
}

// Discard destroys ac and replaces it with a freshly booted connection. Use it
// for connections whose client vanished while connection-local state (temp
// tables, SET, ATTACH, an open transaction) may still be present.
//
// Closing is what makes this airtight: DuckDB rolls back the connection's
// active transaction and drops its temporary objects and setting overrides
// when the connection goes away, and the replacement re-runs the connector's
// boot SQL, so it starts from exactly the same state as the originals.
// Callers must not touch ac afterwards.
func (p *ArrowPool) Discard(ac *ArrowConn) {
	p.discard(ac, recycleDiscarded)
}

func (p *ArrowPool) discard(ac *ArrowConn, reason string) {
	if ac == nil {
		return
	}
	p.metrics.recordRecycled(reason)
	p.mu.Lock()
	closed := p.closed
	delete(p.live, ac)
	p.mu.Unlock()
	_ = ac.conn.Close()
	if closed {
		return
	}

	replacement, err := p.newConn()
	if err != nil {
		// Losing a connection shrinks the pool but keeps the server serving;
		// a permanently smaller pool is far better than handing out a dirty
		// or closed connection.
		p.metrics.lost.Add(context.Background(), 1)
		slog.Error("failed to replace pooled connection", slog.String("error", err.Error()))
		return
	}
	p.track(replacement)
	p.put(replacement)
}

// put returns ac to the idle channel, or closes it if the pool is shutting
// down. The lock makes the closed-check and the send atomic with respect to
// Close, so a late Release can never push into a drained or closed pool.
func (p *ArrowPool) put(ac *ArrowConn) {
	p.mu.Lock()
	if !p.closed {
		// Non-blocking: the pool can only be full if a connection was
		// released twice, and blocking here while holding the lock would
		// wedge the whole pool. Drop the surplus connection instead.
		select {
		case p.pool <- ac:
			p.mu.Unlock()
			return
		default:
			slog.Error("pool overflow on release; closing surplus connection")
		}
	}
	delete(p.live, ac)
	p.mu.Unlock()
	_ = ac.conn.Close()
}

// Len returns the number of currently available connections in the pool.
func (p *ArrowPool) Len() int { return len(p.pool) }

// Cap returns the total capacity of the pool.
func (p *ArrowPool) Cap() int { return cap(p.pool) }

// Close closes every connection the pool owns, including ones still checked
// out. Releasing a connection after Close closes it instead of pooling it.
func (p *ArrowPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.metrics.stop()
	conns := make([]*ArrowConn, 0, len(p.live))
	for ac := range p.live {
		conns = append(conns, ac)
	}
	p.live = make(map[*ArrowConn]struct{})
	// Closing the channel makes a blocked or subsequent Acquire fail with
	// ErrPoolClosed instead of hanging forever. Sends are guarded by closed
	// under this same lock, so no send can race with the close.
	close(p.pool)
	p.mu.Unlock()

	// Drain whatever was idle, then close everything the pool owned —
	// including connections still checked out by in-flight calls.
	for range p.pool { //nolint:revive // draining
	}
	for _, ac := range conns {
		_ = ac.conn.Close()
	}
}
