//go:build duckdb_arrow

package engine

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
)

// ArrowConn wraps a raw DuckDB connection and its Arrow interface.
type ArrowConn struct {
	conn  driver.Conn
	Arrow *duckdb.Arrow
}

// ExecContext executes a SQL statement that does not return rows.
func (ac *ArrowConn) ExecContext(ctx context.Context, query string, args ...any) (int64, error) {
	execer, ok := ac.conn.(driver.ExecerContext)
	if !ok {
		return 0, context.Canceled
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
		return nil, context.Canceled
	}
	return beginner.BeginTx(ctx, driver.TxOptions{})
}

// TxnIntent classifies a transaction-control statement's direction —
// whether it starts a new transaction or ends an existing one. Returned by
// [ArrowConn.ClassifyTxnStatement] alongside the parser-level confirmation
// that the statement is in fact transaction-control.
type TxnIntent int

const (
	TxnIntentNone TxnIntent = iota // not a transaction-control statement
	TxnIntentBegin                 // BEGIN / START TRANSACTION
	TxnIntentEnd                   // COMMIT / ROLLBACK / ABORT / END
)

// ClassifyTxnStatement asks DuckDB's parser whether query is a
// transaction-control statement and, if so, whether it begins or ends a
// transaction. The "begin vs end" decision uses the first SQL keyword,
// which is safe because DuckDB's parser has already validated the
// statement is in the TRANSACTION category — within that category only
// BEGIN/START open a transaction and COMMIT/ROLLBACK/ABORT/END close one.
//
// Returns TxnIntentNone on any parse error.
func (ac *ArrowConn) ClassifyTxnStatement(ctx context.Context, query string) TxnIntent {
	preparer, ok := ac.conn.(driver.ConnPrepareContext)
	if !ok {
		return TxnIntentNone
	}
	stmt, err := preparer.PrepareContext(ctx, query)
	if err != nil {
		return TxnIntentNone
	}
	defer func() { _ = stmt.Close() }()

	duckStmt, ok := stmt.(*duckdb.Stmt)
	if !ok {
		return TxnIntentNone
	}
	t, err := duckStmt.StatementType()
	if err != nil || t != duckdb.STATEMENT_TYPE_TRANSACTION {
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
// Detection trick: ``current_transaction_id()`` is stable across all
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
	errUnexpectedTxnIDType = errors.New("unexpected current_transaction_id() type")
)

// ArrowPool is a bounded pool of Arrow-enabled DuckDB connections.
type ArrowPool struct {
	pool chan *ArrowConn
}

// NewArrowPool creates a pool of size Arrow connections from the given connector.
func NewArrowPool(connector *duckdb.Connector, size int) (*ArrowPool, error) {
	p := &ArrowPool{pool: make(chan *ArrowConn, size)}
	for i := 0; i < size; i++ {
		conn, err := connector.Connect(context.Background())
		if err != nil {
			p.Close()
			return nil, err
		}
		ar, err := duckdb.NewArrowFromConn(conn)
		if err != nil {
			_ = conn.Close()
			p.Close()
			return nil, err
		}
		p.pool <- &ArrowConn{conn: conn, Arrow: ar}
	}
	return p, nil
}

// Acquire blocks until an ArrowConn is available or ctx is canceled.
func (p *ArrowPool) Acquire(ctx context.Context) (*ArrowConn, error) {
	select {
	case ac := <-p.pool:
		return ac, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Release returns an ArrowConn to the pool.
func (p *ArrowPool) Release(ac *ArrowConn) {
	p.pool <- ac
}

// Len returns the number of currently available connections in the pool.
func (p *ArrowPool) Len() int { return len(p.pool) }

// Cap returns the total capacity of the pool.
func (p *ArrowPool) Cap() int { return cap(p.pool) }

// Close drains and closes all connections in the pool.
func (p *ArrowPool) Close() {
	for {
		select {
		case ac := <-p.pool:
			_ = ac.conn.Close()
		default:
			return
		}
	}
}
