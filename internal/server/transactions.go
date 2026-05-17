//go:build duckdb_arrow

package server

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/fairtier/duckflight/internal/auth"
	"github.com/fairtier/duckflight/internal/engine"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// txnState tracks one open Flight transaction. Exactly one of (sid, conn) is
// meaningful: if sid != "", the transaction runs on that session's pinned
// connection; otherwise conn is a pool-borrowed connection held for the
// transaction's lifetime.
type txnState struct {
	sid       string
	conn      *engine.ArrowConn
	tx        driver.Tx
	createdAt time.Time
}

// BeginTransaction starts a transaction. If the caller has an authenticated
// session, the transaction binds to that session's connection (so subsequent
// statements without an explicit txnID still see the open transaction);
// otherwise it pins a one-off pool connection for the txn's lifetime.
func (s *DuckFlightSQLServer) BeginTransaction(
	ctx context.Context,
	_ flightsql.ActionBeginTransactionRequest,
) ([]byte, error) {
	ctx, span := s.tracer.Start(ctx, "transaction.begin")
	defer span.End()

	sid := auth.SessionIDFromContext(ctx)

	var (
		ac      *engine.ArrowConn
		release func()
	)
	if sid != "" {
		sess, rel, err := s.sessions.Acquire(ctx, sid)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		ac, release = sess.Conn(), rel
	} else {
		c, err := s.acquirePoolConn(ctx)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		ac, release = c, func() { s.engine.Pool.Release(c) }
	}

	tx, err := ac.BeginTx(ctx)
	if err != nil {
		span.RecordError(err)
		release()
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %s", err)
	}

	// Force DuckDB to take a snapshot immediately by reading from a real table.
	// Without this, the snapshot is deferred to the first actual statement,
	// which breaks snapshot isolation guarantees for the client.
	rdr, err := ac.Arrow.QueryContext(ctx, "SELECT 0 FROM duckdb_tables() LIMIT 0")
	if err != nil {
		span.RecordError(err)
		_ = tx.Rollback()
		release()
		return nil, status.Errorf(codes.Internal, "failed to initialize transaction snapshot: %s", err)
	}
	rdr.Release()

	state := &txnState{tx: tx, createdAt: time.Now()}
	if sid != "" {
		state.sid = sid
		// Drop the per-RPC session lock; future RPCs on this txn re-acquire it.
		release()
	} else {
		state.conn = ac
		// Anonymous: conn stays pinned until EndTransaction.
	}

	handle := genHandle()
	s.openTransactions.Store(string(handle), state)
	return handle, nil
}

// EndTransaction commits or rolls back a transaction. If it ran on a session,
// re-acquires the session lock for the commit/rollback so the underlying
// connection isn't touched concurrently with another RPC.
func (s *DuckFlightSQLServer) EndTransaction(
	ctx context.Context,
	req flightsql.ActionEndTransactionRequest,
) error {
	if req.GetAction() == flightsql.EndTransactionUnspecified {
		return status.Error(codes.InvalidArgument, "must specify Commit or Rollback")
	}

	var op string
	switch req.GetAction() {
	case flightsql.EndTransactionCommit:
		op = "commit"
	case flightsql.EndTransactionRollback:
		op = "rollback"
	}

	_, span := s.tracer.Start(ctx, "transaction.end",
		trace.WithAttributes(attribute.String("db.operation", op)))
	defer span.End()

	handle := string(req.GetTransactionId())
	val, loaded := s.openTransactions.LoadAndDelete(handle)
	if !loaded {
		return status.Error(codes.InvalidArgument, "transaction id not found")
	}
	ts := val.(*txnState)

	// Re-acquire the session lock (if any) so the commit/rollback driver call
	// is serialized with other RPCs on the same session. For anonymous txns we
	// just need to remember to release the pinned conn.
	var release func()
	if ts.sid != "" {
		_, rel, err := s.sessions.Acquire(ctx, ts.sid)
		if err != nil {
			return err
		}
		release = rel
	} else {
		release = func() { s.engine.Pool.Release(ts.conn) }
	}
	defer release()

	switch req.GetAction() {
	case flightsql.EndTransactionCommit:
		if err := ts.tx.Commit(); err != nil {
			span.RecordError(err)
			return status.Errorf(codes.Internal, "failed to commit: %s", err)
		}
	case flightsql.EndTransactionRollback:
		if err := ts.tx.Rollback(); err != nil {
			span.RecordError(err)
			return status.Errorf(codes.Internal, "failed to rollback: %s", err)
		}
	}
	return nil
}
