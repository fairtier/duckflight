//go:build duckdb_arrow

package server

import (
	"context"
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
	createdAt time.Time
}

// BeginTransaction starts a transaction. If the caller has an authenticated
// session, the transaction binds to that session's connection (so subsequent
// statements without an explicit txnID still see the open transaction);
// otherwise it pins a one-off pool connection for the txn's lifetime.
//
// If the underlying connection is already in a DuckDB transaction — because
// a concurrent client layer (e.g. SQLAlchemy's dialect autobegin) ran a raw
// SQL BEGIN first — we attach to that existing transaction rather than
// failing; see [isRedundantTxnCollision].
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

	// Pre-flight: avoid issuing BEGIN if DuckDB is already in an explicit
	// transaction on this connection (e.g. a concurrent client layer ran SQL
	// BEGIN first). Running BEGIN inside a txn doesn't just fail — it leaves
	// DuckDB's txn in an aborted state, breaking every subsequent statement.
	alreadyInTxn, probeErr := ac.InExplicitTransaction(ctx)
	if probeErr != nil {
		span.RecordError(probeErr)
		release()
		return nil, status.Errorf(codes.Internal, "failed to probe transaction state: %s", probeErr)
	}

	if !alreadyInTxn {
		if _, err := ac.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
			span.RecordError(err)
			release()
			return nil, status.Errorf(codes.Internal, "failed to begin transaction: %s", err)
		}
		// Force DuckDB to take a snapshot immediately by reading from a real
		// table. Without this, the snapshot is deferred to the first actual
		// statement, which breaks snapshot isolation guarantees for the client.
		rdr, err := ac.Arrow.QueryContext(ctx, "SELECT 0 FROM duckdb_tables() LIMIT 0")
		if err != nil {
			span.RecordError(err)
			_, _ = ac.ExecContext(ctx, "ROLLBACK")
			release()
			return nil, status.Errorf(codes.Internal, "failed to initialize transaction snapshot: %s", err)
		}
		rdr.Release()
	}
	// else: attach the new Flight handle to the existing DuckDB txn. The
	// existing txn already took its snapshot when it opened.

	state := &txnState{createdAt: time.Now()}
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
// connection isn't touched concurrently with another RPC. Tolerates the case
// where the DuckDB transaction was already finalized via raw SQL by a
// concurrent client layer (see [isRedundantTxnCollision]).
func (s *DuckFlightSQLServer) EndTransaction(
	ctx context.Context,
	req flightsql.ActionEndTransactionRequest,
) error {
	if req.GetAction() == flightsql.EndTransactionUnspecified {
		return status.Error(codes.InvalidArgument, "must specify Commit or Rollback")
	}

	var op, endSQL string
	switch req.GetAction() {
	case flightsql.EndTransactionCommit:
		op, endSQL = "commit", "COMMIT"
	case flightsql.EndTransactionRollback:
		op, endSQL = "rollback", "ROLLBACK"
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

	// Resolve the conn for the end-of-txn statement and the cleanup callback.
	var (
		ac      *engine.ArrowConn
		release func()
	)
	if ts.sid != "" {
		sess, rel, err := s.sessions.Acquire(ctx, ts.sid)
		if err != nil {
			return err
		}
		ac, release = sess.Conn(), rel
	} else {
		ac, release = ts.conn, func() { s.engine.Pool.Release(ts.conn) }
	}
	defer release()

	// Pre-flight: if DuckDB isn't actually in an explicit transaction (a
	// concurrent client layer may have already committed/rolled back via SQL),
	// the corresponding COMMIT/ROLLBACK would error with "no transaction is
	// active". Skip it.
	inTxn, probeErr := ac.InExplicitTransaction(ctx)
	if probeErr != nil {
		span.RecordError(probeErr)
		return status.Errorf(codes.Internal, "failed to probe transaction state: %s", probeErr)
	}
	if !inTxn {
		return nil
	}

	if _, err := ac.ExecContext(ctx, endSQL); err != nil {
		span.RecordError(err)
		return status.Errorf(codes.Internal, "failed to %s: %s", op, err)
	}
	return nil
}
