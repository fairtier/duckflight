//go:build duckdb_arrow

package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/fairtier/duckflight/internal/auth"
	"github.com/fairtier/duckflight/internal/ctxlock"
	"github.com/fairtier/duckflight/internal/engine"
	"github.com/fairtier/duckflight/internal/otelutil"
	"github.com/fairtier/duckflight/internal/session"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// txnState tracks one open Flight transaction. Exactly one of (sess, conn) is
// meaningful: if sess != nil, the transaction runs on that session's pinned
// connection; otherwise conn is a pool-borrowed connection held for the
// transaction's lifetime.
//
// The session is held as a pointer rather than as a session id so that a
// session which has since been reaped is detectable — re-resolving by id would
// silently mint a fresh, transaction-less connection and let a COMMIT report
// success without committing anything.
type txnState struct {
	sess      *session.Session
	createdAt time.Time
	lastUsed  atomic.Int64 // unix nanos

	// mu serializes use of conn across RPCs. DuckDB connections are not
	// goroutine-safe, and an anonymous transaction's connection is otherwise
	// reachable from any concurrent RPC carrying the same transaction id.
	mu   sync.Mutex
	conn *engine.ArrowConn
}

func newTxnState() *txnState {
	ts := &txnState{createdAt: time.Now()}
	ts.touch()
	return ts
}

func (ts *txnState) touch() { ts.lastUsed.Store(time.Now().UnixNano()) }

func (ts *txnState) idleFor(now time.Time) time.Duration {
	return now.Sub(time.Unix(0, ts.lastUsed.Load()))
}

// acquire locks the transaction for one call and returns its connection with
// the matching release function.
//
// ts.mu is taken for both kinds of transaction, session-bound included. It is
// what makes the transaction visibly in-use: the resource reaper TryLocks it
// before collecting, so a DoGet streaming for longer than the idle TTL can't
// have its handle deleted mid-stream and leave the client's EndTransaction
// reporting "transaction id not found". Lock order is always ts.mu → session
// lock, never the reverse.
func (ts *txnState) acquire(ctx context.Context) (*engine.ArrowConn, func(), error) {
	if err := ctxlock.Lock(ctx, &ts.mu); err != nil {
		return nil, nil, err
	}
	ts.touch()

	if ts.sess != nil {
		release, err := ts.sess.Lock(ctx)
		if err != nil {
			ts.mu.Unlock()
			return nil, nil, err
		}
		return ts.sess.Conn(), func() {
			ts.touch()
			release()
			ts.mu.Unlock()
		}, nil
	}

	if ts.conn == nil {
		ts.mu.Unlock()
		return nil, nil, status.Error(codes.FailedPrecondition,
			"transaction was reaped for inactivity and rolled back")
	}
	return ts.conn, func() { ts.touch(); ts.mu.Unlock() }, nil
}

// BeginTransaction starts a transaction. If the caller has an authenticated
// session, the transaction binds to that session's connection (so subsequent
// statements without an explicit txnID still see the open transaction);
// otherwise it pins a one-off pool connection for the txn's lifetime.
//
// If the underlying connection is already in a DuckDB transaction — because
// a concurrent client layer (e.g. SQLAlchemy's dialect autobegin) ran a raw
// SQL BEGIN first — we attach to that existing transaction rather than
// failing.
func (s *DuckFlightSQLServer) BeginTransaction(
	ctx context.Context,
	_ flightsql.ActionBeginTransactionRequest,
) ([]byte, error) {
	ctx, span := s.tracer.Start(ctx, "transaction.begin", trace.WithAttributes(
		dbSystem,
		attribute.String(attrDBOperation, "begin"),
	))
	defer span.End()

	sid := auth.SessionIDFromContext(ctx)

	var (
		ac      *engine.ArrowConn
		sess    *session.Session
		release func()
	)
	if sid != "" {
		span.SetAttributes(
			attribute.String(attrConnSource, connSourceSession),
			attribute.String(attrSessionID, sid),
		)
		sw, rel, err := s.sessions.Acquire(ctx, sid)
		if err != nil {
			return nil, otelutil.Failed(span, err)
		}
		sess, ac, release = sw, sw.Conn(), rel
	} else {
		span.SetAttributes(attribute.String(attrConnSource, connSourcePool))
		c, err := s.acquirePoolConn(ctx)
		if err != nil {
			return nil, otelutil.Failed(span, err)
		}
		ac, release = c, func() { s.engine.Pool.Release(c) }
	}

	// Pre-flight: avoid issuing BEGIN if DuckDB is already in an explicit
	// transaction on this connection (e.g. a concurrent client layer ran SQL
	// BEGIN first). Running BEGIN inside a txn doesn't just fail — it leaves
	// DuckDB's txn in an aborted state, breaking every subsequent statement.
	alreadyInTxn, probeErr := ac.InExplicitTransaction(ctx)
	if probeErr != nil {
		release()
		return nil, otelutil.Failed(span,
			status.Errorf(codes.Internal, "failed to probe transaction state: %s", probeErr))
	}

	if !alreadyInTxn {
		// Mark before issuing BEGIN, not after: if the connection ends up in a
		// transaction the pool must recycle it rather than hand it to the next
		// borrower, and that has to hold even if the call below fails midway.
		ac.MarkDirty()
		if _, err := ac.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
			release()
			return nil, otelutil.Failed(span,
				status.Errorf(codes.Internal, "failed to begin transaction: %s", err))
		}
		// Force DuckDB to take a snapshot immediately by reading from a real
		// table. Without this, the snapshot is deferred to the first actual
		// statement, which breaks snapshot isolation guarantees for the client.
		rdr, err := ac.Arrow.QueryContext(ctx, "SELECT 0 FROM duckdb_tables() LIMIT 0")
		if err != nil {
			_, _ = ac.ExecContext(ctx, "ROLLBACK")
			release()
			return nil, otelutil.Failed(span,
				status.Errorf(codes.Internal, "failed to initialize transaction snapshot: %s", err))
		}
		rdr.Release()
	} else {
		// Attach the new Flight handle to the existing DuckDB txn, which took
		// its snapshot when it opened. Recorded as an event because it changes
		// what the client's eventual COMMIT covers: writes another layer made
		// before this handle existed.
		span.AddEvent("transaction.attached_to_existing")
	}

	state := newTxnState()
	if sess != nil {
		state.sess = sess
		// Drop the per-RPC session lock; future RPCs on this txn re-acquire it.
		release()
	} else {
		state.conn = ac
		// Anonymous: conn stays pinned until EndTransaction.
	}

	handle := genHandle()
	s.openTransactions.Store(string(handle), state)
	span.SetAttributes(attribute.String(attrTransactionID, string(handle)))
	txnCountAdd(ctx, "begin")
	return handle, nil
}

// EndTransaction commits or rolls back a transaction. It re-acquires the
// connection's lock (the session lock, or the transaction's own mutex for
// anonymous transactions) so the commit can't run concurrently with a DoGet
// still streaming on that connection. Tolerates the case where the DuckDB
// transaction was already finalized via raw SQL by a concurrent client layer.
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

	handle := string(req.GetTransactionId())

	ctx, span := s.tracer.Start(ctx, "transaction.end", trace.WithAttributes(
		dbSystem,
		attribute.String(attrDBOperation, op),
		attribute.String(attrTransactionID, handle),
	))
	defer span.End()

	// LoadAndDelete makes this the single owner of the transaction: the
	// resource reaper uses the same call, so exactly one of the two ever gets
	// to finalize the transaction and hand its connection back.
	val, loaded := s.openTransactions.LoadAndDelete(handle)
	if !loaded {
		return status.Error(codes.InvalidArgument, "transaction id not found")
	}
	ts := val.(*txnState)

	ac, release, err := ts.acquire(ctx)
	if err != nil {
		// We deleted the handle but never got to touch the connection, so put
		// the transaction back: for an anonymous one its pooled connection is
		// reachable only through this map entry, and dropping it here would
		// leak that connection for the lifetime of the process. Restoring also
		// lets a retry see the same explanation (a reaped session reports
		// FailedPrecondition) instead of "transaction id not found".
		s.openTransactions.Store(handle, ts)
		return otelutil.Failed(span, err)
	}
	// For an anonymous transaction the pool connection is ours to give back.
	defer func() {
		conn := ts.conn
		ts.conn = nil
		release()
		if conn != nil {
			s.engine.Pool.Release(conn)
		}
	}()

	// Pre-flight: if DuckDB isn't actually in an explicit transaction (a
	// concurrent client layer may have already committed/rolled back via SQL),
	// the corresponding COMMIT/ROLLBACK would error with "no transaction is
	// active". Skip it.
	inTxn, probeErr := ac.InExplicitTransaction(ctx)
	if probeErr != nil {
		return otelutil.Failed(span,
			status.Errorf(codes.Internal, "failed to probe transaction state: %s", probeErr))
	}
	if !inTxn {
		// Another client layer already finalized the DuckDB transaction via raw
		// SQL, so there is nothing to commit or roll back here. Still counted:
		// from the client's point of view the transaction ended.
		span.AddEvent("transaction.already_finalized")
		txnCountAdd(ctx, op)
		return nil
	}

	if _, err := ac.ExecContext(ctx, endSQL); err != nil {
		return otelutil.Failed(span,
			status.Errorf(codes.Internal, "failed to %s: %s", op, err))
	}
	txnCountAdd(ctx, op)
	return nil
}
