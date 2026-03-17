//go:build duckdb_arrow

package server

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/prochac/duckflight/internal/engine"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type txnState struct {
	conn      *engine.ArrowConn
	tx        driver.Tx
	createdAt time.Time
}

// BeginTransaction acquires a connection from the pool and starts a transaction.
func (s *DuckFlightSQLServer) BeginTransaction(
	ctx context.Context,
	_ flightsql.ActionBeginTransactionRequest,
) ([]byte, error) {
	ctx, span := s.tracer.Start(ctx, "transaction.begin")
	defer span.End()

	ac, err := s.acquirePoolConn(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	tx, err := ac.BeginTx(ctx)
	if err != nil {
		span.RecordError(err)
		s.engine.Pool.Release(ac)
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %s", err)
	}

	// Force DuckDB to take a snapshot immediately by reading from a real table.
	// Without this, the snapshot is deferred to the first actual statement,
	// which breaks snapshot isolation guarantees for the client.
	rdr, err := ac.Arrow.QueryContext(ctx, "SELECT 0 FROM duckdb_tables() LIMIT 0")
	if err != nil {
		span.RecordError(err)
		_ = tx.Rollback()
		s.engine.Pool.Release(ac)
		return nil, status.Errorf(codes.Internal, "failed to initialize transaction snapshot: %s", err)
	}
	rdr.Release()

	handle := genHandle()
	s.openTransactions.Store(string(handle), &txnState{conn: ac, tx: tx, createdAt: time.Now()})
	return handle, nil
}

// EndTransaction commits or rolls back a transaction and releases the connection.
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
	defer s.engine.Pool.Release(ts.conn)

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
