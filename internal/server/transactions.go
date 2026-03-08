//go:build duckdb_arrow

package server

import (
	"context"
	"database/sql/driver"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/prochac/duckflight/internal/engine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type txnState struct {
	conn *engine.ArrowConn
	tx   driver.Tx
}

// BeginTransaction acquires a connection from the pool and starts a transaction.
func (s *DuckFlightSQLServer) BeginTransaction(
	ctx context.Context,
	_ flightsql.ActionBeginTransactionRequest,
) ([]byte, error) {
	ac, err := s.engine.Pool.Acquire(ctx)
	if err != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "pool acquire: %s", err)
	}

	tx, err := ac.BeginTx(ctx)
	if err != nil {
		s.engine.Pool.Release(ac)
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %s", err)
	}

	// Force DuckDB to take a snapshot immediately by reading from a real table.
	// Without this, the snapshot is deferred to the first actual statement,
	// which breaks snapshot isolation guarantees for the client.
	rdr, err := ac.Arrow.QueryContext(ctx, "SELECT 0 FROM duckdb_tables() LIMIT 0")
	if err != nil {
		_ = tx.Rollback()
		s.engine.Pool.Release(ac)
		return nil, status.Errorf(codes.Internal, "failed to initialize transaction snapshot: %s", err)
	}
	rdr.Release()

	handle := genHandle()
	s.openTransactions.Store(string(handle), &txnState{conn: ac, tx: tx})
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
			return status.Errorf(codes.Internal, "failed to commit: %s", err)
		}
	case flightsql.EndTransactionRollback:
		if err := ts.tx.Rollback(); err != nil {
			return status.Errorf(codes.Internal, "failed to rollback: %s", err)
		}
	}
	return nil
}
