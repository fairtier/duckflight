//go:build duckdb_arrow

package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/prochac/duckflight/internal/engine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cachedQuery struct {
	query         string
	transactionID string
}

var (
	queryCache sync.Map // handle string -> cachedQuery
)

// duckDBToGRPCCode maps DuckDB error types to appropriate gRPC status codes.
func duckDBToGRPCCode(err error) codes.Code {
	var duckErr *duckdb.Error
	if errors.As(err, &duckErr) && duckErr.Type == duckdb.ErrorTypeCatalog {
		return codes.NotFound
	}
	return codes.InvalidArgument
}

func genHandle() []byte {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return []byte(hex.EncodeToString(b))
}

// acquireConn returns either the transaction's connection or a pool connection.
// If fromPool is true, the caller must release it when done.
func (s *DuckFlightSQLServer) acquireConn(ctx context.Context, txnID string) (ac *engine.ArrowConn, fromPool bool, err error) {
	if txnID != "" {
		val, ok := s.openTransactions.Load(txnID)
		if !ok {
			return nil, false, status.Error(codes.InvalidArgument, "invalid transaction handle")
		}
		ts := val.(*txnState)
		return ts.conn, false, nil
	}
	ac, err = s.engine.Pool.Acquire(ctx)
	if err != nil {
		return nil, false, status.Errorf(codes.ResourceExhausted, "pool acquire: %s", err)
	}
	return ac, true, nil
}

// GetFlightInfoStatement validates the query and stores it for
// later execution by DoGetStatement.
func (s *DuckFlightSQLServer) GetFlightInfoStatement(
	ctx context.Context,
	cmd flightsql.StatementQuery,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	query := cmd.GetQuery()
	txnID := string(cmd.GetTransactionId())

	// Validate the query by running EXPLAIN — catches syntax errors
	// without actually executing the query.
	ac, fromPool, err := s.acquireConn(ctx, txnID)
	if err != nil {
		return nil, err
	}
	if fromPool {
		defer s.engine.Pool.Release(ac)
	}
	if _, err := ac.ExecContext(ctx, "EXPLAIN "+query); err != nil {
		return nil, status.Errorf(duckDBToGRPCCode(err), "query error: %s", err)
	}

	handle := genHandle()
	queryCache.Store(string(handle), cachedQuery{query: query, transactionID: txnID})

	tkt, err := flightsql.CreateStatementQueryTicket(handle)
	if err != nil {
		queryCache.Delete(string(handle))
		return nil, status.Errorf(codes.Internal, "failed to encode ticket: %s", err)
	}

	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: tkt},
		}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

// DoGetStatement retrieves the cached query and executes it via DuckDB's
// Arrow interface, streaming zero-copy record batches to the client.
func (s *DuckFlightSQLServer) DoGetStatement(
	ctx context.Context,
	cmd flightsql.StatementQueryTicket,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	handle := string(cmd.GetStatementHandle())

	val, ok := queryCache.LoadAndDelete(handle)
	if !ok {
		return nil, nil, status.Error(codes.NotFound, fmt.Sprintf("statement handle not found: %q", handle))
	}
	cq := val.(cachedQuery)

	ac, fromPool, err := s.acquireConn(ctx, cq.transactionID)
	if err != nil {
		return nil, nil, err
	}

	queryCtx := ctx
	var queryCancel context.CancelFunc
	if s.queryTimeout > 0 {
		queryCtx, queryCancel = context.WithTimeout(ctx, s.queryTimeout)
	}

	rdr, err := ac.Arrow.QueryContext(queryCtx, cq.query)
	if err != nil {
		if queryCancel != nil {
			queryCancel()
		}
		if fromPool {
			s.engine.Pool.Release(ac)
		}
		if queryCtx.Err() == context.DeadlineExceeded {
			return nil, nil, status.Error(codes.DeadlineExceeded, "query exceeded time limit")
		}
		return nil, nil, status.Errorf(codes.Internal, "query execution error: %s", err)
	}

	metered := newMeteredReader(rdr, s.maxResultBytes)
	schema := metered.Schema()
	ch := make(chan flight.StreamChunk)

	start := time.Now()
	activeQueries.Inc()

	go func() {
		defer close(ch)
		defer metered.Release()
		if queryCancel != nil {
			defer queryCancel()
		}
		if fromPool {
			defer s.engine.Pool.Release(ac)
		}
		defer func() {
			activeQueries.Dec()
			queryDuration.WithLabelValues().Observe(time.Since(start).Seconds())
		}()

		for metered.Next() {
			rec := metered.RecordBatch()
			rec.Retain()
			select {
			case ch <- flight.StreamChunk{Data: rec}:
			case <-queryCtx.Done():
				queryCount.WithLabelValues("timeout").Inc()
				return
			}
		}
		if err := metered.Err(); err != nil {
			queryCount.WithLabelValues("error").Inc()
			select {
			case ch <- flight.StreamChunk{Err: err}:
			case <-queryCtx.Done():
			}
			return
		}
		queryCount.WithLabelValues("ok").Inc()
	}()

	return schema, ch, nil
}

// DoPutCommandStatementUpdate executes a SQL update (INSERT/UPDATE/DELETE/DDL).
func (s *DuckFlightSQLServer) DoPutCommandStatementUpdate(
	ctx context.Context,
	cmd flightsql.StatementUpdate,
) (int64, error) {
	txnID := string(cmd.GetTransactionId())

	ac, fromPool, err := s.acquireConn(ctx, txnID)
	if err != nil {
		return 0, err
	}
	if fromPool {
		defer s.engine.Pool.Release(ac)
	}

	n, err := ac.ExecContext(ctx, cmd.GetQuery())
	if err != nil {
		return 0, status.Errorf(codes.Internal, "update error: %s", err)
	}
	return n, nil
}

// GetSchemaStatement returns the schema of a query without executing it.
func (s *DuckFlightSQLServer) GetSchemaStatement(
	ctx context.Context,
	cmd flightsql.StatementQuery,
	desc *flight.FlightDescriptor,
) (*flight.SchemaResult, error) {
	query := cmd.GetQuery()

	ac, err := s.engine.Pool.Acquire(ctx)
	if err != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "pool acquire: %s", err)
	}
	defer s.engine.Pool.Release(ac)

	// Execute with LIMIT 0 to get schema without results.
	rdr, err := ac.Arrow.QueryContext(ctx, fmt.Sprintf("SELECT * FROM (%s) AS t LIMIT 0", query))
	if err != nil {
		return nil, status.Errorf(duckDBToGRPCCode(err), "query error: %s", err)
	}
	defer rdr.Release()

	schema := rdr.Schema()
	return &flight.SchemaResult{
		Schema: flight.SerializeSchema(schema, s.Alloc),
	}, nil
}
