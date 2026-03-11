//go:build duckdb_arrow

package server

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type preparedStatement struct {
	query string
}

// CreatePreparedStatement validates a query and stores it for later execution.
func (s *DuckFlightSQLServer) CreatePreparedStatement(
	ctx context.Context,
	req flightsql.ActionCreatePreparedStatementRequest,
) (flightsql.ActionCreatePreparedStatementResult, error) {
	query := req.GetQuery()

	handle := genHandle()
	s.preparedStmts.Store(string(handle), preparedStatement{query: query})

	// Get schema via LIMIT 0 query.
	ac, err := s.engine.Pool.Acquire(ctx)
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{Handle: handle}, nil
	}
	defer s.engine.Pool.Release(ac)

	rdr, err := ac.Arrow.QueryContext(ctx, query+" LIMIT 0")
	if err != nil {
		// Return handle without schema if schema detection fails.
		return flightsql.ActionCreatePreparedStatementResult{Handle: handle}, nil
	}
	defer rdr.Release()

	return flightsql.ActionCreatePreparedStatementResult{
		Handle:        handle,
		DatasetSchema: rdr.Schema(),
	}, nil
}

// ClosePreparedStatement removes a prepared statement.
func (s *DuckFlightSQLServer) ClosePreparedStatement(
	_ context.Context,
	req flightsql.ActionClosePreparedStatementRequest,
) error {
	handle := string(req.GetPreparedStatementHandle())
	s.preparedStmts.Delete(handle)
	return nil
}

// GetFlightInfoPreparedStatement returns FlightInfo for a prepared statement query.
func (s *DuckFlightSQLServer) GetFlightInfoPreparedStatement(
	_ context.Context,
	cmd flightsql.PreparedStatementQuery,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	handle := string(cmd.GetPreparedStatementHandle())
	if _, ok := s.preparedStmts.Load(handle); !ok {
		return nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

// DoGetPreparedStatement executes a prepared statement query and streams results.
func (s *DuckFlightSQLServer) DoGetPreparedStatement(
	ctx context.Context,
	cmd flightsql.PreparedStatementQuery,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	handle := string(cmd.GetPreparedStatementHandle())
	val, ok := s.preparedStmts.Load(handle)
	if !ok {
		return nil, nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}
	ps := val.(preparedStatement)

	ac, err := s.engine.Pool.Acquire(ctx)
	if err != nil {
		return nil, nil, status.Errorf(codes.ResourceExhausted, "pool acquire: %s", err)
	}

	rdr, err := ac.Arrow.QueryContext(ctx, ps.query)
	if err != nil {
		s.engine.Pool.Release(ac)
		return nil, nil, status.Errorf(codes.Internal, "query execution error: %s", err)
	}

	schema := rdr.Schema()
	ch := make(chan flight.StreamChunk)

	go func() {
		defer close(ch)
		defer rdr.Release()
		defer s.engine.Pool.Release(ac)

		for rdr.Next() {
			rec := rdr.RecordBatch()
			rec.Retain()
			select {
			case ch <- flight.StreamChunk{Data: rec}:
			case <-ctx.Done():
				return
			}
		}
		if err := rdr.Err(); err != nil {
			select {
			case ch <- flight.StreamChunk{Err: err}:
			case <-ctx.Done():
			}
		}
	}()

	return schema, ch, nil
}

// DoPutPreparedStatementQuery accepts parameter bindings for a prepared query.
// Parameters are drained but not applied (current model is string-based).
func (s *DuckFlightSQLServer) DoPutPreparedStatementQuery(
	_ context.Context,
	cmd flightsql.PreparedStatementQuery,
	rdr flight.MessageReader,
	_ flight.MetadataWriter,
) ([]byte, error) {
	handle := cmd.GetPreparedStatementHandle()
	if _, ok := s.preparedStmts.Load(string(handle)); !ok {
		return nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	// Drain the reader — params accepted but not applied.
	for rdr.Next() {
	}

	return handle, nil
}

// DoPutPreparedStatementUpdate executes a prepared update statement.
func (s *DuckFlightSQLServer) DoPutPreparedStatementUpdate(
	ctx context.Context,
	cmd flightsql.PreparedStatementUpdate,
	_ flight.MessageReader,
) (int64, error) {
	handle := string(cmd.GetPreparedStatementHandle())
	val, ok := s.preparedStmts.Load(handle)
	if !ok {
		return 0, status.Error(codes.InvalidArgument, "prepared statement not found")
	}
	ps := val.(preparedStatement)

	ac, err := s.engine.Pool.Acquire(ctx)
	if err != nil {
		return 0, status.Errorf(codes.ResourceExhausted, "pool acquire: %s", err)
	}
	defer s.engine.Pool.Release(ac)

	n, err := ac.ExecContext(ctx, ps.query)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "update error: %s", err)
	}
	return n, nil
}
