//go:build duckdb_arrow

package server

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type preparedStatement struct {
	query     string
	params    [][]any
	createdAt time.Time
}

// scalarToIFace converts an Arrow scalar to a Go interface value.
func scalarToIFace(s scalar.Scalar) (any, error) {
	if !s.IsValid() {
		return nil, nil
	}

	switch val := s.(type) {
	case *scalar.Int8:
		return val.Value, nil
	case *scalar.Int16:
		return val.Value, nil
	case *scalar.Int32:
		return val.Value, nil
	case *scalar.Int64:
		return val.Value, nil
	case *scalar.Uint8:
		return val.Value, nil
	case *scalar.Uint16:
		return val.Value, nil
	case *scalar.Uint32:
		return val.Value, nil
	case *scalar.Uint64:
		return val.Value, nil
	case *scalar.Float32:
		return val.Value, nil
	case *scalar.Float64:
		return val.Value, nil
	case *scalar.String:
		return string(val.Value.Bytes()), nil
	case *scalar.Binary:
		return val.Value.Bytes(), nil
	case *scalar.Boolean:
		return val.Value, nil
	case scalar.DateScalar:
		return val.ToTime(), nil
	case *scalar.Timestamp:
		return val.ToTime(), nil
	case scalar.TimeScalar:
		return val.ToTime(), nil
	case *scalar.Decimal128:
		return val.Value.ToString(val.Type.(*arrow.Decimal128Type).Scale), nil
	case *scalar.DenseUnion:
		return scalarToIFace(val.Value)
	case *scalar.Null:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported scalar type: %s", s.DataType())
	}
}

// extractParams reads parameter rows from an Arrow record batch stream.
func extractParams(rdr flight.MessageReader) ([][]any, error) {
	var params [][]any
	for rdr.Next() {
		rec := rdr.RecordBatch()
		nrows := int(rec.NumRows())
		ncols := int(rec.NumCols())

		for i := 0; i < nrows; i++ {
			row := make([]any, ncols)
			for c := 0; c < ncols; c++ {
				sc, err := scalar.GetScalar(rec.Column(c), i)
				if err != nil {
					return nil, err
				}
				if r, ok := sc.(scalar.Releasable); ok {
					r.Release()
				}
				row[c], err = scalarToIFace(sc)
				if err != nil {
					return nil, err
				}
			}
			params = append(params, row)
		}
	}
	return params, rdr.Err()
}

// CreatePreparedStatement validates a query and stores it for later execution.
func (s *DuckFlightSQLServer) CreatePreparedStatement(
	ctx context.Context,
	req flightsql.ActionCreatePreparedStatementRequest,
) (flightsql.ActionCreatePreparedStatementResult, error) {
	query := req.GetQuery()

	handle := genHandle()
	s.preparedStmts.Store(string(handle), preparedStatement{query: query, createdAt: time.Now()})

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

	// Build args from first parameter row (if any).
	var args []any
	if len(ps.params) > 0 {
		args = ps.params[0]
	}

	rdr, err := ac.Arrow.QueryContext(ctx, ps.query, args...)
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
func (s *DuckFlightSQLServer) DoPutPreparedStatementQuery(
	_ context.Context,
	cmd flightsql.PreparedStatementQuery,
	rdr flight.MessageReader,
	_ flight.MetadataWriter,
) ([]byte, error) {
	handle := cmd.GetPreparedStatementHandle()
	val, ok := s.preparedStmts.Load(string(handle))
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	ps := val.(preparedStatement)
	params, err := extractParams(rdr)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error extracting parameters: %s", err)
	}

	ps.params = params
	s.preparedStmts.Store(string(handle), ps)
	return handle, nil
}

// DoPutPreparedStatementUpdate executes a prepared update statement.
func (s *DuckFlightSQLServer) DoPutPreparedStatementUpdate(
	ctx context.Context,
	cmd flightsql.PreparedStatementUpdate,
	rdr flight.MessageReader,
) (int64, error) {
	handle := string(cmd.GetPreparedStatementHandle())
	val, ok := s.preparedStmts.Load(handle)
	if !ok {
		return 0, status.Error(codes.InvalidArgument, "prepared statement not found")
	}
	ps := val.(preparedStatement)

	// Extract params from the reader (DoPut sends them inline).
	args, err := extractParams(rdr)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "error extracting parameters: %s", err)
	}
	// Merge: prefer inline params, fall back to previously stored params.
	if len(args) == 0 {
		args = ps.params
	}

	ac, err := s.engine.Pool.Acquire(ctx)
	if err != nil {
		return 0, status.Errorf(codes.ResourceExhausted, "pool acquire: %s", err)
	}
	defer s.engine.Pool.Release(ac)

	if len(args) == 0 {
		n, err := ac.ExecContext(ctx, ps.query)
		if err != nil {
			return 0, status.Errorf(codes.Internal, "update error: %s", err)
		}
		return n, nil
	}

	var total int64
	for _, row := range args {
		n, err := ac.ExecContext(ctx, ps.query, row...)
		if err != nil {
			return total, status.Errorf(codes.Internal, "update error: %s", err)
		}
		total += n
	}
	return total, nil
}
