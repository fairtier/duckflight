//go:build duckdb_arrow

package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	queryCache sync.Map // handle string -> query string
)

func genHandle() []byte {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return []byte(hex.EncodeToString(b))
}

// GetFlightInfoStatement validates the query and stores it for
// later execution by DoGetStatement.
func (s *DuckFlightSQLServer) GetFlightInfoStatement(
	ctx context.Context,
	cmd flightsql.StatementQuery,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	query := cmd.GetQuery()

	// Validate the query by running EXPLAIN — catches syntax errors
	// without actually executing the query.
	if err := s.engine.ExecSQL(ctx, "EXPLAIN "+query); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "query error: %s", err)
	}

	handle := genHandle()
	queryCache.Store(string(handle), query)

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
	query := val.(string)

	ac, err := s.engine.Pool.Acquire(ctx)
	if err != nil {
		return nil, nil, status.Errorf(codes.ResourceExhausted, "pool acquire: %s", err)
	}

	rdr, err := ac.Arrow.QueryContext(ctx, query)
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
			rec := rdr.Record()
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
