//go:build duckdb_arrow

package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/prochac/duckflight/internal/engine"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// acquirePoolConn acquires a connection from the pool, wrapping errors with
// the appropriate gRPC status code.
func (s *DuckFlightSQLServer) acquirePoolConn(ctx context.Context) (*engine.ArrowConn, error) {
	ac, err := s.engine.Pool.Acquire(ctx)
	if err != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "pool acquire: %s", err)
	}
	return ac, nil
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

	ctx, span := s.tracer.Start(ctx, "pool.acquire")
	defer span.End()

	ac, err = s.acquirePoolConn(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, false, err
	}
	return ac, true, nil
}

// GetFlightInfoStatement stores the query for later execution by DoGetStatement.
// Errors (syntax, missing tables) surface at DoGet time.
func (s *DuckFlightSQLServer) GetFlightInfoStatement(
	_ context.Context,
	cmd flightsql.StatementQuery,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	query := cmd.GetQuery()
	txnID := string(cmd.GetTransactionId())

	handle := genHandle()
	s.tracker.Register(string(handle), query, txnID)

	tkt, err := flightsql.CreateStatementQueryTicket(handle)
	if err != nil {
		s.tracker.Remove(string(handle))
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

	query, txnID, ok := s.tracker.Load(handle)
	if !ok {
		return nil, nil, status.Error(codes.NotFound, fmt.Sprintf("statement handle not found: %q", handle))
	}

	ac, fromPool, err := s.acquireConn(ctx, txnID)
	if err != nil {
		return nil, nil, err
	}

	queryCtx, queryCancel := context.WithCancel(ctx)
	if s.queryTimeout > 0 {
		queryCtx, queryCancel = context.WithTimeout(ctx, s.queryTimeout)
	}
	s.tracker.SetCancel(handle, queryCancel)

	execCtx, execSpan := s.tracer.Start(queryCtx, "execute",
		trace.WithAttributes(attribute.String("db.statement", query)))
	rdr, err := ac.Arrow.QueryContext(execCtx, query)
	if err != nil {
		execSpan.RecordError(err)
		execSpan.End()
		// Check context state before canceling, so we can distinguish
		// external cancellation from our own cleanup cancel.
		ctxErr := queryCtx.Err()
		queryCancel()
		if fromPool {
			s.engine.Pool.Release(ac)
		}
		s.tracker.Complete(handle)
		switch ctxErr {
		case context.Canceled:
			queryCountAdd(ctx, "canceled")
			return nil, nil, status.Error(codes.Canceled, "query canceled")
		case context.DeadlineExceeded:
			queryCountAdd(ctx, "timeout")
			return nil, nil, status.Error(codes.DeadlineExceeded, "query exceeded time limit")
		}
		return nil, nil, status.Errorf(duckDBToGRPCCode(err), "query execution error: %s", err)
	}
	execSpan.End()

	metered := newMeteredReader(ctx, rdr, s.maxResultBytes)
	schema := metered.Schema()
	ch := make(chan flight.StreamChunk)

	start := time.Now()
	activeQueries.Add(ctx, 1)

	go func() {
		defer close(ch)
		defer metered.Release()
		defer queryCancel()
		if fromPool {
			defer s.engine.Pool.Release(ac)
		}
		defer func() {
			s.tracker.Complete(handle)
			activeQueries.Add(ctx, -1)
			queryDuration.Record(ctx, time.Since(start).Seconds())
		}()

		for metered.Next() {
			rec := metered.RecordBatch()
			rec.Retain()
			select {
			case ch <- flight.StreamChunk{Data: rec}:
			case <-queryCtx.Done():
				if queryCtx.Err() == context.Canceled {
					queryCountAdd(ctx, "canceled")
				} else {
					queryCountAdd(ctx, "timeout")
				}
				return
			}
		}
		if err := metered.Err(); err != nil {
			queryCountAdd(ctx, "error")
			select {
			case ch <- flight.StreamChunk{Err: err}:
			case <-queryCtx.Done():
			}
			return
		}
		queryCountAdd(ctx, "ok")
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

	ac, err := s.acquirePoolConn(ctx)
	if err != nil {
		return nil, err
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

// CancelFlightInfo cancels a running or pending query.
func (s *DuckFlightSQLServer) CancelFlightInfo(
	_ context.Context,
	req *flight.CancelFlightInfoRequest,
) (flight.CancelFlightInfoResult, error) {
	handle, err := extractStatementHandle(req)
	if err != nil {
		return flight.CancelFlightInfoResult{Status: flight.CancelStatusUnspecified}, err
	}

	cs := s.tracker.Cancel(handle)
	return flight.CancelFlightInfoResult{Status: cs}, nil
}

// extractStatementHandle unmarshals the ticket from a CancelFlightInfoRequest
// to recover the statement handle string.
func extractStatementHandle(req *flight.CancelFlightInfoRequest) (string, error) {
	if req.Info == nil || len(req.Info.Endpoint) == 0 || req.Info.Endpoint[0].Ticket == nil {
		return "", status.Error(codes.InvalidArgument, "missing ticket in cancel request")
	}

	tkt, err := flightsql.GetStatementQueryTicket(req.Info.Endpoint[0].Ticket)
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "failed to decode statement handle: %s", err)
	}

	return string(tkt.GetStatementHandle()), nil
}

// PollFlightInfoStatement registers the query and returns a ready-to-consume PollInfo.
// Since DuckFlight uses synchronous execution, the query is immediately ready for DoGet.
func (s *DuckFlightSQLServer) PollFlightInfoStatement(
	_ context.Context,
	cmd flightsql.StatementQuery,
	desc *flight.FlightDescriptor,
) (*flight.PollInfo, error) {
	query := cmd.GetQuery()
	txnID := string(cmd.GetTransactionId())

	handle := genHandle()
	s.tracker.Register(string(handle), query, txnID)

	tkt, err := flightsql.CreateStatementQueryTicket(handle)
	if err != nil {
		s.tracker.Remove(string(handle))
		return nil, status.Errorf(codes.Internal, "failed to encode ticket: %s", err)
	}

	info := &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: tkt},
		}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}

	return &flight.PollInfo{
		Info:             info,
		FlightDescriptor: nil, // nil means query is complete / ready for DoGet
	}, nil
}

// qualifiedTableName builds a fully-qualified, quoted table name from optional
// catalog/schema and required table parts.
func qualifiedTableName(catalog, schema, table string) string {
	var parts []string
	if catalog != "" {
		parts = append(parts, `"`+strings.ReplaceAll(catalog, `"`, `""`)+`"`)
	}
	if schema != "" {
		parts = append(parts, `"`+strings.ReplaceAll(schema, `"`, `""`)+`"`)
	}
	parts = append(parts, `"`+strings.ReplaceAll(table, `"`, `""`)+`"`)
	return strings.Join(parts, ".")
}

// ingestBatch registers a single Arrow record batch as a DuckDB view and
// executes the given SQL against it. Returns the number of affected rows.
func ingestBatch(ctx context.Context, ac *engine.ArrowConn, rec arrow.RecordBatch, query string) (int64, error) {
	recReader, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
	if err != nil {
		return 0, status.Errorf(codes.Internal, "create record reader: %s", err)
	}
	release, err := ac.Arrow.RegisterView(recReader, "__ingest_view")
	if err != nil {
		return 0, status.Errorf(codes.Internal, "register arrow view: %s", err)
	}
	defer release()

	n, err := ac.ExecContext(ctx, query)
	if err != nil {
		return 0, status.Errorf(duckDBToGRPCCode(err), "ingest error: %s", err)
	}
	return n, nil
}

// ingestAll loops over a MessageReader and ingests each batch using the given SQL.
func ingestAll(ctx context.Context, ac *engine.ArrowConn, rdr flight.MessageReader, insertSQL string) (int64, error) {
	var totalRows int64
	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()
		n, err := ingestBatch(ctx, ac, rec, insertSQL)
		rec.Release()
		if err != nil {
			return totalRows, err
		}
		totalRows += n
	}
	if err := rdr.Err(); err != nil {
		return totalRows, status.Errorf(codes.Internal, "reading ingest stream: %s", err)
	}
	return totalRows, nil
}

// DoPutCommandStatementIngest handles bulk ingestion of Arrow record batches
// into a target table via DuckDB's Arrow view registration.
func (s *DuckFlightSQLServer) DoPutCommandStatementIngest(
	ctx context.Context,
	cmd flightsql.StatementIngest,
	rdr flight.MessageReader,
) (int64, error) {
	table := cmd.GetTable()
	if table == "" {
		return 0, status.Error(codes.InvalidArgument, "table name is required for ingestion")
	}

	opts := cmd.GetTableDefinitionOptions()
	if opts == nil {
		return 0, status.Error(codes.InvalidArgument, "table definition options are required")
	}

	ac, fromPool, err := s.acquireConn(ctx, string(cmd.GetTransactionId()))
	if err != nil {
		return 0, err
	}
	if fromPool {
		defer s.engine.Pool.Release(ac)
	}

	target := qualifiedTableName(cmd.GetCatalog(), cmd.GetSchema(), table)
	insertSQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM __ingest_view", target)

	ifNotExist := opts.GetIfNotExist()
	ifExists := opts.GetIfExists()

	// Handle REPLACE: drop the existing table so it gets re-created from the stream schema.
	if ifExists == flightsql.TableDefinitionOptionsTableExistsOptionReplace {
		if _, err := ac.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", target)); err != nil {
			return 0, status.Errorf(codes.Internal, "drop table for replace: %s", err)
		}
	}

	// For CREATE modes, peek at the first batch and create the table from its schema.
	if ifNotExist == flightsql.TableDefinitionOptionsTableNotExistOptionCreate {
		if !rdr.Next() {
			if err := rdr.Err(); err != nil {
				return 0, status.Errorf(codes.Internal, "reading ingest stream: %s", err)
			}
			return 0, nil // empty stream
		}
		firstRec := rdr.RecordBatch()
		firstRec.Retain()
		defer firstRec.Release()

		// Create the table structure (empty) from the batch schema.
		createSQL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM __ingest_view WHERE false", target)
		if _, err := ingestBatch(ctx, ac, firstRec, createSQL); err != nil {
			return 0, err
		}

		// Insert the first batch's data.
		n, err := ingestBatch(ctx, ac, firstRec, insertSQL)
		if err != nil {
			return 0, err
		}

		// Insert remaining batches.
		remaining, err := ingestAll(ctx, ac, rdr, insertSQL)
		return n + remaining, err
	}

	// Non-CREATE mode: just INSERT INTO (table must already exist).
	return ingestAll(ctx, ac, rdr, insertSQL)
}
