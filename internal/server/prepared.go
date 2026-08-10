//go:build duckdb_arrow

package server

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/fairtier/duckflight/internal/engine"
	"github.com/fairtier/duckflight/internal/otelutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// preparedStatement is stored by pointer so parameter binding and idle-timer
// updates don't have to read-modify-write a map value and clobber each other.
type preparedStatement struct {
	query string
	// txnID binds the statement to the Flight transaction it was created in,
	// so executing it runs on that transaction's connection. Without this an
	// INSERT prepared inside a transaction runs on an unrelated pool
	// connection in autocommit and survives the client's rollback.
	txnID     string
	createdAt time.Time
	lastUsed  atomic.Int64 // unix nanos

	mu     sync.Mutex
	params [][]any
}

func (ps *preparedStatement) touch() { ps.lastUsed.Store(time.Now().UnixNano()) }

func (ps *preparedStatement) idleFor(now time.Time) time.Duration {
	return now.Sub(time.Unix(0, ps.lastUsed.Load()))
}

func (ps *preparedStatement) setParams(params [][]any) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.params = params
}

func (ps *preparedStatement) getParams() [][]any {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.params
}

// resolveConn routes a prepared statement to the connection it belongs on.
//
// A statement created inside a transaction stays bound to it. Once that
// transaction is committed or rolled back the statement has no home: falling
// back to a pool connection would silently run it in autocommit, which is the
// exact failure this binding exists to prevent, so say so instead.
func (s *DuckFlightSQLServer) resolveConn(ctx context.Context, ps *preparedStatement) (*engine.ArrowConn, func(), error) {
	if ps.txnID != "" {
		if _, ok := s.openTransactions.Load(ps.txnID); !ok {
			return nil, nil, status.Error(codes.FailedPrecondition,
				"prepared statement belongs to a transaction that has already ended; prepare it again")
		}
	}
	return s.acquireConn(ctx, ps.txnID)
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
		// string() copies, so the result does not alias the scalar's buffer.
		return string(val.Value.Bytes()), nil
	case *scalar.Binary:
		// Bytes() aliases the Arrow buffer, which is released as soon as the
		// batch is; the value has to outlive it.
		return bytes.Clone(val.Value.Bytes()), nil
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
				// Convert before releasing: scalarToIFace reads the scalar's
				// buffers, which Release invalidates.
				row[c], err = scalarToIFace(sc)
				if r, ok := sc.(scalar.Releasable); ok {
					r.Release()
				}
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
	txnID := string(req.GetTransactionId())

	handle := genHandle()
	ps := &preparedStatement{query: query, txnID: txnID, createdAt: time.Now()}
	ps.touch()
	s.preparedStmts.Store(string(handle), ps)

	ctx, span := s.tracer.Start(ctx, "prepared.create", trace.WithAttributes(
		dbSystem,
		attribute.String(attrDBOperation, "prepare"),
		attribute.String(attrPreparedHandle, string(handle)),
		statementAttr(ctx, query),
	))
	defer span.End()

	// Schema probe. Routed through acquireConn so the transaction's or
	// session's connection-local state (attached catalogs, search_path,
	// uncommitted rows) is in scope.
	ac, release, err := s.acquireConn(ctx, txnID)
	if err != nil {
		otelutil.RecordError(span, err)
		return flightsql.ActionCreatePreparedStatementResult{Handle: handle}, nil
	}
	defer release()

	// Wrapping in a subselect rather than appending " LIMIT 0" keeps the probe
	// from executing the statement: a trailing line comment would swallow the
	// suffix, and `UPDATE … RETURNING x LIMIT 0` is a valid statement that
	// performs the update.
	rdr, err := ac.Arrow.QueryContext(ctx, fmt.Sprintf("SELECT * FROM (%s) AS t LIMIT 0", query))
	if err != nil {
		// Return handle without schema if schema detection fails. This covers
		// statements that aren't queries (DML, DDL, bare BEGIN/COMMIT) —
		// DoGet/DoPut will execute the original query on the right connection
		// where it works naturally. An event, not an error: this is the
		// expected outcome for every prepared non-query statement.
		span.AddEvent("prepared.schema_unavailable")
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
	ps := val.(*preparedStatement)
	ps.touch()

	reqSpan := trace.SpanFromContext(ctx)
	reqSpan.SetAttributes(attribute.String(attrPreparedHandle, handle))

	ac, release, err := s.resolveConn(ctx, ps)
	if err != nil {
		return nil, nil, err
	}

	// Redundant BEGIN/COMMIT/ROLLBACK against the connection's current
	// transaction state → no-op (see [shouldSkipTxnControl]).
	skip, err := shouldSkipTxnControl(ctx, ac, ps.query)
	if err != nil {
		release()
		queryCountAdd(ctx, "error")
		return nil, nil, err
	}
	if skip {
		release()
		reqSpan.AddEvent("txn.control.skipped", trace.WithAttributes(statementAttr(ctx, ps.query)))
		schema := arrow.NewSchema([]arrow.Field{}, nil)
		ch := make(chan flight.StreamChunk)
		close(ch)
		queryCountAdd(ctx, "ok")
		return schema, ch, nil
	}

	// Bind the most recently pushed parameter row, if any.
	var args []any
	if params := ps.getParams(); len(params) > 0 {
		args = params[len(params)-1]
	}

	// Prepared statements carry the bulk of real ADBC/JDBC/SQLAlchemy traffic,
	// so the configured query timeout and result-size cap have to apply here
	// exactly as they do to ad-hoc statements.
	var (
		queryCtx    context.Context
		queryCancel context.CancelFunc
	)
	if s.queryTimeout > 0 {
		queryCtx, queryCancel = context.WithTimeout(ctx, s.queryTimeout)
	} else {
		queryCtx, queryCancel = context.WithCancel(ctx)
	}

	execCtx, execSpan := s.tracer.Start(queryCtx, "prepared.execute", trace.WithAttributes(
		dbSystem,
		attribute.String(attrDBOperation, "query"),
		attribute.String(attrPreparedHandle, handle),
		attribute.Int("flight.bound_parameters", len(args)),
		attribute.Bool(attrConnDirty, ac.IsDirty()),
		statementAttr(ctx, ps.query),
	))
	rdr, err := ac.Arrow.QueryContext(execCtx, ps.query, args...)
	if err != nil {
		otelutil.RecordError(execSpan, err)
		execSpan.End()
		ctxErr := queryCtx.Err()
		queryCancel()
		release()
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

	streamCtx, streamSpan := s.tracer.Start(ctx, "prepared.stream", trace.WithAttributes(
		dbSystem,
		attribute.String(attrPreparedHandle, handle),
	))

	metered := newMeteredReader(streamCtx, rdr, s.maxResultBytes)
	schema := metered.Schema()
	ch := make(chan flight.StreamChunk)

	start := time.Now()
	activeQueries.Add(ctx, 1)

	go func() {
		defer close(ch)
		defer metered.Release()
		defer queryCancel()
		defer release()
		defer func() {
			activeQueries.Add(ctx, -1)
			queryDuration.Record(ctx, time.Since(start).Seconds())
			streamSpan.SetAttributes(
				attribute.Int64(attrRows, metered.rows),
				attribute.Int64(attrBytes, metered.bytes),
			)
			streamSpan.End()
		}()

		for metered.Next() {
			rec := metered.RecordBatch()
			rec.Retain()
			select {
			case ch <- flight.StreamChunk{Data: rec}:
			case <-queryCtx.Done():
				rec.Release()
				if queryCtx.Err() == context.Canceled {
					queryCountAdd(ctx, "canceled")
					streamSpan.AddEvent("stream.canceled")
				} else {
					queryCountAdd(ctx, "timeout")
					otelutil.RecordError(streamSpan, queryCtx.Err())
				}
				return
			}
		}
		if err := metered.Err(); err != nil {
			queryCountAdd(ctx, "error")
			otelutil.RecordError(streamSpan, err)
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

	ps := val.(*preparedStatement)
	params, err := extractParams(rdr)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error extracting parameters: %s", err)
	}

	ps.setParams(params)
	ps.touch()
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
	ps := val.(*preparedStatement)
	ps.touch()

	// Extract params from the reader (DoPut sends them inline).
	args, err := extractParams(rdr)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "error extracting parameters: %s", err)
	}
	// Merge: prefer inline params, fall back to previously stored params.
	if len(args) == 0 {
		args = ps.getParams()
	}

	ac, release, err := s.resolveConn(ctx, ps)
	if err != nil {
		return 0, err
	}
	defer release()

	skip, err := shouldSkipTxnControl(ctx, ac, ps.query)
	if err != nil {
		return 0, err
	}
	if skip {
		trace.SpanFromContext(ctx).AddEvent("txn.control.skipped",
			trace.WithAttributes(statementAttr(ctx, ps.query)))
		return 0, nil
	}

	ctx, span := s.tracer.Start(ctx, "prepared.update", trace.WithAttributes(
		dbSystem,
		attribute.String(attrDBOperation, "update"),
		attribute.String(attrPreparedHandle, handle),
		// One RPC can execute the statement once per parameter row, so a slow
		// span here is often batch size rather than a slow statement.
		attribute.Int("flight.parameter_rows", len(args)),
		attribute.Bool(attrConnDirty, ac.IsDirty()),
		statementAttr(ctx, ps.query),
	))
	defer span.End()

	var total int64
	defer func() {
		span.SetAttributes(attribute.Int64(attrRows, total))
		rowsAffectedAdd(ctx, opPreparedUpdate, total)
	}()

	if len(args) == 0 {
		total, err = ac.ExecContext(ctx, ps.query)
		if err != nil {
			total = 0
			return 0, otelutil.Failed(span, status.Errorf(codes.Internal, "update error: %s", err))
		}
		return total, nil
	}

	for _, row := range args {
		n, err := ac.ExecContext(ctx, ps.query, row...)
		if err != nil {
			return total, otelutil.Failed(span, status.Errorf(codes.Internal, "update error: %s", err))
		}
		total += n
	}
	return total, nil
}
