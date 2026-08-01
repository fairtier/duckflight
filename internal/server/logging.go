//go:build duckdb_arrow

package server

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// loggingServer wraps DuckFlightSQLServer to add structured logging around
// every Flight SQL method call. All interface methods are overridden so that
// even unimplemented (BaseServer) calls are visible in the logs.
type loggingServer struct {
	*DuckFlightSQLServer
}

// NewLoggingServer returns a flightsql.Server that logs every Flight SQL call.
func NewLoggingServer(inner *DuckFlightSQLServer) flightsql.Server {
	return &loggingServer{DuckFlightSQLServer: inner}
}

func logCall(ctx context.Context, method string, start time.Time, err error, attrs ...slog.Attr) {
	attrs = append(attrs,
		slog.String("method", method),
		slog.Duration("duration", time.Since(start)),
	)
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
		if s, ok := status.FromError(err); ok {
			attrs = append(attrs, slog.String("grpc_code", s.Code().String()))
		} else {
			attrs = append(attrs, slog.String("grpc_code", codes.Unknown.String()))
		}
		slog.LogAttrs(ctx, slog.LevelError, "flight sql call failed", attrs...)
		return
	}
	slog.LogAttrs(ctx, slog.LevelInfo, "flight sql call", attrs...)
}

func derefStringPtr(s *string) string {
	if s != nil {
		return *s
	}
	return ""
}

// queryAttrs returns the SQL text as a log attribute only when DEBUG logging
// is on. Statements routinely carry credentials (`CREATE SECRET … SECRET
// '…'`, `ATTACH … (TOKEN '…')`) and PII in literals, and these logs go to
// stderr and on to the OTLP collector, so the text is not something to emit at
// INFO on every call. The statement length still gives operators a handle on
// call shape without the contents.
func queryAttrs(ctx context.Context, query string) []slog.Attr {
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		return []slog.Attr{slog.String("query", query)}
	}
	return []slog.Attr{slog.Int("query_len", len(query))}
}

// statementAttr is the tracing counterpart of [queryAttrs]. Spans are exported
// to the OTLP collector just like logs are, so `db.statement` is the same
// exfiltration path for a `CREATE SECRET … SECRET '…'` as a log line is, and
// it gets the same DEBUG gate.
func statementAttr(ctx context.Context, query string) attribute.KeyValue {
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		return attribute.String("db.statement", query)
	}
	return attribute.Int("db.statement_length", len(query))
}

// schemaFields returns a compact string of field names and types, e.g.
// "catalog_name:utf8, db_schema_name:utf8"
func schemaFields(s *arrow.Schema) string {
	if s == nil {
		return "<nil>"
	}
	fields := s.Fields()
	parts := make([]string, len(fields))
	for i, f := range fields {
		parts[i] = fmt.Sprintf("%s:%s", f.Name, f.Type)
	}
	return strings.Join(parts, ", ")
}

// flightInfoAttrs extracts useful attributes from a FlightInfo response.
func flightInfoAttrs(info *flight.FlightInfo) []slog.Attr {
	if info == nil {
		return []slog.Attr{slog.Bool("response_nil", true)}
	}
	attrs := []slog.Attr{
		slog.Int("endpoints", len(info.Endpoint)),
		slog.Int64("total_records", info.TotalRecords),
		slog.Int64("total_bytes", info.TotalBytes),
		slog.Bool("has_schema", len(info.Schema) > 0),
	}
	return attrs
}

// schemaAttrs extracts useful attributes from a DoGet-style schema response.
func schemaAttrs(s *arrow.Schema) []slog.Attr {
	if s == nil {
		return []slog.Attr{slog.Bool("schema_nil", true)}
	}
	return []slog.Attr{
		slog.Int("schema_fields", len(s.Fields())),
		slog.String("schema", schemaFields(s)),
	}
}

// --- Query execution (implemented) ---

func (l *loggingServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoStatement(ctx, cmd, desc)
	attrs := queryAttrs(ctx, cmd.GetQuery())
	attrs = append(attrs, flightInfoAttrs(info)...)
	logCall(ctx, "GetFlightInfoStatement", start, err, attrs...)
	return info, err
}

func (l *loggingServer) DoGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetStatement(ctx, cmd)
	logCall(ctx, "DoGetStatement", start, err, schemaAttrs(schema)...)
	return schema, ch, err
}

func (l *loggingServer) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	start := time.Now()
	n, err := l.DuckFlightSQLServer.DoPutCommandStatementUpdate(ctx, cmd)
	attrs := append(queryAttrs(ctx, cmd.GetQuery()), slog.Int64("affected_rows", n))
	logCall(ctx, "DoPutCommandStatementUpdate", start, err, attrs...)
	return n, err
}

func (l *loggingServer) GetSchemaStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	start := time.Now()
	result, err := l.DuckFlightSQLServer.GetSchemaStatement(ctx, cmd, desc)
	attrs := queryAttrs(ctx, cmd.GetQuery())
	if result != nil {
		attrs = append(attrs, slog.Int("schema_bytes", len(result.Schema)))
	}
	logCall(ctx, "GetSchemaStatement", start, err, attrs...)
	return result, err
}

// --- Catalogs (implemented) ---

func (l *loggingServer) GetFlightInfoCatalogs(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoCatalogs(ctx, desc)
	logCall(ctx, "GetFlightInfoCatalogs", start, err, flightInfoAttrs(info)...)
	return info, err
}

func (l *loggingServer) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetCatalogs(ctx)
	logCall(ctx, "DoGetCatalogs", start, err, schemaAttrs(schema)...)
	return schema, ch, err
}

// --- Schemas (implemented) ---

func (l *loggingServer) GetFlightInfoSchemas(ctx context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoSchemas(ctx, cmd, desc)
	logCall(ctx, "GetFlightInfoSchemas", start, err, flightInfoAttrs(info)...)
	return info, err
}

func (l *loggingServer) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetDBSchemas(ctx, cmd)
	attrs := []slog.Attr{
		slog.String("catalog", derefStringPtr(cmd.GetCatalog())),
		slog.String("schema_filter", derefStringPtr(cmd.GetDBSchemaFilterPattern())),
	}
	attrs = append(attrs, schemaAttrs(schema)...)
	logCall(ctx, "DoGetDBSchemas", start, err, attrs...)
	return schema, ch, err
}

// --- Tables (implemented) ---

func (l *loggingServer) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoTables(ctx, cmd, desc)
	logCall(ctx, "GetFlightInfoTables", start, err, flightInfoAttrs(info)...)
	return info, err
}

func (l *loggingServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetTables(ctx, cmd)
	attrs := []slog.Attr{
		slog.String("catalog", derefStringPtr(cmd.GetCatalog())),
		slog.String("schema_filter", derefStringPtr(cmd.GetDBSchemaFilterPattern())),
		slog.String("table_filter", derefStringPtr(cmd.GetTableNameFilterPattern())),
		slog.Bool("include_schema", cmd.GetIncludeSchema()),
	}
	attrs = append(attrs, schemaAttrs(schema)...)
	logCall(ctx, "DoGetTables", start, err, attrs...)
	return schema, ch, err
}

// --- Table types (implemented) ---

func (l *loggingServer) GetFlightInfoTableTypes(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoTableTypes(ctx, desc)
	logCall(ctx, "GetFlightInfoTableTypes", start, err, flightInfoAttrs(info)...)
	return info, err
}

func (l *loggingServer) DoGetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetTableTypes(ctx)
	logCall(ctx, "DoGetTableTypes", start, err, schemaAttrs(schema)...)
	return schema, ch, err
}

// --- Prepared statements (implemented) ---

func (l *loggingServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	start := time.Now()
	result, err := l.DuckFlightSQLServer.CreatePreparedStatement(ctx, req)
	attrs := queryAttrs(ctx, req.GetQuery())
	if result.DatasetSchema != nil {
		attrs = append(attrs, slog.String("dataset_schema", schemaFields(result.DatasetSchema)))
	}
	logCall(ctx, "CreatePreparedStatement", start, err, attrs...)
	return result, err
}

func (l *loggingServer) ClosePreparedStatement(ctx context.Context, req flightsql.ActionClosePreparedStatementRequest) error {
	start := time.Now()
	err := l.DuckFlightSQLServer.ClosePreparedStatement(ctx, req)
	logCall(ctx, "ClosePreparedStatement", start, err)
	return err
}

func (l *loggingServer) GetFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoPreparedStatement(ctx, cmd, desc)
	logCall(ctx, "GetFlightInfoPreparedStatement", start, err, flightInfoAttrs(info)...)
	return info, err
}

func (l *loggingServer) GetSchemaPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	start := time.Now()
	result, err := l.DuckFlightSQLServer.GetSchemaPreparedStatement(ctx, cmd, desc)
	logCall(ctx, "GetSchemaPreparedStatement", start, err)
	return result, err
}

func (l *loggingServer) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetPreparedStatement(ctx, cmd)
	logCall(ctx, "DoGetPreparedStatement", start, err, schemaAttrs(schema)...)
	return schema, ch, err
}

func (l *loggingServer) DoPutPreparedStatementQuery(ctx context.Context, cmd flightsql.PreparedStatementQuery, rdr flight.MessageReader, w flight.MetadataWriter) ([]byte, error) {
	start := time.Now()
	handle, err := l.DuckFlightSQLServer.DoPutPreparedStatementQuery(ctx, cmd, rdr, w)
	logCall(ctx, "DoPutPreparedStatementQuery", start, err)
	return handle, err
}

func (l *loggingServer) DoPutPreparedStatementUpdate(ctx context.Context, cmd flightsql.PreparedStatementUpdate, rdr flight.MessageReader) (int64, error) {
	start := time.Now()
	n, err := l.DuckFlightSQLServer.DoPutPreparedStatementUpdate(ctx, cmd, rdr)
	logCall(ctx, "DoPutPreparedStatementUpdate", start, err, slog.Int64("affected_rows", n))
	return n, err
}

// --- Ingestion (implemented) ---

func (l *loggingServer) DoPutCommandStatementIngest(ctx context.Context, cmd flightsql.StatementIngest, rdr flight.MessageReader) (int64, error) {
	start := time.Now()
	n, err := l.DuckFlightSQLServer.DoPutCommandStatementIngest(ctx, cmd, rdr)
	logCall(ctx, "DoPutCommandStatementIngest", start, err)
	return n, err
}

// --- Transactions (implemented) ---

func (l *loggingServer) BeginTransaction(ctx context.Context, req flightsql.ActionBeginTransactionRequest) ([]byte, error) {
	start := time.Now()
	id, err := l.DuckFlightSQLServer.BeginTransaction(ctx, req)
	logCall(ctx, "BeginTransaction", start, err)
	return id, err
}

func (l *loggingServer) EndTransaction(ctx context.Context, req flightsql.ActionEndTransactionRequest) error {
	start := time.Now()
	err := l.DuckFlightSQLServer.EndTransaction(ctx, req)
	action := "unknown"
	switch req.GetAction() {
	case flightsql.EndTransactionCommit:
		action = "commit"
	case flightsql.EndTransactionRollback:
		action = "rollback"
	}
	logCall(ctx, "EndTransaction", start, err, slog.String("action", action))
	return err
}

// --- Primary keys (implemented) ---

func (l *loggingServer) GetFlightInfoPrimaryKeys(ctx context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoPrimaryKeys(ctx, cmd, desc)
	attrs := []slog.Attr{slog.String("table", cmd.Table)}
	attrs = append(attrs, flightInfoAttrs(info)...)
	logCall(ctx, "GetFlightInfoPrimaryKeys", start, err, attrs...)
	return info, err
}

func (l *loggingServer) DoGetPrimaryKeys(ctx context.Context, cmd flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetPrimaryKeys(ctx, cmd)
	attrs := []slog.Attr{slog.String("table", cmd.Table)}
	attrs = append(attrs, schemaAttrs(schema)...)
	logCall(ctx, "DoGetPrimaryKeys", start, err, attrs...)
	return schema, ch, err
}

// --- Imported keys (implemented) ---

func (l *loggingServer) GetFlightInfoImportedKeys(ctx context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoImportedKeys(ctx, cmd, desc)
	attrs := []slog.Attr{slog.String("table", cmd.Table)}
	attrs = append(attrs, flightInfoAttrs(info)...)
	logCall(ctx, "GetFlightInfoImportedKeys", start, err, attrs...)
	return info, err
}

func (l *loggingServer) DoGetImportedKeys(ctx context.Context, cmd flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetImportedKeys(ctx, cmd)
	attrs := []slog.Attr{slog.String("table", cmd.Table)}
	attrs = append(attrs, schemaAttrs(schema)...)
	logCall(ctx, "DoGetImportedKeys", start, err, attrs...)
	return schema, ch, err
}

// --- Exported keys (implemented) ---

func (l *loggingServer) GetFlightInfoExportedKeys(ctx context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoExportedKeys(ctx, cmd, desc)
	attrs := []slog.Attr{slog.String("table", cmd.Table)}
	attrs = append(attrs, flightInfoAttrs(info)...)
	logCall(ctx, "GetFlightInfoExportedKeys", start, err, attrs...)
	return info, err
}

func (l *loggingServer) DoGetExportedKeys(ctx context.Context, cmd flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetExportedKeys(ctx, cmd)
	attrs := []slog.Attr{slog.String("table", cmd.Table)}
	attrs = append(attrs, schemaAttrs(schema)...)
	logCall(ctx, "DoGetExportedKeys", start, err, attrs...)
	return schema, ch, err
}

// --- Cross reference (implemented) ---

func (l *loggingServer) GetFlightInfoCrossReference(ctx context.Context, cmd flightsql.CrossTableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoCrossReference(ctx, cmd, desc)
	attrs := []slog.Attr{
		slog.String("pk_table", cmd.PKRef.Table),
		slog.String("fk_table", cmd.FKRef.Table),
	}
	attrs = append(attrs, flightInfoAttrs(info)...)
	logCall(ctx, "GetFlightInfoCrossReference", start, err, attrs...)
	return info, err
}

func (l *loggingServer) DoGetCrossReference(ctx context.Context, cmd flightsql.CrossTableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetCrossReference(ctx, cmd)
	attrs := []slog.Attr{
		slog.String("pk_table", cmd.PKRef.Table),
		slog.String("fk_table", cmd.FKRef.Table),
	}
	attrs = append(attrs, schemaAttrs(schema)...)
	logCall(ctx, "DoGetCrossReference", start, err, attrs...)
	return schema, ch, err
}

// --- XDBC type info (implemented) ---

func (l *loggingServer) GetFlightInfoXdbcTypeInfo(ctx context.Context, cmd flightsql.GetXdbcTypeInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoXdbcTypeInfo(ctx, cmd, desc)
	logCall(ctx, "GetFlightInfoXdbcTypeInfo", start, err, flightInfoAttrs(info)...)
	return info, err
}

func (l *loggingServer) DoGetXdbcTypeInfo(ctx context.Context, cmd flightsql.GetXdbcTypeInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetXdbcTypeInfo(ctx, cmd)
	logCall(ctx, "DoGetXdbcTypeInfo", start, err, schemaAttrs(schema)...)
	return schema, ch, err
}

// --- SqlInfo (BaseServer has real implementation) ---

func (l *loggingServer) GetFlightInfoSqlInfo(ctx context.Context, cmd flightsql.GetSqlInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoSqlInfo(ctx, cmd, desc)
	logCall(ctx, "GetFlightInfoSqlInfo", start, err, flightInfoAttrs(info)...)
	return info, err
}

func (l *loggingServer) DoGetSqlInfo(ctx context.Context, cmd flightsql.GetSqlInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetSqlInfo(ctx, cmd)
	logCall(ctx, "DoGetSqlInfo", start, err, schemaAttrs(schema)...)
	return schema, ch, err
}

// --- Flight management (implemented) ---

func (l *loggingServer) CancelFlightInfo(ctx context.Context, req *flight.CancelFlightInfoRequest) (result flight.CancelFlightInfoResult, err error) {
	start := time.Now()
	defer func() {
		logCall(ctx, "CancelFlightInfo", start, err, slog.String("cancel_status", result.Status.String()))
	}()
	result, err = l.DuckFlightSQLServer.CancelFlightInfo(ctx, req)
	return
}

func (l *loggingServer) PollFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.PollFlightInfoStatement(ctx, cmd, desc)
	logCall(ctx, "PollFlightInfoStatement", start, err, queryAttrs(ctx, cmd.GetQuery())...)
	return info, err
}

// isHealthMethod reports whether method is one of the gRPC health-checking
// service methods.
func isHealthMethod(method string) bool {
	switch method {
	case grpc_health_v1.Health_Check_FullMethodName,
		grpc_health_v1.Health_List_FullMethodName,
		grpc_health_v1.Health_Watch_FullMethodName:
		return true
	}
	return false
}

// GRPCLoggingMiddleware returns a flight.ServerMiddleware that logs every
// gRPC call at the transport level (method, duration, error, grpc_code).
// Health-check calls are logged only on failure to keep probe noise out of
// dashboards while still surfacing real probe regressions.
func GRPCLoggingMiddleware() flight.ServerMiddleware {
	logGRPC := func(ctx context.Context, method string, start time.Time, err error) {
		if err == nil && isHealthMethod(method) {
			return
		}
		attrs := []slog.Attr{
			slog.String("grpc_method", method),
			slog.Duration("duration", time.Since(start)),
		}
		if err != nil {
			attrs = append(attrs, slog.String("error", err.Error()))
			if s, ok := status.FromError(err); ok {
				attrs = append(attrs, slog.String("grpc_code", s.Code().String()))
			} else {
				attrs = append(attrs, slog.String("grpc_code", codes.Unknown.String()))
			}
			slog.LogAttrs(ctx, slog.LevelError, "grpc call failed", attrs...)
			return
		}
		slog.LogAttrs(ctx, slog.LevelInfo, "grpc call", attrs...)
	}

	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logGRPC(ctx, info.FullMethod, start, err)
		return resp, err
	}

	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		logGRPC(ss.Context(), info.FullMethod, start, err)
		return err
	}

	return flight.ServerMiddleware{
		Unary:  unary,
		Stream: stream,
	}
}
