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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

func logCall(method string, start time.Time, err error, attrs ...slog.Attr) {
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
		slog.LogAttrs(context.Background(), slog.LevelError, "flight sql call failed", attrs...)
		return
	}
	slog.LogAttrs(context.Background(), slog.LevelInfo, "flight sql call", attrs...)
}

func derefStringPtr(s *string) string {
	if s != nil {
		return *s
	}
	return ""
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
	attrs := []slog.Attr{slog.String("query", cmd.GetQuery())}
	attrs = append(attrs, flightInfoAttrs(info)...)
	logCall("GetFlightInfoStatement", start, err, attrs...)
	return info, err
}

func (l *loggingServer) DoGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetStatement(ctx, cmd)
	logCall("DoGetStatement", start, err, schemaAttrs(schema)...)
	return schema, ch, err
}

func (l *loggingServer) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	start := time.Now()
	n, err := l.DuckFlightSQLServer.DoPutCommandStatementUpdate(ctx, cmd)
	logCall("DoPutCommandStatementUpdate", start, err,
		slog.String("query", cmd.GetQuery()),
		slog.Int64("affected_rows", n),
	)
	return n, err
}

func (l *loggingServer) GetSchemaStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	start := time.Now()
	result, err := l.DuckFlightSQLServer.GetSchemaStatement(ctx, cmd, desc)
	attrs := []slog.Attr{slog.String("query", cmd.GetQuery())}
	if result != nil {
		attrs = append(attrs, slog.Int("schema_bytes", len(result.Schema)))
	}
	logCall("GetSchemaStatement", start, err, attrs...)
	return result, err
}

// --- Substrait (not implemented, delegated to BaseServer) ---

func (l *loggingServer) GetFlightInfoSubstraitPlan(ctx context.Context, cmd flightsql.StatementSubstraitPlan, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoSubstraitPlan(ctx, cmd, desc)
	logCall("GetFlightInfoSubstraitPlan", start, err, flightInfoAttrs(info)...)
	return info, err
}

func (l *loggingServer) GetSchemaSubstraitPlan(ctx context.Context, cmd flightsql.StatementSubstraitPlan, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	start := time.Now()
	result, err := l.DuckFlightSQLServer.GetSchemaSubstraitPlan(ctx, cmd, desc)
	logCall("GetSchemaSubstraitPlan", start, err)
	return result, err
}

func (l *loggingServer) DoPutCommandSubstraitPlan(ctx context.Context, cmd flightsql.StatementSubstraitPlan) (int64, error) {
	start := time.Now()
	n, err := l.DuckFlightSQLServer.DoPutCommandSubstraitPlan(ctx, cmd)
	logCall("DoPutCommandSubstraitPlan", start, err)
	return n, err
}

// --- Catalogs (implemented) ---

func (l *loggingServer) GetFlightInfoCatalogs(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoCatalogs(ctx, desc)
	logCall("GetFlightInfoCatalogs", start, err, flightInfoAttrs(info)...)
	return info, err
}

func (l *loggingServer) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetCatalogs(ctx)
	logCall("DoGetCatalogs", start, err, schemaAttrs(schema)...)
	return schema, ch, err
}

// --- Schemas (implemented) ---

func (l *loggingServer) GetFlightInfoSchemas(ctx context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoSchemas(ctx, cmd, desc)
	logCall("GetFlightInfoSchemas", start, err, flightInfoAttrs(info)...)
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
	logCall("DoGetDBSchemas", start, err, attrs...)
	return schema, ch, err
}

// --- Tables (implemented) ---

func (l *loggingServer) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoTables(ctx, cmd, desc)
	logCall("GetFlightInfoTables", start, err, flightInfoAttrs(info)...)
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
	logCall("DoGetTables", start, err, attrs...)
	return schema, ch, err
}

// --- Table types (implemented) ---

func (l *loggingServer) GetFlightInfoTableTypes(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoTableTypes(ctx, desc)
	logCall("GetFlightInfoTableTypes", start, err, flightInfoAttrs(info)...)
	return info, err
}

func (l *loggingServer) DoGetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetTableTypes(ctx)
	logCall("DoGetTableTypes", start, err, schemaAttrs(schema)...)
	return schema, ch, err
}

// --- Prepared statements (implemented) ---

func (l *loggingServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	start := time.Now()
	result, err := l.DuckFlightSQLServer.CreatePreparedStatement(ctx, req)
	attrs := []slog.Attr{slog.String("query", req.GetQuery())}
	if result.DatasetSchema != nil {
		attrs = append(attrs, slog.String("dataset_schema", schemaFields(result.DatasetSchema)))
	}
	logCall("CreatePreparedStatement", start, err, attrs...)
	return result, err
}

func (l *loggingServer) ClosePreparedStatement(ctx context.Context, req flightsql.ActionClosePreparedStatementRequest) error {
	start := time.Now()
	err := l.DuckFlightSQLServer.ClosePreparedStatement(ctx, req)
	logCall("ClosePreparedStatement", start, err)
	return err
}

func (l *loggingServer) GetFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoPreparedStatement(ctx, cmd, desc)
	logCall("GetFlightInfoPreparedStatement", start, err, flightInfoAttrs(info)...)
	return info, err
}

func (l *loggingServer) GetSchemaPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	start := time.Now()
	result, err := l.DuckFlightSQLServer.GetSchemaPreparedStatement(ctx, cmd, desc)
	logCall("GetSchemaPreparedStatement", start, err)
	return result, err
}

func (l *loggingServer) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetPreparedStatement(ctx, cmd)
	logCall("DoGetPreparedStatement", start, err, schemaAttrs(schema)...)
	return schema, ch, err
}

func (l *loggingServer) DoPutPreparedStatementQuery(ctx context.Context, cmd flightsql.PreparedStatementQuery, rdr flight.MessageReader, w flight.MetadataWriter) ([]byte, error) {
	start := time.Now()
	handle, err := l.DuckFlightSQLServer.DoPutPreparedStatementQuery(ctx, cmd, rdr, w)
	logCall("DoPutPreparedStatementQuery", start, err)
	return handle, err
}

func (l *loggingServer) DoPutPreparedStatementUpdate(ctx context.Context, cmd flightsql.PreparedStatementUpdate, rdr flight.MessageReader) (int64, error) {
	start := time.Now()
	n, err := l.DuckFlightSQLServer.DoPutPreparedStatementUpdate(ctx, cmd, rdr)
	logCall("DoPutPreparedStatementUpdate", start, err, slog.Int64("affected_rows", n))
	return n, err
}

func (l *loggingServer) CreatePreparedSubstraitPlan(ctx context.Context, req flightsql.ActionCreatePreparedSubstraitPlanRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	start := time.Now()
	result, err := l.DuckFlightSQLServer.CreatePreparedSubstraitPlan(ctx, req)
	logCall("CreatePreparedSubstraitPlan", start, err)
	return result, err
}

// --- Ingestion (not implemented) ---

func (l *loggingServer) DoPutCommandStatementIngest(ctx context.Context, cmd flightsql.StatementIngest, rdr flight.MessageReader) (int64, error) {
	start := time.Now()
	n, err := l.DuckFlightSQLServer.DoPutCommandStatementIngest(ctx, cmd, rdr)
	logCall("DoPutCommandStatementIngest", start, err)
	return n, err
}

// --- Transactions (implemented) ---

func (l *loggingServer) BeginTransaction(ctx context.Context, req flightsql.ActionBeginTransactionRequest) ([]byte, error) {
	start := time.Now()
	id, err := l.DuckFlightSQLServer.BeginTransaction(ctx, req)
	logCall("BeginTransaction", start, err)
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
	logCall("EndTransaction", start, err, slog.String("action", action))
	return err
}

// --- Savepoints (not implemented) ---

func (l *loggingServer) BeginSavepoint(ctx context.Context, req flightsql.ActionBeginSavepointRequest) ([]byte, error) {
	start := time.Now()
	id, err := l.DuckFlightSQLServer.BeginSavepoint(ctx, req)
	logCall("BeginSavepoint", start, err)
	return id, err
}

func (l *loggingServer) EndSavepoint(ctx context.Context, req flightsql.ActionEndSavepointRequest) error {
	start := time.Now()
	err := l.DuckFlightSQLServer.EndSavepoint(ctx, req)
	logCall("EndSavepoint", start, err)
	return err
}

// --- Primary keys (implemented) ---

func (l *loggingServer) GetFlightInfoPrimaryKeys(ctx context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoPrimaryKeys(ctx, cmd, desc)
	attrs := []slog.Attr{slog.String("table", cmd.Table)}
	attrs = append(attrs, flightInfoAttrs(info)...)
	logCall("GetFlightInfoPrimaryKeys", start, err, attrs...)
	return info, err
}

func (l *loggingServer) DoGetPrimaryKeys(ctx context.Context, cmd flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetPrimaryKeys(ctx, cmd)
	attrs := []slog.Attr{slog.String("table", cmd.Table)}
	attrs = append(attrs, schemaAttrs(schema)...)
	logCall("DoGetPrimaryKeys", start, err, attrs...)
	return schema, ch, err
}

// --- Imported keys (implemented) ---

func (l *loggingServer) GetFlightInfoImportedKeys(ctx context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoImportedKeys(ctx, cmd, desc)
	attrs := []slog.Attr{slog.String("table", cmd.Table)}
	attrs = append(attrs, flightInfoAttrs(info)...)
	logCall("GetFlightInfoImportedKeys", start, err, attrs...)
	return info, err
}

func (l *loggingServer) DoGetImportedKeys(ctx context.Context, cmd flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetImportedKeys(ctx, cmd)
	attrs := []slog.Attr{slog.String("table", cmd.Table)}
	attrs = append(attrs, schemaAttrs(schema)...)
	logCall("DoGetImportedKeys", start, err, attrs...)
	return schema, ch, err
}

// --- Exported keys (implemented) ---

func (l *loggingServer) GetFlightInfoExportedKeys(ctx context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoExportedKeys(ctx, cmd, desc)
	attrs := []slog.Attr{slog.String("table", cmd.Table)}
	attrs = append(attrs, flightInfoAttrs(info)...)
	logCall("GetFlightInfoExportedKeys", start, err, attrs...)
	return info, err
}

func (l *loggingServer) DoGetExportedKeys(ctx context.Context, cmd flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetExportedKeys(ctx, cmd)
	attrs := []slog.Attr{slog.String("table", cmd.Table)}
	attrs = append(attrs, schemaAttrs(schema)...)
	logCall("DoGetExportedKeys", start, err, attrs...)
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
	logCall("GetFlightInfoCrossReference", start, err, attrs...)
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
	logCall("DoGetCrossReference", start, err, attrs...)
	return schema, ch, err
}

// --- XDBC type info (implemented) ---

func (l *loggingServer) GetFlightInfoXdbcTypeInfo(ctx context.Context, cmd flightsql.GetXdbcTypeInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoXdbcTypeInfo(ctx, cmd, desc)
	logCall("GetFlightInfoXdbcTypeInfo", start, err, flightInfoAttrs(info)...)
	return info, err
}

func (l *loggingServer) DoGetXdbcTypeInfo(ctx context.Context, cmd flightsql.GetXdbcTypeInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetXdbcTypeInfo(ctx, cmd)
	logCall("DoGetXdbcTypeInfo", start, err, schemaAttrs(schema)...)
	return schema, ch, err
}

// --- SqlInfo (BaseServer has real implementation) ---

func (l *loggingServer) GetFlightInfoSqlInfo(ctx context.Context, cmd flightsql.GetSqlInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.GetFlightInfoSqlInfo(ctx, cmd, desc)
	logCall("GetFlightInfoSqlInfo", start, err, flightInfoAttrs(info)...)
	return info, err
}

func (l *loggingServer) DoGetSqlInfo(ctx context.Context, cmd flightsql.GetSqlInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	start := time.Now()
	schema, ch, err := l.DuckFlightSQLServer.DoGetSqlInfo(ctx, cmd)
	logCall("DoGetSqlInfo", start, err, schemaAttrs(schema)...)
	return schema, ch, err
}

// --- Session management (not implemented) ---

func (l *loggingServer) SetSessionOptions(ctx context.Context, req *flight.SetSessionOptionsRequest) (*flight.SetSessionOptionsResult, error) {
	start := time.Now()
	result, err := l.DuckFlightSQLServer.SetSessionOptions(ctx, req)
	logCall("SetSessionOptions", start, err)
	return result, err
}

func (l *loggingServer) GetSessionOptions(ctx context.Context, req *flight.GetSessionOptionsRequest) (*flight.GetSessionOptionsResult, error) {
	start := time.Now()
	result, err := l.DuckFlightSQLServer.GetSessionOptions(ctx, req)
	logCall("GetSessionOptions", start, err)
	return result, err
}

func (l *loggingServer) CloseSession(ctx context.Context, req *flight.CloseSessionRequest) (*flight.CloseSessionResult, error) {
	start := time.Now()
	result, err := l.DuckFlightSQLServer.CloseSession(ctx, req)
	logCall("CloseSession", start, err)
	return result, err
}

// --- Flight management (not implemented) ---

func (l *loggingServer) CancelFlightInfo(ctx context.Context, req *flight.CancelFlightInfoRequest) (result flight.CancelFlightInfoResult, err error) {
	start := time.Now()
	defer func() { logCall("CancelFlightInfo", start, err) }()
	result, err = l.DuckFlightSQLServer.CancelFlightInfo(ctx, req)
	return
}

func (l *loggingServer) RenewFlightEndpoint(ctx context.Context, req *flight.RenewFlightEndpointRequest) (*flight.FlightEndpoint, error) {
	start := time.Now()
	ep, err := l.DuckFlightSQLServer.RenewFlightEndpoint(ctx, req)
	logCall("RenewFlightEndpoint", start, err)
	return ep, err
}

// --- Polling (not implemented) ---

func (l *loggingServer) PollFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.PollFlightInfo(ctx, desc)
	logCall("PollFlightInfo", start, err)
	return info, err
}

func (l *loggingServer) PollFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.PollFlightInfoStatement(ctx, cmd, desc)
	logCall("PollFlightInfoStatement", start, err)
	return info, err
}

func (l *loggingServer) PollFlightInfoSubstraitPlan(ctx context.Context, cmd flightsql.StatementSubstraitPlan, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.PollFlightInfoSubstraitPlan(ctx, cmd, desc)
	logCall("PollFlightInfoSubstraitPlan", start, err)
	return info, err
}

func (l *loggingServer) PollFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	start := time.Now()
	info, err := l.DuckFlightSQLServer.PollFlightInfoPreparedStatement(ctx, cmd, desc)
	logCall("PollFlightInfoPreparedStatement", start, err)
	return info, err
}

// GRPCLoggingMiddleware returns a flight.ServerMiddleware that logs every
// gRPC call at the transport level (method, duration, error, grpc_code).
func GRPCLoggingMiddleware() flight.ServerMiddleware {
	logGRPC := func(ctx context.Context, method string, start time.Time, err error) {
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

	return flight.ServerMiddleware{
		Unary: func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			start := time.Now()
			resp, err := handler(ctx, req)
			logGRPC(ctx, info.FullMethod, start, err)
			return resp, err
		},
		Stream: func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			start := time.Now()
			err := handler(srv, ss)
			logGRPC(ss.Context(), info.FullMethod, start, err)
			return err
		},
	}
}
