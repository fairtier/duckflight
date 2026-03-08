//go:build duckdb_arrow

package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *DuckFlightSQLServer) flightInfoForCommand(desc *flight.FlightDescriptor, schema *arrow.Schema) *flight.FlightInfo {
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, s.Alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}
}

// queryStrings executes a query that returns a single string column and collects the results.
func (s *DuckFlightSQLServer) queryStrings(ctx context.Context, query string) ([]string, error) {
	ac, err := s.engine.Pool.Acquire(ctx)
	if err != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "pool acquire: %s", err)
	}
	defer s.engine.Pool.Release(ac)

	rdr, err := ac.Arrow.QueryContext(ctx, query)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query error: %s", err)
	}
	defer rdr.Release()

	var results []string
	for rdr.Next() {
		rec := rdr.Record()
		col := rec.Column(0).(*array.String)
		for i := 0; i < col.Len(); i++ {
			results = append(results, col.Value(i))
		}
	}
	if err := rdr.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "reader error: %s", err)
	}
	return results, nil
}

// queryStringPairs executes a query that returns two string columns and collects the results.
func (s *DuckFlightSQLServer) queryStringPairs(ctx context.Context, query string) ([][2]string, error) {
	ac, err := s.engine.Pool.Acquire(ctx)
	if err != nil {
		return nil, status.Errorf(codes.ResourceExhausted, "pool acquire: %s", err)
	}
	defer s.engine.Pool.Release(ac)

	rdr, err := ac.Arrow.QueryContext(ctx, query)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query error: %s", err)
	}
	defer rdr.Release()

	var results [][2]string
	for rdr.Next() {
		rec := rdr.Record()
		col0 := rec.Column(0).(*array.String)
		col1 := rec.Column(1).(*array.String)
		for i := 0; i < col0.Len(); i++ {
			results = append(results, [2]string{col0.Value(i), col1.Value(i)})
		}
	}
	if err := rdr.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "reader error: %s", err)
	}
	return results, nil
}

// --- GetCatalogs ---

func (s *DuckFlightSQLServer) GetFlightInfoCatalogs(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.Catalogs), nil
}

func (s *DuckFlightSQLServer) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	catalogs, err := s.queryStrings(ctx, "SELECT DISTINCT catalog_name FROM information_schema.schemata ORDER BY catalog_name")
	if err != nil {
		return nil, nil, err
	}

	bldr := array.NewStringBuilder(memory.DefaultAllocator)
	defer bldr.Release()
	for _, c := range catalogs {
		bldr.Append(c)
	}

	arr := bldr.NewArray()
	defer arr.Release()

	batch := array.NewRecord(schema_ref.Catalogs, []arrow.Array{arr}, int64(len(catalogs)))

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)

	return schema_ref.Catalogs, ch, nil
}

// --- GetDBSchemas ---

func (s *DuckFlightSQLServer) GetFlightInfoSchemas(_ context.Context, _ flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.DBSchemas), nil
}

func (s *DuckFlightSQLServer) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := "SELECT catalog_name, schema_name FROM information_schema.schemata WHERE 1=1"

	if catalog := cmd.GetCatalog(); catalog != nil {
		query += fmt.Sprintf(" AND catalog_name = '%s'", escapeSQLString(*catalog))
	}
	if schemaFilter := cmd.GetDBSchemaFilterPattern(); schemaFilter != nil {
		query += fmt.Sprintf(" AND schema_name LIKE '%s'", escapeSQLString(*schemaFilter))
	}
	query += " ORDER BY catalog_name, schema_name"

	pairs, err := s.queryStringPairs(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	catalogBldr := array.NewStringBuilder(memory.DefaultAllocator)
	defer catalogBldr.Release()
	schemaBldr := array.NewStringBuilder(memory.DefaultAllocator)
	defer schemaBldr.Release()

	for _, p := range pairs {
		catalogBldr.Append(p[0])
		schemaBldr.Append(p[1])
	}

	catArr := catalogBldr.NewArray()
	defer catArr.Release()
	schArr := schemaBldr.NewArray()
	defer schArr.Release()

	batch := array.NewRecord(schema_ref.DBSchemas, []arrow.Array{catArr, schArr}, int64(len(pairs)))

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)

	return schema_ref.DBSchemas, ch, nil
}

// --- GetTables ---

func (s *DuckFlightSQLServer) GetFlightInfoTables(_ context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}
	return s.flightInfoForCommand(desc, schema), nil
}

func (s *DuckFlightSQLServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE 1=1"

	if catalog := cmd.GetCatalog(); catalog != nil {
		query += fmt.Sprintf(" AND table_catalog = '%s'", escapeSQLString(*catalog))
	}
	if schemaFilter := cmd.GetDBSchemaFilterPattern(); schemaFilter != nil {
		query += fmt.Sprintf(" AND table_schema LIKE '%s'", escapeSQLString(*schemaFilter))
	}
	if tableFilter := cmd.GetTableNameFilterPattern(); tableFilter != nil {
		query += fmt.Sprintf(" AND table_name LIKE '%s'", escapeSQLString(*tableFilter))
	}
	if types := cmd.GetTableTypes(); len(types) > 0 {
		quoted := make([]string, len(types))
		for i, t := range types {
			quoted[i] = fmt.Sprintf("'%s'", escapeSQLString(t))
		}
		query += fmt.Sprintf(" AND table_type IN (%s)", strings.Join(quoted, ", "))
	}
	query += " ORDER BY table_catalog, table_schema, table_name"

	ac, err := s.engine.Pool.Acquire(ctx)
	if err != nil {
		return nil, nil, status.Errorf(codes.ResourceExhausted, "pool acquire: %s", err)
	}
	defer s.engine.Pool.Release(ac)

	rdr, err := ac.Arrow.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "query error: %s", err)
	}
	defer rdr.Release()

	// Collect results
	catalogBldr := array.NewStringBuilder(memory.DefaultAllocator)
	defer catalogBldr.Release()
	schemaBldr := array.NewStringBuilder(memory.DefaultAllocator)
	defer schemaBldr.Release()
	nameBldr := array.NewStringBuilder(memory.DefaultAllocator)
	defer nameBldr.Release()
	typeBldr := array.NewStringBuilder(memory.DefaultAllocator)
	defer typeBldr.Release()

	var numRows int64
	for rdr.Next() {
		rec := rdr.Record()
		col0 := rec.Column(0).(*array.String)
		col1 := rec.Column(1).(*array.String)
		col2 := rec.Column(2).(*array.String)
		col3 := rec.Column(3).(*array.String)
		for i := 0; i < col0.Len(); i++ {
			catalogBldr.Append(col0.Value(i))
			schemaBldr.Append(col1.Value(i))
			nameBldr.Append(col2.Value(i))
			typeBldr.Append(col3.Value(i))
			numRows++
		}
	}
	if err := rdr.Err(); err != nil {
		return nil, nil, status.Errorf(codes.Internal, "reader error: %s", err)
	}

	catArr := catalogBldr.NewArray()
	defer catArr.Release()
	schArr := schemaBldr.NewArray()
	defer schArr.Release()
	nameArr := nameBldr.NewArray()
	defer nameArr.Release()
	typeArr := typeBldr.NewArray()
	defer typeArr.Release()

	outSchema := schema_ref.Tables
	cols := []arrow.Array{catArr, schArr, nameArr, typeArr}

	batch := array.NewRecord(outSchema, cols, numRows)

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)

	return outSchema, ch, nil
}

// --- GetTableTypes ---

func (s *DuckFlightSQLServer) GetFlightInfoTableTypes(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.TableTypes), nil
}

func (s *DuckFlightSQLServer) DoGetTableTypes(_ context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	bldr := array.NewStringBuilder(memory.DefaultAllocator)
	defer bldr.Release()

	bldr.Append("BASE TABLE")
	bldr.Append("LOCAL TEMPORARY")
	bldr.Append("VIEW")

	arr := bldr.NewArray()
	defer arr.Release()

	batch := array.NewRecord(schema_ref.TableTypes, []arrow.Array{arr}, 3)

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)

	return schema_ref.TableTypes, ch, nil
}

// escapeSQLString escapes single quotes in SQL string literals.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
