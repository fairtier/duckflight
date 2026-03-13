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
	"github.com/prochac/duckflight/internal/engine"
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
		rec := rdr.RecordBatch()
		col := rec.Column(0).(*array.String)
		for i := 0; i < col.Len(); i++ {
			results = append(results, strings.Clone(col.Value(i)))
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
		rec := rdr.RecordBatch()
		col0 := rec.Column(0).(*array.String)
		col1 := rec.Column(1).(*array.String)
		for i := 0; i < col0.Len(); i++ {
			results = append(results, [2]string{strings.Clone(col0.Value(i)), strings.Clone(col1.Value(i))})
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

	batch := array.NewRecordBatch(schema_ref.Catalogs, []arrow.Array{arr}, int64(len(catalogs)))

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
	} else {
		query += " AND catalog_name = current_database()"
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

	batch := array.NewRecordBatch(schema_ref.DBSchemas, []arrow.Array{catArr, schArr}, int64(len(pairs)))

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
	} else {
		query += " AND table_catalog = current_database()"
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
		rec := rdr.RecordBatch()
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

	if cmd.GetIncludeSchema() {
		outSchema = schema_ref.TablesWithIncludedSchema

		// For each table, get its Arrow schema by querying with LIMIT 0.
		binaryBldr := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		defer binaryBldr.Release()

		catStrings := catArr.(*array.String)
		schStrings := schArr.(*array.String)
		nameStrings := nameArr.(*array.String)

		for i := 0; i < int(numRows); i++ {
			catalog := catStrings.Value(i)
			dbSchema := schStrings.Value(i)
			tableName := nameStrings.Value(i)
			tableFQN := fmt.Sprintf("%s.%s.%s", catalog, dbSchema, tableName)
			tblRdr, err := ac.Arrow.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableFQN))
			if err != nil {
				binaryBldr.AppendNull()
				continue
			}
			tblSchema := tblRdr.Schema()
			tblRdr.Release()

			// Enrich the Arrow schema with FlightSQL column metadata (TYPE_NAME)
			// by looking up DuckDB's native type names from information_schema.columns.
			enriched := s.enrichSchemaWithTypeNames(ctx, ac, tblSchema, catalog, dbSchema, tableName)
			binaryBldr.Append(flight.SerializeSchema(enriched, s.Alloc))
		}

		schemaArr := binaryBldr.NewArray()
		defer schemaArr.Release()
		cols = append(cols, schemaArr)
	}

	batch := array.NewRecordBatch(outSchema, cols, numRows)

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)

	return outSchema, ch, nil
}

// enrichSchemaWithTypeNames adds ARROW:FLIGHT:SQL:TYPE_NAME metadata to each
// field in the schema by querying information_schema.columns for the DuckDB
// native type names. If the query fails, the original schema is returned.
func (s *DuckFlightSQLServer) enrichSchemaWithTypeNames(
	ctx context.Context, ac *engine.ArrowConn, schema *arrow.Schema,
	catalog, dbSchema, tableName string,
) *arrow.Schema {
	query := fmt.Sprintf(
		"SELECT column_name, data_type FROM information_schema.columns "+
			"WHERE table_catalog = '%s' AND table_schema = '%s' AND table_name = '%s' "+
			"ORDER BY ordinal_position",
		escapeSQLString(catalog), escapeSQLString(dbSchema), escapeSQLString(tableName),
	)

	rdr, err := ac.Arrow.QueryContext(ctx, query)
	if err != nil {
		return schema
	}
	defer rdr.Release()

	typeByName := make(map[string]string)
	for rdr.Next() {
		rec := rdr.RecordBatch()
		col0 := rec.Column(0).(*array.String)
		col1 := rec.Column(1).(*array.String)
		for i := 0; i < col0.Len(); i++ {
			typeByName[strings.Clone(col0.Value(i))] = strings.Clone(col1.Value(i))
		}
	}
	if rdr.Err() != nil {
		return schema
	}

	fields := make([]arrow.Field, len(schema.Fields()))
	for i, f := range schema.Fields() {
		typeName, ok := typeByName[f.Name]
		if !ok {
			fields[i] = f
			continue
		}

		mdBuilder := flightsql.NewColumnMetadataBuilder()
		mdBuilder.TypeName(typeName)
		md := mdBuilder.Metadata()

		// Merge with any existing metadata on the field.
		if f.HasMetadata() {
			existing := f.Metadata.ToMap()
			for k, v := range md.ToMap() {
				existing[k] = v
			}
			md = arrow.MetadataFrom(existing)
		}
		f.Metadata = md
		fields[i] = f
	}

	schemaMd := schema.Metadata()
	return arrow.NewSchema(fields, &schemaMd)
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

	batch := array.NewRecordBatch(schema_ref.TableTypes, []arrow.Array{arr}, 3)

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)

	return schema_ref.TableTypes, ch, nil
}

// escapeSQLString escapes single quotes in SQL string literals.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
