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

	bldr := array.NewStringBuilder(s.Alloc)
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

	catalogBldr := array.NewStringBuilder(s.Alloc)
	defer catalogBldr.Release()
	schemaBldr := array.NewStringBuilder(s.Alloc)
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
	catalogBldr := array.NewStringBuilder(s.Alloc)
	defer catalogBldr.Release()
	schemaBldr := array.NewStringBuilder(s.Alloc)
	defer schemaBldr.Release()
	nameBldr := array.NewStringBuilder(s.Alloc)
	defer nameBldr.Release()
	typeBldr := array.NewStringBuilder(s.Alloc)
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

		binaryBldr := array.NewBinaryBuilder(s.Alloc, arrow.BinaryTypes.Binary)
		defer binaryBldr.Release()

		catStrings := catArr.(*array.String)
		schStrings := schArr.(*array.String)
		nameStrings := nameArr.(*array.String)

		for i := 0; i < int(numRows); i++ {
			catalog := catStrings.Value(i)
			dbSchema := schStrings.Value(i)
			tableName := nameStrings.Value(i)

			tblSchema, err := s.buildTableSchema(ctx, ac, catalog, dbSchema, tableName)
			if err != nil {
				binaryBldr.AppendNull()
				continue
			}
			binaryBldr.Append(flight.SerializeSchema(tblSchema, s.Alloc))
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

// buildTableSchema constructs an Arrow schema for a table entirely from
// information_schema.columns. This avoids querying the table directly, which
// can trigger a SIGSEGV in duckdb-go for tables created via Arrow.RegisterView.
func (s *DuckFlightSQLServer) buildTableSchema(
	ctx context.Context, ac *engine.ArrowConn,
	catalog, dbSchema, tableName string,
) (*arrow.Schema, error) {
	query := fmt.Sprintf(
		"SELECT column_name, data_type, numeric_precision, numeric_scale "+
			"FROM information_schema.columns "+
			"WHERE table_catalog = '%s' AND table_schema = '%s' AND table_name = '%s' "+
			"ORDER BY ordinal_position",
		escapeSQLString(catalog), escapeSQLString(dbSchema), escapeSQLString(tableName),
	)

	rdr, err := ac.Arrow.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query columns: %w", err)
	}
	defer rdr.Release()

	var fields []arrow.Field
	for rdr.Next() {
		rec := rdr.RecordBatch()
		colNames := rec.Column(0).(*array.String)
		colTypes := rec.Column(1).(*array.String)
		colPrec := rec.Column(2).(*array.Int32)
		colScale := rec.Column(3).(*array.Int32)

		for i := 0; i < colNames.Len(); i++ {
			name := strings.Clone(colNames.Value(i))
			dataType := strings.Clone(colTypes.Value(i))

			var precision, scale int32
			if !colPrec.IsNull(i) {
				precision = colPrec.Value(i)
			}
			if !colScale.IsNull(i) {
				scale = colScale.Value(i)
			}

			mdBuilder := flightsql.NewColumnMetadataBuilder()
			mdBuilder.TypeName(dataType)

			fields = append(fields, arrow.Field{
				Name:     name,
				Type:     duckDBDataTypeToArrow(dataType, precision, scale),
				Nullable: true,
				Metadata: mdBuilder.Metadata(),
			})
		}
	}
	if err := rdr.Err(); err != nil {
		return nil, fmt.Errorf("read columns: %w", err)
	}

	return arrow.NewSchema(fields, nil), nil
}

// duckDBDataTypeToArrow maps a DuckDB data_type string to an arrow.DataType.
func duckDBDataTypeToArrow(dataType string, precision, scale int32) arrow.DataType {
	switch dataType {
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16
	case "INTEGER":
		return arrow.PrimitiveTypes.Int32
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64
	case "UTINYINT":
		return arrow.PrimitiveTypes.Uint8
	case "USMALLINT":
		return arrow.PrimitiveTypes.Uint16
	case "UINTEGER":
		return arrow.PrimitiveTypes.Uint32
	case "UBIGINT":
		return arrow.PrimitiveTypes.Uint64
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64
	case "VARCHAR":
		return arrow.BinaryTypes.String
	case "BLOB":
		return arrow.BinaryTypes.Binary
	case "DATE":
		return arrow.FixedWidthTypes.Date32
	case "TIME":
		return arrow.FixedWidthTypes.Time64us
	case "TIMESTAMP":
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	case "TIMESTAMP WITH TIME ZONE":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case "DECIMAL":
		if precision == 0 {
			precision = 18
		}
		return &arrow.Decimal128Type{Precision: precision, Scale: scale}
	case "HUGEINT", "UHUGEINT":
		return &arrow.Decimal128Type{Precision: 38, Scale: 0}
	case "INTERVAL":
		return arrow.FixedWidthTypes.MonthDayNanoInterval
	default:
		return arrow.BinaryTypes.String
	}
}

// --- GetTableTypes ---

func (s *DuckFlightSQLServer) GetFlightInfoTableTypes(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.TableTypes), nil
}

func (s *DuckFlightSQLServer) DoGetTableTypes(_ context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	bldr := array.NewStringBuilder(s.Alloc)
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
