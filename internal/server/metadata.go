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

// streamMetadata executes a query and streams the resulting Arrow batches with
// the given schema stamped on. The SQL must produce columns whose types match
// the target schema exactly — only schema-level metadata (field names,
// nullability) is overwritten. No data is copied.
func (s *DuckFlightSQLServer) streamMetadata(
	ctx context.Context, query string, schema *arrow.Schema,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	ac, err := s.acquirePoolConn(ctx)
	if err != nil {
		return nil, nil, err
	}

	rdr, err := ac.Arrow.QueryContext(ctx, query)
	if err != nil {
		s.engine.Pool.Release(ac)
		return nil, nil, status.Errorf(codes.Internal, "query error: %s", err)
	}

	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		defer rdr.Release()
		defer s.engine.Pool.Release(ac)
		for rdr.Next() {
			rec := rdr.RecordBatch()
			cols := make([]arrow.Array, rec.NumCols())
			for i := range cols {
				cols[i] = rec.Column(i)
			}
			out := array.NewRecordBatch(schema, cols, rec.NumRows())
			ch <- flight.StreamChunk{Data: out}
		}
		if err := rdr.Err(); err != nil {
			ch <- flight.StreamChunk{Err: status.Errorf(codes.Internal, "reader error: %s", err)}
		}
	}()
	return schema, ch, nil
}

// --- GetCatalogs ---

func (s *DuckFlightSQLServer) GetFlightInfoCatalogs(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.Catalogs), nil
}

func (s *DuckFlightSQLServer) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return s.streamMetadata(ctx,
		"SELECT DISTINCT catalog_name FROM information_schema.schemata ORDER BY catalog_name",
		schema_ref.Catalogs)
}

// --- GetDBSchemas ---

func (s *DuckFlightSQLServer) GetFlightInfoSchemas(_ context.Context, _ flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.DBSchemas), nil
}

func (s *DuckFlightSQLServer) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := "SELECT catalog_name, schema_name AS db_schema_name FROM information_schema.schemata WHERE " +
		catalogFilter("catalog_name", cmd.GetCatalog())
	if sf := cmd.GetDBSchemaFilterPattern(); sf != nil {
		query += fmt.Sprintf(" AND schema_name LIKE '%s'", escapeSQLString(*sf))
	}
	query += " ORDER BY catalog_name, db_schema_name"

	return s.streamMetadata(ctx, query, schema_ref.DBSchemas)
}

// --- GetTables ---

func (s *DuckFlightSQLServer) GetFlightInfoTables(_ context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}
	return s.flightInfoForCommand(desc, schema), nil
}

func (s *DuckFlightSQLServer) doGetTablesQuery(cmd flightsql.GetTables) string {
	query := "SELECT table_catalog AS catalog_name, table_schema AS db_schema_name, table_name, table_type FROM information_schema.tables WHERE " +
		catalogFilter("table_catalog", cmd.GetCatalog())
	if sf := cmd.GetDBSchemaFilterPattern(); sf != nil {
		query += fmt.Sprintf(" AND table_schema LIKE '%s'", escapeSQLString(*sf))
	}
	if tf := cmd.GetTableNameFilterPattern(); tf != nil {
		query += fmt.Sprintf(" AND table_name LIKE '%s'", escapeSQLString(*tf))
	}
	if types := cmd.GetTableTypes(); len(types) > 0 {
		quoted := make([]string, len(types))
		for i, t := range types {
			quoted[i] = fmt.Sprintf("'%s'", escapeSQLString(t))
		}
		query += fmt.Sprintf(" AND table_type IN (%s)", strings.Join(quoted, ", "))
	}
	query += " ORDER BY table_catalog, table_schema, table_name"
	return query
}

func (s *DuckFlightSQLServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := s.doGetTablesQuery(cmd)

	if !cmd.GetIncludeSchema() {
		return s.streamMetadata(ctx, query, schema_ref.Tables)
	}

	// With include_schema we need to append a binary column per batch.
	ac, err := s.acquirePoolConn(ctx)
	if err != nil {
		return nil, nil, err
	}

	rdr, err := ac.Arrow.QueryContext(ctx, query)
	if err != nil {
		s.engine.Pool.Release(ac)
		return nil, nil, status.Errorf(codes.Internal, "query error: %s", err)
	}

	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		defer rdr.Release()
		defer s.engine.Pool.Release(ac)
		for rdr.Next() {
			rec := rdr.RecordBatch()
			nrows := rec.NumRows()

			catCol := rec.Column(0).(*array.String)
			schCol := rec.Column(1).(*array.String)
			nameCol := rec.Column(2).(*array.String)

			// Bulk-fetch column metadata for all tables in this batch.
			schemas, err := s.buildBatchTableSchemas(ctx, ac, catCol, schCol, nameCol)
			if err != nil {
				ch <- flight.StreamChunk{Err: status.Errorf(codes.Internal, "build schemas: %s", err)}
				return
			}

			binaryBldr := array.NewBinaryBuilder(s.Alloc, arrow.BinaryTypes.Binary)
			for i := 0; i < int(nrows); i++ {
				key := tableKey{catCol.Value(i), schCol.Value(i), nameCol.Value(i)}
				if tblSchema, ok := schemas[key]; ok {
					binaryBldr.Append(flight.SerializeSchema(tblSchema, s.Alloc))
				} else {
					binaryBldr.AppendNull()
				}
			}

			schemaArr := binaryBldr.NewArray()
			binaryBldr.Release()

			cols := make([]arrow.Array, 5)
			for i := 0; i < 4; i++ {
				cols[i] = rec.Column(i)
			}
			cols[4] = schemaArr

			out := array.NewRecordBatch(schema_ref.TablesWithIncludedSchema, cols, nrows)
			schemaArr.Release()

			ch <- flight.StreamChunk{Data: out}
		}
		if err := rdr.Err(); err != nil {
			ch <- flight.StreamChunk{Err: status.Errorf(codes.Internal, "reader error: %s", err)}
		}
	}()

	return schema_ref.TablesWithIncludedSchema, ch, nil
}

type tableKey struct {
	catalog, schema, table string
}

// buildBatchTableSchemas fetches column metadata for all tables in a batch
// with a single query and returns a map of table → Arrow schema.
func (s *DuckFlightSQLServer) buildBatchTableSchemas(
	ctx context.Context, ac *engine.ArrowConn,
	catCol, schCol, nameCol *array.String,
) (map[tableKey]*arrow.Schema, error) {
	n := catCol.Len()
	if n == 0 {
		return nil, nil
	}

	// Build a VALUES list for the batch filter.
	var sb strings.Builder
	sb.WriteString(
		"SELECT table_catalog, table_schema, table_name, " +
			"column_name, data_type, numeric_precision, numeric_scale " +
			"FROM information_schema.columns WHERE (table_catalog, table_schema, table_name) IN (")
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "('%s', '%s', '%s')",
			escapeSQLString(catCol.Value(i)),
			escapeSQLString(schCol.Value(i)),
			escapeSQLString(nameCol.Value(i)))
	}
	sb.WriteString(") ORDER BY table_catalog, table_schema, table_name, ordinal_position")

	rdr, err := ac.Arrow.QueryContext(ctx, sb.String())
	if err != nil {
		return nil, fmt.Errorf("bulk query columns: %w", err)
	}
	defer rdr.Release()

	// Collect fields grouped by table.
	type tableFields struct {
		key    tableKey
		fields []arrow.Field
	}
	byTable := make(map[tableKey]*tableFields)
	// Preserve insertion order for deterministic iteration.
	var order []tableKey

	for rdr.Next() {
		rec := rdr.RecordBatch()
		cats := rec.Column(0).(*array.String)
		schs := rec.Column(1).(*array.String)
		tbls := rec.Column(2).(*array.String)
		colNames := rec.Column(3).(*array.String)
		colTypes := rec.Column(4).(*array.String)
		colPrec := rec.Column(5).(*array.Int32)
		colScale := rec.Column(6).(*array.Int32)

		for i := 0; i < int(rec.NumRows()); i++ {
			key := tableKey{
				catalog: strings.Clone(cats.Value(i)),
				schema:  strings.Clone(schs.Value(i)),
				table:   strings.Clone(tbls.Value(i)),
			}
			tf, ok := byTable[key]
			if !ok {
				tf = &tableFields{key: key}
				byTable[key] = tf
				order = append(order, key)
			}

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

			tf.fields = append(tf.fields, arrow.Field{
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

	result := make(map[tableKey]*arrow.Schema, len(order))
	for _, key := range order {
		result[key] = arrow.NewSchema(byTable[key].fields, nil)
	}
	return result, nil
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

func catalogFilter(col string, c *string) string {
	if c != nil {
		return fmt.Sprintf("%s = '%s'", col, escapeSQLString(*c))
	}
	return col + " = current_database()"
}

func schemaFilter(col string, s *string) string {
	if s != nil {
		return fmt.Sprintf("%s = '%s'", col, escapeSQLString(*s))
	}
	return col + " = current_schema()"
}

// escapeSQLString escapes single quotes in SQL string literals.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
