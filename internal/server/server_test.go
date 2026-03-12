//go:build duckdb_arrow

package server_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prochac/duckflight/internal/server"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// Suite setup
// ---------------------------------------------------------------------------

type DuckFlightSQLSuite struct {
	suite.Suite

	server flight.Server
	client *flightsql.Client
	mem    *memory.CheckedAllocator
}

func (s *DuckFlightSQLSuite) SetupSuite() {
	srv, err := server.New(server.Config{
		MemoryLimit:  "512MB",
		MaxThreads:   2,
		QueryTimeout: "10s",
		PoolSize:     4,
	})
	s.Require().NoError(err, "failed to create duckflight server")

	s.server = flight.NewServerWithMiddleware(nil)
	s.server.RegisterFlightService(flightsql.NewFlightServer(srv))
	s.Require().NoError(s.server.Init("localhost:0"))

	go func() { _ = s.server.Serve() }()

	cl, err := flightsql.NewClient(
		s.server.Addr().String(),
		nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	s.Require().NoError(err)
	s.client = cl
}

func (s *DuckFlightSQLSuite) TearDownSuite() {
	if s.client != nil {
		_ = s.client.Close()
	}
	if s.server != nil {
		s.server.Shutdown()
	}
}

func (s *DuckFlightSQLSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	s.client.Alloc = s.mem

	// Seed canonical test tables fresh for each test.
	s.seedSQL(`CREATE OR REPLACE TABLE foreignTable (id INTEGER PRIMARY KEY, foreignName VARCHAR, value BIGINT)`)
	s.seedSQL(`INSERT INTO foreignTable VALUES (1, 'one', 1)`)
	s.seedSQL(`DROP TABLE IF EXISTS intTable`)
	s.seedSQL(`DROP SEQUENCE IF EXISTS intTable_id_seq`)
	s.seedSQL(`CREATE SEQUENCE intTable_id_seq START 100`)
	s.seedSQL(`CREATE TABLE intTable (id INTEGER DEFAULT nextval('intTable_id_seq') PRIMARY KEY, keyName VARCHAR, value BIGINT, foreignId BIGINT)`)
	s.seedSQL(`INSERT INTO intTable VALUES (1, 'one', 1, 1), (2, 'zero', 0, 1), (3, 'negative one', -1, 1), (4, NULL, NULL, NULL)`)
}

func (s *DuckFlightSQLSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (s *DuckFlightSQLSuite) seedSQL(sql string) {
	s.T().Helper()
	err := server.SeedSQL(context.Background(), sql)
	s.Require().NoError(err, "seedSQL failed: %s", sql)
}

func (s *DuckFlightSQLSuite) fromJSON(dt arrow.DataType, json string) arrow.Array {
	arr, _, _ := array.FromJSON(s.mem, dt, strings.NewReader(json))
	return arr
}

func (s *DuckFlightSQLSuite) execCountQuery() int64 {
	info, err := s.client.Execute(context.Background(), "SELECT COUNT(*) FROM intTable")
	s.NoError(err)
	rdr, err := s.client.DoGet(context.Background(), info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()
	rec, err := rdr.Read()
	s.NoError(err)
	return rec.Column(0).(*array.Int64).Value(0)
}

// =========================================================================
// M2: Query execution
// =========================================================================

func (s *DuckFlightSQLSuite) TestCommandStatementQuery() {
	ctx := context.Background()
	info, err := s.client.Execute(ctx, "SELECT * FROM intTable")
	s.NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	s.NotNil(rec)
	rec.Retain()
	defer rec.Release()
	s.False(rdr.Next())

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "keyName", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "foreignId", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	s.Truef(expectedSchema.Equal(rec.Schema()), "expected: %s\ngot: %s", expectedSchema, rec.Schema())

	idarr := s.fromJSON(arrow.PrimitiveTypes.Int32, `[1, 2, 3, 4]`)
	defer idarr.Release()
	keyarr := s.fromJSON(arrow.BinaryTypes.String, `["one", "zero", "negative one", null]`)
	defer keyarr.Release()
	valarr := s.fromJSON(arrow.PrimitiveTypes.Int64, `[1, 0, -1, null]`)
	defer valarr.Release()
	foreignarr := s.fromJSON(arrow.PrimitiveTypes.Int64, `[1, 1, 1, null]`)
	defer foreignarr.Release()

	expectedRec := array.NewRecordBatch(expectedSchema, []arrow.Array{idarr, keyarr, valarr, foreignarr}, 4)
	defer expectedRec.Release()

	s.Truef(array.RecordEqual(expectedRec, rec), "expected: %s\ngot: %s", expectedRec, rec)
}

func (s *DuckFlightSQLSuite) TestCommandStatementQueryEmpty() {
	ctx := context.Background()
	s.seedSQL("CREATE OR REPLACE TABLE emptyTable (id INTEGER)")

	info, err := s.client.Execute(ctx, "SELECT * FROM emptyTable")
	s.NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	s.Truef(expectedSchema.Equal(rdr.Schema()), "expected: %s\ngot: %s", expectedSchema, rdr.Schema())

	var total int64
	for rdr.Next() {
		total += rdr.RecordBatch().NumRows()
	}
	s.Equal(int64(0), total)
}

func (s *DuckFlightSQLSuite) TestCommandStatementQuerySyntaxError() {
	ctx := context.Background()
	info, err := s.client.Execute(ctx, "SELECTTTT NOTHING")
	s.Require().NoError(err, "Execute should succeed (errors deferred to DoGet)")

	_, err = s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Error(err, "DoGet should fail for invalid SQL")
}

func (s *DuckFlightSQLSuite) TestCommandStatementQueryMultipleBatches() {
	ctx := context.Background()
	info, err := s.client.Execute(ctx, "SELECT * FROM range(10000) t(id)")
	s.NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	var total int64
	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()
		total += rec.NumRows()
		rec.Release()
	}
	s.Equal(int64(10000), total)
}

func (s *DuckFlightSQLSuite) TestCommandStatementQuerySchemaTypes() {
	ctx := context.Background()
	query := `SELECT
		42::INTEGER AS int_col,
		3.14::DOUBLE AS double_col,
		'hello'::VARCHAR AS str_col,
		true::BOOLEAN AS bool_col,
		DATE '2025-01-15' AS date_col`

	info, err := s.client.Execute(ctx, query)
	s.NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	s.NotNil(rec)
	rec.Retain()
	defer rec.Release()
	s.False(rdr.Next())

	schema := rec.Schema()
	s.Equal(5, schema.NumFields())
	s.Equal("int_col", schema.Field(0).Name)
	s.Equal(arrow.PrimitiveTypes.Int32, schema.Field(0).Type)
	s.Equal("double_col", schema.Field(1).Name)
	s.Equal(arrow.PrimitiveTypes.Float64, schema.Field(1).Type)
	s.Equal("str_col", schema.Field(2).Name)
	s.Equal(arrow.BinaryTypes.String, schema.Field(2).Type)
	s.Equal("bool_col", schema.Field(3).Name)
	s.Equal(arrow.FixedWidthTypes.Boolean, schema.Field(3).Type)
	s.Equal("date_col", schema.Field(4).Name)
	s.Equal(arrow.FixedWidthTypes.Date32, schema.Field(4).Type)
}

// =========================================================================
// M3: Metadata
// =========================================================================

func (s *DuckFlightSQLSuite) TestCommandGetCatalogs() {
	ctx := context.Background()
	info, err := s.client.GetCatalogs(ctx)
	s.NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	s.True(rdr.Schema().Equal(schema_ref.Catalogs), rdr.Schema().String())

	catalog := s.fromJSON(arrow.BinaryTypes.String, `["memory", "system", "temp"]`)
	defer catalog.Release()
	expected := array.NewRecordBatch(schema_ref.Catalogs, []arrow.Array{catalog}, 3)
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	s.NotNil(rec)
	rec.Retain()
	defer rec.Release()
	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)

	s.False(rdr.Next())
}

func (s *DuckFlightSQLSuite) TestCommandGetDbSchemas() {
	ctx := context.Background()
	info, err := s.client.GetDBSchemas(ctx, &flightsql.GetDBSchemasOpts{})
	s.NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	s.True(rdr.Schema().Equal(schema_ref.DBSchemas), rdr.Schema().String())

	catalog := s.fromJSON(arrow.BinaryTypes.String, `["memory", "system", "system", "system", "temp"]`)
	defer catalog.Release()
	schema := s.fromJSON(arrow.BinaryTypes.String, `["main", "information_schema", "main", "pg_catalog", "main"]`)
	defer schema.Release()
	expected := array.NewRecordBatch(schema_ref.DBSchemas, []arrow.Array{catalog, schema}, 5)
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	s.NotNil(rec)
	rec.Retain()
	defer rec.Release()
	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)

	s.False(rdr.Next())
}

func (s *DuckFlightSQLSuite) TestCommandGetTables() {
	ctx := context.Background()
	info, err := s.client.GetTables(ctx, &flightsql.GetTablesOpts{
		DbSchemaFilterPattern: new("main"),
	})
	s.NoError(err)
	s.NotNil(info)

	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	catalogName := s.fromJSON(arrow.BinaryTypes.String, `["memory", "memory"]`)
	defer catalogName.Release()
	schemaName := s.fromJSON(arrow.BinaryTypes.String, `["main", "main"]`)
	defer schemaName.Release()
	tableName := s.fromJSON(arrow.BinaryTypes.String, `["foreignTable", "intTable"]`)
	defer tableName.Release()
	tableType := s.fromJSON(arrow.BinaryTypes.String, `["BASE TABLE", "BASE TABLE"]`)
	defer tableType.Release()

	expectedRec := array.NewRecordBatch(schema_ref.Tables, []arrow.Array{catalogName, schemaName, tableName, tableType}, 2)
	defer expectedRec.Release()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	s.NotNil(rec)
	rec.Retain()
	defer rec.Release()
	s.False(rdr.Next())

	s.Truef(array.RecordEqual(expectedRec, rec), "expected: %s\ngot: %s", expectedRec, rec)
}

func (s *DuckFlightSQLSuite) TestCommandGetTablesWithTableFilter() {
	ctx := context.Background()
	info, err := s.client.GetTables(ctx, &flightsql.GetTablesOpts{
		TableNameFilterPattern: new("int%"),
	})
	s.NoError(err)
	s.NotNil(info)

	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	catalog := s.fromJSON(arrow.BinaryTypes.String, `["memory"]`)
	defer catalog.Release()
	schema := s.fromJSON(arrow.BinaryTypes.String, `["main"]`)
	defer schema.Release()
	table := s.fromJSON(arrow.BinaryTypes.String, `["intTable"]`)
	defer table.Release()
	tabletype := s.fromJSON(arrow.BinaryTypes.String, `["BASE TABLE"]`)
	defer tabletype.Release()

	expected := array.NewRecordBatch(schema_ref.Tables, []arrow.Array{catalog, schema, table, tabletype}, 1)
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	s.NotNil(rec)
	rec.Retain()
	defer rec.Release()
	s.False(rdr.Next())
	s.NoError(rdr.Err())

	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)
}

func (s *DuckFlightSQLSuite) TestCommandGetTablesWithTableTypesFilter() {
	ctx := context.Background()
	info, err := s.client.GetTables(ctx, &flightsql.GetTablesOpts{
		Catalog:               new("memory"),
		DbSchemaFilterPattern: new("main"),
		TableTypes:            []string{"VIEW"},
	})
	s.NoError(err)

	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	s.True(schema_ref.Tables.Equal(rdr.Schema()), rdr.Schema().String())

	var total int64
	for rdr.Next() {
		total += rdr.RecordBatch().NumRows()
	}
	s.Equal(int64(0), total, "expected no tables matching VIEW type filter")
}

func (s *DuckFlightSQLSuite) TestCommandGetTablesWithExistingTableTypeFilter() {
	ctx := context.Background()
	info, err := s.client.GetTables(ctx, &flightsql.GetTablesOpts{
		DbSchemaFilterPattern: new("main"),
		TableTypes:            []string{"BASE TABLE"},
	})
	s.NoError(err)
	s.NotNil(info)

	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	catalogName := s.fromJSON(arrow.BinaryTypes.String, `["memory", "memory"]`)
	defer catalogName.Release()
	schemaName := s.fromJSON(arrow.BinaryTypes.String, `["main", "main"]`)
	defer schemaName.Release()
	tableName := s.fromJSON(arrow.BinaryTypes.String, `["foreignTable", "intTable"]`)
	defer tableName.Release()
	tableType := s.fromJSON(arrow.BinaryTypes.String, `["BASE TABLE", "BASE TABLE"]`)
	defer tableType.Release()

	expectedRec := array.NewRecordBatch(schema_ref.Tables, []arrow.Array{catalogName, schemaName, tableName, tableType}, 2)
	defer expectedRec.Release()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	s.NotNil(rec)
	rec.Retain()
	defer rec.Release()
	s.False(rdr.Next())

	s.Truef(array.RecordEqual(expectedRec, rec), "expected: %s\ngot: %s", expectedRec, rec)
}

func (s *DuckFlightSQLSuite) TestCommandGetTablesWithIncludedSchemasNoFilter() {
	ctx := context.Background()
	info, err := s.client.GetTables(ctx, &flightsql.GetTablesOpts{
		IncludeSchema: true,
	})
	s.NoError(err)
	s.NotNil(info)

	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	s.True(rdr.Next())
	s.False(rdr.Next())
	s.NoError(rdr.Err())
}

func (s *DuckFlightSQLSuite) TestCommandGetTablesWithIncludedSchemas() {
	ctx := context.Background()
	info, err := s.client.GetTables(ctx, &flightsql.GetTablesOpts{
		TableNameFilterPattern: new("int%"),
		IncludeSchema:          true,
	})
	s.NoError(err)
	s.NotNil(info)

	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	catalog := s.fromJSON(arrow.BinaryTypes.String, `["memory"]`)
	schemaName := s.fromJSON(arrow.BinaryTypes.String, `["main"]`)
	table := s.fromJSON(arrow.BinaryTypes.String, `["intTable"]`)
	tabletype := s.fromJSON(arrow.BinaryTypes.String, `["BASE TABLE"]`)

	tableSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{flightsql.TypeNameKey: "INTEGER"})},
		{Name: "keyName", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{flightsql.TypeNameKey: "VARCHAR"})},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{flightsql.TypeNameKey: "BIGINT"})},
		{Name: "foreignId", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{flightsql.TypeNameKey: "BIGINT"})},
	}, nil)
	schemaBuf := flight.SerializeSchema(tableSchema, s.mem)
	binaryBldr := array.NewBinaryBuilder(s.mem, arrow.BinaryTypes.Binary)
	binaryBldr.Append(schemaBuf)
	schemaCol := binaryBldr.NewArray()

	expected := array.NewRecordBatch(schema_ref.TablesWithIncludedSchema, []arrow.Array{catalog, schemaName, table, tabletype, schemaCol}, 1)
	defer func() {
		catalog.Release()
		schemaName.Release()
		table.Release()
		tabletype.Release()
		binaryBldr.Release()
		schemaCol.Release()
		expected.Release()
	}()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	s.NotNil(rec)
	rec.Retain()
	defer rec.Release()
	s.False(rdr.Next())
	s.NoError(rdr.Err())

	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)
}

func (s *DuckFlightSQLSuite) TestCommandGetTableTypes() {
	ctx := context.Background()
	info, err := s.client.GetTableTypes(ctx)
	s.NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	expected := s.fromJSON(arrow.BinaryTypes.String, `["BASE TABLE", "LOCAL TEMPORARY", "VIEW"]`)
	defer expected.Release()
	expectedRec := array.NewRecordBatch(schema_ref.TableTypes, []arrow.Array{expected}, 3)
	defer expectedRec.Release()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	s.Truef(array.RecordEqual(expectedRec, rec), "expected: %s\ngot: %s", expectedRec, rec)
	s.False(rdr.Next())
}

// =========================================================================
// M4+M5: Statement updates & prepared statements (TDD — fail until implemented)
// =========================================================================

func (s *DuckFlightSQLSuite) TestCommandStatementUpdate() {
	ctx := context.Background()
	result, err := s.client.ExecuteUpdate(ctx, `INSERT INTO intTable (keyName, value) VALUES
		('KEYNAME1', 1001), ('KEYNAME2', 1002), ('KEYNAME3', 1003)`)
	s.NoError(err)
	s.EqualValues(3, result)

	result, err = s.client.ExecuteUpdate(ctx, `UPDATE intTable SET keyName = 'KEYNAME1'
		WHERE keyName = 'KEYNAME2' OR keyName = 'KEYNAME3'`)
	s.NoError(err)
	s.EqualValues(2, result)

	result, err = s.client.ExecuteUpdate(ctx, `DELETE FROM intTable WHERE keyName = 'KEYNAME1'`)
	s.NoError(err)
	s.EqualValues(3, result)
}

func (s *DuckFlightSQLSuite) TestCommandStatementUpdateDDL() {
	ctx := context.Background()

	_, err := s.client.ExecuteUpdate(ctx,
		"CREATE TABLE IF NOT EXISTS test_ddl_via_update (x INTEGER)")
	s.NoError(err)

	// Verify the table exists by querying it.
	info, err := s.client.Execute(ctx, "SELECT * FROM test_ddl_via_update")
	s.NoError(err)
	s.NotEmpty(info.Endpoint)
}

func (s *DuckFlightSQLSuite) TestCommandPreparedStatementQuery() {
	ctx := context.Background()
	prep, err := s.client.Prepare(ctx, "SELECT * FROM intTable")
	s.NoError(err)
	defer func() { _ = prep.Close(ctx) }()

	info, err := prep.Execute(ctx)
	s.NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "keyName", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "foreignId", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	idarr := s.fromJSON(arrow.PrimitiveTypes.Int32, `[1, 2, 3, 4]`)
	defer idarr.Release()
	keyarr := s.fromJSON(arrow.BinaryTypes.String, `["one", "zero", "negative one", null]`)
	defer keyarr.Release()
	valarr := s.fromJSON(arrow.PrimitiveTypes.Int64, `[1, 0, -1, null]`)
	defer valarr.Release()
	foreignarr := s.fromJSON(arrow.PrimitiveTypes.Int64, `[1, 1, 1, null]`)
	defer foreignarr.Release()

	expected := array.NewRecordBatch(expectedSchema, []arrow.Array{idarr, keyarr, valarr, foreignarr}, 4)
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)
	s.False(rdr.Next())
}

func (s *DuckFlightSQLSuite) TestCommandPreparedStatementUpdate() {
	ctx := context.Background()
	stmt, err := s.client.Prepare(ctx, "INSERT INTO intTable (keyName, value) VALUES ('new_value', 999)")
	s.NoError(err)
	defer func() { _ = stmt.Close(ctx) }()

	s.EqualValues(4, s.execCountQuery())
	result, err := stmt.ExecuteUpdate(ctx)
	s.NoError(err)
	s.EqualValues(1, result)
	s.EqualValues(5, s.execCountQuery())
	result, err = s.client.ExecuteUpdate(ctx, "DELETE FROM intTable WHERE keyName = 'new_value'")
	s.NoError(err)
	s.EqualValues(1, result)
	s.EqualValues(4, s.execCountQuery())
}

func (s *DuckFlightSQLSuite) TestCommandPreparedStatementClose() {
	ctx := context.Background()
	prep, err := s.client.Prepare(ctx, "SELECT 1")
	s.NoError(err)

	err = prep.Close(ctx)
	s.NoError(err)

	// Executing after close should fail.
	_, err = prep.Execute(ctx)
	s.Error(err)
}

func (s *DuckFlightSQLSuite) TestCommandPreparedStatementUpdateNoTable() {
	ctx := context.Background()
	// Prepare succeeds (validation is deferred to execution time).
	stmt, err := s.client.Prepare(ctx, "INSERT INTO thisTableDoesNotExist (keyName, value) VALUES ('new_value', 2)")
	s.Require().NoError(err)
	s.Require().NotNil(stmt)
	defer func() { _ = stmt.Close(ctx) }()

	// Execution fails because the table does not exist.
	_, err = stmt.ExecuteUpdate(ctx)
	s.Error(err)
}

// =========================================================================
// M3+: Primary keys (TDD — fail until implemented)
// =========================================================================

func (s *DuckFlightSQLSuite) TestCommandGetPrimaryKeys() {
	ctx := context.Background()
	info, err := s.client.GetPrimaryKeys(ctx, flightsql.TableRef{Table: "intTable"})
	s.NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	bldr := array.NewRecordBuilder(s.mem, schema_ref.PrimaryKeys)
	defer bldr.Release()
	bldr.Field(0).(*array.StringBuilder).Append("memory")
	bldr.Field(1).(*array.StringBuilder).Append("main")
	bldr.Field(2).(*array.StringBuilder).Append("intTable")
	bldr.Field(3).(*array.StringBuilder).Append("id")
	bldr.Field(4).(*array.Int32Builder).Append(1)
	bldr.Field(5).AppendNull()
	expected := bldr.NewRecordBatch()
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)
	s.False(rdr.Next())
}

// =========================================================================
// M3+: Foreign keys
// =========================================================================

func (s *DuckFlightSQLSuite) setupForeignKeyTables() {
	s.T().Helper()
	s.seedSQL("DROP TABLE IF EXISTS fk_child_table")
	s.seedSQL("DROP TABLE IF EXISTS fk_pk_table")
	s.seedSQL("CREATE TABLE fk_pk_table (id INTEGER PRIMARY KEY, name VARCHAR)")
	s.seedSQL("CREATE TABLE fk_child_table (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES fk_pk_table(id))")
}

func (s *DuckFlightSQLSuite) teardownForeignKeyTables() {
	s.T().Helper()
	s.seedSQL("DROP TABLE IF EXISTS fk_child_table")
	s.seedSQL("DROP TABLE IF EXISTS fk_pk_table")
}

func (s *DuckFlightSQLSuite) TestCommandGetImportedKeys() {
	ctx := context.Background()
	s.setupForeignKeyTables()
	defer s.teardownForeignKeyTables()

	info, err := s.client.GetImportedKeys(ctx, flightsql.TableRef{Table: "fk_child_table"})
	s.Require().NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	rec.Retain()
	defer rec.Release()

	s.EqualValues(1, rec.NumRows(), "expected exactly 1 imported key row")
	s.Equal("fk_pk_table", rec.Column(2).(*array.String).Value(0))    // pk_table_name
	s.Equal("id", rec.Column(3).(*array.String).Value(0))             // pk_column_name
	s.Equal("fk_child_table", rec.Column(6).(*array.String).Value(0)) // fk_table_name
	s.Equal("parent_id", rec.Column(7).(*array.String).Value(0))      // fk_column_name

	s.False(rdr.Next())
}

func (s *DuckFlightSQLSuite) TestCommandGetExportedKeys() {
	ctx := context.Background()
	s.setupForeignKeyTables()
	defer s.teardownForeignKeyTables()

	info, err := s.client.GetExportedKeys(ctx, flightsql.TableRef{Table: "fk_pk_table"})
	s.Require().NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	rec.Retain()
	defer rec.Release()

	s.EqualValues(1, rec.NumRows(), "expected exactly 1 exported key row")
	s.Equal("fk_pk_table", rec.Column(2).(*array.String).Value(0))    // pk_table_name
	s.Equal("id", rec.Column(3).(*array.String).Value(0))             // pk_column_name
	s.Equal("fk_child_table", rec.Column(6).(*array.String).Value(0)) // fk_table_name
	s.Equal("parent_id", rec.Column(7).(*array.String).Value(0))      // fk_column_name

	s.False(rdr.Next())
}

func (s *DuckFlightSQLSuite) TestCommandGetCrossReference() {
	ctx := context.Background()
	s.setupForeignKeyTables()
	defer s.teardownForeignKeyTables()

	info, err := s.client.GetCrossReference(ctx,
		flightsql.TableRef{Table: "fk_pk_table"},
		flightsql.TableRef{Table: "fk_child_table"},
	)
	s.Require().NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	rec.Retain()
	defer rec.Release()

	s.EqualValues(1, rec.NumRows())
	s.Equal("fk_pk_table", rec.Column(2).(*array.String).Value(0))
	s.Equal("fk_child_table", rec.Column(6).(*array.String).Value(0))

	s.False(rdr.Next())
}

func (s *DuckFlightSQLSuite) TestCommandGetImportedKeysEmpty() {
	ctx := context.Background()
	// intTable has no foreign keys
	info, err := s.client.GetImportedKeys(ctx, flightsql.TableRef{Table: "intTable"})
	s.Require().NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	var total int64
	for rdr.Next() {
		total += rdr.RecordBatch().NumRows()
	}
	s.Equal(int64(0), total, "intTable has no imported keys")
}

// =========================================================================
// M3+: XdbcTypeInfo
// =========================================================================

func (s *DuckFlightSQLSuite) TestCommandGetXdbcTypeInfo() {
	ctx := context.Background()
	info, err := s.client.GetXdbcTypeInfo(ctx, nil)
	s.Require().NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	s.True(schema_ref.XdbcTypeInfo.Equal(rdr.Schema()), "schema mismatch: %s", rdr.Schema())

	var total int64
	for rdr.Next() {
		total += rdr.RecordBatch().NumRows()
	}
	s.NoError(rdr.Err())
	s.Greater(total, int64(0), "expected at least one type info row")
}

func (s *DuckFlightSQLSuite) TestCommandGetXdbcTypeInfoFiltered() {
	ctx := context.Background()
	intType := int32(4) // xdbcInteger
	info, err := s.client.GetXdbcTypeInfo(ctx, &intType)
	s.Require().NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	var total int64
	for rdr.Next() {
		rec := rdr.RecordBatch()
		dataTypeCol := rec.Column(1).(*array.Int32)
		for i := 0; i < dataTypeCol.Len(); i++ {
			s.EqualValues(4, dataTypeCol.Value(i), "all rows should have data_type=4 (INTEGER)")
		}
		total += rec.NumRows()
	}
	s.NoError(rdr.Err())
	s.Greater(total, int64(0), "expected at least one INTEGER type")
}

// =========================================================================
// M3+: SqlInfo
// =========================================================================

func (s *DuckFlightSQLSuite) TestCommandGetSqlInfo() {
	ctx := context.Background()
	info, err := s.client.GetSqlInfo(ctx, []flightsql.SqlInfo{
		flightsql.SqlInfoFlightSqlServerName,
		flightsql.SqlInfoFlightSqlServerVersion,
		flightsql.SqlInfoFlightSqlServerTransaction,
	})
	s.Require().NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	var total int64
	for rdr.Next() {
		total += rdr.RecordBatch().NumRows()
	}
	s.NoError(rdr.Err())
	s.EqualValues(3, total, "expected 3 SqlInfo rows")
}

func (s *DuckFlightSQLSuite) TestCommandGetSqlInfoExtended() {
	ctx := context.Background()
	info, err := s.client.GetSqlInfo(ctx, []flightsql.SqlInfo{
		flightsql.SqlInfoFlightSqlServerCancel,
		flightsql.SqlInfoFlightSqlServerStatementTimeout,
		flightsql.SqlInfoFlightSqlServerTransactionTimeout,
		flightsql.SqlInfoFlightSqlServerBulkIngestion,
		flightsql.SqlInfoFlightSqlServerIngestTransactionsSupported,
	})
	s.Require().NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	var total int64
	for rdr.Next() {
		total += rdr.RecordBatch().NumRows()
	}
	s.NoError(rdr.Err())
	s.EqualValues(5, total, "expected 5 extended SqlInfo rows")
}

func (s *DuckFlightSQLSuite) TestCommandGetSqlInfoKeywords() {
	ctx := context.Background()
	info, err := s.client.GetSqlInfo(ctx, []flightsql.SqlInfo{
		flightsql.SqlInfoKeywords,
	})
	s.Require().NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	s.True(rdr.Next())
	rec := rdr.RecordBatch()
	s.EqualValues(1, rec.NumRows(), "expected 1 row for SqlInfoKeywords")

	// The value column is a dense union; verify it's non-empty by checking
	// the info_name matches SqlInfoKeywords.
	infoNameCol := rec.Column(0).(*array.Uint32)
	s.EqualValues(uint32(flightsql.SqlInfoKeywords), infoNameCol.Value(0))
	s.False(rdr.Next())
}

// =========================================================================
// M7: Transactions (TDD — fail until implemented)
// =========================================================================

func (s *DuckFlightSQLSuite) TestTransactions() {
	ctx := context.Background()
	tx, err := s.client.BeginTransaction(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(tx)
	s.True(tx.ID().IsValid())
	s.NotEmpty(tx.ID())

	info, err := tx.Execute(ctx, "SELECT * FROM intTable")
	s.Require().NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)

	toTable := func(r *flight.Reader) arrow.Table {
		defer r.Release()
		recs := make([]arrow.RecordBatch, 0)
		for r.Next() {
			rec := r.RecordBatch()
			rec.Retain()
			defer rec.Release()
			recs = append(recs, rec)
		}
		return array.NewTableFromRecords(r.Schema(), recs)
	}
	tbl := toTable(rdr)
	defer tbl.Release()
	rowCount := tbl.NumRows()

	result, err := tx.ExecuteUpdate(ctx, `INSERT INTO intTable (keyName, value) VALUES
		('KEYNAME1', 1001), ('KEYNAME2', 1002), ('KEYNAME3', 1003)`)
	s.Require().NoError(err)
	s.EqualValues(3, result)

	info, err = tx.Execute(ctx, "SELECT * FROM intTable")
	s.Require().NoError(err)
	rdr, err = s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	tbl = toTable(rdr)
	defer tbl.Release()
	s.EqualValues(rowCount+3, tbl.NumRows())

	s.Require().NoError(tx.Rollback(ctx))
	// Transaction handle is now invalid.
	s.ErrorIs(tx.Commit(ctx), flightsql.ErrInvalidTxn)
	s.ErrorIs(tx.Rollback(ctx), flightsql.ErrInvalidTxn)

	info, err = s.client.Execute(ctx, "SELECT * FROM intTable")
	s.Require().NoError(err)
	rdr, err = s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	tbl = toTable(rdr)
	defer tbl.Release()
	s.EqualValues(rowCount, tbl.NumRows())
}

func (s *DuckFlightSQLSuite) TestTransactionIsolation() {
	ctx := context.Background()

	tx, err := s.client.BeginTransaction(ctx)
	s.Require().NoError(err)

	// Read initial count within transaction.
	s.EqualValues(4, s.execCountQuery())

	// Insert outside the transaction (via seedSQL, bypasses Flight SQL).
	s.seedSQL("INSERT INTO intTable VALUES (100, 'outside_txn', 100, 1)")

	// Within transaction, snapshot should not see the external insert.
	info, err := tx.Execute(ctx, "SELECT COUNT(*) FROM intTable")
	s.Require().NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	rec, err := rdr.Read()
	s.Require().NoError(err)
	txnCount := rec.Column(0).(*array.Int64).Value(0)
	rdr.Release()

	// Snapshot isolation: txn should see the original 4 rows, not the inserted one.
	s.EqualValues(4, txnCount, "transaction should not see external inserts (snapshot isolation)")

	err = tx.Commit(ctx)
	s.Require().NoError(err)
}

// =========================================================================
// DuckDB-specific tests
// =========================================================================

func (s *DuckFlightSQLSuite) TestStatementTimeoutReturnsDeadlineExceeded() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	timeoutBefore := getMetricValue("flightsql_queries_total", map[string]string{"status": "timeout"})

	// Cross-join aggregation blocks in QueryContext and never returns rows,
	// so the server-side timeout always fires there, returning DeadlineExceeded.
	info, err := s.client.Execute(ctx,
		"SELECT count(*) FROM range(100000000) t1, range(100000000) t2")
	if err != nil {
		st, ok := status.FromError(err)
		s.Require().True(ok)
		s.Equal(codes.DeadlineExceeded, st.Code(), "expected DeadlineExceeded, got: %v", err)
		return
	}

	// If Execute succeeds (lazy), the error should occur during DoGet.
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		st, ok := status.FromError(err)
		s.Require().True(ok)
		s.Equal(codes.DeadlineExceeded, st.Code(), "expected DeadlineExceeded, got: %v", err)
		return
	}
	defer rdr.Release()

	// Consume all batches — timeout should trigger during iteration.
	for rdr.Next() {
	}
	s.Require().Error(rdr.Err(), "expected timeout error during query execution")

	timeoutAfter := getMetricValue("flightsql_queries_total", map[string]string{"status": "timeout"})
	s.Greater(timeoutAfter, timeoutBefore, "flightsql_queries_total{status=timeout} should increment")
}

func (s *DuckFlightSQLSuite) TestMetricsQueryCountOK() {
	before := getMetricValue("flightsql_queries_total", map[string]string{"status": "ok"})

	ctx := context.Background()
	info, err := s.client.Execute(ctx, "SELECT 1")
	s.Require().NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()
	for rdr.Next() {
	}
	s.NoError(rdr.Err())

	after := getMetricValue("flightsql_queries_total", map[string]string{"status": "ok"})
	s.Equal(before+1, after, "flightsql_queries_total{status=ok} should increment by 1")
}

func (s *DuckFlightSQLSuite) TestMetricsBytesStreamed() {
	before := getMetricValue("flightsql_bytes_streamed_total", nil)

	ctx := context.Background()
	info, err := s.client.Execute(ctx, "SELECT * FROM range(1000) t(id)")
	s.Require().NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()
	for rdr.Next() {
	}
	s.NoError(rdr.Err())

	after := getMetricValue("flightsql_bytes_streamed_total", nil)
	s.Greater(after, before, "flightsql_bytes_streamed_total should increase after streaming")
}

func (s *DuckFlightSQLSuite) TestGetExecuteSchema() {
	ctx := context.Background()

	schema, err := s.client.GetExecuteSchema(ctx, "SELECT * FROM intTable")
	s.Require().NoError(err)
	s.Require().NotNil(schema)

	parsed, err := flight.DeserializeSchema(schema.GetSchema(), s.mem)
	s.Require().NoError(err)
	s.Equal(4, parsed.NumFields())
	s.Equal("id", parsed.Field(0).Name)
	s.Equal(arrow.PrimitiveTypes.Int32, parsed.Field(0).Type)
	s.Equal("keyName", parsed.Field(1).Name)
	s.Equal(arrow.BinaryTypes.String, parsed.Field(1).Type)
	s.Equal("value", parsed.Field(2).Name)
	s.Equal(arrow.PrimitiveTypes.Int64, parsed.Field(2).Type)
	s.Equal("foreignId", parsed.Field(3).Name)
	s.Equal(arrow.PrimitiveTypes.Int64, parsed.Field(3).Type)
}

// =========================================================================
// Run
// =========================================================================

func TestDuckFlightSQLSuite(t *testing.T) {
	suite.Run(t, new(DuckFlightSQLSuite))
}
