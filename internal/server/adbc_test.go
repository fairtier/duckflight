//go:build duckdb_arrow

package server_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	flightsqldriver "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-adbc/go/adbc/sqldriver"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prochac/duckflight/internal/config"
	"github.com/prochac/duckflight/internal/server"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
)

// ---------------------------------------------------------------------------
// ADBCSuite — in-process Flight server tested via ADBC + database/sql drivers
// ---------------------------------------------------------------------------

type ADBCSuite struct {
	suite.Suite

	server flight.Server
	srv    *server.DuckFlightSQLServer
	db     *sql.DB
	cnxn   adbc.Connection
	adbcDB adbc.Database
	mem    *memory.CheckedAllocator
}

func (s *ADBCSuite) SetupSuite() {
	srv, err := server.New(&config.Config{
		MemoryLimit:  "512MB",
		MaxThreads:   2,
		QueryTimeout: "10s",
		PoolSize:     4,
	})
	s.Require().NoError(err, "failed to create duckflight server")
	s.srv = srv

	s.server = flight.NewServerWithMiddleware(nil)
	s.server.RegisterFlightService(flightsql.NewFlightServer(srv))
	s.Require().NoError(s.server.Init("localhost:0"))
	go func() { _ = s.server.Serve() }()

	addr := s.server.Addr().String()

	// database/sql connection via ADBC driver
	adbcDriver := flightsqldriver.NewDriver(memory.DefaultAllocator)
	sqlDrv := sqldriver.Driver{Driver: adbcDriver}
	connector, err := sqlDrv.OpenConnector(adbc.OptionKeyURI + "=grpc://" + addr)
	s.Require().NoError(err)
	s.db = sql.OpenDB(connector)

	// Raw ADBC connection
	s.adbcDB, err = adbcDriver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: "grpc://" + addr,
	})
	s.Require().NoError(err)
	s.cnxn, err = s.adbcDB.Open(context.Background())
	s.Require().NoError(err)
}

func (s *ADBCSuite) TearDownSuite() {
	if s.cnxn != nil {
		_ = s.cnxn.Close()
	}
	if s.adbcDB != nil {
		_ = s.adbcDB.Close()
	}
	if s.db != nil {
		_ = s.db.Close()
	}
	if s.server != nil {
		s.server.Shutdown()
	}
}

func (s *ADBCSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	s.srv.Alloc = s.mem
}

func (s *ADBCSuite) TearDownTest() {
	// The ADBC driver may cancel gRPC streams before server-side
	// goroutines run their deferred cleanup (active query decrement,
	// pool release). Wait briefly for in-flight operations to drain.
	pool := s.srv.Engine().Pool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.srv.OpenTransactionCount() == 0 &&
			s.srv.ActiveQueryCount() == 0 &&
			pool.Cap() == pool.Len() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	s.Assert().Equal(0, s.srv.OpenTransactionCount(), "leaked transactions")
	// Note: PreparedStatementCount is not checked here because the ADBC
	// FlightSQL driver does not always call ClosePreparedStatement when
	// closing statements through the database/sql layer.
	s.Assert().Equal(0, s.srv.ActiveQueryCount(), "leaked active queries")
	s.Assert().Equal(pool.Cap(), pool.Len(), "pool connections not returned")

	s.srv.Alloc = memory.DefaultAllocator
	s.mem.AssertSize(s.T(), 0)
}

// ---------------------------------------------------------------------------
// Go database/sql driver tests
// ---------------------------------------------------------------------------

func (s *ADBCSuite) TestGoSQL_GetRow() {
	ctx := context.Background()
	err := s.db.QueryRowContext(ctx, "SELECT 1").Err()
	s.NoError(err)
}

func (s *ADBCSuite) TestGoSQL_GetRows() {
	ctx := context.Background()
	rows, err := s.db.QueryContext(ctx, "SELECT 1")
	s.Require().NoError(err)
	defer func() { s.NoError(rows.Close()) }()
	for rows.Next() {
		var i int
		s.NoError(rows.Scan(&i))
		s.Equal(1, i)
	}
	s.NoError(rows.Err())
}

func (s *ADBCSuite) TestGoSQL_StatementRows() {
	ctx := context.Background()
	stmt, err := s.db.PrepareContext(ctx, "SELECT 123")
	s.Require().NoError(err)
	defer func() { s.NoError(stmt.Close()) }()
	rows, err := stmt.QueryContext(ctx)
	s.Require().NoError(err)
	defer func() { s.NoError(rows.Close()) }()
	for rows.Next() {
		var i int
		s.NoError(rows.Scan(&i))
		s.Equal(123, i)
	}
	s.NoError(rows.Err())
}

func (s *ADBCSuite) TestGoSQL_StatementRow() {
	ctx := context.Background()
	stmt, err := s.db.PrepareContext(ctx, "SELECT 123")
	s.Require().NoError(err)
	defer func() { s.NoError(stmt.Close()) }()
	s.NoError(stmt.QueryRowContext(ctx).Err())
}

// ---------------------------------------------------------------------------
// ADBC driver tests
// ---------------------------------------------------------------------------

func (s *ADBCSuite) TestADBC_LoadSchema() {
	ctx := context.Background()
	s.execDDL("CREATE TABLE IF NOT EXISTS primary_table (id INTEGER PRIMARY KEY, name VARCHAR CHECK (NOT contains(name, ' ')))")
	s.execDDL(`CREATE TABLE IF NOT EXISTS "secondary_table" (id INTEGER PRIMARY KEY, primary_id INTEGER REFERENCES primary_table(id))`)

	dbObjects, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthAll, nil, nil, nil, nil, nil)
	s.Require().NoError(err)

	s.True(dbObjects.Next())
	catalogObj := dbObjects.RecordBatch()
	var buf bytes.Buffer
	err = array.RecordToJSON(catalogObj, &buf)
	s.Require().NoError(err)
	s.False(dbObjects.Next())
	s.NoError(dbObjects.Err())

	dec := json.NewDecoder(&buf)
	for {
		var c map[string]any
		err := dec.Decode(&c)
		if errors.Is(err, io.EOF) {
			break
		}
		s.Require().NoError(err)
		if c["catalog_name"].(string) != "memory" {
			continue
		}
		s.Len(c["catalog_db_schemas"].([]any), 1)
	}
}

func (s *ADBCSuite) TestADBC_TableTypes() {
	ctx := context.Background()
	resp, err := s.cnxn.GetTableTypes(ctx)
	s.Require().NoError(err)
	s.True(resp.Next())

	recBtch := resp.RecordBatch()
	s.True(recBtch.NumCols() == 1)
	col := recBtch.Column(0)

	s.Equal(arrow.BinaryTypes.String, col.DataType())
	strCol, ok := col.(*array.String)
	s.True(ok)
	s.Equal(3, strCol.Len())

	s.Equal("BASE TABLE", strCol.Value(0))
	s.Equal("LOCAL TEMPORARY", strCol.Value(1))
	s.Equal("VIEW", strCol.Value(2))

	s.False(resp.Next())
	s.NoError(resp.Err())
}

func (s *ADBCSuite) TestADBC_StatementRows() {
	ctx := context.Background()
	stmt, err := s.cnxn.NewStatement()
	s.Require().NoError(err)
	defer func() { s.NoError(stmt.Close()) }()
	s.NoError(stmt.SetSqlQuery("SELECT 1"))
	reader, _, err := stmt.ExecuteQuery(ctx)
	s.Require().NoError(err)
	defer reader.Release()
	for reader.Next() {
	}
	s.NoError(reader.Err())
}

func (s *ADBCSuite) TestADBC_ExecStmt() {
	ctx := context.Background()
	stmt, err := s.cnxn.NewStatement()
	s.Require().NoError(err)
	defer func() { s.NoError(stmt.Close()) }()
	s.NoError(stmt.SetSqlQuery("DROP TABLE IF EXISTS test"))
	_, err = stmt.ExecuteUpdate(ctx)
	s.NoError(err)
}

func (s *ADBCSuite) TestADBC_TransactionAndTestTable() {
	ctx := context.Background()

	postopt, ok := s.cnxn.(adbc.PostInitOptions)
	s.Require().True(ok)

	// Begin transaction
	err := postopt.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled)
	s.Require().NoError(err)

	// Empty commit
	s.NoError(s.cnxn.Commit(ctx))

	tableExists := s.checkTable("test_txn")
	s.False(tableExists)

	// Create table inside transaction
	stmt, err := s.cnxn.NewStatement()
	s.Require().NoError(err)
	defer func() { s.NoError(stmt.Close()) }()

	s.NoError(stmt.SetSqlQuery(`CREATE TABLE IF NOT EXISTS test_txn (id INTEGER, name VARCHAR)`))
	_, err = stmt.ExecuteUpdate(ctx)
	s.Require().NoError(err)

	// Commit
	s.NoError(s.cnxn.Commit(ctx))

	// Verify table exists after commit
	tableExists = s.checkTable("test_txn")
	s.True(tableExists)

	// Query the table
	stmt2, err := s.cnxn.NewStatement()
	s.Require().NoError(err)
	defer func() { s.NoError(stmt2.Close()) }()
	s.NoError(stmt2.SetSqlQuery("SELECT * FROM test_txn"))
	_, _, err = stmt2.ExecuteQuery(ctx)
	s.NoError(err)

	// Re-enable autocommit
	s.NoError(postopt.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueEnabled))
}

func (s *ADBCSuite) TestADBC_404Table() {
	ctx := context.Background()

	tableExists := s.checkTable("404_test")
	s.False(tableExists)

	stmt, err := s.cnxn.NewStatement()
	s.Require().NoError(err)
	defer func() { s.NoError(stmt.Close()) }()
	s.NoError(stmt.SetSqlQuery(`SELECT * FROM "404_test"`))
	_, _, err = stmt.ExecuteQuery(ctx)
	s.Error(err)

	var aErr adbc.Error
	s.Require().ErrorAs(err, &aErr)
	s.Equal(int32(codes.NotFound), aErr.VendorCode)
	s.Equal(adbc.StatusNotFound, aErr.Code)
}

// ---------------------------------------------------------------------------
// GetObjects helper structs and parser
// ---------------------------------------------------------------------------

type constraintInfo struct {
	Name        string
	Type        string
	ColumnNames []string
}

type columnInfo struct {
	Name            string
	OrdinalPosition int32
	XdbcDataType    int16
	XdbcTypeName    string
}

type tableInfo struct {
	Name        string
	TableType   string
	Columns     []columnInfo
	Constraints []constraintInfo
}

type schemaInfo struct {
	Name   string
	Tables []tableInfo
}

type catalogInfo struct {
	Name    string
	Schemas []schemaInfo
}

// parseGetObjects traverses the nested Arrow struct/list columns returned by
// GetObjects and returns a slice of catalogInfo for assertion.
func parseGetObjects(rdr array.RecordReader) []catalogInfo {
	var result []catalogInfo
	for rdr.Next() {
		rec := rdr.RecordBatch()
		catalogs := rec.Column(0).(*array.String)
		catalogDbSchemasList := rec.Column(1).(*array.List)
		catalogDbSchemas := catalogDbSchemasList.ListValues().(*array.Struct)

		dbSchemaNames := catalogDbSchemas.Field(0).(*array.String)
		dbSchemaTablesList := catalogDbSchemas.Field(1).(*array.List)
		dbSchemaTables := dbSchemaTablesList.ListValues().(*array.Struct)

		tableNames := dbSchemaTables.Field(0).(*array.String)
		tableTypes := dbSchemaTables.Field(1).(*array.String)
		tableColumnsList := dbSchemaTables.Field(2).(*array.List)
		tableConstraintsList := dbSchemaTables.Field(3).(*array.List)

		tableColumns := tableColumnsList.ListValues().(*array.Struct)
		colNames := tableColumns.Field(0).(*array.String)
		colPositions := tableColumns.Field(1).(*array.Int32)
		colXdbcDataType := tableColumns.Field(3).(*array.Int16)
		colXdbcTypeName := tableColumns.Field(4).(*array.String)

		tableConstraints := tableConstraintsList.ListValues().(*array.Struct)
		constraintNames := tableConstraints.Field(0).(*array.String)
		constraintTypes := tableConstraints.Field(1).(*array.String)
		constraintColNamesList := tableConstraints.Field(2).(*array.List)
		constraintColNames := constraintColNamesList.ListValues().(*array.String)

		for row := 0; row < int(rec.NumRows()); row++ {
			cat := catalogInfo{Name: catalogs.Value(row)}

			if catalogDbSchemasList.IsNull(row) {
				result = append(result, cat)
				continue
			}

			schStart, schEnd := catalogDbSchemasList.ValueOffsets(row)
			for si := schStart; si < schEnd; si++ {
				sch := schemaInfo{Name: dbSchemaNames.Value(int(si))}

				if dbSchemaTablesList.IsNull(int(si)) {
					cat.Schemas = append(cat.Schemas, sch)
					continue
				}

				tblStart, tblEnd := dbSchemaTablesList.ValueOffsets(int(si))
				for ti := tblStart; ti < tblEnd; ti++ {
					tbl := tableInfo{
						Name:      tableNames.Value(int(ti)),
						TableType: tableTypes.Value(int(ti)),
					}

					if !tableColumnsList.IsNull(int(ti)) {
						colStart, colEnd := tableColumnsList.ValueOffsets(int(ti))
						for ci := colStart; ci < colEnd; ci++ {
							col := columnInfo{
								Name:            colNames.Value(int(ci)),
								OrdinalPosition: colPositions.Value(int(ci)),
							}
							if !colXdbcDataType.IsNull(int(ci)) {
								col.XdbcDataType = colXdbcDataType.Value(int(ci))
							}
							if !colXdbcTypeName.IsNull(int(ci)) {
								col.XdbcTypeName = colXdbcTypeName.Value(int(ci))
							}
							tbl.Columns = append(tbl.Columns, col)
						}
					}

					if !tableConstraintsList.IsNull(int(ti)) {
						conStart, conEnd := tableConstraintsList.ValueOffsets(int(ti))
						for ki := conStart; ki < conEnd; ki++ {
							con := constraintInfo{
								Type: constraintTypes.Value(int(ki)),
							}
							if !constraintNames.IsNull(int(ki)) {
								con.Name = constraintNames.Value(int(ki))
							}
							cnStart, cnEnd := constraintColNamesList.ValueOffsets(int(ki))
							for cni := cnStart; cni < cnEnd; cni++ {
								con.ColumnNames = append(con.ColumnNames, constraintColNames.Value(int(cni)))
							}
							tbl.Constraints = append(tbl.Constraints, con)
						}
					}

					sch.Tables = append(sch.Tables, tbl)
				}
				cat.Schemas = append(cat.Schemas, sch)
			}
			result = append(result, cat)
		}
	}
	return result
}

func findCatalog(cats []catalogInfo, name string) *catalogInfo { //nolint:unparam
	for i := range cats {
		if cats[i].Name == name {
			return &cats[i]
		}
	}
	return nil
}

func findSchema(cat *catalogInfo, name string) *schemaInfo { //nolint:unparam
	if cat == nil {
		return nil
	}
	for i := range cat.Schemas {
		if cat.Schemas[i].Name == name {
			return &cat.Schemas[i]
		}
	}
	return nil
}

func findTable(sch *schemaInfo, name string) *tableInfo {
	if sch == nil {
		return nil
	}
	for i := range sch.Tables {
		if sch.Tables[i].Name == name {
			return &sch.Tables[i]
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// GetObjects tests
// ---------------------------------------------------------------------------

func (s *ADBCSuite) TestADBC_GetObjectsSchema() {
	ctx := context.Background()
	rdr, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthAll, nil, nil, nil, nil, nil)
	s.Require().NoError(err)
	defer rdr.Release()

	s.True(rdr.Schema().Equal(adbc.GetObjectsSchema),
		"GetObjects schema mismatch:\ngot:  %s\nwant: %s", rdr.Schema(), adbc.GetObjectsSchema)
}

func (s *ADBCSuite) TestADBC_GetObjectsDepthCatalogs() {
	ctx := context.Background()
	rdr, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthCatalogs, nil, nil, nil, nil, nil)
	s.Require().NoError(err)
	defer rdr.Release()

	cats := parseGetObjects(rdr)
	s.Require().NotEmpty(cats)

	// At catalog depth, catalog_db_schemas should be nil (no schemas populated).
	for _, cat := range cats {
		s.Nil(cat.Schemas, "catalog %q should have nil schemas at ObjectDepthCatalogs", cat.Name)
	}
}

func (s *ADBCSuite) TestADBC_GetObjectsDepthDBSchemas() {
	ctx := context.Background()
	rdr, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthDBSchemas, nil, nil, nil, nil, nil)
	s.Require().NoError(err)
	defer rdr.Release()

	cats := parseGetObjects(rdr)
	mem := findCatalog(cats, "memory")
	s.Require().NotNil(mem, "expected 'memory' catalog")
	s.Require().NotEmpty(mem.Schemas)

	// At schema depth, db_schema_tables should be nil.
	for _, sch := range mem.Schemas {
		s.Nil(sch.Tables, "schema %q should have nil tables at ObjectDepthDBSchemas", sch.Name)
	}
}

func (s *ADBCSuite) TestADBC_GetObjectsDepthTables() {
	ctx := context.Background()
	s.execDDL("CREATE TABLE IF NOT EXISTS go_test_depth_tbl (x INTEGER)")
	defer s.execDDL("DROP TABLE IF EXISTS go_test_depth_tbl")

	rdr, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthTables, nil, nil, nil, nil, nil)
	s.Require().NoError(err)
	defer rdr.Release()

	cats := parseGetObjects(rdr)
	mem := findCatalog(cats, "memory")
	s.Require().NotNil(mem)
	sch := findSchema(mem, "main")
	s.Require().NotNil(sch)
	tbl := findTable(sch, "go_test_depth_tbl")
	s.Require().NotNil(tbl, "expected table go_test_depth_tbl at ObjectDepthTables")

	// At table depth, columns and constraints should be nil.
	s.Nil(tbl.Columns, "columns should be nil at ObjectDepthTables")
	s.Nil(tbl.Constraints, "constraints should be nil at ObjectDepthTables")
}

func (s *ADBCSuite) TestADBC_GetObjectsDepthAll() {
	ctx := context.Background()
	s.execDDL("CREATE TABLE IF NOT EXISTS go_test_all (id INTEGER PRIMARY KEY, name VARCHAR, value BIGINT)")
	defer s.execDDL("DROP TABLE IF EXISTS go_test_all")

	rdr, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthAll, nil, nil, nil, nil, nil)
	s.Require().NoError(err)
	defer rdr.Release()

	cats := parseGetObjects(rdr)
	mem := findCatalog(cats, "memory")
	s.Require().NotNil(mem)
	sch := findSchema(mem, "main")
	s.Require().NotNil(sch)
	tbl := findTable(sch, "go_test_all")
	s.Require().NotNil(tbl, "expected table go_test_all at ObjectDepthAll")

	s.Require().NotNil(tbl.Columns, "columns should be populated at ObjectDepthAll")
	s.Len(tbl.Columns, 3)
	s.Equal("id", tbl.Columns[0].Name)
	s.Equal("name", tbl.Columns[1].Name)
	s.Equal("value", tbl.Columns[2].Name)
}

func (s *ADBCSuite) TestADBC_GetObjectsCatalogFilter() {
	ctx := context.Background()

	// Filter for existing catalog
	cat := strPtr("memory")
	rdr, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthCatalogs, cat, nil, nil, nil, nil)
	s.Require().NoError(err)
	defer rdr.Release()

	cats := parseGetObjects(rdr)
	s.Require().Len(cats, 1)
	s.Equal("memory", cats[0].Name)

	// Filter for non-existent catalog
	cat2 := strPtr("nonexistent_catalog")
	rdr2, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthCatalogs, cat2, nil, nil, nil, nil)
	s.Require().NoError(err)
	defer rdr2.Release()

	cats2 := parseGetObjects(rdr2)
	s.Empty(cats2)
}

func (s *ADBCSuite) TestADBC_GetObjectsSchemaFilter() {
	ctx := context.Background()

	schPat := strPtr("main")
	rdr, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthDBSchemas, nil, schPat, nil, nil, nil)
	s.Require().NoError(err)
	defer rdr.Release()

	cats := parseGetObjects(rdr)
	mem := findCatalog(cats, "memory")
	s.Require().NotNil(mem)
	s.Require().Len(mem.Schemas, 1)
	s.Equal("main", mem.Schemas[0].Name)
}

func (s *ADBCSuite) TestADBC_GetObjectsTableFilter() {
	ctx := context.Background()
	s.execDDL("CREATE TABLE IF NOT EXISTS go_test_filter_a (x INTEGER)")
	s.execDDL("CREATE TABLE IF NOT EXISTS go_test_filter_b (y INTEGER)")
	defer s.execDDL("DROP TABLE IF EXISTS go_test_filter_a")
	defer s.execDDL("DROP TABLE IF EXISTS go_test_filter_b")

	tblPat := strPtr("go_test_filter_a")
	rdr, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthTables, nil, nil, tblPat, nil, nil)
	s.Require().NoError(err)
	defer rdr.Release()

	cats := parseGetObjects(rdr)
	mem := findCatalog(cats, "memory")
	s.Require().NotNil(mem)
	sch := findSchema(mem, "main")
	s.Require().NotNil(sch)
	s.Require().Len(sch.Tables, 1)
	s.Equal("go_test_filter_a", sch.Tables[0].Name)
}

func (s *ADBCSuite) TestADBC_GetObjectsTableTypeFilter() {
	ctx := context.Background()
	s.execDDL("CREATE TABLE IF NOT EXISTS go_test_ttype (x INTEGER)")
	s.execDDL("CREATE OR REPLACE VIEW go_test_ttype_v AS SELECT 1 AS x")
	defer s.execDDL("DROP TABLE IF EXISTS go_test_ttype")
	defer s.execDDL("DROP VIEW IF EXISTS go_test_ttype_v")

	// Filter for BASE TABLE only
	tableTypes := []string{"BASE TABLE"}
	rdr, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthTables, nil, nil, nil, nil, tableTypes)
	s.Require().NoError(err)
	defer rdr.Release()

	cats := parseGetObjects(rdr)
	mem := findCatalog(cats, "memory")
	s.Require().NotNil(mem)
	sch := findSchema(mem, "main")
	s.Require().NotNil(sch)
	for _, tbl := range sch.Tables {
		s.Equal("BASE TABLE", tbl.TableType, "expected only BASE TABLE, got %q for %q", tbl.TableType, tbl.Name)
	}

	// Filter for VIEW only
	tableTypes2 := []string{"VIEW"}
	rdr2, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthTables, nil, nil, nil, nil, tableTypes2)
	s.Require().NoError(err)
	defer rdr2.Release()

	cats2 := parseGetObjects(rdr2)
	mem2 := findCatalog(cats2, "memory")
	s.Require().NotNil(mem2)
	sch2 := findSchema(mem2, "main")
	s.Require().NotNil(sch2)
	found := findTable(sch2, "go_test_ttype_v")
	s.NotNil(found, "expected view go_test_ttype_v in VIEW filter results")
	for _, tbl := range sch2.Tables {
		s.Equal("VIEW", tbl.TableType, "expected only VIEW, got %q for %q", tbl.TableType, tbl.Name)
	}
}

func (s *ADBCSuite) TestADBC_GetObjectsColumnFilter() {
	ctx := context.Background()
	s.execDDL("CREATE TABLE IF NOT EXISTS go_test_colfilt (alpha INTEGER, beta VARCHAR, gamma BIGINT)")
	defer s.execDDL("DROP TABLE IF EXISTS go_test_colfilt")

	colPat := strPtr("beta")
	tblPat := strPtr("go_test_colfilt")
	rdr, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthAll, nil, nil, tblPat, colPat, nil)
	s.Require().NoError(err)
	defer rdr.Release()

	cats := parseGetObjects(rdr)
	mem := findCatalog(cats, "memory")
	s.Require().NotNil(mem)
	sch := findSchema(mem, "main")
	s.Require().NotNil(sch)
	tbl := findTable(sch, "go_test_colfilt")
	s.Require().NotNil(tbl)
	s.Require().Len(tbl.Columns, 1)
	s.Equal("beta", tbl.Columns[0].Name)
}

func (s *ADBCSuite) TestADBC_GetObjectsColumnDetails() {
	ctx := context.Background()
	s.execDDL("CREATE TABLE IF NOT EXISTS go_test_coldetail (id INTEGER PRIMARY KEY, name VARCHAR, value BIGINT)")
	defer s.execDDL("DROP TABLE IF EXISTS go_test_coldetail")

	tblPat := strPtr("go_test_coldetail")
	rdr, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthAll, nil, nil, tblPat, nil, nil)
	s.Require().NoError(err)
	defer rdr.Release()

	cats := parseGetObjects(rdr)
	mem := findCatalog(cats, "memory")
	s.Require().NotNil(mem)
	sch := findSchema(mem, "main")
	s.Require().NotNil(sch)
	tbl := findTable(sch, "go_test_coldetail")
	s.Require().NotNil(tbl)
	s.Require().Len(tbl.Columns, 3)

	// Verify ordinal positions are 1-based and sequential
	for i, col := range tbl.Columns {
		s.Equal(int32(i+1), col.OrdinalPosition,
			"column %q ordinal_position mismatch", col.Name)
	}

	s.Equal("id", tbl.Columns[0].Name)
	s.Equal("name", tbl.Columns[1].Name)
	s.Equal("value", tbl.Columns[2].Name)

	// Verify xdbc_type_name is populated via ARROW:FLIGHT:SQL:TYPE_NAME metadata.
	s.Equal("INTEGER", tbl.Columns[0].XdbcTypeName, "id column should have type name INTEGER")
	s.Equal("VARCHAR", tbl.Columns[1].XdbcTypeName, "name column should have type name VARCHAR")
	s.Equal("BIGINT", tbl.Columns[2].XdbcTypeName, "value column should have type name BIGINT")
}

func (s *ADBCSuite) TestADBC_GetObjectsConstraints() {
	ctx := context.Background()
	s.execDDL("CREATE TABLE IF NOT EXISTS go_test_con_pk (id INTEGER PRIMARY KEY, name VARCHAR)")
	defer s.execDDL("DROP TABLE IF EXISTS go_test_con_pk")

	tblPat := strPtr("go_test_con_pk")
	rdr, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthAll, nil, nil, tblPat, nil, nil)
	s.Require().NoError(err)
	defer rdr.Release()

	cats := parseGetObjects(rdr)
	mem := findCatalog(cats, "memory")
	s.Require().NotNil(mem)
	sch := findSchema(mem, "main")
	s.Require().NotNil(sch)
	tbl := findTable(sch, "go_test_con_pk")
	s.Require().NotNil(tbl)

	// NOTE: The ADBC FlightSQL driver does NOT call GetPrimaryKeys/GetImportedKeys
	// when building GetObjects — the ConstraintLookup map is left empty.
	// At ObjectDepthAll, table_constraints will be an empty list (nil slice in our
	// parsed representation). This is a known limitation and may be the root cause
	// of issues in tools like Harlequin that expect constraint data from GetObjects.
	s.Empty(tbl.Constraints, "ADBC FlightSQL driver does not populate constraints in GetObjects")
}

func (s *ADBCSuite) TestADBC_GetObjectsWithView() {
	ctx := context.Background()
	s.execDDL("CREATE OR REPLACE VIEW go_test_view_obj AS SELECT 1 AS x, 'hello' AS y")
	defer s.execDDL("DROP VIEW IF EXISTS go_test_view_obj")

	tblPat := strPtr("go_test_view_obj")
	rdr, err := s.cnxn.GetObjects(ctx, adbc.ObjectDepthAll, nil, nil, tblPat, nil, nil)
	s.Require().NoError(err)
	defer rdr.Release()

	cats := parseGetObjects(rdr)
	mem := findCatalog(cats, "memory")
	s.Require().NotNil(mem)
	sch := findSchema(mem, "main")
	s.Require().NotNil(sch)
	tbl := findTable(sch, "go_test_view_obj")
	s.Require().NotNil(tbl, "view go_test_view_obj should appear in GetObjects")
	s.Equal("VIEW", tbl.TableType)
	s.Require().NotNil(tbl.Columns)
	s.Len(tbl.Columns, 2)
	s.Equal("x", tbl.Columns[0].Name)
	s.Equal("y", tbl.Columns[1].Name)
}

// ---------------------------------------------------------------------------
// GetTableSchema tests
// ---------------------------------------------------------------------------

func (s *ADBCSuite) TestADBC_GetTableSchemaFound() {
	ctx := context.Background()
	s.execDDL("CREATE TABLE IF NOT EXISTS go_test_gts (id INTEGER, name VARCHAR, value DOUBLE)")
	defer s.execDDL("DROP TABLE IF EXISTS go_test_gts")

	schema, err := s.cnxn.GetTableSchema(ctx, nil, nil, "go_test_gts")
	s.Require().NoError(err)
	s.Require().Equal(3, schema.NumFields())
	s.Equal("id", schema.Field(0).Name)
	s.Equal("name", schema.Field(1).Name)
	s.Equal("value", schema.Field(2).Name)

	// Verify ARROW:FLIGHT:SQL:TYPE_NAME metadata is set on each field.
	for _, f := range schema.Fields() {
		s.True(f.HasMetadata(), "field %q should have metadata", f.Name)
		idx := f.Metadata.FindKey("ARROW:FLIGHT:SQL:TYPE_NAME")
		s.GreaterOrEqual(idx, 0, "field %q should have TYPE_NAME metadata key", f.Name)
	}
	s.Equal("INTEGER", schema.Field(0).Metadata.Values()[schema.Field(0).Metadata.FindKey("ARROW:FLIGHT:SQL:TYPE_NAME")])
	s.Equal("VARCHAR", schema.Field(1).Metadata.Values()[schema.Field(1).Metadata.FindKey("ARROW:FLIGHT:SQL:TYPE_NAME")])
	s.Equal("DOUBLE", schema.Field(2).Metadata.Values()[schema.Field(2).Metadata.FindKey("ARROW:FLIGHT:SQL:TYPE_NAME")])
}

func (s *ADBCSuite) TestADBC_GetTableSchemaNotFound() {
	ctx := context.Background()
	_, err := s.cnxn.GetTableSchema(ctx, nil, nil, "go_test_nonexistent_table_xyz")
	s.Require().Error(err)
	var aErr adbc.Error
	s.Require().ErrorAs(err, &aErr)
	s.Equal(adbc.StatusNotFound, aErr.Code)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func strPtr(s string) *string { return &s }

func (s *ADBCSuite) execDDL(query string) {
	s.T().Helper()
	stmt, err := s.cnxn.NewStatement()
	s.Require().NoError(err)
	defer func() { s.NoError(stmt.Close()) }()
	s.Require().NoError(stmt.SetSqlQuery(query))
	_, err = stmt.ExecuteUpdate(context.Background())
	s.Require().NoError(err)
}

func (s *ADBCSuite) checkTable(tableName string) bool {
	s.T().Helper()
	ctx := context.Background()
	_, err := s.cnxn.GetTableSchema(ctx, nil, nil, tableName)
	if err != nil {
		var aErr adbc.Error
		s.Require().ErrorAs(err, &aErr)
		s.Equal(adbc.StatusNotFound, aErr.Code)
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

func TestADBCSuite(t *testing.T) {
	suite.Run(t, new(ADBCSuite))
}
