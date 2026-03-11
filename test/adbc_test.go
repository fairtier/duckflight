//go:build duckdb_arrow

package duckflight_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	flightsqldriver "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-adbc/go/adbc/sqldriver"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckserver "github.com/prochac/duckflight/internal/server"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
)

// ---------------------------------------------------------------------------
// ADBCSuite — in-process Flight server tested via ADBC + database/sql drivers
// ---------------------------------------------------------------------------

type ADBCSuite struct {
	suite.Suite

	server flight.Server
	db     *sql.DB
	cnxn   adbc.Connection
	adbcDB adbc.Database
}

func (s *ADBCSuite) SetupSuite() {
	srv, err := duckserver.New(duckserver.Config{
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
// Helpers
// ---------------------------------------------------------------------------

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

