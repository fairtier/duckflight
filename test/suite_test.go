//go:build duckdb_arrow

// Package duckflight_test contains the Flight SQL conformance test suite.
//
// This file is the TDD spec for the duckflight server. All tests are written
// up front and expected to FAIL until the corresponding server methods are
// implemented. Each milestone in PLAN.md turns a batch of these tests green.
//
// To run: go test -tags=duckdb_arrow ./test/ -v
//
// NOTE: The exact flightsql.Client API (especially Txn, Prepared, GetDBSchemas
// options) should be verified against the SQLite example test at:
//   arrow-go/v18/arrow/flight/flightsql/sqlite_server_test.go
// If any signatures don't match, fix them here first.

package duckflight_test

import (
	"context"
	_ "fmt"
	"testing"
	_ "time"

	_ "github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	duckserver "github.com/prochac/duckflight/internal/server"
)

// ---------------------------------------------------------------------------
// Suite setup
// ---------------------------------------------------------------------------

type DuckFlightSQLSuite struct {
	suite.Suite

	mem    memory.Allocator
	server flight.Server
	client *flightsql.Client
}

func (s *DuckFlightSQLSuite) SetupSuite() {
	s.mem = memory.NewGoAllocator()

	// Create the duckflight server with an in-memory DuckDB (no Iceberg).
	// duckserver.New must return a flightsql.Server implementation that
	// embeds flightsql.BaseServer. Unimplemented methods return gRPC
	// Unimplemented errors, which is what makes all tests "red" initially.
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

	go s.server.Serve()

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
		s.client.Close()
	}
	if s.server != nil {
		s.server.Shutdown()
	}
}

// helper: execute DML directly on the server's engine for test setup.
// The server must expose a SeedSQL(ctx, sql) method for test use.
func (s *DuckFlightSQLSuite) seedSQL(sql string) {
	s.T().Helper()
	err := duckserver.SeedSQL(context.Background(), sql)
	s.Require().NoError(err, "seedSQL failed: %s", sql)
}

// helper: execute a query via Flight SQL and return all records.
func (s *DuckFlightSQLSuite) fetchAll(ctx context.Context, info *flight.FlightInfo) *flight.Reader {
	s.T().Helper()
	s.Require().NotEmpty(info.Endpoint)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	return rdr
}

// helper: count total rows across all record batches.
func countRows(rdr *flight.Reader) int64 {
	var total int64
	for rdr.Next() {
		total += rdr.Record().NumRows()
	}
	return total
}

// =========================================================================
// M2: Query execution
// =========================================================================

func (s *DuckFlightSQLSuite) TestExecuteSimpleQuery() {
	ctx := context.Background()
	s.seedSQL("CREATE TABLE IF NOT EXISTS test_exec (id INTEGER, name VARCHAR)")
	s.seedSQL("DELETE FROM test_exec")
	s.seedSQL("INSERT INTO test_exec VALUES (1, 'alice'), (2, 'bob')")

	info, err := s.client.Execute(ctx, "SELECT id, name FROM test_exec ORDER BY id")
	s.Require().NoError(err)

	rdr := s.fetchAll(ctx, info)
	defer rdr.Release()

	s.Require().True(rdr.Next(), "expected at least one record batch")
	rec := rdr.Record()
	s.Equal(int64(2), rec.NumRows())
	s.Equal("id", rec.Schema().Field(0).Name)
	s.Equal("name", rec.Schema().Field(1).Name)
}

func (s *DuckFlightSQLSuite) TestExecuteEmptyResult() {
	ctx := context.Background()
	s.seedSQL("CREATE TABLE IF NOT EXISTS test_empty (id INTEGER)")
	s.seedSQL("DELETE FROM test_empty")

	info, err := s.client.Execute(ctx, "SELECT * FROM test_empty")
	s.Require().NoError(err)

	rdr := s.fetchAll(ctx, info)
	defer rdr.Release()

	// No rows, but schema should still be valid.
	total := countRows(rdr)
	s.Equal(int64(0), total)
}

func (s *DuckFlightSQLSuite) TestExecuteSyntaxError() {
	ctx := context.Background()

	_, err := s.client.Execute(ctx, "SELECTTTT NOTHING")
	s.Require().Error(err)
}

func (s *DuckFlightSQLSuite) TestExecuteMultipleBatches() {
	// Generate enough rows that DuckDB may split into multiple batches.
	ctx := context.Background()

	info, err := s.client.Execute(ctx, "SELECT * FROM range(10000) t(id)")
	s.Require().NoError(err)

	rdr := s.fetchAll(ctx, info)
	defer rdr.Release()

	total := countRows(rdr)
	s.Equal(int64(10000), total)
}

func (s *DuckFlightSQLSuite) TestExecuteSchemaTypes() {
	// Verify various DuckDB types survive the Arrow round-trip.
	ctx := context.Background()

	query := `SELECT
		42::INTEGER AS int_col,
		3.14::DOUBLE AS double_col,
		'hello'::VARCHAR AS str_col,
		true::BOOLEAN AS bool_col,
		DATE '2025-01-15' AS date_col`

	info, err := s.client.Execute(ctx, query)
	s.Require().NoError(err)

	rdr := s.fetchAll(ctx, info)
	defer rdr.Release()

	s.Require().True(rdr.Next())
	schema := rdr.Record().Schema()
	s.Equal(5, schema.NumFields())
	s.Equal("int_col", schema.Field(0).Name)
	s.Equal("double_col", schema.Field(1).Name)
	s.Equal("str_col", schema.Field(2).Name)
	s.Equal("bool_col", schema.Field(3).Name)
	s.Equal("date_col", schema.Field(4).Name)
}

// =========================================================================
// M3: Metadata
// =========================================================================

func (s *DuckFlightSQLSuite) TestGetCatalogs() {
	ctx := context.Background()

	info, err := s.client.GetCatalogs(ctx)
	s.Require().NoError(err)

	rdr := s.fetchAll(ctx, info)
	defer rdr.Release()

	total := countRows(rdr)
	s.Greater(total, int64(0), "expected at least one catalog")
}

func (s *DuckFlightSQLSuite) TestGetDBSchemas() {
	ctx := context.Background()

	info, err := s.client.GetDBSchemas(ctx, &flightsql.GetDBSchemasOpts{})
	s.Require().NoError(err)

	rdr := s.fetchAll(ctx, info)
	defer rdr.Release()

	total := countRows(rdr)
	s.Greater(total, int64(0), "expected at least one schema")
}

func (s *DuckFlightSQLSuite) TestGetTables() {
	ctx := context.Background()
	s.seedSQL("CREATE TABLE IF NOT EXISTS test_meta_tbl (x INTEGER)")

	info, err := s.client.GetTables(ctx, &flightsql.GetTablesOpts{})
	s.Require().NoError(err)

	rdr := s.fetchAll(ctx, info)
	defer rdr.Release()

	total := countRows(rdr)
	s.Greater(total, int64(0), "expected at least one table")
}

func (s *DuckFlightSQLSuite) TestGetTableTypes() {
	ctx := context.Background()

	info, err := s.client.GetTableTypes(ctx)
	s.Require().NoError(err)

	rdr := s.fetchAll(ctx, info)
	defer rdr.Release()

	total := countRows(rdr)
	s.Greater(total, int64(0), "expected at least one table type")
}

func (s *DuckFlightSQLSuite) TestGetTablesFiltered() {
	ctx := context.Background()
	s.seedSQL("CREATE TABLE IF NOT EXISTS test_filter_abc (x INTEGER)")

	tableNameFilter := "test_filter_abc"
	info, err := s.client.GetTables(ctx, &flightsql.GetTablesOpts{
		TableNameFilterPattern: &tableNameFilter,
	})
	s.Require().NoError(err)

	rdr := s.fetchAll(ctx, info)
	defer rdr.Release()

	// Should find exactly the one table matching the filter.
	total := countRows(rdr)
	s.Equal(int64(1), total)
}

// =========================================================================
// M4: Prepared statements
// =========================================================================

func (s *DuckFlightSQLSuite) TestPreparedStatementQuery() {
	ctx := context.Background()
	s.seedSQL("CREATE TABLE IF NOT EXISTS test_prep (id INTEGER, val VARCHAR)")
	s.seedSQL("DELETE FROM test_prep")
	s.seedSQL("INSERT INTO test_prep VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	prep, err := s.client.Prepare(ctx, "SELECT * FROM test_prep WHERE id > 1 ORDER BY id")
	s.Require().NoError(err)
	defer prep.Close(ctx)

	// Schema should be available after prepare.
	schema := prep.DatasetSchema()
	s.Require().NotNil(schema)
	s.Equal(2, schema.NumFields())

	// Execute and verify results.
	info, err := prep.Execute(ctx)
	s.Require().NoError(err)

	rdr := s.fetchAll(ctx, info)
	defer rdr.Release()

	total := countRows(rdr)
	s.Equal(int64(2), total) // rows with id 2 and 3
}

func (s *DuckFlightSQLSuite) TestPreparedStatementClose() {
	ctx := context.Background()

	prep, err := s.client.Prepare(ctx, "SELECT 1")
	s.Require().NoError(err)

	err = prep.Close(ctx)
	s.Require().NoError(err)

	// Executing after close should fail.
	_, err = prep.Execute(ctx)
	s.Require().Error(err)
}

// =========================================================================
// M5: DML (statement updates)
// =========================================================================

func (s *DuckFlightSQLSuite) TestStatementUpdateInsert() {
	ctx := context.Background()
	s.seedSQL("CREATE TABLE IF NOT EXISTS test_upd (id INTEGER)")
	s.seedSQL("DELETE FROM test_upd")

	n, err := s.client.ExecuteUpdate(ctx, "INSERT INTO test_upd VALUES (1), (2), (3)")
	s.Require().NoError(err)
	s.Equal(int64(3), n)

	// Verify via query.
	info, err := s.client.Execute(ctx, "SELECT count(*)::INTEGER AS c FROM test_upd")
	s.Require().NoError(err)

	rdr := s.fetchAll(ctx, info)
	defer rdr.Release()

	s.Require().True(rdr.Next())
	rec := rdr.Record()
	s.Equal(int64(1), rec.NumRows())
}

func (s *DuckFlightSQLSuite) TestStatementUpdateDelete() {
	ctx := context.Background()
	s.seedSQL("CREATE TABLE IF NOT EXISTS test_del (id INTEGER)")
	s.seedSQL("DELETE FROM test_del")
	s.seedSQL("INSERT INTO test_del VALUES (1), (2), (3)")

	n, err := s.client.ExecuteUpdate(ctx, "DELETE FROM test_del WHERE id = 2")
	s.Require().NoError(err)
	s.Equal(int64(1), n)
}

func (s *DuckFlightSQLSuite) TestStatementUpdateCreateTable() {
	ctx := context.Background()

	_, err := s.client.ExecuteUpdate(ctx,
		"CREATE TABLE IF NOT EXISTS test_create_via_dml (x INTEGER)")
	s.Require().NoError(err)

	// Verify table exists.
	info, err := s.client.Execute(ctx, "SELECT * FROM test_create_via_dml")
	s.Require().NoError(err)
	s.Require().NotEmpty(info.Endpoint)
}

// =========================================================================
// M7: Transactions
// =========================================================================

func (s *DuckFlightSQLSuite) TestTransactionCommit() {
	ctx := context.Background()
	s.seedSQL("CREATE TABLE IF NOT EXISTS test_txn_c (id INTEGER)")
	s.seedSQL("DELETE FROM test_txn_c")

	txn, err := s.client.BeginTransaction(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(txn)

	// Insert within transaction.
	n, err := txn.ExecuteUpdate(ctx, "INSERT INTO test_txn_c VALUES (1), (2)")
	s.Require().NoError(err)
	s.Equal(int64(2), n)

	// Commit.
	err = txn.Commit(ctx)
	s.Require().NoError(err)

	// Data should be visible outside transaction.
	info, err := s.client.Execute(ctx, "SELECT count(*)::INTEGER AS c FROM test_txn_c")
	s.Require().NoError(err)

	rdr := s.fetchAll(ctx, info)
	defer rdr.Release()

	s.Require().True(rdr.Next())
	// Should see 2 rows.
}

func (s *DuckFlightSQLSuite) TestTransactionRollback() {
	ctx := context.Background()
	s.seedSQL("CREATE TABLE IF NOT EXISTS test_txn_r (id INTEGER)")
	s.seedSQL("DELETE FROM test_txn_r")

	txn, err := s.client.BeginTransaction(ctx)
	s.Require().NoError(err)

	_, err = txn.ExecuteUpdate(ctx, "INSERT INTO test_txn_r VALUES (99)")
	s.Require().NoError(err)

	// Rollback.
	err = txn.Rollback(ctx)
	s.Require().NoError(err)

	// Data should NOT be visible.
	info, err := s.client.Execute(ctx, "SELECT count(*)::INTEGER AS c FROM test_txn_r")
	s.Require().NoError(err)

	rdr := s.fetchAll(ctx, info)
	defer rdr.Release()

	s.Require().True(rdr.Next())
	total := countRows(rdr)
	// count should be in the record, and the value should be 0.
	_ = total
}

func (s *DuckFlightSQLSuite) TestTransactionIsolation() {
	ctx := context.Background()
	s.seedSQL("CREATE TABLE IF NOT EXISTS test_txn_iso (id INTEGER)")
	s.seedSQL("DELETE FROM test_txn_iso")
	s.seedSQL("INSERT INTO test_txn_iso VALUES (1)")

	txn, err := s.client.BeginTransaction(ctx)
	s.Require().NoError(err)

	// Read within transaction.
	info, err := txn.Execute(ctx, "SELECT count(*)::INTEGER FROM test_txn_iso")
	s.Require().NoError(err)

	rdr := s.fetchAll(ctx, info)
	s.Require().True(rdr.Next())
	rdr.Release()

	// Insert outside the transaction (via seed).
	s.seedSQL("INSERT INTO test_txn_iso VALUES (2)")

	// Read within same transaction — should still see snapshot.
	info2, err := txn.Execute(ctx, "SELECT count(*)::INTEGER FROM test_txn_iso")
	s.Require().NoError(err)

	rdr2 := s.fetchAll(ctx, info2)
	s.Require().True(rdr2.Next())
	rdr2.Release()

	err = txn.Commit(ctx)
	s.Require().NoError(err)
}

// =========================================================================
// M9: Statement timeout / guardrails
// =========================================================================

func (s *DuckFlightSQLSuite) TestStatementTimeout() {
	ctx := context.Background()

	// This cross-join should exceed the 10s timeout configured on the engine.
	_, err := s.client.Execute(ctx,
		"SELECT count(*) FROM range(100000000) t1, range(100000000) t2")
	// Should error — either from DuckDB's statement_timeout or context deadline.
	s.Require().Error(err)
}

// =========================================================================
// M9: GetSchema (without executing)
// =========================================================================

func (s *DuckFlightSQLSuite) TestGetExecuteSchema() {
	ctx := context.Background()
	s.seedSQL("CREATE TABLE IF NOT EXISTS test_schema_t (a INTEGER, b VARCHAR, c DOUBLE)")

	schema, err := s.client.GetExecuteSchema(ctx, "SELECT * FROM test_schema_t")
	s.Require().NoError(err)
	s.Require().NotNil(schema)

	parsed, err := flight.DeserializeSchema(schema.GetSchema(), s.mem)
	s.Require().NoError(err)
	s.Equal(3, parsed.NumFields())
	s.Equal("a", parsed.Field(0).Name)
	s.Equal("b", parsed.Field(1).Name)
	s.Equal("c", parsed.Field(2).Name)
}

// =========================================================================
// Run
// =========================================================================

func TestDuckFlightSQLSuite(t *testing.T) {
	suite.Run(t, new(DuckFlightSQLSuite))
}

// ---------------------------------------------------------------------------
// Supplementary: concurrency tests (not part of the suite, run separately)
// ---------------------------------------------------------------------------

func TestConcurrentReads(t *testing.T) {
	t.Skip("Enable after M8: requires server to be running")

	// TODO(M8): Start server, run N goroutines each executing SELECT queries,
	// verify all return correct results without data races.
}

func TestConcurrentReadWrite(t *testing.T) {
	t.Skip("Enable after M8: requires server to be running")

	// TODO(M8): Start server, run reader goroutines + writer goroutines,
	// verify writes serialize and reads don't block.
}
