//go:build duckdb_arrow

package server_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prochac/duckflight/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestSIGSEGV_IngestThenGetTablesWithSchema reproduces the SIGSEGV crash in
// DoGetTables that occurs when:
//
//  1. A table is created via bulk ingest (DoPutCommandStatementIngest), which
//     uses Arrow.RegisterView internally
//  2. DoGetTables with IncludeSchema=true iterates ALL tables and runs
//     "SELECT * FROM <table> LIMIT 0" on each, including the ingested table
//
// The crash is in duckdb_prepare_extracted_statement (CGO null pointer deref).
// It reproduces reliably with AND without -race — this is not a Go data race
// but a bug in duckdb-go or DuckDB itself triggered by the Arrow view path.
//
// Run with:
//
//	go test -tags=duckdb_arrow -count=1 -run TestSIGSEGV_IngestThenGetTablesWithSchema ./internal/server/...
func TestSIGSEGV_IngestThenGetTablesWithSchema(t *testing.T) {
	srv, err := server.New(server.Config{
		MemoryLimit:  "512MB",
		MaxThreads:   2,
		QueryTimeout: "10s",
		PoolSize:     4,
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	flightSrv := flight.NewServerWithMiddleware(nil)
	flightSrv.RegisterFlightService(flightsql.NewFlightServer(srv))
	if err := flightSrv.Init("localhost:0"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	go func() { _ = flightSrv.Serve() }()
	defer flightSrv.Shutdown()

	cl, err := flightsql.NewClient(
		flightSrv.Addr().String(),
		nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = cl.Close() }()

	cl.Alloc = memory.DefaultAllocator

	ctx := context.Background()

	// Seed standard test tables.
	for _, sql := range []string{
		"CREATE TABLE foreignTable (id INTEGER PRIMARY KEY, foreignName VARCHAR, value BIGINT)",
		"INSERT INTO foreignTable VALUES (1, 'one', 1)",
		"CREATE SEQUENCE intTable_id_seq START 100",
		"CREATE TABLE intTable (id INTEGER DEFAULT nextval('intTable_id_seq') PRIMARY KEY, keyName VARCHAR, value BIGINT, foreignId BIGINT)",
		"INSERT INTO intTable VALUES (1, 'one', 1, 1), (2, 'zero', 0, 1), (3, 'negative one', -1, 1), (4, NULL, NULL, NULL)",
	} {
		if err := server.SeedSQL(ctx, sql); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Step 1: Bulk ingest — creates a new table via the Arrow view path.
	rec := buildRecords(memory.DefaultAllocator, 3)
	defer rec.Release()

	rdr, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	defer rdr.Release()

	n, err := cl.ExecuteIngest(ctx, rdr, &flightsql.ExecuteIngestOpts{
		TableDefinitionOptions: &flightsql.TableDefinitionOptions{
			IfNotExist: flightsql.TableDefinitionOptionsTableNotExistOptionCreate,
			IfExists:   flightsql.TableDefinitionOptionsTableExistsOptionAppend,
		},
		Table: "ingest_sigsegv_table",
	})
	if err != nil {
		t.Fatalf("ExecuteIngest: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected 3 rows ingested, got %d", n)
	}

	// Step 2: GetTables with IncludeSchema — triggers the SIGSEGV.
	info, err := cl.GetTables(ctx, &flightsql.GetTablesOpts{
		IncludeSchema: true,
	})
	if err != nil {
		t.Fatalf("GetTables: %v", err)
	}

	reader, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatalf("DoGet: %v", err)
	}
	defer reader.Release()

	for reader.Next() {
		_ = reader.RecordBatch()
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader error: %v", err)
	}

	// No assertion on allocator size — the purpose of this test is to verify
	// that DoGetTables with IncludeSchema does not SIGSEGV after bulk ingest.
}

func buildRecords(mem memory.Allocator, n int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	idBldr := array.NewInt32Builder(mem)
	defer idBldr.Release()
	nameBldr := array.NewStringBuilder(mem)
	defer nameBldr.Release()
	for i := 0; i < n; i++ {
		idBldr.Append(int32(i + 1))
		nameBldr.Append(fmt.Sprintf("row_%d", i+1))
	}
	idCol := idBldr.NewArray()
	defer idCol.Release()
	nameCol := nameBldr.NewArray()
	defer nameCol.Release()
	return array.NewRecordBatch(schema, []arrow.Array{idCol, nameCol}, int64(n))
}
