//go:build duckdb_arrow && iceberg_integration

package duckflight_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckserver "github.com/prochac/duckflight/internal/server"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	minioUser     = "minio-root-user"
	minioPassword = "minio-root-password"
	minioBucket   = "warehouse"
	pgUser        = "postgres"
	pgPassword    = "postgres"
	pgDB          = "lakekeeper"
	warehouseName = "demo"
)

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

type IcebergSuite struct {
	suite.Suite

	server      flight.Server
	client      *flightsql.Client
	mem         *memory.CheckedAllocator
	network     *testcontainers.DockerNetwork
	postgresC   *postgres.PostgresContainer
	minioC      *minio.MinioContainer
	migrateC    testcontainers.Container
	lakekeeperC testcontainers.Container
	catalogURL  string
}

func (s *IcebergSuite) SetupSuite() {
	ctx := context.Background()

	// 1. Docker network
	nw, err := network.New(ctx)
	s.Require().NoError(err)
	s.network = nw

	// 2. PostgreSQL (via module)
	s.postgresC, err = postgres.Run(ctx,
		"postgres:17",
		postgres.WithDatabase(pgDB),
		postgres.WithUsername(pgUser),
		postgres.WithPassword(pgPassword),
		network.WithNetwork([]string{"postgres"}, nw),
	)
	s.Require().NoError(err)

	// 3. MinIO (via module)
	s.minioC, err = minio.Run(ctx,
		"minio/minio",
		minio.WithUsername(minioUser),
		minio.WithPassword(minioPassword),
		network.WithNetwork([]string{"minio"}, nw),
	)
	s.Require().NoError(err)

	// Create bucket via mc sidecar
	mcC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:    "minio/mc",
			Networks: []string{nw.Name},
			Entrypoint: []string{"/bin/sh", "-c",
				fmt.Sprintf(
					"mc alias set myminio http://minio:9000 %s %s && mc mb myminio/%s --ignore-existing",
					minioUser, minioPassword, minioBucket,
				),
			},
			WaitingFor: wait.ForExit().WithExitTimeout(30 * time.Second),
		},
		Started: true,
	})
	s.Require().NoError(err)
	mcState, err := mcC.State(ctx)
	s.Require().NoError(err)
	s.Require().Equal(0, mcState.ExitCode, "mc bucket creation failed")
	_ = mcC.Terminate(ctx)

	// Get MinIO container IP — reachable from both host (Docker bridge) and containers.
	minioInspect, err := s.minioC.Inspect(ctx)
	s.Require().NoError(err)
	minioIP := minioInspect.NetworkSettings.Networks[nw.Name].IPAddress
	s.Require().NotEmpty(minioIP, "could not determine MinIO container IP")
	minioEndpoint := fmt.Sprintf("http://%s:9000", minioIP)

	// 4. Lakekeeper migrate (one-shot)
	pgConnStr := fmt.Sprintf("postgresql://%s:%s@postgres:5432/%s", pgUser, pgPassword, pgDB)
	s.migrateC, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:    "quay.io/lakekeeper/catalog:latest-main",
			Networks: []string{nw.Name},
			Env: map[string]string{
				"LAKEKEEPER__PG_DATABASE_URL_READ":  pgConnStr,
				"LAKEKEEPER__PG_DATABASE_URL_WRITE": pgConnStr,
			},
			Cmd:        []string{"migrate"},
			WaitingFor: wait.ForExit().WithExitTimeout(60 * time.Second),
		},
		Started: true,
	})
	s.Require().NoError(err)

	// 5. Lakekeeper serve
	s.lakekeeperC, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "quay.io/lakekeeper/catalog:latest-main",
			ExposedPorts: []string{"8181/tcp"},
			Networks:     []string{nw.Name},
			NetworkAliases: map[string][]string{
				nw.Name: {"lakekeeper"},
			},
			Env: map[string]string{
				"LAKEKEEPER__PG_DATABASE_URL_READ":  pgConnStr,
				"LAKEKEEPER__PG_DATABASE_URL_WRITE": pgConnStr,
			},
			Cmd:        []string{"serve"},
			WaitingFor: wait.ForHTTP("/health").WithPort("8181/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	s.Require().NoError(err)

	catalogHost, err := s.lakekeeperC.Host(ctx)
	s.Require().NoError(err)
	catalogPort, err := s.lakekeeperC.MappedPort(ctx, "8181/tcp")
	s.Require().NoError(err)
	catalogBase := fmt.Sprintf("http://%s:%s", catalogHost, catalogPort.Port())
	s.catalogURL = catalogBase + "/catalog"

	// 6. Bootstrap Lakekeeper
	s.httpPost(catalogBase+"/management/v1/bootstrap", map[string]any{
		"accept-terms-of-use": true,
	})

	// 7. Create warehouse with S3-compat storage profile pointing to MinIO.
	// No credential vending (sts-enabled=false, remote-signing-enabled=false):
	// DuckDB must supply its own S3 credentials.
	whResp := s.httpPost(catalogBase+"/management/v1/warehouse", map[string]any{
		"warehouse-name": warehouseName,
		"storage-profile": map[string]any{
			"type":                 "s3",
			"bucket":              minioBucket,
			"endpoint":            minioEndpoint,
			"region":              "local-01",
			"path-style-access":   true,
			"flavor":              "s3-compat",
			"sts-enabled":         false,
			"remote-signing-enabled": false,
		},
		"storage-credential": map[string]any{
			"type":                  "s3",
			"credential-type":       "access-key",
			"aws-access-key-id":     minioUser,
			"aws-secret-access-key": minioPassword,
		},
	})
	var whResult struct {
		WarehouseID string `json:"warehouse-id"`
	}
	s.Require().NoError(json.Unmarshal(whResp, &whResult))
	s.Require().NotEmpty(whResult.WarehouseID, "warehouse-id missing from response")

	// 8. Create default namespace
	s.httpPost(s.catalogURL+"/v1/"+whResult.WarehouseID+"/namespaces", map[string]any{
		"namespace": []string{"default"},
	})

	// 9. Boot DuckFlight server with Iceberg catalog attached.
	// S3 credentials are passed so DuckDB can read/write data files in MinIO.
	srv, err := duckserver.New(duckserver.Config{
		MemoryLimit:      "512MB",
		MaxThreads:       2,
		QueryTimeout:     "30s",
		PoolSize:         4,
		IcebergEndpoint:  s.catalogURL,
		IcebergWarehouse: warehouseName,
		// No OAuth2 — Lakekeeper runs without auth in tests
		S3Endpoint:  minioIP + ":9000",
		S3AccessKey: minioUser,
		S3SecretKey: minioPassword,
		S3Region:    "local-01",
		S3URLStyle:  "path",
	})
	s.Require().NoError(err)

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

func (s *IcebergSuite) TearDownSuite() {
	ctx := context.Background()

	if s.client != nil {
		s.client.Close()
	}
	if s.server != nil {
		s.server.Shutdown()
	}
	if s.lakekeeperC != nil {
		_ = s.lakekeeperC.Terminate(ctx)
	}
	if s.migrateC != nil {
		_ = s.migrateC.Terminate(ctx)
	}
	if s.minioC != nil {
		_ = s.minioC.Terminate(ctx)
	}
	if s.postgresC != nil {
		_ = s.postgresC.Terminate(ctx)
	}
	if s.network != nil {
		_ = s.network.Remove(ctx)
	}
}

func (s *IcebergSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	s.client.Alloc = s.mem
}

func (s *IcebergSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (s *IcebergSuite) httpPost(url string, body any) []byte {
	s.T().Helper()
	data, err := json.Marshal(body)
	s.Require().NoError(err)

	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	s.Require().NoError(err)
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	s.Require().True(resp.StatusCode >= 200 && resp.StatusCode < 300,
		"POST %s returned %d: %s", url, resp.StatusCode, string(respBody))
	return respBody
}

func (s *IcebergSuite) execQuery(query string) (*arrow.Schema, []arrow.RecordBatch) {
	ctx := context.Background()
	info, err := s.client.Execute(ctx, query)
	s.Require().NoError(err)

	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	var recs []arrow.RecordBatch
	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()
		recs = append(recs, rec)
	}
	s.Require().NoError(rdr.Err())
	schema := rdr.Schema()
	return schema, recs
}

func releaseRecs(recs []arrow.RecordBatch) {
	for i := range recs {
		recs[i].Release()
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func (s *IcebergSuite) TestIceberg_WriteAndReadBack() {
	ctx := context.Background()

	_, err := s.client.ExecuteUpdate(ctx,
		"CREATE TABLE lake.default.test_rw (id INTEGER, name VARCHAR)")
	s.Require().NoError(err)

	result, err := s.client.ExecuteUpdate(ctx,
		"INSERT INTO lake.default.test_rw VALUES (1, 'alice'), (2, 'bob')")
	s.Require().NoError(err)
	s.EqualValues(2, result)

	_, recs := s.execQuery("SELECT id, name FROM lake.default.test_rw ORDER BY id")
	defer releaseRecs(recs)

	s.Require().NotEmpty(recs)
	var totalRows int64
	for _, rec := range recs {
		totalRows += rec.NumRows()
	}
	s.EqualValues(2, totalRows)

	rec := recs[0]
	s.Equal(int32(1), rec.Column(0).(*array.Int32).Value(0))
	s.Equal("alice", rec.Column(1).(*array.String).Value(0))
}

func (s *IcebergSuite) TestIceberg_TablesAppearInMetadata() {
	ctx := context.Background()

	_, err := s.client.ExecuteUpdate(ctx,
		"CREATE TABLE IF NOT EXISTS lake.default.meta_test (x INTEGER)")
	s.Require().NoError(err)

	// GetCatalogs should include "lake"
	info, err := s.client.GetCatalogs(ctx)
	s.Require().NoError(err)
	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	var catalogs []string
	for rdr.Next() {
		col := rdr.RecordBatch().Column(0).(*array.String)
		for i := 0; i < col.Len(); i++ {
			catalogs = append(catalogs, col.Value(i))
		}
	}
	s.Contains(catalogs, "lake", "GetCatalogs should include 'lake'")

	// GetTables with catalog filter should show our Iceberg table
	info, err = s.client.GetTables(ctx, &flightsql.GetTablesOpts{
		Catalog:               strPtr("lake"),
		TableNameFilterPattern: strPtr("meta_test"),
	})
	s.Require().NoError(err)
	rdr2, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr2.Release()

	var tableCount int64
	for rdr2.Next() {
		tableCount += rdr2.RecordBatch().NumRows()
	}
	s.Greater(tableCount, int64(0), "meta_test table should appear in GetTables")
}

func (s *IcebergSuite) TestIceberg_SchemaEvolution() {
	ctx := context.Background()

	_, err := s.client.ExecuteUpdate(ctx,
		"CREATE TABLE lake.default.schema_evo (id INTEGER)")
	s.Require().NoError(err)

	_, err = s.client.ExecuteUpdate(ctx,
		"INSERT INTO lake.default.schema_evo VALUES (1)")
	s.Require().NoError(err)

	_, err = s.client.ExecuteUpdate(ctx,
		"ALTER TABLE lake.default.schema_evo ADD COLUMN name VARCHAR")
	s.Require().NoError(err)

	schema, recs := s.execQuery("SELECT * FROM lake.default.schema_evo")
	defer releaseRecs(recs)

	s.Equal(2, schema.NumFields())
	s.Equal("id", schema.Field(0).Name)
	s.Equal("name", schema.Field(1).Name)
}

func (s *IcebergSuite) TestIceberg_TransactionCommit() {
	ctx := context.Background()

	_, err := s.client.ExecuteUpdate(ctx,
		"CREATE TABLE lake.default.txn_commit (id INTEGER, val VARCHAR)")
	s.Require().NoError(err)

	tx, err := s.client.BeginTransaction(ctx)
	s.Require().NoError(err)

	_, err = tx.ExecuteUpdate(ctx,
		"INSERT INTO lake.default.txn_commit VALUES (1, 'committed')")
	s.Require().NoError(err)

	s.Require().NoError(tx.Commit(ctx))

	_, recs := s.execQuery("SELECT * FROM lake.default.txn_commit")
	defer releaseRecs(recs)

	var totalRows int64
	for _, rec := range recs {
		totalRows += rec.NumRows()
	}
	s.EqualValues(1, totalRows, "committed row should be visible")
}

func (s *IcebergSuite) TestIceberg_TransactionRollback() {
	ctx := context.Background()

	_, err := s.client.ExecuteUpdate(ctx,
		"CREATE TABLE lake.default.txn_rollback (id INTEGER, val VARCHAR)")
	s.Require().NoError(err)

	tx, err := s.client.BeginTransaction(ctx)
	s.Require().NoError(err)

	_, err = tx.ExecuteUpdate(ctx,
		"INSERT INTO lake.default.txn_rollback VALUES (1, 'rolled_back')")
	s.Require().NoError(err)

	s.Require().NoError(tx.Rollback(ctx))

	_, recs := s.execQuery("SELECT * FROM lake.default.txn_rollback")
	defer releaseRecs(recs)

	var totalRows int64
	for _, rec := range recs {
		totalRows += rec.NumRows()
	}
	s.EqualValues(0, totalRows, "rolled-back row should not be visible")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func strPtr(s string) *string { return &s }

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

func TestIcebergSuite(t *testing.T) {
	suite.Run(t, new(IcebergSuite))
}
