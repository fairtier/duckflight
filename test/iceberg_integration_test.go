//go:build duckdb_arrow

package duckflight_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fairtier/duckflight/internal/config"
	duckserver "github.com/fairtier/duckflight/internal/server"
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
	poolSize      = 4
	siloImage     = "docker.io/pgsty/silo:RELEASE.2026-09-03T13-18-01Z"
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

	// 3. Object storage: Silo, the community continuation of the MinIO
	// server on its last Apache-2.0 lineage, run through the MinIO module
	// (its entrypoint maps the module's `server /data` onto `silo`).
	//
	// Not MinIO itself: `minio/minio` on Docker Hub answers anonymous pulls
	// with 401 since 2026-09, and quay.io's `quay.io/minio/minio` and
	// `quay.io/minio/mc` followed, so both images failed every test in this
	// suite with "unauthorized". Pinned, because an unpinned tag is how a
	// test starts failing without anything in this repo changing.
	s.minioC, err = minio.Run(ctx,
		siloImage,
		minio.WithUsername(minioUser),
		minio.WithPassword(minioPassword),
		network.WithNetwork([]string{"minio"}, nw),
	)
	s.Require().NoError(err)

	// Create the bucket with the client Silo bundles, rather than a separate
	// client image.
	code, out, err := s.minioC.Exec(ctx, []string{"sh", "-c", fmt.Sprintf(
		"mcli alias set local http://127.0.0.1:9000 %s %s && mcli mb --ignore-existing local/%s",
		minioUser, minioPassword, minioBucket,
	)})
	s.Require().NoError(err)
	if code != 0 {
		msg, _ := io.ReadAll(out)
		s.Require().Failf("bucket creation failed", "exit %d: %s", code, msg)
	}

	// Get MinIO container IP — reachable from both host (Docker bridge) and containers.
	minioInspect, err := s.minioC.Inspect(ctx)
	s.Require().NoError(err)
	minioIP := minioInspect.NetworkSettings.Networks[nw.Name].IPAddress
	s.Require().True(minioIP.IsValid(), "could not determine MinIO container IP")
	minioEndpoint := fmt.Sprintf("http://%s:9000", minioIP)

	// 4. Lakekeeper migrate (one-shot)
	pgConnStr := fmt.Sprintf("postgresql://%s:%s@postgres:5432/%s", pgUser, pgPassword, pgDB)
	s.migrateC, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Image:    "quay.io/lakekeeper/catalog:latest-main",
		Networks: []string{nw.Name},
		Env: map[string]string{
			"LAKEKEEPER__PG_DATABASE_URL_READ":  pgConnStr,
			"LAKEKEEPER__PG_DATABASE_URL_WRITE": pgConnStr,
		},
		Cmd:        []string{"migrate"},
		WaitingFor: wait.ForExit().WithExitTimeout(60 * time.Second),
		Started:    true,
	})
	s.Require().NoError(err)

	// 5. Lakekeeper serve
	s.lakekeeperC, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
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
		Started:    true,
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
			"type":                   "s3",
			"bucket":                 minioBucket,
			"endpoint":               minioEndpoint,
			"region":                 "local-01",
			"path-style-access":      true,
			"flavor":                 "s3-compat",
			"sts-enabled":            false,
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

	// 8. Deliberately no `default` namespace: nothing in a real deployment
	// creates one, and a suite that pre-created it hid the bug
	// TestIceberg_MissingTableKeepsPoolAlive guards. DuckFlight creates it.

	// 9. Boot DuckFlight server with Iceberg catalog attached.
	// S3 credentials are passed so DuckDB can read/write data files in MinIO.
	srv, err := duckserver.New(&config.Config{
		MemoryLimit:      "512MB",
		MaxThreads:       2,
		QueryTimeout:     "30s",
		PoolSize:         poolSize,
		IcebergEndpoint:  s.catalogURL,
		IcebergWarehouse: warehouseName,
		// No OAuth2 — Lakekeeper runs without auth in tests
		S3Endpoint:  net.JoinHostPort(minioIP.String(), "9000"),
		S3AccessKey: minioUser,
		S3SecretKey: minioPassword,
		S3Region:    "local-01",
		S3URLStyle:  "path",
	})
	s.Require().NoError(err)

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

func (s *IcebergSuite) TearDownSuite() {
	ctx := context.Background()

	if s.client != nil {
		_ = s.client.Close()
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

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(data))
	s.Require().NoError(err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	s.Require().NoError(err)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	s.Require().True(resp.StatusCode >= 200 && resp.StatusCode < 300,
		"POST %s returned %d: %s", url, resp.StatusCode, string(respBody))
	return respBody
}

func (s *IcebergSuite) execQuery(query string) []arrow.RecordBatch {
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
	return recs
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
		"CREATE TABLE test_rw (id INTEGER, name VARCHAR)")
	s.Require().NoError(err)

	result, err := s.client.ExecuteUpdate(ctx,
		"INSERT INTO test_rw VALUES (1, 'alice'), (2, 'bob')")
	s.Require().NoError(err)
	s.EqualValues(2, result)

	recs := s.execQuery("SELECT id, name FROM test_rw ORDER BY id")
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
		"CREATE TABLE IF NOT EXISTS meta_test (x INTEGER)")
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
		Catalog:                new("lake"),
		TableNameFilterPattern: new("meta_test"),
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

func (s *IcebergSuite) TestIceberg_TransactionCommit() {
	ctx := context.Background()

	_, err := s.client.ExecuteUpdate(ctx,
		"CREATE TABLE txn_commit (id INTEGER, val VARCHAR)")
	s.Require().NoError(err)

	tx, err := s.client.BeginTransaction(ctx)
	s.Require().NoError(err)

	_, err = tx.ExecuteUpdate(ctx,
		"INSERT INTO txn_commit VALUES (1, 'committed')")
	s.Require().NoError(err)

	s.Require().NoError(tx.Commit(ctx))

	recs := s.execQuery("SELECT * FROM txn_commit")
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
		"CREATE TABLE txn_rollback (id INTEGER, val VARCHAR)")
	s.Require().NoError(err)

	tx, err := s.client.BeginTransaction(ctx)
	s.Require().NoError(err)

	_, err = tx.ExecuteUpdate(ctx,
		"INSERT INTO txn_rollback VALUES (1, 'rolled_back')")
	s.Require().NoError(err)

	s.Require().NoError(tx.Rollback(ctx))

	recs := s.execQuery("SELECT * FROM txn_rollback")
	defer releaseRecs(recs)

	var totalRows int64
	for _, rec := range recs {
		totalRows += rec.NumRows()
	}
	s.EqualValues(0, totalRows, "rolled-back row should not be visible")
}

// TestIceberg_MissingTableKeepsPoolAlive guards the pool against a poisoned
// default schema. DuckDB-Iceberg answers a table lookup that misses by asking
// the catalog whether the *schema* exists, and if it does not, marks its
// instance-wide cache entry non-existent for good. `lake.default` is the
// current schema, so one unqualified typo against a catalog with no `default`
// namespace made every later `USE lake."default"` fail — and with it the boot
// of every replacement connection, until the pool ran dry and every query,
// `SELECT 1` included, timed out.
func (s *IcebergSuite) TestIceberg_MissingTableKeepsPoolAlive() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The poisoning lookup. The binder may reject it at GetFlightInfo or at
	// DoGet; either way it must fail.
	info, err := s.client.Execute(ctx, "SELECT * FROM no_such_table")
	if err == nil {
		var rdr *flight.Reader
		rdr, err = s.client.DoGet(ctx, info.Endpoint[0].Ticket)
		if err == nil {
			for rdr.Next() {
			}
			err = rdr.Err()
			rdr.Release()
		}
	}
	s.Require().Error(err, "query against a missing table must fail")

	// Recycle every pooled connection: a SET marks its connection dirty, so
	// the pool closes it and boots a replacement, which runs the boot SQL
	// against the (possibly poisoned) catalog.
	for range 2 * poolSize {
		_, err := s.client.ExecuteUpdate(ctx, "SET threads = 2")
		s.Require().NoError(err)
	}

	recs := s.execQuery("SELECT current_database() || '.' || current_schema()")
	defer releaseRecs(recs)
	s.Require().Len(recs, 1)
	s.Equal("lake.default", recs[0].Column(0).ValueStr(0),
		"replacement connections should still boot into lake.default")
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

func TestIcebergSuite(t *testing.T) {
	suite.Run(t, new(IcebergSuite))
}
