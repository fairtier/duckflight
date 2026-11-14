//go:build duckdb_arrow

package duckflight_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	rwTestToken = "iceberg-suite-rw-token"
	roTestToken = "iceberg-suite-ro-token"
)

type staticToken string

func (t staticToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(t)}, nil
}

func (staticToken) RequireTransportSecurity() bool { return false }

func (s *IcebergSuite) query(c *flightsql.Client, q string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	info, err := c.Execute(ctx, q)
	if err != nil {
		return "", err
	}
	rdr, err := c.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return "", err
	}
	defer rdr.Release()
	var rows []string
	for rdr.Next() {
		rec := rdr.RecordBatch()
		for i := range int(rec.NumRows()) {
			cols := make([]string, 0, rec.NumCols())
			for j := range int(rec.NumCols()) {
				cols = append(cols, rec.Column(j).ValueStr(i))
			}
			rows = append(rows, strings.Join(cols, ","))
		}
	}
	return strings.Join(rows, ";"), rdr.Err()
}

// TestIcebergReadOnly_ReadsLake is the test that matters for read-only
// sessions: with external access disabled, configuration locked and the lake
// attached READ_ONLY on the read-only instance, Iceberg reads through
// httpfs/S3 must keep working — including for a table created, and rows
// committed, after that instance locked itself down.
func (s *IcebergSuite) TestIcebergReadOnly_ReadsLake() {
	ctx := context.Background()
	_, err := s.client.ExecuteUpdate(ctx, "DROP TABLE IF EXISTS lake.default.ro_t")
	s.Require().NoError(err)
	_, err = s.client.ExecuteUpdate(ctx, "CREATE TABLE lake.default.ro_t (id INTEGER, name VARCHAR)")
	s.Require().NoError(err)
	_, err = s.client.ExecuteUpdate(ctx, "INSERT INTO lake.default.ro_t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
	s.Require().NoError(err)

	for q, want := range map[string]string{
		"SELECT count(*) FROM lake.default.ro_t":                                   "3",
		"SELECT count(*) FROM ro_t":                                                "3", // USE lake.default on every read-only connection
		"SELECT name FROM lake.default.ro_t WHERE id = 2":                          "bob",
		"FROM lake.default.ro_t SELECT max(id)":                                    "3",
		"WITH x AS (FROM ro_t) SELECT string_agg(name ORDER BY id) FROM x":         "alice,bob,carol",
		"SELECT count(*) FROM iceberg_snapshots(lake.default.ro_t)":                "1", // the INSERT; CREATE commits no snapshot
		"SELECT count(*) FROM information_schema.tables WHERE table_name = 'ro_t'": "1",
	} {
		got, err := s.query(s.roClient, q)
		s.Require().NoError(err, q)
		s.Equal(want, got, q)
	}
	for _, q := range []string{"DESCRIBE lake.default.ro_t", "SHOW TABLES", "SUMMARIZE lake.default.ro_t", "EXPLAIN SELECT * FROM ro_t"} {
		_, err := s.query(s.roClient, q)
		s.Require().NoError(err, q)
	}

	// Commits after the read-only instance first saw the table are visible.
	_, err = s.client.ExecuteUpdate(ctx, "INSERT INTO lake.default.ro_t VALUES (4, 'dave')")
	s.Require().NoError(err)
	got, err := s.query(s.roClient, "SELECT count(*) FROM lake.default.ro_t")
	s.Require().NoError(err)
	s.Equal("4", got)

	// Flight SQL metadata RPCs route to the read-only instance too.
	info, err := s.roClient.GetTables(ctx, &flightsql.GetTablesOpts{
		Catalog:                ptr("lake"),
		TableNameFilterPattern: ptr("ro_t"),
	})
	s.Require().NoError(err)
	rdr, err := s.roClient.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	var n int64
	for rdr.Next() {
		n += rdr.RecordBatch().NumRows()
	}
	s.Require().NoError(rdr.Err())
	rdr.Release()
	s.EqualValues(1, n)
}

// TestIcebergReadOnly_RefusesWritesAndRawStorage pins what a read-only caller
// cannot do against a real lake: write to it, or go around the catalog to the
// bucket behind it.
func (s *IcebergSuite) TestIcebergReadOnly_RefusesWritesAndRawStorage() {
	ctx := context.Background()
	_, err := s.client.ExecuteUpdate(ctx, "DROP TABLE IF EXISTS lake.default.ro_guarded")
	s.Require().NoError(err)
	_, err = s.client.ExecuteUpdate(ctx, "CREATE TABLE lake.default.ro_guarded AS SELECT 1 AS i")
	s.Require().NoError(err)

	for _, q := range []string{
		"INSERT INTO lake.default.ro_guarded VALUES (2)",
		"DELETE FROM lake.default.ro_guarded",
		"DROP TABLE lake.default.ro_guarded",
		"CREATE TABLE lake.default.ro_new (i INT)",
		"COPY (SELECT 1 AS x) TO 's3://" + minioBucket + "/exfil.parquet'",
		"COPY lake.default.ro_guarded TO 's3://" + minioBucket + "/exfil.parquet'",
		"SELECT * FROM read_parquet('s3://" + minioBucket + "/**/*.parquet')",
		"SELECT * FROM glob('s3://" + minioBucket + "/**')",
		"SELECT * FROM 's3://" + minioBucket + "/x.parquet'",
		"SELECT * FROM iceberg_scan('s3://" + minioBucket + "/x')",
		"DETACH lake",
		"SELECT * FROM read_text('/proc/self/environ')",
	} {
		_, err := s.query(s.roClient, q)
		s.Require().Error(err, q)
		s.Equal(codes.PermissionDenied, status.Code(err), "%q: %v", q, err)
	}
	got, err := s.query(s.client, "SELECT count(*) FROM lake.default.ro_guarded")
	s.Require().NoError(err)
	s.Equal("1", got)
}

// TestIcebergReadOnly_EngineBackstop goes below the statement policy, straight
// to a connection of the read-only instance, to show what DuckDB itself still
// enforces there if the parser check ever missed something. It also records
// what it does not: raw reads inside allowed_directories.
func (s *IcebergSuite) TestIcebergReadOnly_EngineBackstop() {
	ctx := context.Background()
	_, err := s.client.ExecuteUpdate(ctx, "DROP TABLE IF EXISTS lake.default.ro_backstop")
	s.Require().NoError(err)
	_, err = s.client.ExecuteUpdate(ctx, "CREATE TABLE lake.default.ro_backstop AS SELECT 1 AS i")
	s.Require().NoError(err)

	eng := s.srv.ReadOnlyEngine()
	s.Require().NotNil(eng)
	ac, err := eng.Pool.Acquire(ctx)
	s.Require().NoError(err)
	defer eng.Pool.Discard(ac)

	refused := map[string]string{
		"INSERT INTO lake.default.ro_backstop VALUES (2)":                "read-only mode",
		"DROP TABLE lake.default.ro_backstop":                            "read-only mode",
		"CREATE TABLE lake.default.ro_x (i INT)":                         "read-only mode",
		"SELECT * FROM read_text('/proc/self/environ')":                  "disabled by configuration",
		"SELECT * FROM read_csv('http://127.0.0.1:1/x.csv')":             "disabled by configuration",
		"COPY (SELECT 1) TO 's3://some-other-bucket/x.parquet'":          "disabled by configuration",
		"SELECT * FROM read_parquet('s3://some-other-bucket/x.parquet')": "disabled by configuration",
		"LOAD spatial":                                  "disabled through configuration",
		"SET enable_external_access = true":             "locked",
		"SET allowed_directories = ['/']":               "", // locked or refused while external access is off
		"SELECT * FROM duckdb_secrets(redact := false)": "",
	}
	for q, want := range refused {
		_, err := ac.ExecContext(ctx, q)
		s.Require().Error(err, q)
		s.Contains(err.Error(), want, q)
	}

	// Secrets stay redacted at the engine level.
	rdr, err := ac.Arrow.QueryContext(ctx, "SELECT string_agg(secret_string, '|') FROM duckdb_secrets()")
	s.Require().NoError(err)
	s.Require().True(rdr.Next())
	secrets := strings.Clone(rdr.RecordBatch().Column(0).ValueStr(0))
	rdr.Release()
	s.NotContains(secrets, minioPassword)
	s.Contains(secrets, "redacted")

	// The residual the parser check exists for: inside allowed_directories
	// DuckDB permits raw object reads, and (were the statement allowed) writes.
	_, err = ac.ExecContext(ctx, "SELECT * FROM glob('s3://"+minioBucket+"/**')")
	s.Require().NoError(err, "raw listing inside allowed_directories is an engine-level residual")
}

func ptr[T any](v T) *T { return &v }

// TestIcebergReadOnly_SecretRotation covers RECONCILE_SQL_PATH's job on the
// read-only instance: a secret replaced after the lockdown takes effect there
// (httpfs was loaded before it, so an S3 secret can still be created).
func (s *IcebergSuite) TestIcebergReadOnly_SecretRotation() {
	ctx := context.Background()
	_, err := s.client.ExecuteUpdate(ctx, "DROP TABLE IF EXISTS lake.default.ro_rot")
	s.Require().NoError(err)
	_, err = s.client.ExecuteUpdate(ctx, "CREATE TABLE lake.default.ro_rot AS SELECT 1 AS i")
	s.Require().NoError(err)

	ins, err := s.minioC.Inspect(ctx)
	s.Require().NoError(err)
	endpoint := net.JoinHostPort(ins.NetworkSettings.Networks[s.network.Name].IPAddress.String(), "9000")
	secret := func(key string) string {
		return fmt.Sprintf(`CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID '%s', SECRET '%s',
			ENDPOINT '%s', REGION 'local-01', URL_STYLE 'path', USE_SSL false)`, minioUser, key, endpoint)
	}
	eng := s.srv.ReadOnlyEngine()

	s.Require().NoError(eng.ExecSQL(ctx, "rotate to a wrong key", secret("not-the-password")))
	_, err = s.query(s.roClient, "SELECT count(*) FROM lake.default.ro_rot")
	s.Require().Error(err, "a wrong S3 key must break reads: rotation reached the instance")

	s.Require().NoError(eng.ExecSQL(ctx, "rotate back", secret(minioPassword)))
	got, err := s.query(s.roClient, "SELECT count(*) FROM lake.default.ro_rot")
	s.Require().NoError(err)
	s.Equal("1", got)
}
