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
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fairtier/duckflight/internal/auth"
	"github.com/fairtier/duckflight/internal/config"
	"github.com/fairtier/duckflight/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	rwToken = "rw-token-for-read-only-tests"
	roToken = "ro-token-for-read-only-tests"
)

// bearer sends a static token, and optionally the read-only header.
type bearer struct {
	token    string
	readOnly string
}

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	md := map[string]string{"authorization": "Bearer " + b.token}
	if b.readOnly != "" {
		md[auth.ReadOnlyHeader] = b.readOnly
	}
	return md, nil
}

func (bearer) RequireTransportSecurity() bool { return false }

type readOnlyFixture struct {
	addr string
}

func (f readOnlyFixture) dial(t *testing.T, creds *bearer) *flightsql.Client {
	t.Helper()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if creds != nil {
		opts = append(opts, grpc.WithPerRPCCredentials(*creds))
	}
	c, err := flightsql.NewClient(f.addr, nil, nil, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func newReadOnlyFixture(t *testing.T, readOnly bool) readOnlyFixture {
	t.Helper()
	ensureTestMetrics()
	cfg := &config.Config{MemoryLimit: "256MB", MaxThreads: 2, QueryTimeout: "20s", PoolSize: 2}
	if readOnly {
		cfg.ReadOnly = &config.ReadOnlyConfig{PoolSize: 2, MemoryLimit: "128MB"}
	}
	srv, err := server.New(cfg)
	require.NoError(t, err)

	mw, err := auth.Middleware(auth.Config{
		StaticTokens:         []string{rwToken},
		ReadOnlyStaticTokens: []string{roToken},
		Users:                map[string]string{"admin": "admin-pass", "analyst": "analyst-pass"},
		ReadOnlyUsers:        []string{"analyst"},
		JWTSecret:            []byte(strings.Repeat("k", 32)),
	})
	require.NoError(t, err)
	fs := flight.NewServerWithMiddleware([]flight.ServerMiddleware{*mw})
	fs.RegisterFlightService(flightsql.NewFlightServer(srv))
	require.NoError(t, fs.Init("localhost:0"))
	go func() { _ = fs.Serve() }()
	t.Cleanup(func() {
		fs.Shutdown()
		_ = srv.Close()
	})
	return readOnlyFixture{addr: fs.Addr().String()}
}

func requireCode(t *testing.T, want codes.Code, err error, msgAndArgs ...any) {
	t.Helper()
	require.Error(t, err, msgAndArgs...)
	require.Equal(t, want, status.Code(err), append([]any{"%v"}, err)...)
}

func sqlInfoReadOnly(ctx context.Context, t *testing.T, c *flightsql.Client) bool {
	t.Helper()
	info, err := c.GetSqlInfo(ctx, []flightsql.SqlInfo{flightsql.SqlInfoFlightSqlServerReadOnly})
	require.NoError(t, err)
	rdr, err := c.DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	defer rdr.Release()
	require.True(t, rdr.Next())
	rec := rdr.RecordBatch()
	require.EqualValues(t, 1, rec.NumRows())
	u := rec.Column(1).(*array.DenseUnion)
	child := u.Field(u.ChildID(0)).(*array.Boolean)
	return child.Value(int(u.ValueOffset(0)))
}

// TestReadOnlySessions drives the read-only boundary end to end through the
// Flight SQL server: credential → identity → read-only plane → statement
// policy, on every RPC that takes SQL.
func TestReadOnlySessions(t *testing.T) {
	f := newReadOnlyFixture(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rw := f.dial(t, &bearer{token: rwToken})
	ro := f.dial(t, &bearer{token: roToken})
	selfRO := f.dial(t, &bearer{token: rwToken, readOnly: "true"})

	// The read-write credential is unaffected: it writes, and reads files.
	_, err := rw.ExecuteUpdate(ctx, "CREATE TABLE rw_only AS SELECT 1 AS i")
	require.NoError(t, err)
	got, err := queryString(ctx, rw, "SELECT count(*) FROM read_text('/proc/self/environ')")
	require.NoError(t, err)
	require.Equal(t, "1", got)

	readOnlyClients := map[string]*flightsql.Client{"read-only token": ro, "self-restricted header": selfRO}
	for name, c := range readOnlyClients {
		t.Run(name, func(t *testing.T) {
			t.Run("reads", func(t *testing.T) {
				for q, want := range map[string]string{
					"SELECT 42":                         "42",
					"FROM range(3) SELECT count(*)":     "3",
					"WITH x AS (SELECT 2 AS a) FROM x":  "2",
					"SELECT 1; SELECT 7":                "7",
					"SELECT current_setting('threads')": "2",
				} {
					got, err := queryString(ctx, c, q)
					require.NoError(t, err, q)
					require.Equal(t, want, got, q)
				}
				for _, q := range []string{"DESCRIBE SELECT 1 AS a", "SHOW TABLES", "EXPLAIN SELECT 1", "SUMMARIZE SELECT 1 AS a"} {
					_, err := queryString(ctx, c, q)
					require.NoError(t, err, q)
				}
			})

			t.Run("separate instance", func(t *testing.T) {
				// Tables of the main in-memory instance are not visible: the
				// read-only plane is a different DuckDB instance (an Iceberg
				// lake is attached to both — see the integration suite).
				_, err := queryString(ctx, c, "SELECT * FROM rw_only")
				require.Error(t, err)
				got, err := queryString(ctx, c, "SELECT current_setting('enable_external_access')::VARCHAR || ',' || current_setting('lock_configuration')::VARCHAR")
				require.NoError(t, err)
				require.Equal(t, "false,true", got)
			})

			t.Run("refused", func(t *testing.T) {
				for _, q := range []string{
					"CREATE TABLE t (i INT)",
					"CREATE TEMP TABLE t AS SELECT 1",
					"INSERT INTO rw_only VALUES (2)",
					"WITH x AS (SELECT 1) INSERT INTO rw_only SELECT * FROM x",
					"DROP TABLE rw_only",
					"COPY (SELECT 1) TO '/tmp/duckflight-ro-leak.csv'",
					"ATTACH ':memory:' AS m",
					"DETACH memory",
					"SET threads = 1",
					"PRAGMA threads = 1",
					"CALL pragma_version()",
					"LOAD json",
					"INSTALL json",
					"BEGIN TRANSACTION",
					"EXPLAIN ANALYZE SELECT 1",
					"SELECT * FROM read_text('/proc/self/environ')",
					"SELECT * FROM '/proc/self/environ'",
					"SELECT getenv('HOME')",
					"SELECT * FROM query('SELECT 1')",
					"SELECT * FROM duckdb_secrets()",
					"SELECT 1; DROP TABLE rw_only",
				} {
					_, err := c.Execute(ctx, q)
					requireCode(t, codes.PermissionDenied, err, "execute %q", q)
					_, err = c.ExecuteUpdate(ctx, q)
					requireCode(t, codes.PermissionDenied, err, "update %q", q)
					_, err = c.Prepare(ctx, q)
					requireCode(t, codes.PermissionDenied, err, "prepare %q", q)
				}
				// A syntax error is the client's, not a policy refusal.
				_, err := c.Execute(ctx, "SELEC 1")
				requireCode(t, codes.InvalidArgument, err)
				// The schema probe is checked as the SQL it really runs.
				_, err = c.GetExecuteSchema(ctx, "SELECT 1) AS t; CREATE TABLE x (i INT); SELECT * FROM (SELECT 1")
				require.Error(t, err)
				_, err = c.GetExecuteSchema(ctx, "SELECT 1 AS a")
				require.NoError(t, err)
			})

			t.Run("prepared with parameters", func(t *testing.T) {
				st, err := c.Prepare(ctx, "SELECT ?::INT + 1")
				require.NoError(t, err)
				defer func() { _ = st.Close(ctx) }()
				params := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(
					[]arrow.Field{{Name: "p", Type: arrow.PrimitiveTypes.Int64}}, nil))
				defer params.Release()
				params.Field(0).(*array.Int64Builder).Append(41)
				rec := params.NewRecordBatch()
				defer rec.Release()
				st.SetParameters(rec)
				info, err := st.Execute(ctx)
				require.NoError(t, err)
				got, err := drainString(ctx, c, info)
				require.NoError(t, err)
				require.Equal(t, "42", got)
			})

			t.Run("ingest", func(t *testing.T) {
				b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(
					[]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil))
				defer b.Release()
				b.Field(0).(*array.Int64Builder).Append(1)
				rec := b.NewRecordBatch()
				defer rec.Release()
				rdr, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
				require.NoError(t, err)
				defer rdr.Release()
				_, err = c.ExecuteIngest(ctx, rdr, &flightsql.ExecuteIngestOpts{
					Table: "ingested",
					TableDefinitionOptions: &flightsql.TableDefinitionOptions{
						IfNotExist: flightsql.TableDefinitionOptionsTableNotExistOptionCreate,
						IfExists:   flightsql.TableDefinitionOptionsTableExistsOptionAppend,
					},
				})
				requireCode(t, codes.PermissionDenied, err)
			})

			t.Run("sql info", func(t *testing.T) {
				require.True(t, sqlInfoReadOnly(ctx, t, c))
			})

			t.Run("flight transaction", func(t *testing.T) {
				tx, err := c.BeginTransaction(ctx)
				require.NoError(t, err)
				info, err := tx.Execute(ctx, "SELECT 5")
				require.NoError(t, err)
				got, err := drainString(ctx, c, info)
				require.NoError(t, err)
				require.Equal(t, "5", got)
				_, err = tx.ExecuteUpdate(ctx, "CREATE TABLE in_txn (i INT)")
				requireCode(t, codes.PermissionDenied, err)
				require.NoError(t, tx.Commit(ctx))
			})
		})
	}

	t.Run("read-write sql info", func(t *testing.T) {
		require.False(t, sqlInfoReadOnly(ctx, t, rw))
	})

	t.Run("header cannot cross into a read-write transaction", func(t *testing.T) {
		tx, err := rw.BeginTransaction(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		// Same token, same transaction handle, but the request asks to be
		// read-only: it must not reach the read-write connection.
		hdr := metadata.AppendToOutgoingContext(ctx, auth.ReadOnlyHeader, "true")
		_, err = tx.Execute(hdr, "SELECT 1")
		require.NoError(t, err, "GetFlightInfo only registers the statement")
		_, err = tx.ExecuteUpdate(hdr, "SELECT 1")
		requireCode(t, codes.PermissionDenied, err)
	})

	t.Run("header values", func(t *testing.T) {
		for v, want := range map[string]codes.Code{"1": codes.PermissionDenied, "TRUE": codes.PermissionDenied, "false": codes.OK, "0": codes.OK, "yes": codes.InvalidArgument} {
			c := f.dial(t, &bearer{token: rwToken, readOnly: v})
			_, err := c.ExecuteUpdate(ctx, "CREATE TABLE IF NOT EXISTS hdr (i INT)")
			require.Equal(t, want, status.Code(err), "header %q: %v", v, err)
		}
	})

	t.Run("handshake users", func(t *testing.T) {
		c := f.dial(t, nil)
		analystCtx, err := c.Client.AuthenticateBasicToken(ctx, "analyst", "analyst-pass")
		require.NoError(t, err)
		_, err = c.ExecuteUpdate(analystCtx, "CREATE TABLE by_analyst (i INT)")
		requireCode(t, codes.PermissionDenied, err)
		got, err := queryString(analystCtx, c, "SELECT 9")
		require.NoError(t, err)
		require.Equal(t, "9", got)

		adminCtx, err := c.Client.AuthenticateBasicToken(ctx, "admin", "admin-pass")
		require.NoError(t, err)
		_, err = c.ExecuteUpdate(adminCtx, "CREATE TABLE by_admin (i INT)")
		require.NoError(t, err)
	})
}

// TestReadOnlyRequestWithoutReadOnlyEngine pins the fail-closed answer when a
// caller asks for read-only on a server that has no read-only instance: the
// request is refused, never quietly served read-write.
func TestReadOnlyRequestWithoutReadOnlyEngine(t *testing.T) {
	f := newReadOnlyFixture(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, c := range []*flightsql.Client{
		f.dial(t, &bearer{token: roToken}),
		f.dial(t, &bearer{token: rwToken, readOnly: "true"}),
	} {
		_, err := queryString(ctx, c, "SELECT 1")
		requireCode(t, codes.FailedPrecondition, err)
		_, err = c.ExecuteUpdate(ctx, "CREATE TABLE x (i INT)")
		requireCode(t, codes.FailedPrecondition, err)
		_, err = c.BeginTransaction(ctx)
		requireCode(t, codes.FailedPrecondition, err)
	}

	// Default behavior for the read-write credential is unchanged.
	rw := f.dial(t, &bearer{token: rwToken})
	_, err := rw.ExecuteUpdate(ctx, "CREATE TABLE x (i INT)")
	require.NoError(t, err)
	require.False(t, sqlInfoReadOnly(ctx, t, rw))
}
