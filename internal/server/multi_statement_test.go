//go:build duckdb_arrow

package server_test

import (
	"context"
	"strings"
	"testing"

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
	"google.golang.org/grpc/status"
)

type staticBearer struct{}

func (staticBearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer multi-statement-test"}, nil
}

func (staticBearer) RequireTransportSecurity() bool { return false }

// newMultiStatementClients serves a real server over a socket and returns the
// client under test plus an independent one for verification. With sessions,
// every client gets a pinned connection; without, each RPC borrows from the
// pool.
func newMultiStatementClients(t *testing.T, sessions bool, poolSize int, configure ...func(*config.Config)) (client, verify *flightsql.Client) {
	t.Helper()
	ensureTestMetrics()

	cfg := &config.Config{MemoryLimit: "256MB", MaxThreads: 2, QueryTimeout: "20s", PoolSize: poolSize}
	for _, fn := range configure {
		fn(cfg)
	}
	srv, err := server.New(cfg)
	require.NoError(t, err)

	var middlewares []flight.ServerMiddleware
	if sessions {
		mw, err := auth.Middleware(auth.Config{StaticTokens: []string{"multi-statement-test"}})
		require.NoError(t, err)
		middlewares = append(middlewares, *mw)
	}
	fs := flight.NewServerWithMiddleware(middlewares)
	fs.RegisterFlightService(flightsql.NewFlightServer(srv))
	require.NoError(t, fs.Init("localhost:0"))
	go func() { _ = fs.Serve() }()

	dial := func() *flightsql.Client {
		opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
		if sessions {
			opts = append(opts, grpc.WithPerRPCCredentials(staticBearer{}))
		}
		c, err := flightsql.NewClient(fs.Addr().String(), nil, nil, opts...)
		require.NoError(t, err)
		return c
	}
	client, verify = dial(), dial()
	t.Cleanup(func() {
		_ = client.Close()
		_ = verify.Close()
		fs.Shutdown()
		_ = srv.Close()
	})
	return client, verify
}

// queryString runs sql to completion and renders the rows as "a,b;c,d".
func queryString(ctx context.Context, c *flightsql.Client, sql string) (string, error) {
	info, err := c.Execute(ctx, sql)
	if err != nil {
		return "", err
	}
	return drainString(ctx, c, info)
}

func drainString(ctx context.Context, c *flightsql.Client, info *flight.FlightInfo) (string, error) {
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

// TestMultiStatementRunsOnce is the regression test for multi-statement SQL
// running its leading statements twice. The classification every statement goes
// through used a prepare that executes all but the last statement, and then the
// statement ran for real, so `INSERT; INSERT` left three rows.
func TestMultiStatementRunsOnce(t *testing.T) {
	ctx := context.Background()

	const (
		insIns    = "INSERT INTO m VALUES (1); INSERT INTO m VALUES (2)"
		insInsIns = "INSERT INTO m VALUES (1); INSERT INTO m VALUES (2); INSERT INTO m VALUES (3)"
		insSelect = "INSERT INTO m VALUES (1); SELECT 1"
	)
	update := func(sql string) func(*flightsql.Client) error {
		return func(c *flightsql.Client) error {
			_, err := c.ExecuteUpdate(ctx, sql)
			return err
		}
	}
	doGet := func(sql string) func(*flightsql.Client) error {
		return func(c *flightsql.Client) error {
			_, err := queryString(ctx, c, sql)
			return err
		}
	}
	cases := []struct {
		name string
		run  func(*flightsql.Client) error
		want string
	}{
		{"update two", update(insIns), "2"},
		{"update three", update(insInsIns), "3"},
		{"doget insert then select", doGet(insSelect), "1"},
		{"doget two", doGet(insIns), "2"},
		{"prepared update", func(c *flightsql.Client) error {
			st, err := c.Prepare(ctx, insIns)
			if err != nil {
				return err
			}
			defer func() { _ = st.Close(ctx) }()
			_, err = st.ExecuteUpdate(ctx)
			return err
		}, "2"},
		{"prepared query", func(c *flightsql.Client) error {
			st, err := c.Prepare(ctx, insSelect)
			if err != nil {
				return err
			}
			defer func() { _ = st.Close(ctx) }()
			info, err := st.Execute(ctx)
			if err != nil {
				return err
			}
			_, err = drainString(ctx, c, info)
			return err
		}, "1"},
		{"prepare without executing", func(c *flightsql.Client) error {
			st, err := c.Prepare(ctx, insIns)
			if err != nil {
				return err
			}
			return st.Close(ctx)
		}, "0"},
		{"schema without executing", func(c *flightsql.Client) error {
			_, _ = c.GetExecuteSchema(ctx, insSelect) // has no schema; only the row count matters
			return nil
		}, "0"},
		{"flight transaction update", func(c *flightsql.Client) error {
			tx, err := c.BeginTransaction(ctx)
			if err != nil {
				return err
			}
			if _, err := tx.ExecuteUpdate(ctx, insIns); err != nil {
				return err
			}
			return tx.Commit(ctx)
		}, "2"},
		{"flight transaction doget", func(c *flightsql.Client) error {
			tx, err := c.BeginTransaction(ctx)
			if err != nil {
				return err
			}
			info, err := tx.Execute(ctx, insSelect)
			if err != nil {
				return err
			}
			if _, err := drainString(ctx, c, info); err != nil {
				return err
			}
			return tx.Commit(ctx)
		}, "1"},
		{"flight transaction prepared", func(c *flightsql.Client) error {
			tx, err := c.BeginTransaction(ctx)
			if err != nil {
				return err
			}
			st, err := tx.Prepare(ctx, insIns)
			if err != nil {
				return err
			}
			defer func() { _ = st.Close(ctx) }()
			if _, err := st.ExecuteUpdate(ctx); err != nil {
				return err
			}
			return tx.Commit(ctx)
		}, "2"},
		// A script carrying its own transaction control must take effect as
		// written. It used to return OK with the transaction never committed.
		{"script commits, update", update("BEGIN TRANSACTION; INSERT INTO m VALUES (1); COMMIT"), "1"},
		{"script commits, doget", doGet("START TRANSACTION; INSERT INTO m VALUES (1); COMMIT"), "1"},
		{"script spelled with bare BEGIN", update("BEGIN; INSERT INTO m VALUES (1); COMMIT"), "1"},
		{"script rolls back", update("BEGIN; INSERT INTO m VALUES (1); ROLLBACK"), "0"},
	}

	for _, sessions := range []bool{false, true} {
		mode := "pool"
		if sessions {
			mode = "session"
		}
		client, verify := newMultiStatementClients(t, sessions, 4)
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				_, err := verify.ExecuteUpdate(ctx, "CREATE OR REPLACE TABLE m (i INT)")
				require.NoError(t, err)

				require.NoError(t, tc.run(client))

				got, err := queryString(ctx, verify, "SELECT count(*) FROM m")
				require.NoError(t, err)
				require.Equal(t, tc.want, got, "rows visible to another client")
			})
		}
	}
}

// TestMultiStatementValues covers what a row count can't: an UPDATE or a
// sequence that ran twice leaves the same number of rows.
func TestMultiStatementValues(t *testing.T) {
	ctx := context.Background()
	for mode, sessions := range map[string]bool{"pool": false, "session": true} {
		t.Run(mode, func(t *testing.T) {
			client, verify := newMultiStatementClients(t, sessions, 4)

			_, err := verify.ExecuteUpdate(ctx, "CREATE TABLE v (i INT)")
			require.NoError(t, err)
			_, err = verify.ExecuteUpdate(ctx, "INSERT INTO v VALUES (10), (20)")
			require.NoError(t, err)
			_, err = client.ExecuteUpdate(ctx, "UPDATE v SET i = i * 2; DELETE FROM v WHERE i < 0")
			require.NoError(t, err)
			got, err := queryString(ctx, verify, "SELECT sum(i) FROM v")
			require.NoError(t, err)
			require.Equal(t, "60", got)

			_, err = verify.ExecuteUpdate(ctx, "CREATE SEQUENCE s")
			require.NoError(t, err)
			_, err = queryString(ctx, client, "SELECT nextval('s'); SELECT 1")
			require.NoError(t, err)
			got, err = queryString(ctx, verify, "SELECT nextval('s')")
			require.NoError(t, err)
			require.Equal(t, "2", got)

			// The later statements depend on the earlier ones, so classifying this by
			// executing a prefix made the real run fail with "already exists".
			got, err = queryString(ctx, client, "CREATE TABLE d (i INT); INSERT INTO d VALUES (7); SELECT * FROM d")
			require.NoError(t, err)
			require.Equal(t, "7", got)
		})
	}
}

// TestMultiStatementBoundParameters: only the last statement can take
// parameters, and the whole script runs once per parameter row, so its leading
// statements do too.
func TestMultiStatementBoundParameters(t *testing.T) {
	ctx := context.Background()
	client, verify := newMultiStatementClients(t, false, 4)
	_, err := verify.ExecuteUpdate(ctx, "CREATE TABLE m (i INT)")
	require.NoError(t, err)

	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]int64{7, 8, 9}, nil)
	values := builder.NewInt64Array()
	defer values.Release()
	params := array.NewRecordBatch(
		arrow.NewSchema([]arrow.Field{{Name: "p", Type: arrow.PrimitiveTypes.Int64}}, nil),
		[]arrow.Array{values}, 3)
	defer params.Release()

	st, err := client.Prepare(ctx, "INSERT INTO m VALUES (1); INSERT INTO m VALUES (?)")
	require.NoError(t, err)
	defer func() { _ = st.Close(ctx) }()
	st.SetParameters(params)
	_, err = st.ExecuteUpdate(ctx)
	require.NoError(t, err)

	got, err := queryString(ctx, verify, "SELECT i FROM m ORDER BY i")
	require.NoError(t, err)
	require.Equal(t, "1;1;1;7;8;9", got)
}

// TestMultiStatementIsNotAtomic: in autocommit each statement stands alone, so a
// failure midway keeps what ran before it, and nothing after it runs.
func TestMultiStatementIsNotAtomic(t *testing.T) {
	ctx := context.Background()
	client, verify := newMultiStatementClients(t, false, 4)
	_, err := verify.ExecuteUpdate(ctx, "CREATE TABLE m (i INT)")
	require.NoError(t, err)

	_, err = client.ExecuteUpdate(ctx,
		"INSERT INTO m VALUES (1); INSERT INTO no_such_table VALUES (2); INSERT INTO m VALUES (3)")
	require.Error(t, err)

	got, err := queryString(ctx, verify, "SELECT i FROM m ORDER BY i")
	require.NoError(t, err)
	require.Equal(t, "1", got)
}

// TestMultiStatementLeavesTransactionOpenOnSession: a session keeps its
// connection, so a script that doesn't end its transaction leaves it open for
// that client's next statement, as the same statements sent one by one would.
func TestMultiStatementLeavesTransactionOpenOnSession(t *testing.T) {
	ctx := context.Background()
	client, verify := newMultiStatementClients(t, true, 4)
	_, err := verify.ExecuteUpdate(ctx, "CREATE TABLE m (i INT)")
	require.NoError(t, err)

	_, err = queryString(ctx, client, "BEGIN; INSERT INTO m VALUES (1); SELECT 1")
	require.NoError(t, err)

	const count = "SELECT count(*) FROM m"
	got, err := queryString(ctx, client, count)
	require.NoError(t, err)
	require.Equal(t, "1", got, "the script's own session sees its write")

	// A token alone doesn't tell these two clients apart unless their peers
	// differ, which two dials do.
	got, err = queryString(ctx, verify, count)
	require.NoError(t, err)
	require.Equal(t, "0", got, "another session must not")

	_, err = client.ExecuteUpdate(ctx, "ROLLBACK")
	require.NoError(t, err)
	got, err = queryString(ctx, client, count)
	require.NoError(t, err)
	require.Equal(t, "0", got)
}

// TestMultiStatementBeginInOpenTransaction: a script's BEGIN isn't skipped the
// way a lone redundant one is, so it fails and aborts the open transaction.
func TestMultiStatementBeginInOpenTransaction(t *testing.T) {
	ctx := context.Background()
	client, verify := newMultiStatementClients(t, false, 4)
	_, err := verify.ExecuteUpdate(ctx, "CREATE TABLE m (i INT)")
	require.NoError(t, err)

	tx, err := client.BeginTransaction(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.ExecuteUpdate(ctx, "INSERT INTO m VALUES (1)")
	require.NoError(t, err)

	_, err = tx.ExecuteUpdate(ctx, "BEGIN; INSERT INTO m VALUES (2); COMMIT")
	require.Error(t, err)
	_, err = tx.ExecuteUpdate(ctx, "INSERT INTO m VALUES (3)")
	require.ErrorContains(t, err, "aborted")
}

// TestEmptyStatement: SQL with no statement in it can't be typed, which reads as
// multi-statement. It must fail, not reach anything, and only cost a connection.
func TestEmptyStatement(t *testing.T) {
	ctx := context.Background()
	client, _ := newMultiStatementClients(t, false, 1)

	for _, sql := range []string{"", "   ", ";", "-- nothing"} {
		_, err := client.ExecuteUpdate(ctx, sql)
		require.Error(t, err, "update %q", sql)
		_, err = queryString(ctx, client, sql)
		require.Error(t, err, "doget %q", sql)

		got, err := queryString(ctx, client, "SELECT 1")
		require.NoError(t, err, "after %q", sql)
		require.Equal(t, "1", got)
	}
}

// TestRejectClientExtensionsCoverage pins what the guard does today. The first
// half is the guarantee. The second half documents a known open gap, not desired
// behavior: scripts pass because classification only reaches a single statement.
// If the maintainer closes the gap, this test is meant to change.
func TestRejectClientExtensionsCoverage(t *testing.T) {
	ctx := context.Background()
	client, verify := newMultiStatementClients(t, false, 4, func(cfg *config.Config) {
		cfg.RejectClientExtensions = true
	})
	_, err := verify.ExecuteUpdate(ctx, "CREATE TABLE m (i INT)")
	require.NoError(t, err)

	for _, sql := range []string{
		"LOAD json", "LOAD json;", "LOAD json; -- c", "/* c */ LOAD json", "load json", "INSTALL json", "INSTALL json;",
	} {
		_, err := client.ExecuteUpdate(ctx, sql)
		require.Equal(t, codes.PermissionDenied, status.Code(err), "update %q: %v", sql, err)
		_, err = queryString(ctx, client, sql)
		require.Equal(t, codes.PermissionDenied, status.Code(err), "doget %q: %v", sql, err)
	}

	// The known gap.
	for _, sql := range []string{
		"LOAD json; SELECT 1",
		"SELECT 1; LOAD json",
		"INSERT INTO m VALUES (1); LOAD json",
		"LOAD json; LOAD json",
		"SELECT 1; INSTALL json",
	} {
		_, err := client.ExecuteUpdate(ctx, sql)
		require.NoError(t, err, "update %q", sql)
	}
}

// TestMultiStatementDoesNotDirtyPool: only the last statement used to be
// classified, so `SET x; SELECT 1` looked like a SELECT and its connection went
// back to the pool carrying x. A pool of one makes the next borrower get that
// same connection if it was reused.
func TestMultiStatementDoesNotDirtyPool(t *testing.T) {
	ctx := context.Background()
	client, verify := newMultiStatementClients(t, false, 1)

	_, err := verify.ExecuteUpdate(ctx, "CREATE TABLE shared (i INT)")
	require.NoError(t, err)

	_, err = queryString(ctx, client, "SET VARIABLE leak = 42; SELECT 1")
	require.NoError(t, err)
	got, err := queryString(ctx, verify, "SELECT getvariable('leak')")
	require.NoError(t, err)
	require.Equal(t, "(null)", got, "variable leaked to the next borrower")

	_, err = queryString(ctx, client, "CREATE TEMP TABLE leak_tmp (i INT); SELECT 1")
	require.NoError(t, err)
	_, err = queryString(ctx, verify, "SELECT count(*) FROM leak_tmp")
	require.Error(t, err, "temp table leaked to the next borrower")

	// A transaction left open by a script dies with its connection rather than
	// trapping the next borrower inside it.
	_, err = queryString(ctx, client, "BEGIN; INSERT INTO shared VALUES (1); SELECT 1")
	require.NoError(t, err)
	got, err = queryString(ctx, verify, "SELECT count(*) FROM shared")
	require.NoError(t, err)
	require.Equal(t, "0", got)
	_, err = verify.ExecuteUpdate(ctx, "INSERT INTO shared VALUES (2)")
	require.NoError(t, err)
	got, err = queryString(ctx, verify, "SELECT count(*) FROM shared")
	require.NoError(t, err)
	require.Equal(t, "1", got)
}
