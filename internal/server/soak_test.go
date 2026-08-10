//go:build duckdb_arrow

package server_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fairtier/duckflight/internal/config"
	"github.com/fairtier/duckflight/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// soakEnv creates an isolated server/client pair for soak tests.
type soakEnv struct {
	srv    *server.DuckFlightSQLServer
	fs     flight.Server
	client *flightsql.Client
	mem    *memory.CheckedAllocator
}

func newSoakEnv(t *testing.T) *soakEnv {
	t.Helper()
	ensureTestMetrics()

	srv, err := server.New(&config.Config{
		MemoryLimit:  "256MB",
		MaxThreads:   2,
		QueryTimeout: "30s",
		PoolSize:     4,
	})
	require.NoError(t, err)

	fs := flight.NewServerWithMiddleware(nil)
	fs.RegisterFlightService(flightsql.NewFlightServer(srv))
	require.NoError(t, fs.Init("localhost:0"))
	go func() { _ = fs.Serve() }()

	cl, err := flightsql.NewClient(
		fs.Addr().String(),
		nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	cl.Alloc = mem
	srv.Alloc = mem

	return &soakEnv{srv: srv, fs: fs, client: cl, mem: mem}
}

func (e *soakEnv) close(t *testing.T) {
	t.Helper()
	_ = e.client.Close()
	e.fs.Shutdown()
	e.srv.Alloc = memory.DefaultAllocator
	// Closing the server is what releases the DuckDB engine; a Flight shutdown
	// alone leaves it, its pooled connections and its reapers running for the
	// rest of the package.
	_ = e.srv.Close()
	e.mem.AssertSize(t, 0)
}

func (e *soakEnv) assertNoLeaks(t *testing.T) {
	t.Helper()
	require.Equal(t, 0, e.srv.OpenTransactionCount(), "leaked transactions")
	require.Equal(t, 0, e.srv.PreparedStatementCount(), "leaked prepared statements")
	require.Equal(t, 0, e.srv.ActiveQueryCount(), "leaked active queries")
	pool := e.srv.Engine().Pool
	require.Equal(t, pool.Cap(), pool.Len(), "pool connections not returned")
}

// rssKB reads VmRSS from /proc/self/status. Returns 0 on non-Linux.
func rssKB() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				v, _ := strconv.ParseInt(fields[1], 10, 64)
				return v
			}
		}
	}
	return 0
}

// soakChildEnv marks the re-executed child process that actually runs the
// memory soak. See [runSoakInChild].
const soakChildEnv = "DUCKFLIGHT_SOAK_CHILD"

// runSoakInChild re-executes this test binary so that the named test runs
// alone in a fresh process, and fails t with the child's output if it did not
// pass. The child inherits the binary it is spawned from, so it runs with the
// same build tags and the same -race setting as the parent.
func runSoakInChild(t *testing.T, name string) {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), os.Args[0],
		"-test.run=^"+name+"$", "-test.v", "-test.count=1", "-test.timeout=5m")
	cmd.Env = append(os.Environ(), soakChildEnv+"=1")

	out, err := cmd.CombinedOutput()
	t.Logf("soak child output:\n%s", out)
	require.NoError(t, err, "soak child process failed")
}

// TestSoakQueryExecution checks that a long run of query/DoGet cycles neither
// grows the Go heap nor leaks native memory underneath it — the latter being
// the point, since a leaked DuckDB result is invisible to runtime.MemStats.
//
// Both numbers are process-global, which makes them meaningless inside the
// shared test binary: by the time this test runs, a minute and a half of
// earlier suites has left the heap holding hundreds of megabytes that the
// scavenger returns on its own schedule, and under -race the detector maps
// shadow memory for every newly touched address and never unmaps it. Measured
// in-package, this same unchanged loop swings between -145MB and +133MB of RSS
// — noise two orders of magnitude above the ~3MB the loop actually costs, in
// both directions, so no threshold can separate them. It also makes the heap
// bound vacuous: 3x a 245MB warmup reading asserts nothing.
//
// So the loop runs in a process of its own, where a 250MB baseline becomes a
// 4MB one and the growth figures are repeatable to within a megabyte.
func TestSoakQueryExecution(t *testing.T) {
	if os.Getenv(soakChildEnv) == "" {
		runSoakInChild(t, "TestSoakQueryExecution")
		return
	}

	env := newSoakEnv(t)
	defer env.close(t)

	ctx := context.Background()
	const iterations = 500
	const warmup = 50

	var warmupHeap uint64
	var warmupRSS int64

	for i := 0; i < iterations; i++ {
		info, err := env.client.Execute(ctx, fmt.Sprintf("SELECT %d AS val, 'hello' AS msg", i))
		require.NoError(t, err)

		rdr, err := env.client.DoGet(ctx, info.Endpoint[0].Ticket)
		require.NoError(t, err)

		for rdr.Next() {
			rec := rdr.RecordBatch()
			rec.Retain()
			rec.Release()
		}
		require.NoError(t, rdr.Err())
		rdr.Release()

		env.assertNoLeaks(t)

		if i == warmup {
			runtime.GC()
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			warmupHeap = m.HeapInuse
			warmupRSS = rssKB()
		}
	}

	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	finalHeap := m.HeapInuse
	finalRSS := rssKB()

	t.Logf("heap: warmup=%dKB final=%dKB", warmupHeap/1024, finalHeap/1024)
	require.LessOrEqual(t, finalHeap, 3*warmupHeap, "heap grew more than 3x after warmup")

	if warmupRSS > 0 && finalRSS > 0 {
		growthKB := finalRSS - warmupRSS
		t.Logf("RSS: warmup=%dKB final=%dKB growth=%dKB", warmupRSS, finalRSS, growthKB)
		// Alone in a process the 450 measured iterations cost ~3MB of RSS, or
		// ~15MB under -race, where the detector maps shadow memory for newly
		// touched addresses and never gives it back. 50MB leaves room for that
		// without leaving room for a per-query native leak, which at this
		// iteration count would run to hundreds of megabytes.
		require.LessOrEqual(t, growthKB, int64(50*1024), "RSS grew more than 50MB")
	}
}

func TestSoakMetadata(t *testing.T) {
	env := newSoakEnv(t)
	defer env.close(t)

	ctx := context.Background()

	// Seed a table for metadata queries.
	require.NoError(t, server.SeedSQL(ctx, "CREATE TABLE IF NOT EXISTS soak_meta (id INTEGER PRIMARY KEY, name VARCHAR)"))
	defer func() { _ = server.SeedSQL(ctx, "DROP TABLE IF EXISTS soak_meta") }()

	const iterations = 200

	for i := 0; i < iterations; i++ {
		// GetCatalogs
		info, err := env.client.GetCatalogs(ctx)
		require.NoError(t, err)
		drainFlightInfo(t, env.client, info)

		// GetDBSchemas
		info, err = env.client.GetDBSchemas(ctx, &flightsql.GetDBSchemasOpts{})
		require.NoError(t, err)
		drainFlightInfo(t, env.client, info)

		// GetTables
		info, err = env.client.GetTables(ctx, &flightsql.GetTablesOpts{})
		require.NoError(t, err)
		drainFlightInfo(t, env.client, info)

		// GetTableTypes
		info, err = env.client.GetTableTypes(ctx)
		require.NoError(t, err)
		drainFlightInfo(t, env.client, info)

		// GetXdbcTypeInfo
		info, err = env.client.GetXdbcTypeInfo(ctx, nil)
		require.NoError(t, err)
		drainFlightInfo(t, env.client, info)

		// GetPrimaryKeys
		info, err = env.client.GetPrimaryKeys(ctx, flightsql.TableRef{Table: "soak_meta"})
		require.NoError(t, err)
		drainFlightInfo(t, env.client, info)

		env.assertNoLeaks(t)
	}
}

func TestSoakPreparedStatements(t *testing.T) {
	env := newSoakEnv(t)
	defer env.close(t)

	ctx := context.Background()
	const iterations = 200

	for i := 0; i < iterations; i++ {
		prep, err := env.client.Prepare(ctx, fmt.Sprintf("SELECT %d AS val", i))
		require.NoError(t, err)

		info, err := prep.Execute(ctx)
		require.NoError(t, err)

		rdr, err := env.client.DoGet(ctx, info.Endpoint[0].Ticket)
		require.NoError(t, err)

		for rdr.Next() {
			rec := rdr.RecordBatch()
			rec.Retain()
			rec.Release()
		}
		require.NoError(t, rdr.Err())
		rdr.Release()

		require.NoError(t, prep.Close(ctx))
		env.assertNoLeaks(t)
	}
}

func TestSoakTransactions(t *testing.T) {
	env := newSoakEnv(t)
	defer env.close(t)

	ctx := context.Background()
	const iterations = 200

	require.NoError(t, server.SeedSQL(ctx, "CREATE TABLE IF NOT EXISTS soak_txn (id INTEGER, val VARCHAR)"))
	defer func() { _ = server.SeedSQL(ctx, "DROP TABLE IF EXISTS soak_txn") }()

	for i := 0; i < iterations; i++ {
		tx, err := env.client.BeginTransaction(ctx)
		require.NoError(t, err)

		_, err = tx.ExecuteUpdate(ctx, fmt.Sprintf("INSERT INTO soak_txn VALUES (%d, 'iter_%d')", i, i))
		require.NoError(t, err)

		require.NoError(t, tx.Commit(ctx))
		env.assertNoLeaks(t)
	}
}

// drainFlightInfo fetches and drains all endpoints from a FlightInfo.
func drainFlightInfo(t *testing.T, client *flightsql.Client, info *flight.FlightInfo) {
	t.Helper()
	ctx := context.Background()
	for _, ep := range info.Endpoint {
		rdr, err := client.DoGet(ctx, ep.Ticket)
		require.NoError(t, err)
		for rdr.Next() {
		}
		require.NoError(t, rdr.Err())
		rdr.Release()
	}
}
