//go:build duckdb_arrow

package server_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prochac/duckflight/internal/server"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ---------------------------------------------------------------------------
// loadConfig — all knobs, configurable via environment variables
// ---------------------------------------------------------------------------

// loadConfig controls load test intensity. All values have conservative
// defaults suitable for CI. Set LOAD_HEAVY=1 for a preset that exercises
// the server harder, or override individual values.
//
// Environment variables:
//
//	LOAD_HEAVY=1             — preset: longer durations, more clients
//	LOAD_POOL_SIZE           — DuckDB connection pool size (default 4)
//	LOAD_MAX_CLIENTS         — maximum concurrent Flight SQL clients (default 16, heavy: 64)
//	LOAD_STAGE_DURATION      — time.Duration per ramp-up stage (default 3s, heavy: 10s)
//	LOAD_ITERATIONS          — iterations per goroutine in fixed tests (default 100, heavy: 2000)
//	LOAD_SUSTAINED_DURATION  — total duration for sustained test (default 10s, heavy: 60s)
type loadConfig struct {
	PoolSize          int
	MaxClients        int
	StageDuration     time.Duration
	Iterations        int
	SustainedDuration time.Duration
}

func defaultLoadConfig() loadConfig {
	cfg := loadConfig{
		PoolSize:          4,
		MaxClients:        16,
		StageDuration:     3 * time.Second,
		Iterations:        100,
		SustainedDuration: 10 * time.Second,
	}
	if os.Getenv("LOAD_HEAVY") == "1" {
		cfg.MaxClients = 64
		cfg.StageDuration = 10 * time.Second
		cfg.Iterations = 2000
		cfg.SustainedDuration = 60 * time.Second
	}
	if v := envInt("LOAD_POOL_SIZE"); v > 0 {
		cfg.PoolSize = v
	}
	if v := envInt("LOAD_MAX_CLIENTS"); v > 0 {
		cfg.MaxClients = v
	}
	if v := envDuration("LOAD_STAGE_DURATION"); v > 0 {
		cfg.StageDuration = v
	}
	if v := envInt("LOAD_ITERATIONS"); v > 0 {
		cfg.Iterations = v
	}
	if v := envDuration("LOAD_SUSTAINED_DURATION"); v > 0 {
		cfg.SustainedDuration = v
	}
	return cfg
}

func envInt(key string) int {
	s := os.Getenv(key)
	if s == "" {
		return 0
	}
	v, _ := strconv.Atoi(s)
	return v
}

func envDuration(key string) time.Duration {
	s := os.Getenv(key)
	if s == "" {
		return 0
	}
	d, _ := time.ParseDuration(s)
	return d
}

// ---------------------------------------------------------------------------
// loadEnv — isolated server with multiple clients for concurrent testing
// ---------------------------------------------------------------------------

type loadEnv struct {
	srv     *server.DuckFlightSQLServer
	fs      flight.Server
	clients []*flightsql.Client
}

func newLoadEnv(t testing.TB, poolSize, numClients int) *loadEnv {
	t.Helper()
	ensureTestMetrics()

	srv, err := server.New(server.Config{
		MemoryLimit:  "512MB",
		MaxThreads:   4,
		QueryTimeout: "120s",
		PoolSize:     poolSize,
	})
	require.NoError(t, err)

	fs := flight.NewServerWithMiddleware(nil)
	fs.RegisterFlightService(flightsql.NewFlightServer(srv))
	require.NoError(t, fs.Init("localhost:0"))
	go func() { _ = fs.Serve() }()

	// Use DefaultAllocator — CheckedAllocator is too slow for concurrent use.
	srv.Alloc = memory.DefaultAllocator

	clients := make([]*flightsql.Client, numClients)
	for i := range clients {
		cl, err := flightsql.NewClient(
			fs.Addr().String(),
			nil, nil,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		cl.Alloc = memory.DefaultAllocator
		clients[i] = cl
	}

	return &loadEnv{srv: srv, fs: fs, clients: clients}
}

func (e *loadEnv) close(t testing.TB) {
	t.Helper()
	for _, cl := range e.clients {
		_ = cl.Close()
	}
	e.fs.Shutdown()
}

func (e *loadEnv) assertNoLeaks(t testing.TB) {
	t.Helper()
	require.Equal(t, 0, e.srv.OpenTransactionCount(), "leaked transactions")
	require.Equal(t, 0, e.srv.PreparedStatementCount(), "leaked prepared statements")
	require.Equal(t, 0, e.srv.ActiveQueryCount(), "leaked active queries")
	pool := e.srv.Engine().Pool
	require.Equal(t, pool.Cap(), pool.Len(), "pool connections not returned")
}

// waitClean waits for in-flight server operations to drain after context
// cancellation. Duration-based tests interrupt mid-flight work, so the server
// needs a moment to clean up. Returns true if clean, false if timed out.
func (e *loadEnv) waitClean(t testing.TB, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pool := e.srv.Engine().Pool
		if e.srv.OpenTransactionCount() == 0 &&
			e.srv.PreparedStatementCount() == 0 &&
			e.srv.ActiveQueryCount() == 0 &&
			pool.Cap() == pool.Len() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Report what's still lingering — don't fail, this is expected for
	// duration-based tests that cancel in-flight operations.
	pool := e.srv.Engine().Pool
	t.Logf("post-cleanup state: txns=%d prepared=%d active_queries=%d pool=%d/%d",
		e.srv.OpenTransactionCount(), e.srv.PreparedStatementCount(),
		e.srv.ActiveQueryCount(), pool.Len(), pool.Cap())
}

// ---------------------------------------------------------------------------
// latencyCollector — thread-safe duration collector with percentile reporting
// ---------------------------------------------------------------------------

type latencyCollector struct {
	mu      sync.Mutex
	samples []time.Duration
	errors  atomic.Int64
}

func (lc *latencyCollector) Record(d time.Duration) {
	lc.mu.Lock()
	lc.samples = append(lc.samples, d)
	lc.mu.Unlock()
}

func (lc *latencyCollector) RecordError() {
	lc.errors.Add(1)
}

func (lc *latencyCollector) Snapshot() (sorted []time.Duration, errors int64) {
	lc.mu.Lock()
	s := make([]time.Duration, len(lc.samples))
	copy(s, lc.samples)
	lc.mu.Unlock()
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s, lc.errors.Load()
}

func (lc *latencyCollector) Report(t testing.TB, label string) {
	t.Helper()
	s, errs := lc.Snapshot()
	total := int64(len(s)) + errs
	if len(s) == 0 {
		t.Logf("%s: no successful samples (%d errors)", label, errs)
		return
	}
	t.Logf("%s: n=%d errors=%d (%.1f%%) p50=%v p95=%v p99=%v",
		label, total, errs, float64(errs)/float64(total)*100,
		percentile(s, 50), percentile(s, 95), percentile(s, 99))
}

func percentile(sorted []time.Duration, pct int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := len(sorted) * pct / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// ---------------------------------------------------------------------------
// stageResult — per-stage metrics for ramp-up reporting
// ---------------------------------------------------------------------------

type stageResult struct {
	concurrency int
	duration    time.Duration
	ops         int
	errors      int64
	p50         time.Duration
	p95         time.Duration
	p99         time.Duration
	opsPerSec   float64
}

func collectStageResult(concurrency int, wall time.Duration, lc *latencyCollector) stageResult {
	s, errs := lc.Snapshot()
	res := stageResult{
		concurrency: concurrency,
		duration:    wall,
		ops:         len(s),
		errors:      errs,
	}
	if len(s) > 0 {
		res.p50 = percentile(s, 50)
		res.p95 = percentile(s, 95)
		res.p99 = percentile(s, 99)
	}
	if wall > 0 {
		res.opsPerSec = float64(len(s)) / wall.Seconds()
	}
	return res
}

func reportStageTable(t testing.TB, label string, results []stageResult) {
	t.Helper()
	t.Logf("")
	t.Logf("=== %s — ramp-up results ===", label)
	t.Logf("%-12s %8s %8s %8s %12s %12s %12s %10s",
		"concurrency", "ops", "errors", "err%", "p50", "p95", "p99", "ops/sec")
	t.Logf("%-12s %8s %8s %8s %12s %12s %12s %10s",
		"───────────", "────────", "────────", "────────", "────────────", "────────────", "────────────", "──────────")
	for _, r := range results {
		total := int64(r.ops) + r.errors
		errPct := 0.0
		if total > 0 {
			errPct = float64(r.errors) / float64(total) * 100
		}
		t.Logf("%-12d %8d %8d %7.1f%% %12v %12v %12v %10.0f",
			r.concurrency, r.ops, r.errors, errPct, r.p50, r.p95, r.p99, r.opsPerSec)
	}
	t.Logf("")
}

// ---------------------------------------------------------------------------
// Data seeding
// ---------------------------------------------------------------------------

func seedLoadData(t testing.TB) func() {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, server.SeedSQL(ctx, `
		CREATE TABLE IF NOT EXISTS load_data AS
		SELECT
			i AS id,
			'cat_' || (i % 10) AS category,
			random() * 1000 AS value,
			timestamp '2024-01-01' + interval (i) second AS ts
		FROM generate_series(0, 99999) t(i)
	`))
	return func() { _ = server.SeedSQL(ctx, "DROP TABLE IF EXISTS load_data") }
}

func seedLoadWide(t testing.TB) func() {
	t.Helper()
	ctx := context.Background()

	cols := "i AS c0"
	for j := 1; j < 50; j++ {
		cols += fmt.Sprintf(", random() * 1000 AS c%d", j)
	}
	require.NoError(t, server.SeedSQL(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS load_wide AS
		SELECT %s FROM generate_series(0, 9999) t(i)
	`, cols)))
	return func() { _ = server.SeedSQL(ctx, "DROP TABLE IF EXISTS load_wide") }
}

func seedLoadRW(t testing.TB) func() {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, server.SeedSQL(ctx, `
		CREATE TABLE IF NOT EXISTS load_rw AS
		SELECT i AS id, 'val_' || i AS val
		FROM generate_series(0, 999) t(i)
	`))
	return func() { _ = server.SeedSQL(ctx, "DROP TABLE IF EXISTS load_rw") }
}

func seedLoadTxn(t testing.TB) func() {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, server.SeedSQL(ctx, `CREATE TABLE IF NOT EXISTS load_txn (id INTEGER, val VARCHAR)`))
	return func() { _ = server.SeedSQL(ctx, "DROP TABLE IF EXISTS load_txn") }
}

// ---------------------------------------------------------------------------
// runStage — run a workload at fixed concurrency for a duration, collect stats
// ---------------------------------------------------------------------------

// workloadFunc is called in a tight loop by each goroutine.
// It should perform one unit of work and return its latency.
type workloadFunc func(ctx context.Context, cl *flightsql.Client) error

func runStage(
	ctx context.Context,
	clients []*flightsql.Client,
	concurrency int,
	duration time.Duration,
	work workloadFunc,
) *latencyCollector {
	lc := &latencyCollector{}

	stageCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	var wg sync.WaitGroup
	for i := range concurrency {
		cl := clients[i%len(clients)]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if stageCtx.Err() != nil {
					return
				}
				start := time.Now()
				if err := work(stageCtx, cl); err != nil {
					// Don't count context cancellation as errors — stage just ended.
					if stageCtx.Err() != nil {
						return
					}
					lc.RecordError()
					continue
				}
				lc.Record(time.Since(start))
			}
		}()
	}
	wg.Wait()
	return lc
}

// concurrencySteps returns exponential steps from 1 up to max: 1,2,4,8,...,max.
func concurrencySteps(max int) []int {
	var steps []int
	for c := 1; c <= max; c *= 2 {
		steps = append(steps, c)
	}
	// Ensure max is always the last step.
	if steps[len(steps)-1] != max {
		steps = append(steps, max)
	}
	return steps
}

// ---------------------------------------------------------------------------
// selectWorkload — Execute + DoGet for a query
// ---------------------------------------------------------------------------

func selectWorkload(query string) workloadFunc {
	return func(ctx context.Context, cl *flightsql.Client) error {
		info, err := cl.Execute(ctx, query)
		if err != nil {
			return err
		}
		rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			return err
		}
		for rdr.Next() {
		}
		err = rdr.Err()
		rdr.Release()
		return err
	}
}

// ---------------------------------------------------------------------------
// TestLoadRampUpSelect — THE ramp-up load test for SELECT queries
// ---------------------------------------------------------------------------

func TestLoadRampUpSelect(t *testing.T) {
	cfg := defaultLoadConfig()
	t.Logf("config: pool=%d max_clients=%d stage_duration=%v",
		cfg.PoolSize, cfg.MaxClients, cfg.StageDuration)

	env := newLoadEnv(t, cfg.PoolSize, cfg.MaxClients)
	defer env.close(t)

	cleanup := seedLoadData(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.StageDuration*time.Duration(cfg.MaxClients)+30*time.Second)
	defer cancel()

	steps := concurrencySteps(cfg.MaxClients)
	results := make([]stageResult, 0, len(steps))

	work := selectWorkload("SELECT * FROM load_data WHERE id % 1000 < 10 LIMIT 100")

	for _, c := range steps {
		// Let server drain between stages.
		env.waitClean(t, 2*time.Second)
		t.Logf("stage: concurrency=%d ...", c)
		start := time.Now()
		lc := runStage(ctx, env.clients, c, cfg.StageDuration, work)
		wall := time.Since(start)
		res := collectStageResult(c, wall, lc)
		results = append(results, res)
		t.Logf("  -> %d ops in %v (%.0f ops/sec) p50=%v p95=%v p99=%v errors=%d",
			res.ops, wall.Round(time.Millisecond), res.opsPerSec,
			res.p50, res.p95, res.p99, res.errors)
	}

	reportStageTable(t, "SELECT ramp-up", results)

	// Verify no errors at low concurrency (first stage).
	require.Equal(t, int64(0), results[0].errors, "errors at concurrency=1")

	env.waitClean(t, 5*time.Second)
}

// ---------------------------------------------------------------------------
// TestLoadRampUpMetadata — ramp-up for metadata operations
// ---------------------------------------------------------------------------

func TestLoadRampUpMetadata(t *testing.T) {
	cfg := defaultLoadConfig()
	t.Logf("config: pool=%d max_clients=%d stage_duration=%v",
		cfg.PoolSize, cfg.MaxClients, cfg.StageDuration)

	env := newLoadEnv(t, cfg.PoolSize, cfg.MaxClients)
	defer env.close(t)

	ctx := context.Background()
	require.NoError(t, server.SeedSQL(ctx, "CREATE TABLE IF NOT EXISTS load_meta (id INTEGER PRIMARY KEY, name VARCHAR)"))
	defer func() { _ = server.SeedSQL(ctx, "DROP TABLE IF EXISTS load_meta") }()

	ctx, cancel := context.WithTimeout(ctx, cfg.StageDuration*time.Duration(cfg.MaxClients)+30*time.Second)
	defer cancel()

	work := func(ctx context.Context, cl *flightsql.Client) error {
		info, err := cl.GetTables(ctx, &flightsql.GetTablesOpts{})
		if err != nil {
			return err
		}
		for _, ep := range info.Endpoint {
			rdr, err := cl.DoGet(ctx, ep.Ticket)
			if err != nil {
				return err
			}
			for rdr.Next() {
			}
			if err := rdr.Err(); err != nil {
				rdr.Release()
				return err
			}
			rdr.Release()
		}
		return nil
	}

	steps := concurrencySteps(cfg.MaxClients)
	results := make([]stageResult, 0, len(steps))

	for _, c := range steps {
		env.waitClean(t, 2*time.Second)
		t.Logf("stage: concurrency=%d ...", c)
		start := time.Now()
		lc := runStage(ctx, env.clients, c, cfg.StageDuration, work)
		wall := time.Since(start)
		res := collectStageResult(c, wall, lc)
		results = append(results, res)
		t.Logf("  -> %d ops in %v (%.0f ops/sec) p50=%v p95=%v p99=%v errors=%d",
			res.ops, wall.Round(time.Millisecond), res.opsPerSec,
			res.p50, res.p95, res.p99, res.errors)
	}

	reportStageTable(t, "metadata ramp-up", results)
	require.Equal(t, int64(0), results[0].errors, "errors at concurrency=1")
	env.waitClean(t, 5*time.Second)
}

// ---------------------------------------------------------------------------
// TestLoadRampUpTransactions — ramp-up for transactional writes
// ---------------------------------------------------------------------------

func TestLoadRampUpTransactions(t *testing.T) {
	cfg := defaultLoadConfig()
	t.Logf("config: pool=%d max_clients=%d stage_duration=%v",
		cfg.PoolSize, cfg.MaxClients, cfg.StageDuration)

	env := newLoadEnv(t, cfg.PoolSize, cfg.MaxClients)
	defer env.close(t)

	cleanup := seedLoadTxn(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.StageDuration*time.Duration(cfg.MaxClients)+30*time.Second)
	defer cancel()

	var txnID atomic.Int64
	work := func(ctx context.Context, cl *flightsql.Client) error {
		id := txnID.Add(1)
		tx, err := cl.BeginTransaction(ctx)
		if err != nil {
			return err
		}
		_, err = tx.ExecuteUpdate(ctx,
			fmt.Sprintf("INSERT INTO load_txn VALUES (%d, 'ramp')", id))
		if err != nil {
			// Use background context for rollback — the stage context may be expired.
			_ = tx.Rollback(context.Background())
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			_ = tx.Rollback(context.Background())
			return err
		}
		return nil
	}

	steps := concurrencySteps(cfg.MaxClients)
	results := make([]stageResult, 0, len(steps))

	for _, c := range steps {
		env.waitClean(t, 2*time.Second)
		t.Logf("stage: concurrency=%d ...", c)
		start := time.Now()
		lc := runStage(ctx, env.clients, c, cfg.StageDuration, work)
		wall := time.Since(start)
		res := collectStageResult(c, wall, lc)
		results = append(results, res)
		t.Logf("  -> %d ops in %v (%.0f ops/sec) p50=%v p95=%v p99=%v errors=%d",
			res.ops, wall.Round(time.Millisecond), res.opsPerSec,
			res.p50, res.p95, res.p99, res.errors)
	}

	reportStageTable(t, "transaction ramp-up", results)
	require.Equal(t, int64(0), results[0].errors, "errors at concurrency=1")
	env.waitClean(t, 5*time.Second)
}

// ---------------------------------------------------------------------------
// TestLoadSustainedMixed — sustained mixed read/write at max concurrency
// ---------------------------------------------------------------------------

func TestLoadSustainedMixed(t *testing.T) {
	cfg := defaultLoadConfig()
	t.Logf("config: pool=%d clients=%d duration=%v",
		cfg.PoolSize, cfg.MaxClients, cfg.SustainedDuration)

	env := newLoadEnv(t, cfg.PoolSize, cfg.MaxClients)
	defer env.close(t)

	cleanup := seedLoadData(t)
	defer cleanup()

	ctx := context.Background()
	require.NoError(t, server.SeedSQL(ctx, `CREATE TABLE IF NOT EXISTS load_sustained_txn (id INTEGER, val VARCHAR)`))
	defer func() { _ = server.SeedSQL(ctx, "DROP TABLE IF EXISTS load_sustained_txn") }()

	// Stage context controls the sustained load duration.
	stageCtx, stageCancel := context.WithTimeout(ctx, cfg.SustainedDuration)
	defer stageCancel()

	readLC := &latencyCollector{}
	writeLC := &latencyCollector{}

	// Report stats every 5 seconds.
	const reportInterval = 5 * time.Second
	var lastReport atomic.Int64
	lastReport.Store(time.Now().UnixMilli())
	var intervalReadOps, intervalWriteOps, intervalErrors atomic.Int64

	numWriters := max(cfg.MaxClients/4, 1)
	numReaders := cfg.MaxClients - numWriters

	var writeID atomic.Int64
	var wg sync.WaitGroup

	// Readers.
	for i := range numReaders {
		cl := env.clients[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for stageCtx.Err() == nil {
				start := time.Now()
				err := selectWorkload("SELECT * FROM load_data WHERE id % 1000 < 5 LIMIT 50")(stageCtx, cl)
				if err != nil {
					if stageCtx.Err() != nil {
						return
					}
					readLC.RecordError()
					intervalErrors.Add(1)
					continue
				}
				readLC.Record(time.Since(start))
				intervalReadOps.Add(1)

				// Periodic reporting.
				nowMs := time.Now().UnixMilli()
				lastMs := lastReport.Load()
				if time.Duration(nowMs-lastMs)*time.Millisecond >= reportInterval {
					if lastReport.CompareAndSwap(lastMs, nowMs) {
						elapsed := time.Duration(nowMs-lastMs) * time.Millisecond
						r := intervalReadOps.Swap(0)
						w := intervalWriteOps.Swap(0)
						e := intervalErrors.Swap(0)
						t.Logf("[sustained] reads=%.0f/s writes=%.0f/s errors=%d (last %v)",
							float64(r)/elapsed.Seconds(),
							float64(w)/elapsed.Seconds(),
							e, elapsed.Round(time.Millisecond))
					}
				}
			}
		}()
	}

	// Writers.
	for i := range numWriters {
		cl := env.clients[numReaders+i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for stageCtx.Err() == nil {
				start := time.Now()
				id := writeID.Add(1)
				tx, err := cl.BeginTransaction(stageCtx)
				if err != nil {
					if stageCtx.Err() != nil {
						return
					}
					writeLC.RecordError()
					intervalErrors.Add(1)
					continue
				}
				_, err = tx.ExecuteUpdate(stageCtx,
					fmt.Sprintf("INSERT INTO load_sustained_txn VALUES (%d, 'sustained')", id))
				if err != nil {
					_ = tx.Rollback(context.Background())
					if stageCtx.Err() != nil {
						return
					}
					writeLC.RecordError()
					intervalErrors.Add(1)
					continue
				}
				if err := tx.Commit(stageCtx); err != nil {
					_ = tx.Rollback(context.Background())
					if stageCtx.Err() != nil {
						return
					}
					writeLC.RecordError()
					intervalErrors.Add(1)
					continue
				}
				writeLC.Record(time.Since(start))
				intervalWriteOps.Add(1)
			}
		}()
	}

	wg.Wait()

	t.Logf("")
	t.Logf("=== sustained mixed results (%d readers, %d writers, %v) ===",
		numReaders, numWriters, cfg.SustainedDuration)
	readLC.Report(t, "reads")
	writeLC.Report(t, "writes")

	readSamples, readErrors := readLC.Snapshot()
	writeSamples, writeErrors := writeLC.Snapshot()
	totalOps := len(readSamples) + len(writeSamples)
	totalErrors := readErrors + writeErrors
	t.Logf("total: %d ops, %d errors (%.2f%% error rate) in %v",
		int64(totalOps)+totalErrors, totalErrors,
		float64(totalErrors)/math.Max(float64(int64(totalOps)+totalErrors), 1)*100,
		cfg.SustainedDuration)

	// Tolerate < 1% errors — context cancellation at stage boundaries
	// can cause a handful of gRPC errors that aren't true failures.
	if totalErrors > 0 {
		errorRate := float64(totalErrors) / math.Max(float64(int64(totalOps)+totalErrors), 1)
		require.Less(t, errorRate, 0.01,
			"error rate %.2f%% exceeds 1%%", errorRate*100)
	}

	env.waitClean(t, 5*time.Second)
}

// ---------------------------------------------------------------------------
// TestLoadConcurrentSelect — fixed iteration tests with configurable intensity
// ---------------------------------------------------------------------------

func TestLoadConcurrentSelect(t *testing.T) {
	cfg := defaultLoadConfig()

	cases := []struct {
		name       string
		poolSize   int
		numClients int
	}{
		{"pool=2/clients=8", 2, 8},
		{"pool=4/clients=16", 4, 16},
		{"pool=8/clients=32", 8, 32},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newLoadEnv(t, tc.poolSize, tc.numClients)
			defer env.close(t)

			cleanup := seedLoadData(t)
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			iterations := cfg.Iterations
			t.Logf("iterations_per_goroutine=%d total_ops=%d", iterations, iterations*tc.numClients)
			lc := &latencyCollector{}

			g, gCtx := errgroup.WithContext(ctx)
			for i := range tc.numClients {
				cl := env.clients[i]
				modVal := i
				g.Go(func() error {
					for j := range iterations {
						if gCtx.Err() != nil {
							return gCtx.Err()
						}
						start := time.Now()
						query := fmt.Sprintf("SELECT * FROM load_data WHERE id %% 1000 = %d LIMIT 100", modVal+j%10)
						info, err := cl.Execute(gCtx, query)
						if err != nil {
							lc.RecordError()
							continue
						}
						rdr, err := cl.DoGet(gCtx, info.Endpoint[0].Ticket)
						if err != nil {
							lc.RecordError()
							continue
						}
						for rdr.Next() {
						}
						if err := rdr.Err(); err != nil {
							lc.RecordError()
							rdr.Release()
							continue
						}
						rdr.Release()
						lc.Record(time.Since(start))
					}
					return nil
				})
			}
			require.NoError(t, g.Wait())

			lc.Report(t, tc.name)

			errorRate := float64(lc.errors.Load()) / float64(int64(tc.numClients*iterations))
			require.Less(t, errorRate, 0.01, "error rate %.1f%% exceeds 1%%", errorRate*100)

			env.assertNoLeaks(t)
		})
	}
}

// ---------------------------------------------------------------------------
// TestLoadMixedReadWrite — concurrent reads + transactional writes
// ---------------------------------------------------------------------------

func TestLoadMixedReadWrite(t *testing.T) {
	cfg := defaultLoadConfig()

	const (
		poolSize   = 8
		numReaders = 8
		numWriters = 4
		numClients = numReaders + numWriters
	)
	readIters := cfg.Iterations
	writeIters := cfg.Iterations / 4

	env := newLoadEnv(t, poolSize, numClients)
	defer env.close(t)

	cleanup := seedLoadRW(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	t.Logf("read_iters=%d write_iters=%d", readIters, writeIters)

	readLC := &latencyCollector{}
	writeLC := &latencyCollector{}

	g, gCtx := errgroup.WithContext(ctx)

	for i := range numReaders {
		cl := env.clients[i]
		g.Go(func() error {
			for range readIters {
				if gCtx.Err() != nil {
					return gCtx.Err()
				}
				start := time.Now()
				info, err := cl.Execute(gCtx, "SELECT count(*) FROM load_rw")
				if err != nil {
					readLC.RecordError()
					continue
				}
				rdr, err := cl.DoGet(gCtx, info.Endpoint[0].Ticket)
				if err != nil {
					readLC.RecordError()
					continue
				}
				for rdr.Next() {
				}
				if err := rdr.Err(); err != nil {
					readLC.RecordError()
					rdr.Release()
					continue
				}
				rdr.Release()
				readLC.Record(time.Since(start))
			}
			return nil
		})
	}

	for i := range numWriters {
		cl := env.clients[numReaders+i]
		writerID := i
		g.Go(func() error {
			for j := range writeIters {
				if gCtx.Err() != nil {
					return gCtx.Err()
				}
				start := time.Now()
				tx, err := cl.BeginTransaction(gCtx)
				if err != nil {
					writeLC.RecordError()
					continue
				}
				_, err = tx.ExecuteUpdate(gCtx,
					fmt.Sprintf("INSERT INTO load_rw VALUES (%d, 'w%d_iter%d')", 1000+writerID*writeIters+j, writerID, j))
				if err != nil {
					_ = tx.Rollback(context.Background())
					writeLC.RecordError()
					continue
				}
				if err := tx.Commit(gCtx); err != nil {
					_ = tx.Rollback(context.Background())
					writeLC.RecordError()
					continue
				}
				writeLC.Record(time.Since(start))
			}
			return nil
		})
	}

	require.NoError(t, g.Wait())

	readLC.Report(t, "read")
	writeLC.Report(t, "write")

	require.Equal(t, int64(0), readLC.errors.Load(), "expected zero read errors")

	writeErrors := writeLC.errors.Load()
	writeTotal := int64(numWriters * writeIters)
	writeErrorRate := float64(writeErrors) / float64(writeTotal)
	require.Less(t, writeErrorRate, 0.05, "write error rate %.1f%% exceeds 5%%", writeErrorRate*100)

	// Verify final row count.
	info, err := env.clients[0].Execute(ctx, "SELECT count(*) FROM load_rw")
	require.NoError(t, err)
	rdr, err := env.clients[0].DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	defer rdr.Release()

	require.True(t, rdr.Next())
	rec := rdr.RecordBatch()
	finalCount := rec.Column(0).(*array.Int64).Value(0)
	expectedMin := int64(1000 + numWriters*writeIters - int(writeErrors))
	t.Logf("final row count: %d (expected >= %d)", finalCount, expectedMin)
	require.GreaterOrEqual(t, finalCount, expectedMin)

	env.assertNoLeaks(t)
}

// ---------------------------------------------------------------------------
// TestLoadLargeResultStreaming — throughput test for large result sets
// ---------------------------------------------------------------------------

func TestLoadLargeResultStreaming(t *testing.T) {
	cfg := defaultLoadConfig()

	const (
		poolSize   = 4
		numClients = 4
	)
	iterations := max(cfg.Iterations/10, 10)

	env := newLoadEnv(t, poolSize, numClients)
	defer env.close(t)

	cleanup := seedLoadWide(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	t.Logf("iterations_per_client=%d", iterations)

	lc := &latencyCollector{}
	var totalBytes atomic.Int64

	g, gCtx := errgroup.WithContext(ctx)
	for i := range numClients {
		cl := env.clients[i]
		g.Go(func() error {
			var clientBytes int64
			for range iterations {
				if gCtx.Err() != nil {
					return gCtx.Err()
				}
				start := time.Now()
				info, err := cl.Execute(gCtx, "SELECT * FROM load_wide")
				if err != nil {
					lc.RecordError()
					continue
				}
				rdr, err := cl.DoGet(gCtx, info.Endpoint[0].Ticket)
				if err != nil {
					lc.RecordError()
					continue
				}
				for rdr.Next() {
					rec := rdr.RecordBatch()
					for c := range int(rec.NumCols()) {
						col := rec.Column(c).Data()
						for _, buf := range col.Buffers() {
							if buf != nil {
								clientBytes += int64(buf.Len())
							}
						}
					}
				}
				if err := rdr.Err(); err != nil {
					lc.RecordError()
					rdr.Release()
					continue
				}
				rdr.Release()
				lc.Record(time.Since(start))
			}
			totalBytes.Add(clientBytes)
			return nil
		})
	}

	require.NoError(t, g.Wait())

	lc.Report(t, "large_result_streaming")
	tb := totalBytes.Load()
	t.Logf("total bytes streamed: %d (%.1f MB)", tb, float64(tb)/(1024*1024))

	env.assertNoLeaks(t)
}

// ---------------------------------------------------------------------------
// TestLoadTransactionContention — pool exhaustion via transactions
// ---------------------------------------------------------------------------

func TestLoadTransactionContention(t *testing.T) {
	cfg := defaultLoadConfig()

	const poolSize = 4
	numClients := max(cfg.MaxClients, 12)
	cycles := cfg.Iterations

	env := newLoadEnv(t, poolSize, numClients)
	defer env.close(t)

	cleanup := seedLoadTxn(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	t.Logf("pool=%d clients=%d cycles_per_client=%d total_txns=%d",
		poolSize, numClients, cycles, numClients*cycles)

	lc := &latencyCollector{}
	var completedTxns atomic.Int64

	g, gCtx := errgroup.WithContext(ctx)
	for i := range numClients {
		cl := env.clients[i]
		clientID := i
		g.Go(func() error {
			for j := range cycles {
				if gCtx.Err() != nil {
					return gCtx.Err()
				}

				opCtx, opCancel := context.WithTimeout(gCtx, 30*time.Second)

				start := time.Now()
				tx, err := cl.BeginTransaction(opCtx)
				if err != nil {
					opCancel()
					lc.RecordError()
					continue
				}
				_, err = tx.ExecuteUpdate(opCtx,
					fmt.Sprintf("INSERT INTO load_txn VALUES (%d, 'c%d_j%d')", clientID*cycles+j, clientID, j))
				if err != nil {
					_ = tx.Rollback(context.Background())
					opCancel()
					lc.RecordError()
					continue
				}
				err = tx.Commit(opCtx)
				opCancel()
				if err != nil {
					_ = tx.Rollback(context.Background())
					lc.RecordError()
					continue
				}
				lc.Record(time.Since(start))
				completedTxns.Add(1)
			}
			return nil
		})
	}

	require.NoError(t, g.Wait())

	lc.Report(t, "transaction_contention")

	completed := completedTxns.Load()
	expected := int64(numClients * cycles)
	t.Logf("completed transactions: %d/%d", completed, expected)
	require.Equal(t, expected, completed, "not all transactions completed — possible deadlock")

	env.assertNoLeaks(t)
}

// ---------------------------------------------------------------------------
// Parallel Benchmarks
// ---------------------------------------------------------------------------

func newLoadBenchEnv(b *testing.B, poolSize int) *loadEnv {
	b.Helper()
	ensureTestMetrics()

	srv, err := server.New(server.Config{
		MemoryLimit:  "512MB",
		MaxThreads:   4,
		QueryTimeout: "60s",
		PoolSize:     poolSize,
	})
	if err != nil {
		b.Fatal(err)
	}
	fs := flight.NewServerWithMiddleware(nil)
	fs.RegisterFlightService(flightsql.NewFlightServer(srv))
	if err := fs.Init("localhost:0"); err != nil {
		b.Fatal(err)
	}
	go func() { _ = fs.Serve() }()

	srv.Alloc = memory.DefaultAllocator

	ctx := context.Background()
	if err := server.SeedSQL(ctx, `
		CREATE TABLE IF NOT EXISTS load_bench_data AS
		SELECT i AS id, 'cat_' || (i % 10) AS category, random() * 1000 AS value
		FROM generate_series(0, 99999) t(i)
	`); err != nil {
		b.Fatalf("seed load_bench_data: %v", err)
	}
	if err := server.SeedSQL(ctx, `CREATE TABLE IF NOT EXISTS load_bench_txn (id INTEGER, val VARCHAR)`); err != nil {
		b.Fatalf("seed load_bench_txn: %v", err)
	}

	const maxClients = 64
	clients := make([]*flightsql.Client, maxClients)
	for i := range clients {
		cl, err := flightsql.NewClient(
			fs.Addr().String(),
			nil, nil,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			b.Fatal(err)
		}
		cl.Alloc = memory.DefaultAllocator
		clients[i] = cl
	}

	env := &loadEnv{srv: srv, fs: fs, clients: clients}
	b.Cleanup(func() { env.close(b) })
	return env
}

func BenchmarkParallelSelect(b *testing.B) {
	for _, poolSize := range []int{4, 8} {
		b.Run(fmt.Sprintf("pool=%d", poolSize), func(b *testing.B) {
			env := newLoadBenchEnv(b, poolSize)
			ctx := context.Background()

			var idx atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				ci := int(idx.Add(1)-1) % len(env.clients)
				cl := env.clients[ci]
				for pb.Next() {
					info, err := cl.Execute(ctx, "SELECT * FROM load_bench_data LIMIT 100")
					if err != nil {
						b.Fatal(err)
					}
					rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
					if err != nil {
						b.Fatal(err)
					}
					for rdr.Next() {
					}
					if err := rdr.Err(); err != nil {
						b.Fatal(err)
					}
					rdr.Release()
				}
			})
		})
	}
}

func BenchmarkParallelMetadata(b *testing.B) {
	env := newLoadBenchEnv(b, 4)
	ctx := context.Background()

	var idx atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ci := int(idx.Add(1)-1) % len(env.clients)
		cl := env.clients[ci]
		for pb.Next() {
			info, err := cl.GetTables(ctx, &flightsql.GetTablesOpts{})
			if err != nil {
				b.Fatal(err)
			}
			drainFlightInfoB(b, cl, info)
		}
	})
}

func BenchmarkParallelMixedWorkload(b *testing.B) {
	env := newLoadBenchEnv(b, 8)
	ctx := context.Background()

	var idx atomic.Int64
	var insertID atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ci := int(idx.Add(1)-1) % len(env.clients)
		cl := env.clients[ci]
		iter := 0
		for pb.Next() {
			if iter%4 == 0 {
				id := insertID.Add(1)
				tx, err := cl.BeginTransaction(ctx)
				if err != nil {
					b.Fatal(err)
				}
				_, err = tx.ExecuteUpdate(ctx, fmt.Sprintf("INSERT INTO load_bench_txn VALUES (%d, 'bench')", id))
				if err != nil {
					_ = tx.Rollback(context.Background())
					b.Fatal(err)
				}
				if err := tx.Commit(ctx); err != nil {
					b.Fatal(err)
				}
			} else {
				info, err := cl.Execute(ctx, "SELECT * FROM load_bench_data LIMIT 100")
				if err != nil {
					b.Fatal(err)
				}
				rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
				if err != nil {
					b.Fatal(err)
				}
				for rdr.Next() {
				}
				if err := rdr.Err(); err != nil {
					b.Fatal(err)
				}
				rdr.Release()
			}
			iter++
		}
	})
}
