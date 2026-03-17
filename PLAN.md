# DuckFlight — Future Plan

Current state as of 2026-03-13: the core Flight SQL server is complete and deployed.
See CLAUDE.md for architecture and current implementation details.

---

## ~~1. Prepared Statement Parameter Binding~~ (Done)

Implemented. Parameters are extracted from Arrow record batches via `scalar.GetScalar`,
converted to Go values, and passed to `QueryContext`/`ExecContext` as positional args.
Supports single-row and multi-row (batch) parameter binding. Tested with `flightsql.Client`.

---

## ~~BUG: SIGSEGV in `DoGetTables`~~ (Fixed)

Worked around. The crash was triggered by `SELECT * FROM <table> LIMIT 0` on tables
created via `Arrow.RegisterView` (bulk ingest path). `DoGetTables` now builds Arrow
schemas entirely from `information_schema.columns` instead of querying tables directly,
avoiding the duckdb-go bug. Regression test: `TestSIGSEGV_IngestThenGetTablesWithSchema`.

---

## ~~2. Zero-Copy Metadata Streaming~~ (Done)

Implemented in two steps:

1. **Zero-copy streaming** (`e526dd4`): Replaced manual row-by-row Arrow batch rebuilding
   with `streamMetadata` helper that streams DuckDB's Arrow batches directly to Flight SQL
   clients. SQL queries alias columns to match `schema_ref` field names and cast types where
   needed, eliminating the Go-side `RecordReader` wrapper. Net -261 lines.

2. **Bulk column metadata** for `include_schema`: Replaced per-table `buildTableSchema`
   (N queries per batch) with `buildBatchTableSchemas` (1 query per batch). Fetches all
   column metadata for the batch in a single `information_schema.columns` query with an
   `IN` filter, groups by table, then looks up each schema from the map.

**Benchstat results** — zero-copy streaming (baseline `e0c91ab` vs `e526dd4`, n=6):

| Endpoint                     | sec/op            | B/op               | allocs/op   |
|------------------------------|-------------------|--------------------|-------------|
| DoGetCatalogs                | ~ (p=0.065)       | **-3.40%**         | **-3.43%**  |
| DoGetDBSchemas               | ~ (p=0.589)       | **-6.12%**         | **-6.52%**  |
| DoGetTables/100              | +28.66%           | **-21.62%**        | **-12.04%** |
| DoGetTables/1000             | ~ (p=0.132)       | ~ (p=0.240)        | **-15.51%** |
| DoGetTables/5000             | +25.65%           | **-60.32%**        | ~ (p=0.372) |
| DoGetPrimaryKeys (all tiers) | **-12.70%** (100) | **-8.2%**          | **-8.8%**   |
| DoGetExportedKeys (all)      | **-8% to -17%**   | **-7.2% to -7.4%** | **-9.4%**   |
| DoGetCrossReference (all)    | **-14% to -16%**  | **-7.3% to -7.4%** | **-9.4%**   |
| **geomean**                  | **-0.62%**        | **-12.15%**        | **-7.20%**  |

**`include_schema` bulk query** improvement (baseline `e0c91ab` vs bulk, n=6):

| Tier | sec/op       | B/op     | allocs/op |
|------|--------------|----------|-----------|
| 100  | 1.47s → 48ms | **-46%** | **-62%**  |
| 1000 | 13.9s → 66ms | **-41%** | **-64%**  |
| 5000 | 88s → 167ms  | **-45%** | **-64%**  |

---

## ~~3. Flight SQL Cancellation & Polling~~ (Done)

Implemented. `queryTracker` replaces the old `queryCache` and tracks active queries with
cancel functions and completion state. `CancelFlightInfo` cancels running queries (returns
`Cancelling`) or removes pending ones (returns `Cancelled`). `PollFlightInfoStatement`
registers the query and returns a ready-to-consume `PollInfo` (synchronous model).
`SqlInfoFlightSqlServerCancel` is now `true`. New `"canceled"` metric label on
`flightsql_queries_total`. TTL-based cleanup reaps abandoned entries.

---

## ~~4. Bulk Ingestion (`DoPutCommandStatementIngest`)~~ (Done)

Implemented. Each incoming Arrow record batch is registered as a DuckDB view via
`Arrow.RegisterView`, then inserted with `INSERT INTO <target> SELECT * FROM <view>`.
Supports `TableDefinitionOptions` (CREATE/FAIL × APPEND/REPLACE/FAIL), transactions,
and multi-batch streams. `SqlInfoFlightSqlServerBulkIngestion` and
`SqlInfoFlightSqlServerIngestTransactionsSupported` are now `true`.

---

## ~~5. Distributed Tracing (OpenTelemetry)~~ (Done)

Implemented full three-signal OTel: tracing, metrics, and logging. Added `internal/telemetry`
package with `TracerProvider`, `MeterProvider`, and `LoggerProvider`. gRPC interceptors
(`otelgrpc.NewServerHandler`) propagate trace context. Prometheus metrics now go through
OTel SDK (`otel/exporters/prometheus`). Logging uses `otelslog` bridge for automatic
trace↔log correlation, replacing the custom `TraceHandler`. All `slog` calls pass `ctx`
for correlation. OTLP export is configured via `OTEL_EXPORTER_OTLP_ENDPOINT`; without it,
stdout exporters are used.

---

## ~~6. Rate Limiting~~ (Done)

Implemented global token-bucket rate limiter as a `flight.ServerMiddleware` gRPC
interceptor. Uses `golang.org/x/time/rate` with configurable RPS and burst size
(`RATE_LIMIT_RPS`, `RATE_LIMIT_BURST` env vars; 0 = disabled). Rejects excess
requests with `codes.ResourceExhausted`. Prometheus counter
`flightsql.ratelimit.rejected` tracks rejections.

Also refactored auth middleware from `[]grpc.ServerOption` to `*flight.ServerMiddleware`
so all middleware goes through `NewServerWithMiddleware`'s middleware slice.
Middleware order: logging (outer) → auth → rate limit (inner) — unauthenticated
requests are rejected before consuming rate limit tokens.

---

## 7. Savepoints

`BeginSavepoint` and `EndSavepoint` are part of the Flight SQL spec but not implemented.

### Status

**Blocked.** DuckDB does not support `SAVEPOINT`, `RELEASE SAVEPOINT`, or
`ROLLBACK TO SAVEPOINT`. The transaction docs only cover `BEGIN`, `COMMIT`,
`ROLLBACK`, and `ABORT`. Cannot implement without upstream DuckDB support.

### Priority

Low. Most Flight SQL clients do not use savepoints. Revisit if DuckDB adds savepoint support.

---

## ~~8. Polling for Long-Running Queries~~ (Done)

Implemented as part of cancellation (#3). `PollFlightInfoStatement` returns an immediately
ready `PollInfo` (synchronous model). If async execution is needed in the future, this can
be extended to return a retry descriptor and track query progress.

---

## ~~9. Static Extension Build~~ (Done)

Implemented. Dockerfile is now a 3-stage build: DuckDB compilation with extensions
(`libduckdb_bundle.a`), Go build with `duckdb_use_static_lib` tag, and slim runtime.
Boot SQL conditionally skips `INSTALL`/`LOAD` and disables `autoinstall_known_extensions`
when statically linked. Local dev unchanged (pre-built bindings, dynamic extension loading).
GitHub Actions release workflow uses BuildKit with GHA cache to avoid rebuilding DuckDB.

---

## ~~10. TLS~~ (Done)

Implemented. The gRPC server optionally enables TLS via `TLS_CERT` and `TLS_KEY`
environment variables. When both are set, `tls.LoadX509KeyPair` loads the certificate
and `grpc.Creds(credentials.NewTLS(...))` is passed to `NewServerWithMiddleware`.
If `TLS_CA` is also set, mutual TLS (mTLS) is enabled — clients must present a
certificate signed by the specified CA. Helm chart supports TLS via `tls.enabled`,
`tls.existingSecret`, and `tls.caSecret` values.

---

## ~~11. Load Testing~~ (Done)

Implemented in `internal/server/load_test.go`. Custom Go client using `flightsql.Client`
(Flight SQL requires two-step GetFlightInfo → DoGet that generic gRPC tools can't orchestrate).

**Ramp-up tests** (`TestLoadRampUp{Select,Metadata,Transactions}`): Gradually increase
concurrency from 1 to `LOAD_MAX_CLIENTS` in exponential steps (1, 2, 4, 8, ...). Each stage
runs for `LOAD_STAGE_DURATION`, collecting per-stage p50/p95/p99 latency, throughput (ops/sec),
and error rate. Output is a comparison table showing how latency degrades as load increases.

**Sustained mixed test** (`TestLoadSustainedMixed`): Runs readers and writers at full
concurrency for `LOAD_SUSTAINED_DURATION` with periodic interval reporting (reads/s, writes/s,
errors every 5s).

**Fixed-iteration tests**: `TestLoadConcurrentSelect` (pool saturation at various pool/client
ratios), `TestLoadMixedReadWrite` (transactional writes + concurrent reads with row count
verification), `TestLoadLargeResultStreaming` (throughput in bytes/sec for wide result sets),
`TestLoadTransactionContention` (pool exhaustion via held transactions).

**Parallel benchmarks**: `BenchmarkParallel{Select,Metadata,MixedWorkload}` using `b.RunParallel`
for `go test -bench` integration.

**Configurable via environment variables** (defaults suit CI, `LOAD_HEAVY=1` for real load):

| Variable                  | Default | Heavy |
|---------------------------|---------|-------|
| `LOAD_POOL_SIZE`          | 4       | 4     |
| `LOAD_MAX_CLIENTS`        | 16      | 64    |
| `LOAD_STAGE_DURATION`     | 3s      | 10s   |
| `LOAD_ITERATIONS`         | 100     | 2000  |
| `LOAD_SUSTAINED_DURATION` | 10s     | 60s   |

---

## ~~12. CGO Memory Leak Testing~~ (Done)

Implemented. Server-side `memory.DefaultAllocator` replaced with injectable `s.Alloc` in all
12 call sites (metadata, primary keys, foreign keys, xdbc type info). `CheckedAllocator` now
tracks both client-side and server-side Arrow allocations in all three test suites.

**Leak detection infrastructure:**
- `ArrowPool.Len()`/`Cap()` for pool connection leak detection.
- `OpenTransactionCount()`, `PreparedStatementCount()`, `ActiveQueryCount()` diagnostics.
- All test suites assert zero leaked resources in `TearDownTest`.

**Soak tests** (`soak_test.go`): 4 standalone tests exercising query execution (500 iterations),
metadata endpoints (200), prepared statements (200), and transactions (200) with per-iteration
resource assertions and post-warmup heap/RSS growth checks.

**Server-side resource reaper:** Background goroutine reaps stale prepared statements and
abandoned transactions after `resourceTTL` (2× query timeout, default 30min). Rolled-back
transactions release their pool connections.

**Findings:** No CGO memory leaks detected. ADBC FlightSQL driver does not call
`ClosePreparedStatement` through `database/sql` — mitigated by the TTL reaper. Valgrind/ASAN
for C-side leak detection remains a future option.

---

## Priority Order

| #   | Feature                                  | Impact                       | Effort     | Priority |
|-----|------------------------------------------|------------------------------|------------|----------|
| BUG | ~~SIGSEGV in DoGetTables~~               | ~~Critical (process crash)~~ | ~~Low~~    | Fixed    |
| 1   | ~~Prepared statement parameter binding~~ | ~~High (correctness)~~       | ~~Medium~~ | Done     |
| 2   | ~~Zero-copy metadata streaming~~         | ~~Medium (performance)~~     | ~~Medium~~ | Done     |
| 3   | ~~Cancellation & polling~~               | ~~Medium (usability)~~       | ~~Low~~    | Done     |
| 4   | ~~Bulk ingestion~~                       | ~~Medium (completeness)~~    | ~~Medium~~ | Done     |
| 5   | ~~Distributed tracing~~                  | ~~Medium (operability)~~     | ~~Low~~    | Done     |
| 6   | ~~Rate limiting~~                        | ~~Low (defense)~~            | ~~Low~~    | Done     |
| 7   | ~~TLS~~                                  | ~~Low (deploy-dependent)~~   | ~~Low~~    | Done     |
| 8   | ~~Load testing~~                         | ~~Medium (confidence)~~      | ~~Medium~~ | Done     |
| 9   | ~~Static extension build~~               | ~~Medium (cold start)~~      | ~~Medium~~ | Done     |
| 10  | Savepoints                               | Low (niche)                  | Low        | Blocked  |
| 11  | ~~Polling~~                              | ~~Low (niche)~~              | ~~Medium~~ | Done     |
| 12  | ~~CGO memory leak testing~~              | ~~High (reliability)~~       | ~~Medium~~ | Done     |
