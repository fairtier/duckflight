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

## 2. Zero-Copy Metadata Streaming

Metadata endpoints currently iterate rows, clone strings, and rebuild Arrow batches manually.
DuckDB already produces Arrow batches — we throw them away and reconstruct.

See `docs/plan-zero-copy-metadata.md` for the full design.

### Summary

- Build a nullability-fixing `RecordReader` wrapper (O(1) per batch, no data copy).
- Build a schema-enriching reader for `GetTables` with `include_schema`.
- Replace manual builder pattern in `DoGetCatalogs`, `DoGetDBSchemas`, `DoGetTables`,
  `DoGetPrimaryKeys`, and foreign key endpoints.
- Benchmark with 100, 1000, 5000 tables to validate the gain is worth the complexity.

### Baseline benchmarks (done)

`internal/server/bench_test.go` — full round-trip (`GetFlightInfo` → `DoGet` → drain) benchmarks
for all metadata endpoints. Seeds 3 schemas (`bench_100`, `bench_1000`, `bench_5000`) with
plain tables (PK) and FK parent/child pairs. Run baseline before optimization, compare with
`benchstat` afterward:

```bash
go test -tags=duckdb_arrow -run='^$' -bench=Benchmark -benchmem -count=6 -timeout=30m ./internal/server/ | tee bench_baseline.txt
# ... implement zero-copy ...
go test -tags=duckdb_arrow -run='^$' -bench=Benchmark -benchmem -count=6 -timeout=30m ./internal/server/ | tee bench_zerocopy.txt
benchstat bench_baseline.txt bench_zerocopy.txt
```

### Affected files

- `internal/server/metadata.go`
- `internal/server/primarykeys.go`
- `internal/server/foreignkeys.go`

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

## 6. Rate Limiting

No application-level or gateway-level rate limiting is configured.

### What's needed

- Add Envoy Gateway `BackendTrafficPolicy` rate limiting rules (per-tenant, per-IP).
- Consider application-level query rate limiting (queries per second per connection).
- Expose rate limit metrics to Prometheus.

### Why it matters

Defense against runaway clients or misconfigured ETL pipelines that flood the server
with queries. The connection pool provides backpressure, but a burst of cheap metadata
queries can still overwhelm the server.

---

## 7. Savepoints

`BeginSavepoint` and `EndSavepoint` are part of the Flight SQL spec but not implemented.

### What's needed

- Implement `BeginSavepoint` → `SAVEPOINT <name>` on the transaction's connection.
- Implement `EndSavepoint` → `RELEASE SAVEPOINT <name>` or `ROLLBACK TO SAVEPOINT <name>`.
- DuckDB supports savepoints natively.

### Priority

Low. Most Flight SQL clients do not use savepoints. Implement on demand.

---

## ~~8. Polling for Long-Running Queries~~ (Done)

Implemented as part of cancellation (#3). `PollFlightInfoStatement` returns an immediately
ready `PollInfo` (synchronous model). If async execution is needed in the future, this can
be extended to return a retry descriptor and track query progress.

---

## 9. Static Extension Build

Extensions (Iceberg, httpfs) are currently downloaded at runtime on first boot.
This adds cold-start latency and requires network access.

### What's needed

Build a custom `libduckdb_bundle.a` with extensions statically linked, then compile
duckflight with the `duckdb_use_static_lib` build tag.

**Build DuckDB with extensions:**
```bash
git clone https://github.com/duckdb/duckdb.git && cd duckdb
git checkout <version matching duckdb-go-bindings>
make bundle-library \
  EXTENSION_CONFIGS='.github/config/extensions/iceberg.cmake' \
  BUILD_EXTENSIONS='icu;json;parquet;autocomplete'
```

**Build Go binary:**
```bash
CGO_ENABLED=1 \
  CGO_LDFLAGS="-lduckdb_bundle -lstdc++ -lm -ldl -L/path/to/build/release" \
  go build -tags="duckdb_arrow duckdb_use_static_lib" -o duckflight ./cmd/server
```

**Update boot SQL:** remove `INSTALL iceberg; LOAD iceberg` and disable auto-install
(`SET autoinstall_known_extensions = false`) since extensions are already embedded.

Adding more extensions in the future means adding their cmake config to
`EXTENSION_CONFIGS` (semicolon-separated) and rebuilding.

### What changes

- **Dockerfile**: multi-stage build gains a DuckDB compile stage.
- **CI**: needs to build `libduckdb_bundle.a` (cache between runs).
- **Version pinning**: DuckDB source version must match `duckdb-go-bindings` version exactly.

### Why it matters

- Zero cold-start latency — no extension downloads on pod startup.
- Air-gapped deployments work without network access.
- Deterministic builds — extension versions are pinned at compile time.

---

## 10. TLS

gRPC server currently runs without TLS. Encryption is delegated to the load balancer.

### What's needed

- Accept `TLS_CERT` and `TLS_KEY` environment variables.
- Configure gRPC server with `credentials.NewServerTLSFromFile`.
- Update Helm chart to support TLS secrets.

### When to implement

When deploying without a TLS-terminating load balancer, or when mTLS between
clients and server is required.

---

## 11. Load Testing

No load testing has been performed.

### What's needed

- Use `ghz` (gRPC benchmarking tool) or a custom Go client to generate load.
- Test scenarios:
  - Concurrent SELECT queries (pool saturation).
  - Mixed read/write workload.
  - Metadata endpoint throughput.
  - Large result set streaming (bytes/sec).
- Tune `POOL_SIZE`, `MAX_THREADS`, `MEMORY_LIMIT` based on results.
- Establish baseline performance numbers for documentation.

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
| 2   | Zero-copy metadata streaming             | Medium (performance)         | Medium     | P1       |
| 3   | ~~Cancellation & polling~~               | ~~Medium (usability)~~       | ~~Low~~    | Done     |
| 4   | ~~Bulk ingestion~~                       | ~~Medium (completeness)~~    | ~~Medium~~ | Done     |
| 5   | ~~Distributed tracing~~                  | ~~Medium (operability)~~     | ~~Low~~    | Done     |
| 6   | Rate limiting                            | Low (defense)                | Low        | P2       |
| 7   | TLS                                      | Low (deploy-dependent)       | Low        | P2       |
| 8   | Load testing                             | Medium (confidence)          | Medium     | P2       |
| 9   | Static extension build                   | Medium (cold start)          | Medium     | P2       |
| 10  | Savepoints                               | Low (niche)                  | Low        | P3       |
| 11  | ~~Polling~~                              | ~~Low (niche)~~              | ~~Medium~~ | Done     |
| 12  | ~~CGO memory leak testing~~              | ~~High (reliability)~~       | ~~Medium~~ | Done     |
