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

## 12. CGO Memory Leak Testing

The duckdb-go Arrow integration crosses the CGO boundary heavily — every query allocates
Arrow buffers in C that must be freed on the Go side. Leaks are easy to introduce and
hard to detect without dedicated tooling.

### What's needed

- Use `memory.CheckedAllocator` in tests to assert zero leaked bytes after each operation.
- Cover key paths: query execution, prepared statements, bulk ingest, metadata endpoints,
  transaction lifecycle (BEGIN → query → COMMIT/ROLLBACK).
- Add a long-running soak test that executes queries in a loop and asserts stable RSS
  (e.g. via `/proc/self/status` or `runtime.MemStats`).
- Consider running tests under Valgrind or AddressSanitizer for C-side leak detection.

### Why it matters

A slow CGO memory leak in production is invisible to Go's runtime and pprof — RSS grows
while `runtime.MemStats` stays flat. The process eventually OOMs with no actionable signal.
The Arrow C Data Interface makes this especially risky because ownership transfer between
C and Go is implicit and easy to get wrong.

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
| 12  | CGO memory leak testing                  | High (reliability)           | Medium     | P1       |
