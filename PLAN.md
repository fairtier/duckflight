# DuckFlight — Future Plan

Current state as of 2026-03-13: the core Flight SQL server is complete and deployed.
See CLAUDE.md for architecture and current implementation details.

---

## ~~1. Prepared Statement Parameter Binding~~ (Done)

Implemented. Parameters are extracted from Arrow record batches via `scalar.GetScalar`,
converted to Go values, and passed to `QueryContext`/`ExecContext` as positional args.
Supports single-row and multi-row (batch) parameter binding. Tested with `flightsql.Client`.

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

## 3. Flight SQL Cancellation

`SqlInfoFlightSqlServerCancel` is currently `false`. Clients cannot cancel in-flight queries.

### What's needed

- Implement `CancelFlightInfo` on the server.
- Track active query contexts in a map (handle -> cancel func).
- On cancel request, call the context cancel function.
- DuckDB respects context cancellation via `QueryContext`, so the query should abort.
- Set `SqlInfoFlightSqlServerCancel` to `true` once implemented.

### Why it matters

Long-running analytical queries against large Iceberg tables need a cancel mechanism.
Without it, the only recourse is waiting for `statement_timeout` or killing the connection.

---

## 4. Bulk Ingestion (`DoPutCommandStatementIngest`)

Currently not implemented. Clients cannot stream Arrow batches for INSERT.

### What's needed

- Implement `DoPutCommandStatementIngest` to accept Arrow record batches from clients.
- Use `duckdb.Arrow.RegisterView` to register the incoming batch as a virtual table,
  then `INSERT INTO <target> SELECT * FROM <view>`.
- Handle schema validation (incoming schema must match target table).
- Enforce write serialization if needed.

### Why it matters

Bulk ingestion is the standard Flight SQL mechanism for loading data. BI tools and ETL
pipelines use it to push data without constructing INSERT statements.

---

## 5. Distributed Tracing (OpenTelemetry)

Only Prometheus metrics are implemented. No request-level tracing.

### What's needed

- Add OpenTelemetry gRPC interceptors (unary + stream).
- Propagate trace context through DuckDB query execution.
- Export spans to OTLP collector.
- Key spans: gRPC method, query execution, pool acquire/release, transaction lifecycle.

### Why it matters

In a multi-service deployment (LB -> DuckFlight -> Iceberg catalog -> S3), tracing
is essential for diagnosing latency. Without it, operators cannot distinguish between
slow queries, slow catalog lookups, and slow object storage reads.

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

## 8. Polling for Long-Running Queries

`PollFlightInfo` methods exist in the Flight SQL spec but are not implemented.

### What's needed

- For long-running queries, `GetFlightInfoStatement` could return a "not ready" FlightInfo.
- `PollFlightInfoStatement` would check if results are ready and return updated FlightInfo.
- Requires decoupling query submission from execution (async execution model).

### Priority

Low. Current synchronous model works for most workloads. Consider if query timeouts
become a problem for legitimate long queries that clients want to poll.

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

## Priority Order

| #  | Feature                                  | Impact                 | Effort     | Priority |
|----|------------------------------------------|------------------------|------------|----------|
| 1  | ~~Prepared statement parameter binding~~ | ~~High (correctness)~~ | ~~Medium~~ | Done     |
| 2  | Zero-copy metadata streaming             | Medium (performance)   | Medium     | P1       |
| 3  | Cancellation                             | Medium (usability)     | Low        | P1       |
| 4  | Bulk ingestion                           | Medium (completeness)  | Medium     | P2       |
| 5  | Distributed tracing                      | Medium (operability)   | Low        | P2       |
| 6  | Rate limiting                            | Low (defense)          | Low        | P2       |
| 7  | TLS                                      | Low (deploy-dependent) | Low        | P2       |
| 8  | Load testing                             | Medium (confidence)    | Medium     | P2       |
| 9  | Static extension build                   | Medium (cold start)    | Medium     | P2       |
| 10 | Savepoints                               | Low (niche)            | Low        | P3       |
| 11 | Polling                                  | Low (niche)            | Medium     | P3       |
