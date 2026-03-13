# DuckFlight — Future Plan

Current state as of 2026-03-13: the core Flight SQL server is complete and deployed.
See CLAUDE.md for architecture and current implementation details.

---

## 1. Prepared Statement Parameter Binding

`DoPutPreparedStatementQuery` accepts parameter batches but does not apply them.
The query runs as-is regardless of bound parameters.

### What's needed

- Extract parameter values from the Arrow record batch sent by the client.
- Rewrite the prepared query to inject parameters via DuckDB's `$1`, `$2` positional syntax,
  or use `duckdb.Arrow.QueryContext` with `args ...any`.
- Verify behavior with JDBC and ADBC clients that rely on parameterized queries.

### Why it matters

Parameterized queries are required for:
- SQL injection safety when clients build queries dynamically.
- Query plan caching (DuckDB can reuse plans for parameterized queries).
- JDBC driver compatibility — many BI tools use prepared statements exclusively.

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

## 9. Extension Caching for Kubernetes

The Iceberg extension is downloaded on first `INSTALL iceberg` call. In Kubernetes,
pods restart frequently, causing repeated downloads.

### Options

- **Init container**: pre-download extensions to a shared volume.
- **PVC**: mount a persistent volume for `~/.duckdb/extensions/`.
- **Custom DuckDB build**: statically link the Iceberg extension.
- **Extension repository mirror**: host extensions internally for air-gapped clusters.

### Why it matters

Cold start latency. First boot downloads ~50MB of extensions. In auto-scaling scenarios,
new replicas are slow to become ready.

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

| #  | Feature                              | Impact                 | Effort | Priority |
|----|--------------------------------------|------------------------|--------|----------|
| 1  | Prepared statement parameter binding | High (correctness)     | Medium | P0       |
| 2  | Zero-copy metadata streaming         | Medium (performance)   | Medium | P1       |
| 3  | Cancellation                         | Medium (usability)     | Low    | P1       |
| 4  | Bulk ingestion                       | Medium (completeness)  | Medium | P2       |
| 5  | Distributed tracing                  | Medium (operability)   | Low    | P2       |
| 6  | Rate limiting                        | Low (defense)          | Low    | P2       |
| 7  | TLS                                  | Low (deploy-dependent) | Low    | P2       |
| 8  | Load testing                         | Medium (confidence)    | Medium | P2       |
| 9  | Extension caching                    | Low (cold start)       | Low    | P3       |
| 10 | Savepoints                           | Low (niche)            | Low    | P3       |
| 11 | Polling                              | Low (niche)            | Medium | P3       |
