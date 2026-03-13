# DuckFlight - Project Guide

## What is this

A Flight SQL server backed by DuckDB with native Apache Iceberg support. Standard Flight SQL clients (ADBC, JDBC, Python, Go) connect and query Iceberg tables over gRPC. One deployment per tenant, stateless compute, horizontally scalable.

## Build & Test

```bash
# Build (CGO required, gcc must be available)
go build -tags=duckdb_arrow -o duckflight ./cmd/server

# Unit + protocol tests
go test -tags=duckdb_arrow -race -count=1 ./internal/... ./test/...

# Iceberg integration tests (requires Docker)
go test -tags="duckdb_arrow iceberg_integration" -race -count=1 ./test/...

# Lint
golangci-lint run --build-tags=duckdb_arrow
```

The `duckdb_arrow` build tag is **always required** — it enables the Arrow C Data Interface in go-duckdb.

## Architecture

```
cmd/server/main.go           Entry point, env config, graceful shutdown
internal/
  config/config.go           Config struct + DefaultConfig()
  engine/
    engine.go                DuckDB connector lifecycle, boot SQL, Iceberg ATTACH
    pool.go                  Bounded channel-based ArrowConn pool
  auth/middleware.go         Bearer token gRPC interceptors (unary + stream)
  server/
    server.go                DuckFlightSQLServer (embeds flightsql.BaseServer), SqlInfo registration
    statements.go            GetFlightInfoStatement, DoGetStatement, DoPutCommandStatementUpdate, GetSchemaStatement
    prepared.go              CreatePreparedStatement, ClosePreparedStatement, DoGet/DoPut prepared
    transactions.go          BeginTransaction, EndTransaction (pool-based, not session-based)
    metadata.go              DoGetCatalogs, DoGetDBSchemas, DoGetTables, DoGetTableTypes
    primarykeys.go           DoGetPrimaryKeys via duckdb_constraints()
    foreignkeys.go           DoGetImportedKeys, DoGetExportedKeys, DoGetCrossReference
    xdbctypeinfo.go          DoGetXdbcTypeInfo (23 DuckDB types mapped to JDBC types)
    metering.go              Prometheus metrics + meteredReader (byte counting, max limit)
    logging.go               loggingServer wrapper, GRPCLoggingMiddleware
test/
  iceberg_integration_test.go  Full-stack tests with testcontainers (Postgres, MinIO, Lakekeeper)
```

## Key Design Decisions

**Stateless sessions.** No SessionManager or dedicated session connections. Transactions borrow a pool connection for their lifetime (BEGIN to COMMIT/ROLLBACK), then release it. `CloseSession` is a no-op.

**Query execution flow.** `GetFlightInfoStatement` caches the query under a random handle. `DoGetStatement` executes it (load-and-delete from cache), streams Arrow batches via channel.

**Transaction snapshot.** `BeginTransaction` forces snapshot initialization with `SELECT 0 FROM duckdb_tables() LIMIT 0` immediately after `BEGIN`, because DuckDB defers snapshot to first statement otherwise.

**Prepared statements.** Query-string-based. Parameters are accepted (`DoPutPreparedStatementQuery`) but not yet applied — the query runs as-is.

**Metadata queries.** Use `information_schema` and `duckdb_constraints()`. When catalog filter is unset, defaults to `current_database()`. When schema filter is unset, defaults to `current_schema()`.

**Write serialization.** `Engine.WriteMu` mutex exists but is currently unused in shipping code. DuckDB handles single-writer semantics per connection.

## Key Dependencies

| Package                                       | Version | Purpose                                  |
|-----------------------------------------------|---------|------------------------------------------|
| `github.com/apache/arrow-go/v18`              | v18.5.2 | Arrow types, Flight SQL server framework |
| `github.com/duckdb/duckdb-go/v2`              | v2.5.5  | DuckDB driver with Arrow interface       |
| `github.com/prometheus/client_golang`         | v1.23.2 | Prometheus metrics                       |
| `google.golang.org/grpc`                      | v1.79.2 | gRPC framework                           |
| `github.com/apache/arrow-adbc/go/adbc`        | v1.10.0 | ADBC driver (used in tests)              |
| `github.com/testcontainers/testcontainers-go` | v0.40.0 | Docker containers for integration tests  |

## Prometheus Metrics

| Metric                             | Type      | Labels                      |
|------------------------------------|-----------|-----------------------------|
| `flightsql_queries_total`          | Counter   | `status` (ok/error/timeout) |
| `flightsql_query_duration_seconds` | Histogram | —                           |
| `flightsql_bytes_streamed_total`   | Counter   | —                           |
| `flightsql_active_queries`         | Gauge     | —                           |

## Configuration

All via environment variables. See README.md for the full table. Key ones:
- `LISTEN_ADDR` (default `0.0.0.0:31337`) — gRPC server
- `METRIC_ADDR` (default `0.0.0.0:9090`) — Prometheus metrics
- `MEMORY_LIMIT`, `MAX_THREADS`, `QUERY_TIMEOUT`, `POOL_SIZE` — DuckDB tuning
- `ICEBERG_*` — Iceberg REST Catalog connection (optional)
- `S3_*` — S3 storage credentials (optional, for when catalog doesn't vend credentials)
- `AUTH_TOKENS` — comma-separated bearer tokens (empty = auth disabled)
- `LOG_LEVEL` — slog level (DEBUG, INFO, WARN, ERROR)

## Testing Patterns

- **server_test.go** — Uses native `flightsql.Client` against in-process server
- **adbc_test.go** — Uses ADBC driver + `database/sql` for compatibility testing
- **metering_test.go** — Unit tests for meteredReader
- **engine_test.go** — Engine init and pool tests
- **middleware_test.go** — Auth token validation
- **iceberg_integration_test.go** — Full Docker stack via testcontainers (build tag: `iceberg_integration`)

Test helper: `server.SeedSQL(ctx, sql)` executes setup SQL on the global engine.

## Deployment

- **Dockerfile**: multi-stage (golang:1.26 builder, debian:bookworm-slim runtime)
- **docker-compose.yml**: full local stack (PostgreSQL, MinIO, Lakekeeper, DuckFlight)
- **helm/duckflight/**: Helm chart with Deployment, Service, HPA, GRPCRoute, BackendTrafficPolicy
- **CI**: `.github/workflows/ci.yml` (lint + test), `release.yml` (Docker to GHCR), `release-helm.yml`
