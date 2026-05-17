# DuckFlight - Project Guide

## What is this

A Flight SQL server backed by DuckDB with native Apache Iceberg support.
Standard Flight SQL clients (ADBC, JDBC, Python, Go) connect and query Iceberg
tables over gRPC. One deployment per tenant, stateless compute, horizontally
scalable.

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

The `duckdb_arrow` build tag is **always required** — it enables the Arrow C
Data Interface in go-duckdb.

## Architecture

```
cmd/server/main.go           Entry point, env config, graceful shutdown
internal/
  config/config.go           Config struct + DefaultConfig()
  engine/
    engine.go                DuckDB connector lifecycle, boot SQL, Iceberg ATTACH
    pool.go                  Bounded channel-based ArrowConn pool
  auth/middleware.go         Bearer token flight.ServerMiddleware; stamps a sid into Handshake-issued JWTs
  auth/jwt.go                HS256 token mint/verify with sid (session id) claim
  ratelimit/middleware.go    Token-bucket rate limit flight.ServerMiddleware
  session/manager.go         Pins one DuckDB connection per Flight session, idle reaper
  server/
    server.go                DuckFlightSQLServer (embeds flightsql.BaseServer), SqlInfo registration, CloseSession → session.Manager.Close
    statements.go            GetFlightInfoStatement, DoGetStatement, DoPutCommandStatementUpdate, GetSchemaStatement; acquireConn routing
    prepared.go              CreatePreparedStatement, ClosePreparedStatement, DoGet/DoPut prepared
    transactions.go          BeginTransaction, EndTransaction — bind to session conn when present, pool conn otherwise
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

**Per-client sessions.** When a request carries an authenticated session id
(`auth.SessionIDFromContext`), every RPC for that client routes to a single
DuckDB connection pinned by `session.Manager`. Handshake-issued JWTs stamp a
fresh UUID into the `sid` claim; OIDC/static tokens derive a stable sid from
`sha256(token)`. This makes DuckDB-native `BEGIN`/`COMMIT`/`ROLLBACK`, `CREATE
TEMP TABLE`, `SET`, `PRAGMA`, `ATTACH`, and prepared statements behave the
way standard SQL clients (SQLAlchemy, ADBC DBAPI with `autocommit=False`,
JDBC) expect. Anonymous (no-auth) requests fall back to one-shot pool
borrowing. Idle sessions are reaped on a 1-minute tick. See
[internal/session/manager.go](internal/session/manager.go).

**`acquireConn` precedence** (`internal/server/statements.go`):
1. explicit Flight transaction (`txnID != ""`) → the txn's pinned conn,
2. session id in ctx → the session's pinned conn (lock held for the call),
3. otherwise → one-shot pool conn.

**Query execution flow.** `GetFlightInfoStatement` caches the query under a
random handle. `DoGetStatement` executes it (load-and-delete from cache),
streams Arrow batches via channel.

**Transaction snapshot.** `BeginTransaction` forces snapshot initialization with
`SELECT 0 FROM duckdb_tables() LIMIT 0` immediately after `BEGIN`, because
DuckDB defers snapshot to first statement otherwise.

**Prepared statements.** Query-string-based with bind parameters applied at
execute time (`DoGetPreparedStatement`/`DoPutPreparedStatementUpdate` pass the
last parameter row through DuckDB's positional bind).

**Metadata queries.** Use `information_schema` and `duckdb_constraints()`. When
catalog filter is unset, defaults to `current_database()`. When schema filter is
unset, defaults to `current_schema()`.

**Write serialization.** `Engine.WriteMu` mutex exists but is currently unused
in shipping code. DuckDB handles single-writer semantics per connection.

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
| `flightsql_ratelimit_rejected`     | Counter   | —                           |

## Configuration

All via environment variables. See README.md for the full table. Key ones:

- `LISTEN_ADDR` (default `0.0.0.0:31337`) — gRPC server
- `METRIC_ADDR` (default `0.0.0.0:9090`) — Prometheus metrics
- `MEMORY_LIMIT`, `MAX_THREADS`, `QUERY_TIMEOUT`, `POOL_SIZE` — DuckDB tuning
- `ICEBERG_*` — Iceberg REST Catalog connection (optional)
- `S3_*` — S3 storage credentials (optional, for when catalog doesn't vend
  credentials)
- `AUTH_TOKENS` — comma-separated bearer tokens (empty = auth disabled)
- `RATE_LIMIT_RPS`, `RATE_LIMIT_BURST` — global token-bucket rate limiter (0 =
  disabled)
- `LOG_LEVEL` — slog level (DEBUG, INFO, WARN, ERROR)

## Testing Patterns

- **server_test.go** — Uses native `flightsql.Client` against in-process server
- **adbc_test.go** — Uses ADBC driver + `database/sql` for compatibility testing
- **metering_test.go** — Unit tests for meteredReader
- **engine_test.go** — Engine init and pool tests
- **middleware_test.go** — Auth token validation
- **ratelimit/middleware_test.go** — Rate limit middleware tests
- **iceberg_integration_test.go** — Full Docker stack via testcontainers (
  `duckdb_arrow`)
- **sqlalchemy_integration_test.go** — Spins up a Python container running
  pytest against the in-process server. Exercises the gizmosql SQLAlchemy
  dialect and raw `adbc_driver_flightsql`. Build tag: `sqlalchemy_integration`.
  Python test sources live under `test/python/` and are embedded into the test
  binary.

Test helper: `server.SeedSQL(ctx, sql)` executes setup SQL on the global engine.

**Bare BEGIN/COMMIT/ROLLBACK** sent as SQL strings (e.g. by the gizmosql
SQLAlchemy dialect's `do_begin`/`do_commit`/`do_rollback`) are handled
natively by DuckDB: all RPCs for a session land on the same pinned
connection, so `ROLLBACK` after `BEGIN` works without any server-side
classification. The Flight RPC path
(`BeginTransaction`/`EndTransaction`) is independent — it gets its own txn
handle and binds to the session conn when one exists.

## Deployment

- **Dockerfile**: multi-stage (golang:1.26 builder, debian:bookworm-slim
  runtime)
- **docker-compose.yml**: full local stack (PostgreSQL, MinIO, Lakekeeper,
  DuckFlight)
- **helm/duckflight/**: Helm chart with Deployment, Service, HPA, GRPCRoute,
  BackendTrafficPolicy
- **CI**: `.github/workflows/ci.yml` (lint + test), `release.yml` (Docker to
  GHCR), `release-helm.yml`

## Reference implementation:

- https://github.com/gizmodata/gizmosql
- https://github.com/voltrondata/sqlflite
- https://github.com/apache/arrow-go/tree/main/arrow/flight/flightsql/example
