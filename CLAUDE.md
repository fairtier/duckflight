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
    metrics.go               Per-pool instruments (idle/max gauges, acquire duration, recycle counters)
  otelutil/otelutil.go       Shared OTel scope name, Tracer()/Meter(), span error recording
  telemetry/telemetry.go     OTel SDK setup: OTLP traces/logs, Prometheus metrics, stderr logs
  auth/middleware.go         Bearer token flight.ServerMiddleware (own header parsing, not arrow-go's); resolves the sid (JWT claim → cookie → peer)
  auth/jwt.go                HS256 token mint/verify with sid (session id) claim
  auth/cookie.go             HMAC-signed session cookie mint/verify/parse (Flight arrow_flight_session_id)
  ratelimit/middleware.go    Token-bucket rate limit flight.ServerMiddleware
  session/manager.go         Pins one DuckDB connection per Flight session, idle reaper
  session/metrics.go         Per-manager instruments (active gauge, created/evicted counters)
  server/
    server.go                DuckFlightSQLServer (embeds flightsql.BaseServer), SqlInfo registration, CloseSession → session.Manager.Close
    statements.go            GetFlightInfoStatement, DoGetStatement, DoPutCommandStatementUpdate, GetSchemaStatement; acquireConn routing
    prepared.go              CreatePreparedStatement, ClosePreparedStatement, DoGet/DoPut prepared
    transactions.go          BeginTransaction, EndTransaction — bind to session conn when present, pool conn otherwise
    metadata.go              DoGetCatalogs, DoGetDBSchemas, DoGetTables, DoGetTableTypes
    primarykeys.go           DoGetPrimaryKeys via duckdb_constraints()
    foreignkeys.go           DoGetImportedKeys, DoGetExportedKeys, DoGetCrossReference
    xdbctypeinfo.go          DoGetXdbcTypeInfo (23 DuckDB types mapped to JDBC types)
    metering.go              OTel instruments + meteredReader (row/byte counting incl. nested/dictionary data, max limit)
    tracing.go               Span attribute keys and their conventions
    recovery.go              Panic-recovery flight.ServerMiddleware (outermost)
    logging.go               loggingServer wrapper, GRPCLoggingMiddleware
test/
  iceberg_integration_test.go  Full-stack tests with testcontainers (Postgres, MinIO, Lakekeeper)
```

## Key Design Decisions

**Per-client sessions.** When a request carries an authenticated session id
(`auth.SessionIDFromContext`), every RPC for that client routes to a single
DuckDB connection pinned by `session.Manager`. Sid precedence: a
handshake-issued JWT's `sid` claim (a per-handshake UUID) → a valid echoed
session cookie (`sid = sha256(cookieID ‖ binding)`) → `sha256(token + peer
address)`. The cookie (`arrow_flight_session_id`, HMAC-signed, minted in
`auth/cookie.go`) identifies the *client* rather than the socket, so it
survives L7 proxies that pool upstream connections (the shipped Envoy
GRPCRoute does) — without it, clients sharing one token behind such a proxy
share the proxy's peer address and land on one session. Cookie echo is
client-opt-in (ADBC `adbc.flight.sql.rpc.with_cookie_middleware`, JDBC default
on); non-echoing clients keep the peer-derived fallback. The cookie's binding
is the raw token for static tokens but `issuer+subject` for OIDC, so OAuth
token refresh keeps the session. Forged/foreign cookies read as absent (fail
open to peer derivation — the sid mixes in the token/subject, so forging only
selects among the forger's own sessions). arrow-go's `flight/session`
middleware was deliberately not used: no TTL, unbounded store growth from
non-echoing clients, panics on store errors, no identity binding. Envoy
stickiness still hashes the `authorization` header (one token → one pod); the
cookie splits sessions *within* the pod. This makes DuckDB-native
`BEGIN`/`COMMIT`/`ROLLBACK`, `CREATE TEMP TABLE`, `SET`, `PRAGMA`, `ATTACH`,
and prepared statements behave the way standard SQL clients (SQLAlchemy, ADBC
DBAPI with `autocommit=False`, JDBC) expect. Anonymous (no-auth) requests fall
back to one-shot pool borrowing. Sessions idle longer than the resource TTL
(`max(10m, 2 × QUERY_TIMEOUT)`) are reaped on a 1-minute tick; an RPC arriving
on a reaped session gets a fresh one, but an open transaction bound to it fails
with `FailedPrecondition` rather than silently continuing in autocommit. See
[internal/session/manager.go](internal/session/manager.go).

**`acquireConn` precedence** (`internal/server/statements.go`):
1. explicit Flight transaction (`txnID != ""`) → the txn's pinned conn,
2. session id in ctx → the session's pinned conn (lock held for the call),
3. otherwise → one-shot pool conn.

**Query execution flow.** `GetFlightInfoStatement` caches the query under a
random handle. `DoGetStatement` looks it up (the entry stays, so a
single-endpoint DoGet retry works), claims it for execution via
`tracker.Start` — which refuses a handle a `CancelFlightInfo` already
cancelled — and streams Arrow batches via a channel.

**Transaction snapshot.** `BeginTransaction` forces snapshot initialization with
`SELECT 0 FROM duckdb_tables() LIMIT 0` immediately after `BEGIN`, because
DuckDB defers snapshot to first statement otherwise.

**Connections are never pooled dirty.** Every statement run on a client's
behalf is classified by DuckDB's parser first (`ArrowConn.ClassifyStatement`);
anything that leaves connection-local state behind — `BEGIN`, `SET`, `PRAGMA`,
`ATTACH`, DDL — marks the connection dirty, and `ArrowPool.Release` destroys a
dirty connection and boots a replacement instead of reusing it. Closing is what
makes it airtight: DuckDB rolls back the open transaction and drops temp
objects and setting overrides when the connection goes away. Without this, a
client that vanishes mid-transaction hands the next borrower a connection still
inside its transaction — `shouldSkipTxnControl` then skips that client's
`BEGIN` as redundant and the two silently share one transaction. Plain
SELECT/INSERT/UPDATE/DELETE never marks a connection dirty, so the hot path
still costs nothing.

**Prepared statements.** Query-string-based with bind parameters applied at
execute time (`DoGetPreparedStatement` binds the last parameter row through
DuckDB's positional bind; `DoPutPreparedStatementUpdate` loops all rows). A
statement created inside a Flight transaction records its `transaction_id` and
executes on that transaction's connection, so a rollback actually undoes it.
The schema probe wraps the query in `SELECT * FROM (…) LIMIT 0` rather than
appending `LIMIT 0`, which would land inside a trailing line comment and
execute the statement at prepare time.

**Metadata queries.** Use `information_schema` and `duckdb_constraints()`, and
route through `acquireConn` like every other statement so a session's temp
tables and a transaction's snapshot are visible. Per the Flight SQL spec, an
omitted catalog/schema filter does *not* narrow the result — narrowing to
`current_database()` would hide ATTACHed catalogs, so the Iceberg lake would
never appear in a client's schema tree.

**Write serialization.** `Engine.WriteMu` mutex exists but is currently unused
in shipping code. DuckDB handles single-writer semantics per connection.

**Middleware order** (`cmd/server/main.go`): recovery (outer) → logging → rate
limit → auth (inner). Recovery is outermost because gRPC does not recover
handler panics — one panic anywhere below it would otherwise kill the process
and every in-flight query with it. Rate limiting sits *above* auth so that
signature verification and JWKS lookups are themselves rate limited. Health
RPCs bypass both auth and the rate limiter: probes can't carry tokens, and
shedding a probe turns a load spike into a restart loop.

## Key Dependencies

| Package                                       | Version | Purpose                                  |
|-----------------------------------------------|---------|------------------------------------------|
| `github.com/apache/arrow-go/v18`              | v18.6.0 | Arrow types, Flight SQL server framework |
| `github.com/duckdb/duckdb-go/v2`              | v2.10500.0 | DuckDB driver with Arrow interface    |
| `github.com/prometheus/client_golang`         | v1.23.2 | Prometheus metrics                       |
| `google.golang.org/grpc`                      | v1.81.1 | gRPC framework                           |
| `github.com/apache/arrow-adbc/go/adbc`        | v1.10.0 | ADBC driver (used in tests)              |
| `github.com/testcontainers/testcontainers-go` | v0.42.0 | Docker containers for integration tests  |

## Observability

Everything goes through OpenTelemetry
([internal/telemetry/telemetry.go](internal/telemetry/telemetry.go)): metrics
are always exported to Prometheus on `METRIC_ADDR`, and traces plus logs go to
an OTLP collector when `OTEL_EXPORTER_OTLP_ENDPOINT` is set (logs also always go
to stderr via the `otelslog` bridge). Every package reports under one
instrumentation scope, `duckflight` ([internal/otelutil](internal/otelutil)).

### Metrics

Names below are the exported Prometheus names; the OTel instrument names are
the dotted equivalents. `flightsql.*` is protocol-level, `duckflight.*` is the
infrastructure underneath it.

| Metric                                    | Type      | Labels                                        |
|-------------------------------------------|-----------|-----------------------------------------------|
| `flightsql_queries_total`                 | Counter   | `status` (ok/error/timeout/canceled)          |
| `flightsql_query_duration_seconds`        | Histogram | —                                             |
| `flightsql_bytes_streamed_total`          | Counter   | —                                             |
| `flightsql_result_size_bytes`             | Histogram | — (per-result size, not a total)              |
| `flightsql_rows_streamed_total`           | Counter   | —                                             |
| `flightsql_rows_affected_total`           | Counter   | `operation` (update/prepared_update/ingest)   |
| `flightsql_active_queries`                | Gauge     | —                                             |
| `flightsql_transactions_total`            | Counter   | `action` (begin/commit/rollback/reaped)       |
| `flightsql_transactions_active`           | Gauge     | —                                             |
| `flightsql_prepared_statements_active`    | Gauge     | —                                             |
| `flightsql_ratelimit_rejected`            | Counter   | —                                             |
| `duckflight_pool_connections_idle`        | Gauge     | —                                             |
| `duckflight_pool_connections_max`         | Gauge     | —                                             |
| `duckflight_pool_acquire_duration_seconds`| Histogram | —                                             |
| `duckflight_pool_connections_recycled_total` | Counter | `reason` (dirty/discarded)                   |
| `duckflight_pool_connections_lost_total`  | Counter   | — (replacement failed to boot; pool shrank)   |
| `duckflight_sessions_active`              | Gauge     | —                                             |
| `duckflight_sessions_created_total`       | Counter   | —                                             |
| `duckflight_sessions_evicted_total`       | Counter   | `reason` (closed/reaped/reclaimed/shutdown)   |

The gauges are OTel observable instruments whose callbacks read live server
state; each is unregistered when its pool/manager/server closes, so a shut-down
component stops reporting instead of reporting stale numbers.

### Spans

gRPC server spans come from `otelgrpc` (health checks filtered out). Below them:

| Span                | Where                                        |
|---------------------|----------------------------------------------|
| `session.acquire`   | per-RPC session lookup incl. lock wait       |
| `pool.acquire`      | one-shot pool borrow (anonymous requests)    |
| `statement.execute` | DuckDB execution of an ad-hoc statement      |
| `statement.stream`  | delivery of result batches to the client     |
| `statement.update`  | DoPut update                                 |
| `statement.ingest`  | bulk ingestion                               |
| `schema.probe`      | GetSchema `LIMIT 0` probe                    |
| `prepared.create` / `prepared.execute` / `prepared.stream` / `prepared.update` | prepared statement lifecycle |
| `metadata.query`    | catalog/schema/table/keys endpoints (`flight.metadata_kind`) |
| `transaction.begin` / `transaction.end` | Flight transaction RPCs  |

`statement.execute` ends when DuckDB returns; `statement.stream` covers the
goroutine that feeds batches to the client, which is where most of the wall
clock of a large result goes.

**Attributes, not spans, for facts about a call.** `db.connection.source`
(transaction/session/pool) records which rung of `acquireConn`'s precedence
served the request — the first thing to check when a client reports a missing
temp table. `auth.method` and `session.source` (jwt/cookie/peer) record how the
session id was derived. `db.connection.dirty` says whether the statement will
force the connection to be recycled. Deliberately absent: the authenticated
subject — a username or OIDC `sub` is a user identifier and spans go to the
collector. SQL text (`db.statement`) is DEBUG-gated for the same reason logs
are (see `statementAttr` in [internal/server/logging.go](internal/server/logging.go)):
statements carry credentials in `CREATE SECRET`/`ATTACH` and PII in literals.

**Events, not spans, for instants.** `pool.exhausted` (the caller had to wait
for a connection), `session.reclaimed_idle` (a live session lost its connection
to pool pressure), `result.limit_exceeded` (`MAX_RESULT_BYTES` tripped, with
bytes/rows already sent), `txn.control.skipped` (a redundant BEGIN/COMMIT was
no-oped), `transaction.attached_to_existing`, `stream.canceled`.

None of the unbounded identifiers — session id, statement handle, transaction
id — are ever used as metric attributes; they live on spans only.

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
- `RECONCILE_SQL_PATH` — operator-managed SQL file (typically a mounted
  Secret), executed instance-wide at startup and re-executed on content
  change (`internal/engine/reconcile.go`). DuckDB temporary secrets and
  loaded extensions are instance-wide, so rotation reaches every pooled
  connection without a restart. Errors report a label, never the SQL text.
- `REJECT_CLIENT_EXTENSIONS` — refuse client-issued `INSTALL`/`LOAD` (both
  classify as `STATEMENT_TYPE_LOAD`); extensions become operator-only
- `TEMP_DIRECTORY` — DuckDB spill directory; without it an in-memory engine
  errors on larger-than-memory operations

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
