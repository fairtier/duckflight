# DuckFlight

A Flight SQL server backed by [DuckDB](https://duckdb.org/) with native [Apache Iceberg](https://iceberg.apache.org/) support. Any standard Flight SQL client (ADBC, JDBC, Python, Go) can connect and query data stored in Iceberg tables — no custom SDKs required.

## How it works

DuckFlight embeds DuckDB as an in-memory compute engine and attaches to an Iceberg REST Catalog. After attaching, all Iceberg tables are queryable with standard SQL. DuckDB handles query execution and streams results back as Arrow record batches over the Flight SQL protocol.

```
Flight SQL Client (ADBC/JDBC/Python/Go)
        │
        │ gRPC
        ▼
   DuckFlight Server
        │
     DuckDB (in-memory)
        │
     ATTACH iceberg
        │
        ▼
  Iceberg REST Catalog ──► Object Storage (S3/GCS/MinIO)
```

### Key properties

- **One deployment per tenant** — no multi-tenant multiplexing; isolation comes from deployment isolation
- **Stateless compute** — DuckDB is ephemeral; all durable state lives in Iceberg
- **Horizontally scalable** — scale replicas behind a load balancer with session stickiness via auth header hashing; per-client sessions survive L7 proxies via a server-minted session cookie (`arrow_flight_session_id`)
- **Full Flight SQL protocol** — queries, prepared statements, transactions, catalog metadata

## Getting started

### Prerequisites

- Go 1.27+ with CGO enabled (`gcc` must be available)
- Docker and Docker Compose (for local development with Iceberg)

### Build

```bash
go build -tags=duckdb_arrow -o duckflight ./cmd/server
```

The `duckdb_arrow` build tag is required to enable the Arrow interface in go-duckdb.

### Docker build

```bash
docker build -t duckflight .
```

The Docker image pre-downloads DuckDB extensions at build time into `/extensions` (JSON, Parquet and ICU are already built into the `duckdb-go` binary). No extension downloads happen at runtime — the image works in air-gapped environments. The DuckDB version is derived automatically from the `duckdb-go` module in `go.mod`.

The extension set is build-arg configurable without forking the Dockerfile:

```bash
# core repo (extensions.duckdb.org) and community repo (community-extensions.duckdb.org)
docker build \
  --build-arg CORE_EXTENSIONS="iceberg avro httpfs" \
  --build-arg COMMUNITY_EXTENSIONS="gsheets" \
  -t duckflight-custom .

# extensions-only image, meant to run as an initContainer that copies
# /extensions into an emptyDir mounted over the server's EXTENSION_DIR
# (see helm values `extensions.image`). Build it from the same git ref as
# the server image — extension files are tied to the DuckDB version.
docker build --target extensions \
  --build-arg COMMUNITY_EXTENSIONS="gsheets" \
  -t duckdb-extensions .
```

### Run locally with Iceberg

The included `docker-compose.yml` starts a full local stack: PostgreSQL, MinIO (S3-compatible storage), [Lakekeeper](https://github.com/lakekeeper/lakekeeper) (Iceberg REST Catalog), and DuckFlight.

```bash
docker compose up
```

Once running, the Flight SQL server is available at `localhost:31337` and Prometheus metrics at `localhost:9090/metrics`.

### Run standalone (no Iceberg)

```bash
LISTEN_ADDR=0.0.0.0:31337 ./duckflight
```

Without Iceberg configuration, the server starts with a plain in-memory DuckDB — useful for testing the Flight SQL protocol.

## Configuration

All configuration is via environment variables:

| Variable                | Default         | Description                                                                                                            |
|-------------------------|-----------------|------------------------------------------------------------------------------------------------------------------------|
| `LISTEN_ADDR`           | `0.0.0.0:31337` | gRPC listen address                                                                                                    |
| `METRIC_ADDR`           | `0.0.0.0:9090`  | Prometheus metrics address                                                                                             |
| `MEMORY_LIMIT`          | `1GB`           | DuckDB memory limit                                                                                                    |
| `MAX_THREADS`           | `4`             | DuckDB thread count                                                                                                    |
| `QUERY_TIMEOUT`         | `30s`           | DuckDB statement timeout                                                                                               |
| `POOL_SIZE`             | `8`             | Arrow connection pool size                                                                                             |
| `MAX_RESULT_BYTES`      | `0`             | Max bytes per query result, counting nested/dictionary data (0 = unlimited). Exceeding it fails the stream with `ResourceExhausted` |
| `AUTH_TOKENS`           |                 | Comma-separated opaque bearer tokens (long-lived API keys)                                                             |
| `AUTH_USERS`            |                 | Comma-separated `user:password` pairs for the Flight Handshake basic-auth flow                                         |
| `AUTH_JWT_SECRET`       |                 | HMAC secret signing handshake-issued JWTs (used when `AUTH_USERS` is set; min 32 bytes; auto-generated with a warning if unset) |
| `AUTH_JWT_TTL`          | `1h`            | Lifetime of handshake-issued JWTs                                                                                      |
| `OIDC_ISSUER`           |                 | OIDC issuer URL; enables JWT validation against its JWKS                                                               |
| `OIDC_AUDIENCE`         |                 | Optional; if set, JWT `aud` claim must contain this value                                                              |
| `RATE_LIMIT_RPS`        | `0`             | Max requests per second (0 = disabled)                                                                                 |
| `RATE_LIMIT_BURST`      | `0`             | Burst size (0 = defaults to RPS value)                                                                                 |
| `TLS_CERT`              |                 | Path to TLS certificate file (PEM). Must be set together with `TLS_KEY`                                                |
| `TLS_KEY`               |                 | Path to TLS private key file (PEM). Must be set together with `TLS_CERT`                                               |
| `TLS_CA`                |                 | Path to CA cert for client verification (mTLS)                                                                         |
| `ICEBERG_ENDPOINT`      |                 | Iceberg REST Catalog URL                                                                                               |
| `ICEBERG_WAREHOUSE`     |                 | Warehouse name to ATTACH                                                                                               |
| `ICEBERG_CLIENT_ID`     |                 | OAuth2 client ID for catalog auth                                                                                      |
| `ICEBERG_CLIENT_SECRET` |                 | OAuth2 client secret                                                                                                   |
| `ICEBERG_OAUTH2_URI`    |                 | OAuth2 token endpoint                                                                                                  |
| `S3_ENDPOINT`           |                 | S3-compatible storage endpoint                                                                                         |
| `S3_ACCESS_KEY`         |                 | S3 access key                                                                                                          |
| `S3_SECRET_KEY`         |                 | S3 secret key                                                                                                          |
| `S3_REGION`             |                 | S3 region                                                                                                              |
| `S3_URL_STYLE`          |                 | `path` for MinIO, `vhost` for AWS                                                                                      |
| `EXTENSION_DIR`         | `/extensions` (image) | DuckDB `extension_directory` holding pre-installed extensions                                                    |
| `RECONCILE_SQL_PATH`    |                 | Path to a SQL file executed instance-wide at startup and re-executed whenever its content changes (~10s poll). Meant for operator-managed `CREATE OR REPLACE SECRET` / `LOAD` statements delivered as a mounted Secret; rotation reaches every pooled connection with no restart. Missing file = nothing to do; a failing file is logged (label only, never the SQL text) and does not stop the server |
| `REJECT_CLIENT_EXTENSIONS` | `false`      | `true`/`1` to refuse a client-issued `INSTALL`/`LOAD`/`UPDATE EXTENSIONS` (`PermissionDenied`), alone or anywhere in a multi-statement script — the script is split with DuckDB's parser and refused before any of it runs. Provision extensions via `EXTENSION_DIR` and reconcile SQL. Note this only covers extensions: for a caller that must not write or reach files/network, use a read-only credential (see [Read-only sessions](#read-only-sessions)) |
| `AUTH_READONLY_TOKENS`  |                 | Comma-separated bearer tokens whose every request is read-only (see [Read-only sessions](#read-only-sessions)). Setting it turns read-only sessions on. A token in both this and `AUTH_TOKENS` is read-only |
| `AUTH_READONLY_USERS`   |                 | Comma-separated usernames from `AUTH_USERS` whose handshake-issued tokens are read-only (decided per request, so it also applies to tokens already issued). An unknown name refuses startup. Setting it turns read-only sessions on |
| `READ_ONLY_SESSIONS`    | `false`         | `true`/`1` to start the read-only instance without any read-only credential, for callers that only self-restrict with the `x-duckflight-read-only` header |
| `READ_ONLY_POOL_SIZE`   | `POOL_SIZE`     | Connection pool of the read-only instance                                                                              |
| `READ_ONLY_MEMORY_LIMIT`| `MEMORY_LIMIT`  | Buffer pool of the read-only instance. It is separate from the main one: the process can use up to the sum of both     |
| `READ_ONLY_ALLOWED_DIRECTORIES` | `s3://` with Iceberg, else none | Comma-separated path/URL prefixes the read-only instance may open (DuckDB `allowed_directories`). Must cover the warehouse location; narrow it to the bucket, e.g. `s3://warehouse/`. Set but empty = nothing at all |
| `TEMP_DIRECTORY`        |                 | DuckDB `temp_directory`. Without it an in-memory engine errors on larger-than-memory operations instead of spilling to disk |
| `LOG_LEVEL`             | `INFO`          | Log level: DEBUG, INFO, WARN, ERROR. `DEBUG` also puts SQL text on logs and spans                                      |
| `OTEL_EXPORTER_OTLP_ENDPOINT` |           | OTLP collector (`host:port`, scheme optional). Enables trace and log export; empty = traces off, logs to stderr only    |
| `OTEL_EXPORTER_OTLP_INSECURE` | `false`   | `true`/`1` to talk to the collector without TLS                                                                        |
| `OTEL_SERVICE_NAME`     | `duckflight`    | `service.name` on exported telemetry                                                                                   |

## Connecting

Any Flight SQL client works. Example with the ADBC Go driver:

```go
db, _ := sql.Open("flightsql", "grpc://localhost:31337")
rows, _ := db.QueryContext(ctx, "SELECT * FROM lake.default.my_table LIMIT 10")
```

## Authentication

Three auth backends can be combined freely. With none configured, auth is disabled.

Examples below use [databow](https://github.com/columnar-tech/databow), an ADBC CLI client.

**Opaque bearer tokens** (`AUTH_TOKENS`) — long-lived API keys for service-to-service:

```bash
databow --driver flightsql --uri grpc+tcp://localhost:31337 \
  --option "adbc.flight.sql.authorization_header=Bearer my_token" \
  --query "SELECT 1"
```

**Local users via Flight Handshake** (`AUTH_USERS`) — username/password, server mints a short-lived JWT on Handshake:

```bash
AUTH_USERS="alice:secret123" AUTH_JWT_SECRET="$(openssl rand -hex 32)" ./duckflight
# ...
databow --driver flightsql --uri grpc+tcp://localhost:31337 \
  --username alice --password secret123 --query "SELECT 1"
```

`AUTH_JWT_SECRET` must be at least 32 bytes, and must be set explicitly for
multi-replica deployments so any pod can verify any pod's tokens.

The server refuses to start on a malformed value rather than falling back to a
default: a typo in `AUTH_USERS`, `RATE_LIMIT_RPS` or `MAX_RESULT_BYTES` would
otherwise silently disable authentication, rate limiting or the result cap. It
logs one explicit line at startup saying which auth backends are active, or
`AUTH DISABLED` when none are.

Each client gets its own server-side session, and with it a dedicated DuckDB
connection holding its temp tables, `SET` overrides and transactions. Session
identity comes from the handshake-issued JWT for `AUTH_USERS`. For
`AUTH_TOKENS`/OIDC the server mints an HMAC-signed session cookie
(`arrow_flight_session_id`) on first contact; a client that echoes it keeps a
stable per-client session no matter which connection — or which L7 proxy — its
requests arrive on. The request that obtains the cookie still runs on the
fallback session (token + peer address), so connection-local state created on
the very first RPC does not carry over. Clients that never echo cookies keep
the fallback behavior entirely: behind a proxy that pools upstream connections,
such clients sharing a single token may share a session — give them their own
tokens, or enable cookies. `CloseSession` releases the server-side session;
the cookie stays valid and simply names a fresh session on next use.

Enabling cookie echo per client:

- **ADBC** (all languages): `--option "adbc.flight.sql.rpc.with_cookie_middleware=true"` (off by default)
- **JDBC**: the Arrow Flight SQL driver handles cookies by default (`retainCookies=true`)
- **arrow-go**: pass `flight.NewClientCookieMiddleware()` when constructing the client

`AUTH_JWT_SECRET` also keys the cookie HMAC: set it for multi-replica
deployments so a cookie minted by one pod verifies on another. Without it each
process uses a random key, and foreign cookies just fall back to
peer-address-derived sessions.

**OIDC** (`OIDC_ISSUER`) — clients fetch JWTs directly from your IdP; the server validates against its JWKS. ADBC's FlightSQL driver handles the OAuth2 flow including refresh:

```bash
databow --driver flightsql --uri grpc+tls://duckflight.example.com:443 \
  --option "adbc.flight.sql.oauth.flow=client_credentials" \
  --option "adbc.flight.sql.oauth.token_uri=https://idp.example.com/oauth/token" \
  --option "adbc.flight.sql.oauth.client_id=…" \
  --option "adbc.flight.sql.oauth.client_secret=…" \
  --query "SELECT 1"
```

## Read-only sessions

A caller can be limited to reading. The credential decides — the client cannot
opt out:

- **`AUTH_READONLY_TOKENS`** — bearer tokens that are always read-only.
- **`AUTH_READONLY_USERS`** — `AUTH_USERS` names whose handshake tokens are read-only.
- **`x-duckflight-read-only: true`** (or `1`) — a request header with which a
  caller holding a read-write credential restricts *itself*, e.g. one service
  that runs both trusted queries and queries written by an AI agent. The header
  only ever takes privileges away; `false`/`0`/absent leave the credential's
  mode, any other value is refused (`InvalidArgument`) rather than guessed at.
  OIDC tokens have no read-only mapping yet; use the header.

```bash
AUTH_TOKENS="$RW_TOKEN" AUTH_READONLY_TOKENS="$AGENT_TOKEN" ./duckflight
databow --driver flightsql --uri grpc+tcp://localhost:31337 \
  --option "adbc.flight.sql.authorization_header=Bearer $AGENT_TOKEN" \
  --query "SELECT count(*) FROM lake.default.orders"
```

A read-only request:

1. is served by a **second DuckDB instance** with its own pool, sessions and
   memory limit. It boots like the main one (extensions, secrets, the Iceberg
   catalog — attached `READ_ONLY`) and then, before serving anything, sets
   `allowed_directories` (`READ_ONLY_ALLOWED_DIRECTORIES`),
   `enable_external_access = false` and `lock_configuration = true`. These
   settings are instance-wide and one-way in DuckDB, which is why it is a
   separate instance rather than a pool on the main one;
2. has every statement checked by **DuckDB's own parser** before it reaches
   that instance (`internal/sqlguard`, on a third, empty, isolated DuckDB
   instance): only what parses as a SELECT is allowed — `SELECT`, `WITH … SELECT`,
   `FROM tbl`, `VALUES`, `DESCRIBE`, `SHOW`, `SUMMARIZE`, plain `EXPLAIN` —
   a script only if every statement is; table functions by allowlist (generators,
   `duckdb_*` catalog functions, `iceberg_snapshots`/`iceberg_metadata`), no
   `read_*`/`glob`/`query()`/`duckdb_secrets()`/`getenv`/`FROM 'path'` scans.
   A refusal is `PermissionDenied` with a reason naming the construct; a syntax
   error is `InvalidArgument`;
3. cannot ingest (`DoPut` bulk ingestion is refused), cannot use a transaction a
   read-write request began, and gets `SqlInfoFlightSqlServerReadOnly = true`
   (and no DDL/ingestion support) from `GetSqlInfo`.

A read-only request to a server without read-only sessions enabled is refused
(`FailedPrecondition`), never served read-write. With no read-only setting at
all, nothing changes: no second instance is started.

The read-only instance sees the Iceberg lake exactly like the main one (tables
and commits made after it started included). It does **not** see the main
instance's in-memory tables, temp tables or `SET` overrides. `RECONCILE_SQL_PATH`
is applied to both instances; on the read-only one, statements that need
external access after its lockdown (`INSTALL`/`LOAD`, a secret type whose
extension was not loaded at boot) fail and are logged.

## Security

**Threat model.** Any holder of a read-write credential is fully trusted: it
runs arbitrary DuckDB SQL with the server's privileges — it can write and drop
lake tables, `COPY … TO 's3://…'`, read local files (including
`/proc/self/environ`, which holds the S3 and catalog credentials) and reach the
network. Read-write credentials belong to operators and trusted services only.
`REJECT_CLIENT_EXTENSIONS` keeps extension management server-side but is not a
privilege boundary.

A read-only credential is meant for a less trusted caller — an AI agent, an
ad-hoc explore UI. The boundary is two independent layers: the parser check
above, and the engine confinement below it, which holds on its own if the
parser check ever misses something. Verified against Lakekeeper + S3 in
`test/iceberg_integration_test.go`, the read-only *instance itself* (with the
parser check bypassed) refuses: writes to the lake (`READ_ONLY` attach); local
files and `http(s)://` URLs; any S3 path outside `allowed_directories`;
`INSTALL`/`LOAD`; any `SET`/`RESET`/`PRAGMA` of a setting
(`lock_configuration`); and unredacted secrets (`duckdb_secrets()` is redacted,
`redact := false` is refused).

**What a read-only caller can still do:**

- Read every table in the attached lake, and the catalog metadata. There is no
  per-table authorization.
- Consume resources: queries are bounded by `QUERY_TIMEOUT`,
  `READ_ONLY_MEMORY_LIMIT`, `MAX_RESULT_BYTES` and the rate limiter, not by
  anything read-only specific.
- Within `allowed_directories`, DuckDB itself permits raw reads *and writes*
  of objects. The parser check is what stops `read_parquet('s3://…')`,
  `FROM 's3://…'` and `COPY … TO` there; with the default `s3://` that is every
  bucket the S3 credential reaches. Narrow `READ_ONLY_ALLOWED_DIRECTORIES` to
  the warehouse bucket to shrink what a parser-check gap could expose.
- Call any scalar function not on the short denylist. The table-function
  allowlist and the scalar denylist in `internal/sqlguard/readonly.go` are the
  place to review on every DuckDB upgrade: a new function that touches files,
  the network or state is admitted by the denylist until it is named there
  (the engine confinement still applies to it).

**Known gaps.** The Iceberg REST catalog's HTTP traffic is not subject to
`allowed_directories` (it does not go through DuckDB's file system), which is
why Iceberg keeps working; a read-only caller can make the instance send
catalog lookups for any table name, but only to the configured catalog. The
DuckDB-specific `PIVOT t ON … USING …` *statement* is refused (the parser
check cannot serialize it); the SQL-standard `FROM t PIVOT (… FOR … IN …)`
and `UNPIVOT` work. OAuth2-authenticated catalogs were not exercised by the
integration tests (Lakekeeper runs without auth there), so token refresh after
the lockdown is untested.
Settings tried and rejected: `disabled_filesystems = 'LocalFileSystem'` is
global and breaks spilling to `temp_directory`; `enable_external_access =
false` at open time (DSN) prevents loading the Iceberg extensions at all.

## Observability

DuckFlight is instrumented with OpenTelemetry end to end.

**Metrics** are always on, in Prometheus format at `METRIC_ADDR` (`:9090/metrics`
by default). Beyond query counts, durations and bytes there are per-result size
and row counters, transaction and prepared-statement gauges, and the numbers
that explain saturation: pool idle/capacity, how long acquiring a connection
took, how many connections were recycled or lost, and how many sessions were
created or evicted (and why — `closed`, `reaped`, `reclaimed`, `shutdown`).

**Traces and logs** are exported when `OTEL_EXPORTER_OTLP_ENDPOINT` is set; logs
also always go to stderr. Spans cover the DuckDB execution and the result
streaming separately — for a large result most of the time a client waits is
delivery, after execution has already finished — plus session acquisition,
prepared statements, transactions and metadata endpoints. Spans carry the facts
you need to explain surprising behaviour: which connection a request was routed
to (`db.connection.source`: transaction, session or pool), how the session id
was derived (`auth.method`, `session.source`), rows and bytes delivered, and
events for pool exhaustion, idle-session reclamation and result-size cutoffs.

SQL text is attached to logs and spans only at `LOG_LEVEL=DEBUG`: statements
routinely carry credentials (`CREATE SECRET`, `ATTACH … (TOKEN …)`) and PII in
literals. Authenticated usernames and OIDC subjects are never put on spans.

## Kubernetes deployment

A Helm chart is included at `helm/duckflight/`:

```bash
helm install my-tenant helm/duckflight/ \
  --set iceberg.endpoint=https://catalog.example.com/v1 \
  --set iceberg.warehouse=my_warehouse
```

The chart includes: Deployment, Service, HPA, Gateway API GRPCRoute, and Envoy Gateway BackendTrafficPolicy for consistent-hash routing on the `authorization` header.

## Testing

```bash
# Unit and protocol conformance tests
go test -tags=duckdb_arrow ./test/... ./internal/...

# Iceberg integration tests (requires Docker)
go test -tags="duckdb_arrow iceberg_integration" ./test/...
```

## Project structure

```
cmd/server/          Entry point, env config, graceful shutdown
internal/
  auth/              gRPC bearer token middleware, read-only credentials
  config/            Config struct and defaults
  engine/            DuckDB lifecycle, Arrow connection pool, hardened read-only instance
  sqlguard/          Parser-based statement policy on an isolated DuckDB instance
  server/
    server.go          FlightSQL server core, SqlInfo registration
    statements.go      Query execution, DML
    prepared.go        Prepared statements
    transactions.go    BEGIN/COMMIT/ROLLBACK
    readonly.go        Read-only plane routing and statement policy
    metadata.go        Catalog/schema/table/table-type endpoints
    primarykeys.go     Primary key metadata
    foreignkeys.go     Imported/exported keys, cross-reference
    xdbctypeinfo.go    JDBC type info metadata
    metering.go        OpenTelemetry instruments, metered reader
    tracing.go         Span attribute keys and conventions
    logging.go         Structured Flight SQL + gRPC logging
  otelutil/          Shared instrumentation scope and span helpers
  telemetry/         OpenTelemetry SDK setup (OTLP traces/logs, Prometheus metrics)
helm/duckflight/     Helm chart for Kubernetes deployment
test/                Iceberg integration tests (testcontainers)
docs/                Design documents for planned features
```

## License

TODO
