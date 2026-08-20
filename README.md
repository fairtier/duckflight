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

- Go 1.26+ with CGO enabled (`gcc` must be available)
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
| `REJECT_CLIENT_EXTENSIONS` | `false`      | `true`/`1` to refuse client-issued `INSTALL`/`LOAD` statements (`PermissionDenied`); extensions are then managed exclusively via `EXTENSION_DIR` and reconcile SQL |
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
  auth/              gRPC bearer token middleware
  config/            Config struct and defaults
  engine/            DuckDB lifecycle, Arrow connection pool
  server/
    server.go          FlightSQL server core, SqlInfo registration
    statements.go      Query execution, DML
    prepared.go        Prepared statements
    transactions.go    BEGIN/COMMIT/ROLLBACK
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
