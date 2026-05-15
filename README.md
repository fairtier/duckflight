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
- **Horizontally scalable** — scale replicas behind a load balancer with session stickiness via auth header hashing
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

The Docker image compiles DuckDB from source with extensions (Iceberg, httpfs, JSON, Parquet) statically linked. No extension downloads happen at runtime — the image works in air-gapped environments. The DuckDB version is derived automatically from the `duckdb-go` module in `go.mod`.

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
| `MAX_RESULT_BYTES`      | `0`             | Max bytes per query result (0 = unlimited)                                                                             |
| `AUTH_TOKENS`           |                 | Comma-separated opaque bearer tokens (long-lived API keys)                                                             |
| `AUTH_USERS`            |                 | Comma-separated `user:password` pairs for the Flight Handshake basic-auth flow                                         |
| `AUTH_JWT_SECRET`       |                 | HMAC secret signing handshake-issued JWTs (required when `AUTH_USERS` is set; auto-generated otherwise with a warning) |
| `AUTH_JWT_TTL`          | `1h`            | Lifetime of handshake-issued JWTs                                                                                      |
| `OIDC_ISSUER`           |                 | OIDC issuer URL; enables JWT validation against its JWKS                                                               |
| `OIDC_AUDIENCE`         |                 | Optional; if set, JWT `aud` claim must contain this value                                                              |
| `RATE_LIMIT_RPS`        | `0`             | Max requests per second (0 = disabled)                                                                                 |
| `RATE_LIMIT_BURST`      | `0`             | Burst size (0 = defaults to RPS value)                                                                                 |
| `TLS_CERT`              |                 | Path to TLS certificate file (PEM)                                                                                     |
| `TLS_KEY`               |                 | Path to TLS private key file (PEM)                                                                                     |
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
| `LOG_LEVEL`             | `INFO`          | Log level: DEBUG, INFO, WARN, ERROR                                                                                    |

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

`AUTH_JWT_SECRET` must be set explicitly for multi-replica deployments so any pod can verify any pod's tokens.

**OIDC** (`OIDC_ISSUER`) — clients fetch JWTs directly from your IdP; the server validates against its JWKS. ADBC's FlightSQL driver handles the OAuth2 flow including refresh:

```bash
databow --driver flightsql --uri grpc+tls://duckflight.example.com:443 \
  --option "adbc.flight.sql.oauth.flow=client_credentials" \
  --option "adbc.flight.sql.oauth.token_uri=https://idp.example.com/oauth/token" \
  --option "adbc.flight.sql.oauth.client_id=…" \
  --option "adbc.flight.sql.oauth.client_secret=…" \
  --query "SELECT 1"
```

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
    metering.go        Prometheus metrics, metered reader
    logging.go         Structured Flight SQL + gRPC logging
helm/duckflight/     Helm chart for Kubernetes deployment
test/                Iceberg integration tests (testcontainers)
docs/                Design documents for planned features
```

## License

TODO
