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

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `0.0.0.0:31337` | gRPC listen address |
| `METRIC_ADDR` | `0.0.0.0:9090` | Prometheus metrics address |
| `MEMORY_LIMIT` | `1GB` | DuckDB memory limit |
| `MAX_THREADS` | `4` | DuckDB thread count |
| `QUERY_TIMEOUT` | `30s` | DuckDB statement timeout |
| `POOL_SIZE` | `8` | Arrow connection pool size |
| `MAX_RESULT_BYTES` | `0` | Max bytes per query result (0 = unlimited) |
| `AUTH_TOKENS` | | Comma-separated bearer tokens (empty = auth disabled) |
| `ICEBERG_ENDPOINT` | | Iceberg REST Catalog URL |
| `ICEBERG_WAREHOUSE` | | Warehouse name to ATTACH |
| `ICEBERG_CLIENT_ID` | | OAuth2 client ID for catalog auth |
| `ICEBERG_CLIENT_SECRET` | | OAuth2 client secret |
| `ICEBERG_OAUTH2_URI` | | OAuth2 token endpoint |
| `S3_ENDPOINT` | | S3-compatible storage endpoint |
| `S3_ACCESS_KEY` | | S3 access key |
| `S3_SECRET_KEY` | | S3 secret key |
| `S3_REGION` | | S3 region |
| `S3_URL_STYLE` | | `path` for MinIO, `vhost` for AWS |

## Connecting

Any Flight SQL client works. Example with the ADBC Go driver:

```go
db, _ := sql.Open("flightsql", "grpc://localhost:31337")
rows, _ := db.QueryContext(ctx, "SELECT * FROM lake.default.my_table LIMIT 10")
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
cmd/server/          Entry point, env-based configuration
internal/
  auth/              gRPC bearer token middleware
  config/            Config struct and defaults
  engine/            DuckDB lifecycle, Arrow connection pool
  server/            Flight SQL server implementation
    metadata.go        Catalog/schema/table endpoints
    statements.go      Query execution
    transactions.go    BEGIN/COMMIT/ROLLBACK
    metering.go        Prometheus metrics, metered reader
    prepared.go        Prepared statements
helm/duckflight/     Helm chart for Kubernetes deployment
test/                Flight SQL conformance and integration tests
```

## License

TODO
