# Flight SQL Server on DuckDB with Iceberg — Implementation Plan v2

## 1. Product Context

A SaaS platform where each tenant gets their own Iceberg data lake. Tenants query their data through any standard Flight SQL client (ADBC, JDBC, Python, Go). The Flight SQL server is deployed per-tenant as a sidecar alongside their Iceberg catalog, with horizontal scaling, execution limits, and usage metering for billing.

### Design Constraints

- **Generic Flight SQL clients only.** No custom client SDKs, no smart-client routing. Everything must work with off-the-shelf ADBC/JDBC drivers.
- **One deployment per tenant.** Each tenant's Flight SQL service connects to exactly one Iceberg catalog.
- **Horizontally scalable.** Noisy tenants scale out replicas; quiet tenants run a single replica.
- **Stateful sessions and transactions** must work correctly across gRPC load balancing.

---

## 2. Architecture

```
                 Generic Flight SQL Clients
                 (ADBC, JDBC, Python, Go)
                           │
                           │ gRPC + auth header
                           ▼
                ┌─────────────────────┐
                │  Envoy Gateway      │
                │  (Gateway API)      │
                │                     │
                │  consistent hash on │
                │  authorization hdr  │
                └──┬──────┬──────┬────┘
                   │      │      │
            ┌──────▼──┐ ┌─▼────┐ ▼──────┐
            │Replica 0│ │Rep. 1│ │Rep. N │    ◄── HPA scales these
            │         │ │      │ │       │
            │ Flight  │ │      │ │       │
            │ SQL Srv │ │      │ │       │
            │    │    │ │      │ │       │
            │ DuckDB  │ │      │ │       │
            │(in-mem) │ │      │ │       │
            │    │    │ │      │ │       │
            │ ATTACH  │ │      │ │       │
            │ iceberg │ │      │ │       │
            └────┬────┘ └──┬───┘ └───┬───┘
                 │         │         │
                 └─────────┼─────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │ Iceberg REST      │
                 │ Catalog           │
                 │ (per-tenant)      │
                 └────────┬──────────┘
                          │
                          ▼
                 ┌───────────────────┐
                 │ Object Storage    │
                 │ (S3 / GCS / MinIO)│
                 └───────────────────┘
```

### Key Architectural Decisions

**One service = one tenant = one DuckDB = one Iceberg ATTACH.** No multi-tenant multiplexing within a process. Tenant isolation comes from deployment isolation. Each replica is an independent, stateless-ish compute node that attaches to the same Iceberg catalog.

**Auth header hashing for stickiness.** Envoy Gateway (via Gateway API + `BackendTrafficPolicy`) uses consistent hashing on the `authorization` header. Same token → same replica → sessions and transactions stay pinned. No `Location` in `FlightEndpoint`, no direct pod connections, no client cooperation needed.

**DuckDB is ephemeral compute.** All durable state lives in Iceberg (object storage + catalog). DuckDB instances are in-memory, disposable, and can be killed and restarted at any time. The only cost of a restart is re-attaching the Iceberg catalog and re-downloading extension cache.

---

## 3. Dependencies

| Library                  | Import Path                           | Version | Purpose                                          |
|--------------------------|---------------------------------------|---------|--------------------------------------------------|
| go-duckdb                | `github.com/duckdb/duckdb-go/v2`      | v2.5+   | DuckDB driver with Arrow C Data Interface        |
| arrow-go                 | `github.com/apache/arrow-go/v18`      | v18     | Arrow types, Flight, Flight SQL server framework |
| DuckDB Iceberg ext       | auto-installed                        | latest  | Iceberg catalog ATTACH, reads, writes            |
| prometheus/client_golang | `github.com/prometheus/client_golang` | v1.x    | Metrics exposition                               |
| Envoy Gateway            | K8s CRDs (BackendTrafficPolicy)       | v1.x    | Consistent hash routing via Gateway API extension |

### Build

```bash
go build -tags=duckdb_arrow -o flightsql-server ./cmd/server
```

The `duckdb_arrow` tag is required in go-duckdb v2 to opt-in to the Arrow interface.

CGO is required. Ensure `gcc` is available. On Alpine: `apk add gcc musl-dev`.

---

## 4. Iceberg Integration

### No replacement scans needed

DuckDB v1.4.0+ supports `ATTACH` with `TYPE iceberg` against any Iceberg REST Catalog. After attaching, all tables are queryable with normal SQL. DuckDB resolves table metadata dynamically through the catalog on each query/transaction. No pre-registration, no `iceberg_scan()`, no replacement scans.

```sql
INSTALL iceberg;
LOAD iceberg;

CREATE SECRET iceberg_secret (
    TYPE iceberg,
    CLIENT_ID 'xxx',
    CLIENT_SECRET 'yyy',
    OAUTH2_SERVER_URI 'https://catalog.example.com/v1/oauth/tokens'
);

ATTACH 'warehouse_name' AS lake (
    TYPE iceberg,
    ENDPOINT 'https://catalog.example.com/v1',
    SECRET 'iceberg_secret'
);

-- Now just query normally
SELECT * FROM lake.default.orders WHERE region = 'EU';
```

### Extension auto-install

The Iceberg extension is not bundled in go-duckdb's pre-built binaries, but DuckDB has auto-install/auto-load enabled by default. Running `INSTALL iceberg; LOAD iceberg;` at startup downloads and caches the extension automatically. The startup init needs network access on first run.

For air-gapped deployments: build a custom DuckDB static library with the Iceberg extension statically linked, and use `CGO_LDFLAGS` with `duckdb_use_static_lib`.

### What ATTACH gives you

After attaching, Iceberg tables appear in `information_schema` under the attached catalog name. The Flight SQL metadata endpoints (GetCatalogs, GetSchemas, GetTables) automatically include them. Write support (INSERT, UPDATE, DELETE) works on non-partitioned Iceberg v2 tables as of DuckDB v1.4.2.

---

## 5. Project Structure

```
flightsql-duckberg/
├── cmd/
│   └── server/
│       └── main.go                 # Entry point, config, boot
├── internal/
│   ├── engine/
│   │   ├── engine.go               # DuckDB lifecycle + Arrow wrapper
│   │   └── pool.go                 # Arrow connection pool
│   ├── server/
│   │   ├── server.go               # FlightSQL server (embeds BaseServer)
│   │   ├── metadata.go             # Catalog/schema/table endpoints
│   │   ├── statements.go           # Query execution + prepared statements
│   │   ├── transactions.go         # BEGIN / COMMIT / ROLLBACK
│   │   ├── sessions.go             # Session lifecycle + reaping
│   │   └── metering.go             # Metered reader + Prometheus metrics
│   ├── auth/
│   │   └── middleware.go           # gRPC auth middleware
│   └── config/
│       └── config.go               # Env/flag-based configuration
├── test/
│   ├── suite_test.go               # Flight SQL conformance suite
│   ├── concurrency_test.go         # Pool + session concurrency tests
│   └── iceberg_integration_test.go # Iceberg-specific tests (build tag)
├── deploy/
│   ├── Dockerfile
│   ├── docker-compose.yml          # Local dev: Lakekeeper + MinIO + server
│   └── helm/
│       └── duckflight/             # Helm chart
│           ├── Chart.yaml
│           ├── values.yaml
│           └── templates/
│               ├── deployment.yaml
│               ├── service.yaml
│               ├── hpa.yaml
│               ├── grpcroute.yaml  # Gateway API GRPCRoute
│               └── backend-policy.yaml # Envoy Gateway BackendTrafficPolicy
├── go.mod
└── go.sum
```

---

## 6. Implementation Details

### 6.1 DuckDB Engine (`internal/engine/engine.go`)

Owns the DuckDB Connector, runs boot SQL, provides Arrow query access.

```go
type Engine struct {
    connector *duckdb.Connector
    pool      *ArrowPool
    writeMu   sync.Mutex   // serializes all DML/DDL
    cfg       *config.Config
}

func NewEngine(cfg *config.Config) (*Engine, error) {
    connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
        bootSQL := []string{
            "INSTALL iceberg",
            "LOAD iceberg",
            "SET autoinstall_known_extensions = true",
            "SET autoload_known_extensions = true",
            // Resource limits
            fmt.Sprintf("SET memory_limit = '%s'", cfg.MemoryLimit),
            fmt.Sprintf("SET threads = %d", cfg.MaxThreads),
            fmt.Sprintf("SET statement_timeout = '%s'", cfg.QueryTimeout),
            // Iceberg catalog
            fmt.Sprintf(`CREATE SECRET iceberg_secret (
                TYPE iceberg,
                CLIENT_ID '%s',
                CLIENT_SECRET '%s',
                OAUTH2_SERVER_URI '%s'
            )`, cfg.IcebergClientID, cfg.IcebergClientSecret, cfg.IcebergOAuth2URI),
            fmt.Sprintf(`ATTACH '%s' AS lake (
                TYPE iceberg,
                ENDPOINT '%s',
                SECRET 'iceberg_secret'
            )`, cfg.IcebergWarehouse, cfg.IcebergEndpoint),
        }
        for _, sql := range bootSQL {
            if _, err := execer.ExecContext(context.Background(), sql, nil); err != nil {
                return fmt.Errorf("boot SQL failed (%s): %w", sql, err)
            }
        }
        return nil
    })
    if err != nil {
        return nil, err
    }

    pool, err := NewArrowPool(connector, cfg.PoolSize)
    if err != nil {
        return nil, err
    }

    return &Engine{connector: connector, pool: pool, cfg: cfg}, nil
}
```

### 6.2 Arrow Connection Pool (`internal/engine/pool.go`)

Arrow connections in go-duckdb are not safe for concurrent use. Pool them.

```go
type ArrowConn struct {
    conn  driver.Conn
    arrow *duckdb.Arrow
}

type ArrowPool struct {
    pool chan *ArrowConn
}

func NewArrowPool(connector *duckdb.Connector, size int) (*ArrowPool, error) {
    p := &ArrowPool{pool: make(chan *ArrowConn, size)}
    for i := 0; i < size; i++ {
        conn, err := connector.Connect(context.Background())
        if err != nil { return nil, err }
        ar, err := duckdb.NewArrowFromConn(conn)
        if err != nil { return nil, err }
        p.pool <- &ArrowConn{conn: conn, arrow: ar}
    }
    return p, nil
}

func (p *ArrowPool) Acquire(ctx context.Context) (*ArrowConn, error) {
    select {
    case ac := <-p.pool:
        return ac, nil
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}

func (p *ArrowPool) Release(ac *ArrowConn) {
    p.pool <- ac
}
```

### 6.3 Sessions and Transactions (`internal/server/sessions.go`, `transactions.go`)

A session gets a **dedicated** connection (not pooled) because it carries state: open transactions, prepared statements, SET variables. Stateless queries use the pool.

```go
type Session struct {
    id         string
    conn       driver.Conn
    arrow      *duckdb.Arrow
    stmts      map[string]*PreparedStmt
    activeTx   *Transaction
    lastActive time.Time
    holdsWrite bool
}

type Transaction struct {
    id      []byte
    session *Session
    started time.Time
}

type SessionManager struct {
    engine   *Engine
    sessions sync.Map   // id → *Session

    maxIdle  time.Duration
    maxAge   time.Duration
}
```

Session lifecycle:

- **Created** when a client calls a Flight SQL session action (or implicitly on `BeginTransaction`)
- **Dedicated connection** allocated from the Connector (not the pool)
- **Reaped** by a background goroutine after idle timeout
- **Reaper rolls back** any open transaction before closing the connection

Transaction lifecycle:

- `BeginTransaction` → executes `BEGIN TRANSACTION` on the session's connection
- All subsequent queries with that transaction ID use the session's Arrow handle
- `EndTransaction(COMMIT)` → `COMMIT`, releases write lock if held
- `EndTransaction(ROLLBACK)` → `ROLLBACK`, releases write lock if held
- Write serialization: the Engine's `writeMu` is acquired on first write within a transaction (optimistic), held until commit/rollback

```go
func (sm *SessionManager) reapLoop(ctx context.Context) {
    ticker := time.NewTicker(1 * time.Minute)
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            now := time.Now()
            sm.sessions.Range(func(key, value any) bool {
                sess := value.(*Session)
                if now.Sub(sess.lastActive) > sm.maxIdle {
                    if sess.activeTx != nil {
                        sess.exec(ctx, "ROLLBACK")
                        if sess.holdsWrite {
                            sm.engine.writeMu.Unlock()
                        }
                    }
                    sess.conn.Close()
                    sm.sessions.Delete(key)
                }
                return true
            })
        }
    }
}
```

### 6.4 Flight SQL Server (`internal/server/server.go`)

Modeled after `arrow-go/v18/arrow/flight/flightsql/example/sqlite_server.go`. Embeds `flightsql.BaseServer`.

**Core methods to implement:**

| Method                                    | Description                                               | State                      |
|-------------------------------------------|-----------------------------------------------------------|----------------------------|
| `GetFlightInfoStatement`                  | Accept SQL, store handle, return FlightInfo (no Location) | Stateless or session-bound |
| `DoGetStatement`                          | Execute query, stream Arrow batches back                  | Uses pool or session conn  |
| `CreatePreparedStatement`                 | Prepare SQL, return schema + handle                       | Session-bound              |
| `DoGetPreparedStatement`                  | Execute prepared stmt, stream results                     | Session-bound              |
| `DoPutPreparedStatementQuery`             | Bind parameters                                           | Session-bound              |
| `ClosePreparedStatement`                  | Clean up                                                  | Session-bound              |
| `DoPutCommandStatementUpdate`             | Execute DML (INSERT/UPDATE/DELETE)                        | Acquires write lock        |
| `BeginTransaction`                        | BEGIN on session connection                               | Session-bound              |
| `EndTransaction`                          | COMMIT or ROLLBACK                                        | Session-bound              |
| `GetFlightInfoCatalogs` / `DoGetCatalogs` | `information_schema.schemata`                             | Stateless                  |
| `GetFlightInfoSchemas` / `DoGetDBSchemas` | `information_schema.schemata`                             | Stateless                  |
| `GetFlightInfoTables` / `DoGetTables`     | `information_schema.tables`                               | Stateless                  |
| `DoGetTableTypes`                         | Returns static list                                       | Stateless                  |

**The critical bridge — DoGetStatement:**

```go
func (s *DuckDBFlightSQLServer) DoGetStatement(
    ctx context.Context,
    ticket flightsql.StatementQueryTicket,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
    handle := ticket.GetStatementHandle()
    queryInfo := s.getQueryInfo(handle)

    var ar *duckdb.Arrow
    var release func()

    if queryInfo.session != nil {
        // Session-bound: use session's dedicated connection
        ar = queryInfo.session.arrow
        queryInfo.session.lastActive = time.Now()
    } else {
        // Stateless: borrow from pool
        ac, err := s.engine.pool.Acquire(ctx)
        if err != nil { return nil, nil, err }
        ar = ac.arrow
        release = func() { s.engine.pool.Release(ac) }
    }

    // Execute via Arrow interface — zero-copy from DuckDB
    reader, err := s.executeAndMeter(ctx, ar, queryInfo.sql)
    if err != nil {
        if release != nil { release() }
        return nil, nil, err
    }

    schema := reader.Schema()
    ch := make(chan flight.StreamChunk)

    go func() {
        defer close(ch)
        defer reader.Release()
        if release != nil { defer release() }
        for reader.Next() {
            rec := reader.RecordBatch()
            rec.Retain()
            ch <- flight.StreamChunk{Data: rec}
        }
    }()

    return schema, ch, nil
}
```

**No `Location` in `FlightEndpoint`:**

```go
func (s *DuckDBFlightSQLServer) GetFlightInfoStatement(
    ctx context.Context,
    cmd flightsql.StatementQuery,
    desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
    handle := s.storeQueryHandle(ctx, cmd)

    return &flight.FlightInfo{
        Schema: schemaBytes,
        Endpoint: []*flight.FlightEndpoint{{
            Ticket: &flight.Ticket{Ticket: encodeTicket(handle)},
            // No Location — client uses original connection
            // LB hashes auth header — same replica every time
        }},
    }, nil
}
```

### 6.5 Metadata Endpoints (`internal/server/metadata.go`)

DuckDB's `information_schema` covers both local and attached Iceberg catalogs:

| Flight SQL Call  | DuckDB Query                                                                  |
|------------------|-------------------------------------------------------------------------------|
| `GetCatalogs`    | `SELECT DISTINCT catalog_name FROM information_schema.schemata`               |
| `GetDBSchemas`   | `SELECT catalog_name, schema_name FROM information_schema.schemata WHERE ...` |
| `GetTables`      | `SELECT * FROM information_schema.tables WHERE ...`                           |
| `GetTableTypes`  | Static: `['BASE TABLE', 'VIEW', 'LOCAL TEMPORARY']`                           |
| `GetPrimaryKeys` | `SELECT * FROM information_schema.key_column_usage WHERE ...`                 |

### 6.6 Concurrency Summary

| Scenario                | Connection  | Lock                                             |
|-------------------------|-------------|--------------------------------------------------|
| Stateless SELECT        | Pool        | None                                             |
| Session SELECT (no txn) | Dedicated   | None                                             |
| Session SELECT (in txn) | Dedicated   | None (reads are free)                            |
| Stateless DML           | Pool        | `writeMu` for duration of exec                   |
| Session DML (in txn)    | Dedicated   | `writeMu` from first write until commit/rollback |
| Multiple readers        | Concurrent  | None                                             |
| Multiple writers        | Serialized  | `writeMu`                                        |
| Cross-replica           | Independent | Each replica has its own DuckDB, own locks       |

---

## 7. Load Balancing and Routing

Uses **Gateway API** (standard K8s API) with **Envoy Gateway** as the implementation. Envoy Gateway adds `BackendTrafficPolicy` CRD for advanced load balancing features like consistent hashing — extending the standard Gateway API without vendor lock-in.

### Auth Header Stickiness

Every Flight SQL client sends an `authorization` header on every RPC. Envoy Gateway hashes on this header for consistent routing.

**GRPCRoute** (standard Gateway API):

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: GRPCRoute
metadata:
  name: flightsql
spec:
  parentRefs:
    - name: flightsql-gateway
  rules:
    - backendRefs:
        - name: flightsql
          port: 31337
```

**BackendTrafficPolicy** (Envoy Gateway extension — consistent hash on auth header):

```yaml
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: BackendTrafficPolicy
metadata:
  name: flightsql-sticky
spec:
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: GRPCRoute
      name: flightsql
  loadBalancer:
    type: ConsistentHash
    consistentHash:
      type: Header
      header:
        name: authorization
```

Same token → same hash → same replica → sessions and transactions stay pinned automatically. No cookies, no Location, no client cooperation.

### Why this works with generic clients

A generic Flight SQL client:
1. Connects to one gRPC endpoint (your LB)
2. Sends auth header on every RPC
3. When `FlightEndpoint` has no `Location`, uses the same connection for `DoGet`
4. All RPCs from one client go to the same replica via the auth hash

### Token rotation caveat

If the client refreshes its bearer token (OAuth2 expiry), the hash changes and the LB may route to a different replica. Mitigations:

- **Long enough token TTL.** If tokens live 1 hour and transactions live seconds, this is a non-issue.
- **Hash on a stable claim.** Use an Envoy Gateway `EnvoyExtensionPolicy` to extract tenant ID from JWT and hash on that instead of the full auth header.

### Replica failure

If a replica dies mid-transaction, the client gets gRPC `UNAVAILABLE`. The transaction is lost. Client must retry from `BeginTransaction`. This is correct — Iceberg's catalog-level concurrency control guarantees atomicity. Either the commit reached the catalog or it didn't.

---

## 8. Execution Limits and Resource Control

### DuckDB-Level Limits

Set at engine boot time, driven by tenant tier:

```go
bootSQL := []string{
    fmt.Sprintf("SET memory_limit = '%s'", cfg.MemoryLimit),     // e.g. "2GB"
    fmt.Sprintf("SET threads = %d", cfg.MaxThreads),              // e.g. 4
    fmt.Sprintf("SET statement_timeout = '%s'", cfg.QueryTimeout),// e.g. "60s"
    fmt.Sprintf("SET temp_directory = '%s'", cfg.TempDir),
    fmt.Sprintf("SET max_temp_directory_size = '%s'", cfg.MaxTempSize),
}
```

### Kubernetes-Level Limits

DuckDB `memory_limit` should be ~70–80% of the K8s memory limit, leaving headroom for Go runtime, gRPC buffers, Arrow allocations, and extension overhead.

| Tier       | `memory_limit` | `threads` | `statement_timeout` | K8s memory | K8s CPU | HPA max |
|------------|----------------|-----------|---------------------|------------|---------|---------|
| Free       | 512MB          | 1         | 10s                 | 768Mi      | 0.5     | 1       |
| Pro        | 2GB            | 4         | 60s                 | 2560Mi     | 2       | 4       |
| Enterprise | 8GB            | 8         | 300s                | 10Gi       | 4       | 8       |

### Application-Level Guardrails

Defense in depth beyond DuckDB's built-in limits:

```go
func (s *Server) executeWithGuardrails(ctx context.Context, ar *duckdb.Arrow, query string, tier *Tier) (array.RecordReader, error) {
    // Context timeout (defense in depth alongside statement_timeout)
    ctx, cancel := context.WithTimeout(ctx, tier.QueryTimeout)
    defer cancel()

    reader, err := ar.QueryContext(ctx, query)
    if err != nil {
        if ctx.Err() == context.DeadlineExceeded {
            return nil, status.Error(codes.DeadlineExceeded, "query exceeded time limit")
        }
        return nil, err
    }

    // Wrap in metered reader for billing + result size cap
    return &meteredReader{
        RecordReader: reader,
        maxBytes:     tier.MaxResultBytes,
    }, nil
}
```

---

## 9. Metering and Billing

### Layer 1: Kubernetes Metrics (free)

Container-level CPU and memory via metrics-server or Prometheus. Pod labels carry tenant ID:

```promql
# CPU-hours per tenant over billing period
sum by (tenant) (
  rate(container_cpu_usage_seconds_total{container="flightsql"}[5m])
)

# Peak memory per tenant
max by (tenant) (
  container_memory_working_set_bytes{container="flightsql"}
)
```

### Layer 2: Application Metrics (Prometheus)

```go
var (
    queryCount = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "flightsql_queries_total",
        },
        []string{"tenant", "status"},
    )
    queryDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "flightsql_query_duration_seconds",
            Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120},
        },
        []string{"tenant"},
    )
    queryBytesStreamed = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "flightsql_bytes_streamed_total",
        },
        []string{"tenant"},
    )
    activeSessions = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "flightsql_active_sessions",
        },
        []string{"tenant"},
    )
    activeQueries = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "flightsql_active_queries",
        },
        []string{"tenant"},
    )
)
```

### Metered Reader

Wraps `array.RecordReader`, counts bytes as they stream, reports on close:

```go
type meteredReader struct {
    array.RecordReader
    tenant    string
    startTime time.Time
    bytes     int64
    maxBytes  int64
    onClose   func(int64)
    closed    bool
}

func (r *meteredReader) Next() bool {
    if !r.RecordReader.Next() {
        r.finish()
        return false
    }
    rec := r.RecordReader.RecordBatch()
    for i := 0; i < int(rec.NumCols()); i++ {
        for _, buf := range rec.Column(i).Data().Buffers() {
            if buf != nil {
                r.bytes += int64(buf.Len())
            }
        }
    }
    if r.maxBytes > 0 && r.bytes > r.maxBytes {
        r.finish()
        return false
    }
    return true
}

func (r *meteredReader) finish() {
    if !r.closed {
        r.closed = true
        r.onClose(r.bytes)
    }
}
```

### Fair Use Alerts

Prometheus alerting rules — no custom billing pipeline needed:

```yaml
groups:
  - name: fair-use
    rules:
      - alert: TenantExcessiveCPU
        expr: |
          sum by (tenant) (
            rate(container_cpu_usage_seconds_total{container="flightsql"}[1h])
          ) > 3.5
        for: 10m
      - alert: TenantQueryStorm
        expr: |
          sum by (tenant) (
            rate(flightsql_queries_total[5m])
          ) > 100
        for: 5m
```

### Billing Aggregation

```
Prometheus ──► Thanos/Mimir (long-term)
                    │
                    ▼
            Monthly aggregation
                    │
                    ▼
            Per-tenant usage:
              CPU-hours: 142
              Peak mem: 3.2 GB
              Queries: 48,291
              Data streamed: 1.2 TB
              Tier: Pro
```

---

## 10. Configuration

```go
type Config struct {
    // Server
    Port       int    `env:"PORT" default:"31337"`
    MetricPort int    `env:"METRIC_PORT" default:"9090"`
    InstanceID string `env:"INSTANCE_ID" default:""`
    TenantID   string `env:"TENANT_ID"`
    TLSCert    string `env:"TLS_CERT"`
    TLSKey     string `env:"TLS_KEY"`

    // DuckDB resource limits
    MemoryLimit string `env:"MEMORY_LIMIT" default:"2GB"`
    MaxThreads  int    `env:"MAX_THREADS" default:"4"`
    QueryTimeout string `env:"QUERY_TIMEOUT" default:"60s"`
    TempDir     string `env:"TEMP_DIR" default:"/tmp/duckdb"`
    MaxTempSize string `env:"MAX_TEMP_SIZE" default:"2GB"`
    PoolSize    int    `env:"POOL_SIZE" default:"8"`

    // Iceberg catalog
    IcebergEndpoint     string `env:"ICEBERG_ENDPOINT"`
    IcebergWarehouse    string `env:"ICEBERG_WAREHOUSE"`
    IcebergClientID     string `env:"ICEBERG_CLIENT_ID"`
    IcebergClientSecret string `env:"ICEBERG_CLIENT_SECRET"`
    IcebergOAuth2URI    string `env:"ICEBERG_OAUTH2_URI"`

    // Session management
    MaxSessionIdle time.Duration `env:"MAX_SESSION_IDLE" default:"30m"`
    MaxSessionAge  time.Duration `env:"MAX_SESSION_AGE" default:"4h"`

    // Billing tier limits
    MaxResultBytes int64 `env:"MAX_RESULT_BYTES" default:"0"` // 0 = unlimited
}
```

---

## 11. Testing Strategy

### Philosophy

TDD for the Flight SQL protocol layer. Test-last for DuckDB engine glue and Iceberg integration. Write the full conformance suite on day one with all tests red, then make them green as each milestone is implemented.

### Three test layers

#### Layer 1: Flight SQL Conformance Suite (TDD, the core)

A test suite using `testify/suite` that boots an in-process Flight SQL server with in-memory DuckDB (no Iceberg), connects a Flight SQL client, and exercises every RPC.

```go
// test/suite_test.go

type FlightSQLSuite struct {
    suite.Suite
    server  flight.Server
    client  *flightsql.Client
    engine  *engine.Engine
}

func (s *FlightSQLSuite) SetupSuite() {
    // In-memory DuckDB, no Iceberg needed for protocol tests
    cfg := &config.Config{
        MemoryLimit:  "512MB",
        MaxThreads:   2,
        QueryTimeout: "10s",
        PoolSize:     4,
    }
    s.engine, _ = engine.NewEngine(cfg)

    srv := server.NewDuckDBFlightSQLServer(s.engine)
    s.server = flight.NewServerWithMiddleware(nil)
    flightsql.RegisterFlightSQLService(s.server, srv)
    s.server.Init("localhost:0")
    go s.server.Serve()

    s.client, _ = flightsql.NewClient(
        s.server.Addr().String(),
        nil, nil,
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    )
}

func (s *FlightSQLSuite) TearDownSuite() {
    s.client.Close()
    s.server.Shutdown()
    s.engine.Close()
}
```

**Write all tests up front, implement to make them green:**

```go
// --- Query execution ---

func (s *FlightSQLSuite) TestExecuteSimpleQuery() {
    s.seedData("CREATE TABLE test_t (id INT, name VARCHAR)")
    s.seedData("INSERT INTO test_t VALUES (1, 'alice'), (2, 'bob')")

    info, err := s.client.Execute(ctx, "SELECT * FROM test_t ORDER BY id")
    s.Require().NoError(err)
    s.Require().Len(info.Endpoint, 1)

    rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
    s.Require().NoError(err)
    defer rdr.Release()

    s.True(rdr.Next())
    rec := rdr.RecordBatch()
    s.Equal(int64(2), rec.NumRows())
    s.Equal("id", rec.ColumnName(0))
    s.Equal("name", rec.ColumnName(1))
}

func (s *FlightSQLSuite) TestExecuteEmptyResult() {
    s.seedData("CREATE TABLE empty_t (id INT)")

    info, err := s.client.Execute(ctx, "SELECT * FROM empty_t")
    s.Require().NoError(err)

    rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
    s.Require().NoError(err)
    defer rdr.Release()

    s.False(rdr.Next()) // no rows
}

func (s *FlightSQLSuite) TestExecuteSyntaxError() {
    _, err := s.client.Execute(ctx, "SELECTTTT NOTHING")
    s.Require().Error(err)
    s.Contains(err.Error(), "Parser Error")
}

func (s *FlightSQLSuite) TestStatementTimeout() {
    // Create a query that exceeds the 10s timeout
    _, err := s.client.Execute(ctx,
        "SELECT count(*) FROM range(1000000000) t1, range(1000000000) t2")
    s.Require().Error(err)
}

// --- Metadata ---

func (s *FlightSQLSuite) TestGetCatalogs() {
    info, err := s.client.GetCatalogs(ctx)
    s.Require().NoError(err)

    rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
    s.Require().NoError(err)
    defer rdr.Release()

    s.True(rdr.Next())
    s.Greater(rdr.RecordBatch().NumRows(), int64(0))
}

func (s *FlightSQLSuite) TestGetSchemas() {
    info, err := s.client.GetDBSchemas(ctx, &flightsql.GetDBSchemas{})
    s.Require().NoError(err)

    rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
    s.Require().NoError(err)
    defer rdr.Release()

    s.True(rdr.Next())
}

func (s *FlightSQLSuite) TestGetTables() {
    s.seedData("CREATE TABLE meta_test (x INT)")

    info, err := s.client.GetTables(ctx, &flightsql.GetTables{})
    s.Require().NoError(err)

    rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
    s.Require().NoError(err)
    defer rdr.Release()

    s.True(rdr.Next())
    // Should include meta_test
}

func (s *FlightSQLSuite) TestGetTableTypes() {
    info, err := s.client.GetTableTypes(ctx)
    s.Require().NoError(err)

    rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
    s.Require().NoError(err)
    defer rdr.Release()

    s.True(rdr.Next())
}

// --- Prepared statements ---

func (s *FlightSQLSuite) TestPreparedStatement() {
    s.seedData("CREATE TABLE prep_t (id INT, val VARCHAR)")
    s.seedData("INSERT INTO prep_t VALUES (1, 'a'), (2, 'b')")

    stmt, err := s.client.Prepare(ctx, "SELECT * FROM prep_t WHERE id = ?")
    s.Require().NoError(err)
    defer stmt.Close(ctx)

    // Bind parameter and execute
    stmt.SetParameters(/* bind id=1 */)
    info, err := stmt.Execute(ctx)
    s.Require().NoError(err)

    rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
    s.Require().NoError(err)
    defer rdr.Release()

    s.True(rdr.Next())
    s.Equal(int64(1), rdr.RecordBatch().NumRows())
}

// --- DML ---

func (s *FlightSQLSuite) TestStatementUpdate() {
    s.seedData("CREATE TABLE upd_t (id INT)")

    n, err := s.client.ExecuteUpdate(ctx,
        "INSERT INTO upd_t VALUES (1), (2), (3)")
    s.Require().NoError(err)
    s.Equal(int64(3), n)

    // Verify via query
    info, _ := s.client.Execute(ctx, "SELECT count(*) as c FROM upd_t")
    rdr, _ := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
    defer rdr.Release()
    rdr.Next()
    // assert count = 3
}

// --- Transactions ---

func (s *FlightSQLSuite) TestTransactionCommit() {
    s.seedData("CREATE TABLE tx_t (id INT)")

    txn, err := s.client.BeginTransaction(ctx)
    s.Require().NoError(err)

    _, err = s.client.ExecuteUpdate(ctx,
        "INSERT INTO tx_t VALUES (1)",
        grpc.PerRPCCredentials(txn))
    s.Require().NoError(err)

    err = s.client.EndTransaction(ctx, txn, flightsql.EndTransactionCommit)
    s.Require().NoError(err)

    // Data should be visible
    info, _ := s.client.Execute(ctx, "SELECT count(*) FROM tx_t")
    rdr, _ := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
    defer rdr.Release()
    rdr.Next()
    // assert count = 1
}

func (s *FlightSQLSuite) TestTransactionRollback() {
    s.seedData("CREATE TABLE txr_t (id INT)")

    txn, err := s.client.BeginTransaction(ctx)
    s.Require().NoError(err)

    _, err = s.client.ExecuteUpdate(ctx,
        "INSERT INTO txr_t VALUES (1)",
        grpc.PerRPCCredentials(txn))
    s.Require().NoError(err)

    err = s.client.EndTransaction(ctx, txn, flightsql.EndTransactionRollback)
    s.Require().NoError(err)

    // Data should NOT be visible
    info, _ := s.client.Execute(ctx, "SELECT count(*) FROM txr_t")
    rdr, _ := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
    defer rdr.Release()
    rdr.Next()
    // assert count = 0
}

// --- Run ---

func TestFlightSQLSuite(t *testing.T) {
    suite.Run(t, new(FlightSQLSuite))
}
```

**This gives you ~15 tests on day one. All red. Each milestone turns a batch green.**

#### Layer 2: Concurrency Tests

Test the pool, session management, and write serialization:

```go
// test/concurrency_test.go

func TestConcurrentReads(t *testing.T) {
    engine := setupTestEngine(t)
    engine.Exec(ctx, "CREATE TABLE conc_t AS SELECT * FROM range(10000)")

    var wg sync.WaitGroup
    for i := 0; i < 20; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            ac, _ := engine.Pool().Acquire(ctx)
            defer engine.Pool().Release(ac)
            rdr, _ := ac.arrow.QueryContext(ctx, "SELECT count(*) FROM conc_t")
            defer rdr.Release()
            for rdr.Next() {}
        }()
    }
    wg.Wait()
}

func TestWriteSerialization(t *testing.T) {
    engine := setupTestEngine(t)
    engine.Exec(ctx, "CREATE TABLE ws_t (id INT)")

    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(n int) {
            defer wg.Done()
            engine.ExecWrite(ctx,
                fmt.Sprintf("INSERT INTO ws_t VALUES (%d)", n))
        }(i)
    }
    wg.Wait()

    // All 10 rows should exist
    count := queryScalar[int64](engine, "SELECT count(*) FROM ws_t")
    assert.Equal(t, int64(10), count)
}

func TestSessionReaping(t *testing.T) {
    mgr := NewSessionManager(engine, 100*time.Millisecond, 1*time.Second)
    sess, _ := mgr.CreateSession(ctx)

    // Start a transaction
    sess.BeginTransaction(ctx)

    // Wait for reap
    time.Sleep(500 * time.Millisecond)

    // Session should be gone, transaction rolled back
    _, err := mgr.GetSession(sess.id)
    assert.Error(t, err)
}
```

#### Layer 3: Iceberg Integration Tests (separate build tag)

These need infrastructure. Run against docker-compose with Lakekeeper + MinIO.

```go
//go:build iceberg_integration

// test/iceberg_integration_test.go

func TestIceberg_AttachAndQuery(t *testing.T) {
    engine := setupIcebergEngine(t)

    rdr, err := engine.QueryArrow(ctx, "SELECT * FROM lake.default.test_table LIMIT 10")
    require.NoError(t, err)
    defer rdr.Release()
    require.True(t, rdr.Next())
}

func TestIceberg_TablesAppearInMetadata(t *testing.T) {
    engine := setupIcebergEngine(t)

    rdr, _ := engine.QueryArrow(ctx,
        "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'lake'")
    defer rdr.Release()

    require.True(t, rdr.Next())
    // Should see Iceberg tables
}

func TestIceberg_WriteAndReadBack(t *testing.T) {
    engine := setupIcebergEngine(t)

    engine.ExecWrite(ctx,
        "CREATE TABLE lake.default.write_test (id INT, name VARCHAR)")
    engine.ExecWrite(ctx,
        "INSERT INTO lake.default.write_test VALUES (1, 'hello')")

    rdr, _ := engine.QueryArrow(ctx,
        "SELECT * FROM lake.default.write_test")
    defer rdr.Release()

    require.True(t, rdr.Next())
    assert.Equal(t, int64(1), rdr.RecordBatch().NumRows())
}

func TestIceberg_SchemaEvolution(t *testing.T) {
    engine := setupIcebergEngine(t)

    engine.ExecWrite(ctx,
        "ALTER TABLE lake.default.test_table ADD COLUMN new_col VARCHAR")

    rdr, _ := engine.QueryArrow(ctx,
        "SELECT new_col FROM lake.default.test_table LIMIT 1")
    defer rdr.Release()
    require.True(t, rdr.Next())
}

func TestIceberg_TransactionIsolation(t *testing.T) {
    // Verify that reads within a transaction see a consistent snapshot
    engine := setupIcebergEngine(t)

    // Read once to snapshot
    engine.Exec(ctx, "BEGIN TRANSACTION")
    rdr1, _ := engine.QueryArrow(ctx,
        "SELECT count(*) FROM lake.default.test_table")
    count1 := readScalar[int64](rdr1)

    // External write (simulated by another engine instance)
    // ... insert rows via catalog ...

    // Second read in same txn should see same count
    rdr2, _ := engine.QueryArrow(ctx,
        "SELECT count(*) FROM lake.default.test_table")
    count2 := readScalar[int64](rdr2)

    assert.Equal(t, count1, count2)
    engine.Exec(ctx, "COMMIT")
}
```

### Test Infrastructure

```yaml
# deploy/docker-compose.yml (for local dev + CI)
services:
  minio:
    image: minio/minio
    command: server /data
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"

  lakekeeper:
    image: lakekeeper/lakekeeper
    environment:
      LAKEKEEPER__LISTEN_PORT: 8181
      # ... storage config pointing to MinIO ...
    ports:
      - "8181:8181"
    depends_on:
      - minio

  # Optional: seed test data
  seed:
    build:
      context: ./test/seed
    depends_on:
      - lakekeeper
```

### Running Tests

```bash
# Fast loop — protocol conformance, concurrency (no infra needed)
go test -tags=duckdb_arrow ./test/...

# Full loop — including Iceberg integration
docker compose -f deploy/docker-compose.yml up -d --wait
go test -tags="duckdb_arrow iceberg_integration" ./test/...
docker compose -f deploy/docker-compose.yml down

# CI pipeline
# Step 1: conformance (fast, every PR)
# Step 2: iceberg integration (slower, every merge to main)
```

---

## 12. Deployment

### Dockerfile

```dockerfile
FROM golang:1.22-bookworm AS build
RUN apt-get update && apt-get install -y gcc g++
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -tags=duckdb_arrow -o /flightsql-server ./cmd/server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /flightsql-server /usr/local/bin/
EXPOSE 31337 9090
ENTRYPOINT ["flightsql-server"]
```

### Helm Chart

All K8s resources are packaged as a Helm chart at `deploy/helm/duckflight/`. One chart release per tenant.

```yaml
# deploy/helm/duckflight/values.yaml (defaults)
image:
  repository: yourorg/duckflight
  tag: latest

tenant: ""

iceberg:
  endpoint: ""
  warehouse: ""
  existingSecret: "iceberg-credentials"  # must have client-id, client-secret keys

# S3 storage credentials (optional — for when catalog doesn't vend credentials)
s3:
  endpoint: ""
  existingSecret: ""  # must have access-key, secret-key keys
  region: ""
  urlStyle: "path"

resources:
  memoryLimit: "2GB"
  maxThreads: 4
  queryTimeout: "60s"
  poolSize: 8

  requests:
    memory: "2560Mi"
    cpu: "1"
  limits:
    memory: "2560Mi"
    cpu: "2"

hpa:
  minReplicas: 1
  maxReplicas: 4
  targetCPUUtilization: 70

gateway:
  enabled: true
  parentRef: "flightsql-gateway"  # name of the Gateway resource
```

Templates include:
- **deployment.yaml** — Deployment with env vars from values + secrets
- **service.yaml** — ClusterIP service exposing gRPC (31337) and metrics (9090)
- **hpa.yaml** — HorizontalPodAutoscaler with CPU target
- **grpcroute.yaml** — Gateway API `GRPCRoute` (standard, when `gateway.enabled`)
- **backend-policy.yaml** — Envoy Gateway `BackendTrafficPolicy` for consistent hash on `authorization` header (when `gateway.enabled`)

Install per tenant:

```bash
helm install flightsql-tenant-abc deploy/helm/duckflight/ \
  --set tenant=abc \
  --set iceberg.endpoint=https://catalog.example.com/v1 \
  --set iceberg.warehouse=abc_warehouse
```

---

## 13. Implementation Milestones

| #       | Milestone                                                                                         | Tests Turn Green                                                       | Effort   |
|---------|---------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|----------|
| **M1**  | Project scaffold + Engine boots with in-memory DuckDB + pool                                      | —                                                                      | 0.5 day  |
| **M2**  | `GetFlightInfoStatement` + `DoGetStatement`                                                       | TestExecuteSimpleQuery, TestExecuteEmptyResult, TestExecuteSyntaxError | 2 days   |
| **M3**  | Metadata endpoints (catalogs, schemas, tables, table types)                                       | TestGetCatalogs, TestGetSchemas, TestGetTables, TestGetTableTypes      | 1 day    |
| **M4**  | Prepared statements                                                                               | TestPreparedStatement                                                  | 1 day    |
| **M5**  | DML via `DoPutCommandStatementUpdate`                                                             | TestStatementUpdate                                                    | 0.5 day  |
| **M6**  | Sessions + session-bound connections                                                              | — (infra for M7-M8)                                                    | 1 day    |
| **M7**  | Transactions (BEGIN, COMMIT, ROLLBACK) + write serialization                                      | TestTransactionCommit, TestTransactionRollback                         | 1.5 days |
| **M8**  | Concurrency: pool under load, write serialization, reaping                                        | concurrency_test.go suite                                              | 1 day    |
| **M9**  | Statement timeout + guardrails                                                                    | TestStatementTimeout                                                   | 0.5 day  |
| **M10** | Metering: Prometheus metrics + metered reader                                                     | — (verify via /metrics endpoint)                                       | 0.5 day  |
| **M11** | Auth middleware (bearer token validation)                                                         | —                                                                      | 0.5 day  |
| **M12** | Iceberg ATTACH integration                                                                        | iceberg_integration_test.go suite                                      | 1 day    |
| **M13** | Dockerfile + docker-compose + CI pipeline                                                         | —                                                                      | 0.5 day  |
| **M14** | Helm chart + Gateway API (GRPCRoute) + Envoy Gateway BackendTrafficPolicy (consistent hash) + HPA | —                                                                      | 1 day    |
| **M15** | TLS                                                                                               | —                                                                      | 0.5 day  |
| **M16** | Load testing + tuning                                                                             | —                                                                      | 1 day    |

**Total: ~13 days** from scaffold to production-ready, with tests green at each step.

---

## 14. Reference Materials

| Resource                                                              | What to Use It For                                                                 |
|-----------------------------------------------------------------------|------------------------------------------------------------------------------------|
| `apache/arrow-go/v18/arrow/flight/flightsql/example/sqlite_server.go` | Primary template — every method you need to implement has a working example here   |
| `apache/arrow-go/v18/arrow/flight/flightsql/sqlite_server_test.go`    | Test patterns — how to boot an in-process server and test with a Flight SQL client |
| `duckdb/duckdb-go/blob/main/arrow.go`                                 | Arrow interface usage — `QueryContext`, `RegisterView`, `NewArrowFromConn`         |
| `voltrondata/sqlflite`                                                | Production reference with DuckDB backend (Python, but same architecture)           |
| `duckdb.org/docs/stable/core_extensions/iceberg/`                     | Iceberg ATTACH, secrets, catalog options                                           |
| `duckdb.org/2025/11/28/iceberg-writes-in-duckdb`                      | Iceberg write support (INSERT, UPDATE, DELETE), transaction semantics              |
| `arrow.apache.org/docs/format/FlightSql.html`                         | Flight SQL protocol spec — all RPC methods and Protobuf message definitions        |

---

## 15. Gotchas and Tips

1. **CGO required.** go-duckdb links against native DuckDB. Ensure `gcc` in all build environments.

2. **arrow-go v18 alignment.** go-duckdb v2 uses `apache/arrow-go/v18`. Don't mix Arrow versions.

3. **Extension auto-install needs network.** First `INSTALL iceberg` downloads from the DuckDB extension repo. Docker builds need network at runtime, or pre-cache in a startup init container.

4. **DuckDB `memory_limit` < K8s `limits.memory`.** Leave ~20-30% headroom for Go runtime, gRPC, Arrow buffers. OOM killer will visit otherwise.

5. **Arrow `rec.Retain()` before channel send.** Records returned by the RecordReader are invalidated on the next `Next()` call. Retain before sending to the streaming channel.

6. **`reader.Release()` after streaming completes.** Always in a `defer` in the streaming goroutine.

7. **Iceberg first-query latency.** The first query against an Iceberg table fetches metadata from the REST catalog. Subsequent queries within the same transaction use cached metadata. Keep transactions short.

8. **Token rotation.** If clients rotate OAuth2 tokens, the LB auth-header hash changes and stickiness breaks. Use long enough TTLs or hash on a stable JWT claim via Envoy Lua filter.

9. **DuckDB single writer.** Only one connection can write at a time across the entire instance. Serialize writes with a mutex. For transactions with writes, hold the mutex from first write to commit/rollback.

10. **`information_schema` unifies catalogs.** After `ATTACH`, Iceberg tables appear in `information_schema` under the attached catalog name. Your metadata endpoints get Iceberg tables for free.

11. Good inspiration can be GizmoSQL, DuckDB Flight SQL server implemented in C++: https://github.com/gizmodata/gizmosql
