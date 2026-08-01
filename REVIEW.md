# DuckFlight — Correctness & Security Review

Date: 2026-08-01
Commit reviewed: `dcd3749` (master)
Scope: `cmd/server`, `internal/{auth,config,engine,ratelimit,server,session,telemetry}`
Dependencies pinned at review time: arrow-go v18.6.0, grpc v1.81.1, duckdb-go v2.10500.0

Findings marked **[verified]** were reproduced or confirmed by reading the exact
code path (including into vendored dependencies). Nothing below is speculative;
claims that could not be confirmed were dropped.

---

## Executive summary

The server has two **unauthenticated remote crash vectors** that kill the whole
process, and a set of connection-lifecycle bugs in the session layer that can
leak transactions *between different clients*. Those two classes are the ones
worth fixing before anything else — the first is trivially exploitable by
anyone who can reach the port, the second silently corrupts data.

Beyond that, the recurring theme is **silent failure**: result-set truncation
reported as success, resource limits that don't apply to prepared statements,
auth that disables itself on a config typo, and rate limiting / TLS / metering
that vanish on an unparsed env var. Several protections are believed-on but
effectively off.

Counts: 5 critical, 7 high, 12 medium, 15 low.

---

## CRITICAL

### C1. Unauthenticated remote DoS — malformed `Authorization` header panics the process
**[verified — reproduced]**

`internal/auth/middleware.go:89,95,101` delegates to arrow-go's basic-auth
middleware, which parses the header before any validator runs:

- `arrow/flight/server_auth.go:157-158` (unary): after `HasPrefix(v, "Bearer")`
  it slices `vals[0][len("Bearer")+1:]`. A header whose value is exactly
  `Bearer` (6 chars, no space) → `s[7:]` on a length-6 string.
- `arrow/flight/server_auth.go:198-199` (Handshake stream): `strings.SplitN(val, ":", 2)`
  then unconditional `creds[1]`. A Basic credential with no colon → index panic.

Reproduced against the real middleware:

```
PANIC (unary/Bearer):        runtime error: slice bounds out of range [7:6]
PANIC (stream/Basic no colon): runtime error: index out of range [1] with length 1
```

The process registers **no recovery interceptor** (`grep -rn "recover()" cmd internal`
→ nothing; `main.go:144` builds `grpcOpts` with only a stats handler), and
grpc-go does not recover handler panics. So the panic unwinds the serving
goroutine and terminates the process, dropping every in-flight query and every
DuckDB session.

Attack: one gRPC call with metadata `authorization: Bearer`, or a Handshake with
`authorization: Basic YWxpY2U=`. No credentials needed — this happens *before*
validation. Infinitely repeatable → crashloop.

Fix: validate the header shape in our own `Unary`/`Stream` wrappers (require
`scheme SP non-empty-credential`, and for Handshake Basic require a `:` in the
decoded value) and return `codes.Unauthenticated` before delegating to `inner`.
Add a recovery interceptor as defense in depth regardless — any future panic in
a handler currently kills the server.

### C2. Connections are returned to the pool dirty — transactions leak across clients
**[verified]**

`engine/pool.go:201-203` — `Release` is a bare channel send. No `ROLLBACK`, no
reset of connection-local state. `session/manager.go:112-118` (`Close`) and
`:158-162` (reap) both hand a pinned connection straight back.

Scenario: a client sends raw `BEGIN`, inserts rows, then goes idle past
`idleTTL`. The reaper returns the connection to the pool **mid-transaction**.
The next borrower issues `BEGIN`; `shouldSkipTxnControl` (`server/statements.go:53-69`)
probes, sees `inTxn=true`, and *silently skips* it — the new client is now
inside the previous client's transaction, sees its uncommitted writes and stale
snapshot, and its eventual `COMMIT` commits the dead client's changes.

Temp tables, `SET`, `PRAGMA` and `ATTACH` state leak the same way. Note
`idleTTL` is only `2 × QUERY_TIMEOUT` (60s at the default 30s timeout), so this
is reachable in normal operation, not just at the margin.

Fix: sanitize on every release — `ROLLBACK` if `current_transaction_id()`
indicates an open txn, plus `RESET ALL` / drop temp schema; or close and
re-open the connection. Cheapest correct version: make `Release` do the rollback
probe centrally so no caller can forget.

### C3. Session manager blocks on pool acquisition while holding its global lock
**[verified]**

`session/manager.go:65-82` — `m.pool.Acquire(ctx)` (blocking) is called with
`m.mu` held. `m.mu` is also required by `Close`, `reap`, `Count`, and every
other session's `Acquire`.

Scenario: `POOL_SIZE=4`, four idle sessions hold all four connections. A fifth
client with a new sid calls `Acquire`, takes `m.mu`, and blocks on the empty
pool channel. The reaper tick — the only thing that could free those
connections — blocks on `m.mu` at line 140. `CloseSession` blocks at line 101.
The whole session subsystem stalls until the fifth client's context is
canceled; with no client deadline, indefinitely.

Fix: don't hold `m.mu` across `pool.Acquire`. Insert a placeholder session and
fill its connection outside the lock, or double-check the map after acquiring.

### C4. Reaper/`Close` race nils a live session's connection → nil deref crash
**[verified]**

`session/manager.go:66-95` reads `sess` from the map, unlocks `m.mu` (line 82),
*then* races for `sess.mu` (line 86); `lastUsed` is only stamped after the lock
is won (line 89). The reaper uses `TryLock` (line 152), which succeeds because
the waiter hasn't locked yet, sets `sess.conn = nil`, and releases the conn.

`server/statements.go:111,121` returns `sess.Conn()` with **no nil check**. The
first use (`ac.Arrow.QueryContext`) nil-derefs → panic → process death (no
recovery interceptor, per C1).

Trigger: an RPC arriving on a session just as it crosses the idle cutoff. Also
reachable *on demand* by an authenticated client issuing `CloseSession`
concurrently with any other RPC on the same sid.

Fix: re-check `sess.conn` under the session lock and re-pin if nil; and have
reap re-verify `lastUsed` under the session lock before evicting.

### C5. Resource cleanup destroys live transactions and can double-release a connection
**[verified]**

`server/server.go:299-316`, `resourceTTL` at `:60-63`.

- **(a)** Cleanup reaps Flight transactions by **`createdAt` age, not idleness**,
  with `resourceTTL = 2 × queryTimeout`. At the default that means *any*
  anonymous transaction older than 60 seconds is force-rolled-back and its
  connection returned to the pool while the client still holds the handle. Worse,
  that `ROLLBACK` + `Release` can run concurrently with an in-flight `DoGet`
  streaming on the same `ts.conn` — concurrent use of a non-goroutine-safe DuckDB
  connection, plus the connection being handed to a new borrower mid-stream.
- **(b)** Double release. Cleanup does `Delete(key)` then `pool.Release(ts.conn)`;
  a concurrent `EndTransaction` whose `LoadAndDelete` (`transactions.go:139`)
  succeeded also releases via its deferred release. The unguarded channel pool
  then holds the same `*ArrowConn` twice → two borrowers share one DuckDB
  connection.

Fix: track `lastUsed` on transactions and reap by idleness; make exactly one
party release by having cleanup use `LoadAndDelete` and skip if it loses.

---

## HIGH

### H1. Static and OIDC tokens collapse every client onto one shared DuckDB session
**[verified]** — `internal/auth/middleware.go:140,157`, `deriveSessionID` at `:175-178`

The session id is `sha256(token)`, so *every client presenting the same token*
maps to one session and therefore one pinned connection.

- **State leakage across principals:** with a shared `AUTH_TOKENS=abc123`, analyst A's
  `CREATE TEMP TABLE scratch` and `SET s3_access_key_id=…` are visible to analyst B.
- **Transaction corruption:** one SQLAlchemy/ADBC client with `pool_size=5` and OIDC
  auth gets 5 logical connections on **one** DuckDB connection; connection 1's
  `COMMIT` commits connection 2's uncommitted writes.
- **Serialization DoS:** the session mutex is held for the whole RPC, so N clients
  sharing a token execute strictly serially regardless of `POOL_SIZE`.

The Handshake-JWT path is correct (fresh UUID per handshake, `jwt.go:38`); only
the derived path is affected. Fix: derive the sid per *connection/handshake*,
not per token — e.g. mint a server-side session id on first contact and return
it, or incorporate the gRPC peer/stream identity.

**Resolved** in two steps. First, the derived sid mixes in the peer address
(`sha256(token ‖ peer)`), separating direct connections. Second, for clients
behind L7 proxies that pool upstream connections (where the peer address
collapses), the server now mints an HMAC-signed session cookie
(`arrow_flight_session_id`, `internal/auth/cookie.go`) on first contact;
echoing clients get `sid = sha256(cookieID ‖ binding)` regardless of
transport. Binding is the raw token for static tokens (all share the "static"
subject) and issuer+subject for OIDC (so OAuth refresh keeps the session).
Forged cookies fail open to peer derivation — safe, because the sid mixes in
the caller's own token/subject, so forging only selects among the forger's own
sessions. arrow-go's `flight/session` middleware was rejected (no TTL,
unbounded store growth from non-echoing clients, panics on store errors, no
identity binding); the cookie layer is stateless server-side, and
`session.Manager`'s existing lazy creation + idle reaper carry the lifecycle.

### H2. Malformed `AUTH_USERS` silently disables authentication entirely (fail-open)
**[verified]** — `cmd/server/main.go:281-302`, `:123-139`; `auth/middleware.go:60-62`

`parseUsers` skips entries lacking `:` and returns `nil` if nothing parsed.
`Middleware` then returns `(nil, nil)`, and `main` appends no auth middleware —
no error, no fatal, no positive log line.

`AUTH_USERS="alice"` (colon typo) with no `AUTH_TOKENS`/`OIDC_ISSUER` starts a
**fully open server**. The only signal is one WARN line; the banner still says
"listening". Any network peer can execute arbitrary SQL.

Fix: make a non-empty `AUTH_USERS` that parses to zero users a startup error,
and log an explicit `auth enabled (backends: …)` / `AUTH DISABLED` line at INFO.

### H3. Prepared statements bypass transactions *and* every resource limit
**[verified]** — `server/prepared.go:115,178,280` (all `acquireConn(ctx, "")`)

The Flight SQL `transaction_id` on `ActionCreatePreparedStatementRequest` is
never read, and `preparedStatement` stores no txn. An anonymous client doing
`BeginTransaction` → prepare `INSERT` → execute → `Rollback` runs the INSERT on
a *different pool connection in autocommit*: it is durable and the rollback is a
no-op. Atomicity is silently broken (masked for authenticated clients only by
accident of session pinning).

Separately, `DoGetPreparedStatement` (`prepared.go:167-231`) applies **no**
`queryTimeout`, **no** `meteredReader`/`maxResultBytes` cap, **no** metrics, and
is invisible to `CancelFlightInfo`. Since most ADBC/JDBC/SQLAlchemy traffic uses
prepared statements, the configured limits are void for the majority of real
clients.

### H4. Iceberg/S3 secrets leak into logs via boot-SQL error messages
**[verified]** — `internal/engine/engine.go:92`

`fmt.Errorf("boot SQL failed (%s): %w", sql, err)` embeds the full statement,
and the `CREATE SECRET` statements (`:46-51`, `:60-68`) contain
`ICEBERG_CLIENT_SECRET` and `S3_SECRET_KEY` in cleartext. The error propagates
`NewArrowPool` → `NewEngine` → `duckserver.New` and is logged at
`main.go:105`, going to stderr **and** to the OTLP collector when configured.
Any boot failure at/after the secret statement — unreachable OAuth URI, bad
endpoint, or a quote in the secret (M1) — exfiltrates live credentials.

Fix: log the statement *index* or a redacted form, never the text.

### H5. Reaper re-insert clobbers a concurrently created session → permanent pool leak
**[verified]** — `session/manager.go:148-157`

Reap deletes the session, fails `TryLock`, and re-inserts it **without checking
the map**. In that window an `Acquire` for the same sid finds nothing, pins a
*second* pool connection, and stores it; the re-insert overwrites it. The second
session is now unreachable — in no map, so neither `Close` nor reap will ever
return its connection. Repeat `POOL_SIZE` times (default 8) → pool permanently
exhausted until restart.

Fix: compare-and-swap on re-insert (only restore if the key is still absent).

### H6. Anonymous transaction connections have no serialization
**[verified]** — `server/statements.go:98-113`

For transactions not bound to a session, `acquireConn` returns `ts.conn` with a
**no-op release and no lock**. Two parallel `DoPut`s on the same txnID — or an
`EndTransaction` running `COMMIT` while a `DoGet` still streams on that
connection (`transactions.go:174`) — use one non-goroutine-safe DuckDB
connection concurrently. Session-bound txns are safe; anonymous ones are not.

Fix: give `txnState` its own mutex and hold it for the call.

### H7. Session-bound transactions become phantoms after reaping — `COMMIT` falsely reports success
**[verified]** — `server/statements.go:106-112`, `server/transactions.go:150-171`

When `ts.sid != ""`, resolution goes through `sessions.Acquire`, which **lazily
creates a brand-new session and connection** if the old one was reaped. The new
connection is not in a transaction, so subsequent statements run in autocommit
(each immediately durable), and `EndTransaction`'s `!inTxn` probe returns
success **without committing anything**. The client believes it got an atomic
transaction; it got neither atomicity nor the earlier statements' effects —
those sit uncommitted on a pooled connection for an unrelated client to commit
(see C2).

Fix: fail with `InvalidArgument`/`FailedPrecondition` when a txn's session no
longer exists, rather than silently recreating it.

---

## MEDIUM

### M1. Config values interpolated into boot SQL without escaping
`engine/engine.go:30,35,46-51,60-68,72-73` — `MEMORY_LIMIT`, `EXTENSION_DIR`,
`ICEBERG_*`, `S3_*` are spliced into single-quoted literals via `fmt.Sprintf`. A
value containing `'` terminates the literal: an S3 secret with a quote breaks
startup (and logs the secret, per H4); a value from a compromised values file /
secret store executes arbitrary SQL on every new connection. Minimum fix:
double embedded quotes.

### M2. Exceeding `MAX_RESULT_BYTES` is reported as a successful, complete result
**[verified]** — `server/metering.go:88-102`, consumed at `statements.go:245-267`

On limit, `Next()` returns `false` and `Err()` stays `nil`, so the streaming
goroutine records status `"ok"` and closes the channel normally. A client
(ADBC/pandas ETL) receives a **truncated dataset indistinguishable from a
complete one** and gets gRPC OK. Must surface as a `ResourceExhausted` chunk.

### M3. Byte metering misses nested/dictionary data — limit and billing bypassable
**[verified]** — `server/metering.go:104-112`

`arrayBytes` sums only top-level `Data().Buffers()` and never walks
`Data().Children()` or `.Dictionary()`. For LIST/STRUCT/MAP/dictionary columns
the top-level buffers are just validity + offsets. `SELECT list(payload) …`
streams gigabytes while the counter sees a few KB — the cap never trips and the
`bytes_streamed` metering metric (the tenant-billing signal) undercounts by
orders of magnitude.

### M4. Rate limiter sits *inside* auth, so unauthenticated work is unmetered
`main.go:133-142`; `ratelimit/middleware.go:58-71`. All pre-auth work — JWT
parse, JWKS lookup, RSA signature verification — happens before
`limiter.Allow()` is reached. With OIDC configured, an attacker floods
syntactically valid RS256 tokens; each costs a full RSA verification and
consumes **zero** tokens. A `kid` miss additionally triggers a JWKS refresh, so
attacker-chosen kids drive outbound requests to the IdP.

### M5. Health RPCs: probe-induced restart loop, and unbounded unauthenticated `Watch`
`auth/middleware.go:20-23,92,98` exempts health from auth, but the rate limiter
(chained inner) has **no** such exemption — under load shedding, the kubelet's
gRPC liveness probe gets `ResourceExhausted`, the probe fails, and a healthy pod
is killed. Separately `/Watch` is a server-streaming RPC exempt from auth, and
`main.go` sets no `MaxConcurrentStreams`, keepalive enforcement, or connection
limit — an unauthenticated peer can open Watch streams in a loop, each pinning a
goroutine and a watcher map entry.

### M6. Full SQL text logged at INFO on every statement RPC
`server/logging.go:99,102,115,119,128,217,431`. `CREATE SECRET s3 (… SECRET 'wJalr…')`,
`ATTACH … (TOKEN 'eyJ…')`, and PII in literals go verbatim to stderr and the
OTLP collector. Move to DEBUG, or redact `CREATE SECRET`/`ATTACH`.

### M7. TLS silently disabled when only one of `TLS_CERT`/`TLS_KEY` is set
`main.go:148` — `certFile != "" && keyFile != ""` treats half-configured TLS as
"not requested". A key mounted at an unexpected path → server listens in
plaintext, bearer tokens and JWTs cross the network in the clear, and the only
missing signal is an absent log line. Should be a hard error, plus a WARN for
auth-without-TLS.

### M8. No strength floor on `AUTH_JWT_SECRET`; ephemeral fallback breaks multi-replica
`main.go:111`, `auth/middleware.go:73-81`. Any non-empty secret is accepted for
HS256 — `AUTH_JWT_SECRET="dev"` is brute-forceable offline in seconds, after
which an attacker mints tokens with arbitrary `sub` **and arbitrary `sid`**
(session hijack, since sid is the only thing pinning a client to a session).
Unset, each of 3 replicas generates its own secret → tokens issued by replica 1
are `Unauthenticated` on 2 and 3, looking like a load-balancer bug. Enforce ≥32 bytes.

### M9. `OTEL_EXPORTER_OTLP_INSECURE=true` is silently ineffective
**[verified against otlptracegrpc v1.44.0]** — `telemetry/telemetry.go:64-66,106-108`

Insecure credentials are passed via `WithDialOption(...)` instead of
`WithInsecure()`. In `otlpconfig.NewGRPCConfig` (`options.go:136-146`) user dial
options are applied *first*, then — because the `Insecure` flag was never set —
the exporter appends its own TLS credentials **after** them, which win. Against
a plaintext collector every export fails asynchronously; traces and logs simply
never arrive. Related: `main.go:56` passes the endpoint raw to `WithEndpoint`,
which does no scheme stripping, so `http://collector:4317` yields a broken dial target.

### M10. Prepared-statement schema probe can execute the statement
`server/prepared.go:121` — `QueryContext(ctx, query+" LIMIT 0")`. A query ending
in a line comment (`DELETE FROM t WHERE x=1 --note`) puts ` LIMIT 0` inside the
comment, so the **full DELETE executes at prepare time**, then again at DoPut.
`UPDATE … RETURNING col` + `LIMIT 0` is likewise valid DuckDB and performs the
update. Wrap in a subselect (as `GetSchemaStatement` does) or use a real prepare API.

### M11. Metadata RPCs never use the session connection
`server/metadata.go:37,143` always use `acquirePoolConn`, contradicting the
routing used everywhere else. A session's `CREATE TEMP TABLE` is invisible to
`GetTables` even though `DoGetTableTypes` advertises `LOCAL TEMPORARY`, so
SQLAlchemy/JDBC reflection concludes the table doesn't exist; metadata requested
inside an open transaction reflects a different connection's snapshot.

### M12. If `Serve()` fails the process wedges forever; shutdown closes almost nothing
`main.go:202-210` — the Serve goroutine logs and exits while `run()` still
blocks on `<-sig`: gRPC is dead, the health service still reports SERVING, and a
restart-on-exit deployment never restarts. `main.go:212-215` — `stopCleanup`
(`server.go:74`) is never invoked, `Engine.Close` (`engine.go:134`) has no
non-test callers, the session manager is never drained, and the metrics server
is never shut down. Also `flight.Server.Shutdown` is `GracefulStop` with **no
deadline** (arrow-go `server.go:395`), so one stalled DoGet stream blocks exit
until the pod is SIGKILLed; a second SIGTERM is swallowed.

---

## LOW

- **L1.** `metadata.go:396-408` — omitted catalog/schema filters narrow to
  `current_database()`/`current_schema()` instead of "no filter", contrary to the
  Flight SQL spec. `getTables(null, null, "%")` never shows ATTACHed Iceberg
  catalogs or non-current schemas, so DBeaver-style tools render a partial tree.
  Documented as intentional in CLAUDE.md, but it is a real interop defect, and
  `DoGetCatalogs` (`:84`) lists all catalogs, inconsistently.
- **L2.** Retained records leak when a stream is canceled mid-send:
  `statements.go:246-257`, `prepared.go:214-220`, `metadata.go:59-64,196-203` —
  `rec.Retain()` then `select { case ch <- …; case <-ctx.Done(): return }` never
  releases on the Done branch. One CGo-backed batch leaks per canceled stream.
- **L3.** `POOL_SIZE=0` starts "successfully" with an empty unbuffered pool and every
  query blocks forever (metadata paths use `context.Background()`); `POOL_SIZE=-1`
  panics at boot (`pool.go:172`). No validation.
- **L4.** `ArrowPool.Close` (`pool.go:212-221`) only closes *idle* connections; checked-out
  ones are never closed, and their later `Release` pushes them into a drained-but-open
  channel. `NewEngine` also leaks the connector when `NewArrowPool` fails (`engine.go:101-104`).
- **L5.** Silent fail-open on unparseable numeric env vars (`main.go:225-264`):
  `RATE_LIMIT_RPS="100/s"` → 0 → rate limiting off; `MAX_RESULT_BYTES="1e9"` → 0 →
  cap unlimited. Both are protections that vanish on a typo with zero signal.
- **L6.** `main.go:291-295` logs raw `AUTH_USERS` entries on parse failure —
  `AUTH_USERS=":hunter2"` writes the password to stderr and the OTLP collector.
- **L7.** `deriveSessionID` truncates SHA-256 to 8 bytes (`middleware.go:177`); the
  "collision-resistant" comment overstates a 64-bit space, and the truncation buys nothing.
- **L8.** OIDC verification doesn't pin signing algorithms (`oidc.go:38-46`, no
  `jwt.WithValidMethods`). I could **not** construct a working bypass — jwt/v5 rejects a
  non-`[]byte` HMAC key and `none` requires a sentinel keyfunc never returns — so this is
  hardening, not an exploitable defect.
- **L9.** Static-token comparison is a map lookup, not constant time (`middleware.go:139`),
  unlike the password path. The hash step destroys prefix correlation, so not remotely
  exploitable; noted for consistency.
- **L10.** JDBC type mappings (`xdbctypeinfo.go`): FLOAT→6 should be REAL (7) for single
  precision (`:65`); TIMESTAMP WITH TIME ZONE→93 should be 2014 (`:72`), so tz-aware clients
  shift instants; UBIGINT→signed BIGINT overflows `getLong` above 2^63-1 (`:74`);
  `create_params` null for DECIMAL/VARCHAR (`:142,160-161`).
- **L11.** `foreignkeys.go:30-31,87-96` — pk catalog/schema are aliased from the FK side, and
  `DoGetCrossReference` ignores `cmd.FKRef.Catalog`/`DBSchema` entirely (pk filters applied to
  fk columns); `update_rule`/`delete_rule` hardcoded to NO ACTION.
- **L12.** `SqlInfoFlightSqlServerStatementTimeout` registered as 0 even when `QUERY_TIMEOUT`
  is set (`server.go:138`) — clients are told statements never time out, then get `DeadlineExceeded`.
- **L13.** `shouldSkipTxnControl` errors are discarded at every call site
  (`statements.go:187,287`, `prepared.go:185,286`); on probe failure the statement executes
  anyway, recreating the BEGIN-in-BEGIN abort the helper exists to prevent.
- **L14.** Lost cancel: `statements.go:197-200` — `WithCancel` result immediately shadowed by
  `WithTimeout`; a `go vet lostcancel` hit. Also `pool.go:24,42` return `context.Canceled` as an
  "interface not implemented" sentinel, so an invariant failure would be misreported as client cancellation.
- **L15.** Cancellation races (`querytracker.go:94-101`): a `CancelFlightInfo` between DoGet's
  `Load` and `SetCancel` reports `Cancelled` while the query runs to completion; two concurrent
  DoGets on one ticket both execute and the second `SetCancel` makes the first uncancellable.
  TTL cleanup (`:118-127`) also drops entries for still-running queries.
- **L16.** Metrics listener (`main.go:190-198`) is unauthenticated on `0.0.0.0:9090` with no
  `ReadHeaderTimeout` (Slowloris-able); `telemetry.go:99` uses `SimpleProcessor`, which
  serializes every log record through one mutex and a blocking write.
- **L17.** Dead config fields: `config.Config.ListenAddr/MetricAddr/AuthTokens` are never read
  outside `config.go` — `main.go` reads the env directly with different defaults. A future change
  setting `cfg.AuthTokens` expecting it to gate access would have no effect.
- **L18.** `prepared.go:53-54,89-92` — binary parameter values alias Arrow buffers and the scalar
  is Released *before* `scalarToIFace` reads it. Safe today only because the buffers are
  Go-allocated and stay GC-reachable; violates the project's own aliasing rule (`bytes.Clone`).
  Also CLAUDE.md says prepared queries use the "last parameter row" but `prepared.go:195-197`
  uses `ps.params[0]` (the update path correctly loops all rows).

---

## Verified non-issues

Checked and found *not* to be defects — recorded so they aren't re-reviewed:

- **No SQL injection in metadata RPCs.** Every interpolation routes through
  `escapeSQLString` (`metadata.go:98,119,122,127,241-244,398,405`;
  `primarykeys.go:38`; `foreignkeys.go:58,75,94-95`). Doubling `'` is sufficient
  for DuckDB, whose standard single-quoted literals have no backslash escapes.
- LIKE-vs-exact semantics match the spec: filter *patterns* use LIKE, catalog and
  TableRef fields use `=`.
- The interleaved query on one pool conn in the `include_schema` path is safe —
  go-duckdb's Arrow `QueryContext` materializes the result first.
- arrow-go's DoGet drains the chunk channel and releases delivered records, so
  producer goroutines don't leak on disconnect (except L2's retained record).
- `tracker.Load` does not delete (contrary to CLAUDE.md's "load-and-delete"), so
  single-endpoint DoGet retries work.
- `lockWithCtx`'s compensating-unlock goroutine correctly hands the mutex back.
- `fetchDuckDBVersion`/`fetchKeywords` correctly `strings.Clone` CGo-aliased strings.
- `auth/jwt.go` pins HS256 and requires expiry; `oidc.go` bounds the discovery body and
  closes the response; `QUERY_TIMEOUT` parse errors fail fast at startup.
- gRPC reflection (`main.go:177`) is covered by auth middleware when auth is enabled.

---

## Test coverage gaps

Each maps to a finding above; none is currently covered:

- malformed `Authorization` headers — `Bearer` bare, `Basic <no colon>` (C1)
- connection reuse after a session is reaped mid-transaction (C2)
- concurrent `CloseSession` vs in-flight RPC on the same sid (C4)
- two clients sharing one static/OIDC token landing on one session (H1)
- `AUTH_USERS` parse failure resulting in a fully open server (H2)
- prepared statement inside a Flight transaction, then rollback (H3)
- result set exceeding `MAX_RESULT_BYTES` — must error, not truncate (M2)
- byte accounting for LIST/STRUCT/dictionary columns (M3)
- HS256 token signed with a foreign secret but correct `iss`
- local JWT with a forged or absent `sid` claim

---

## Suggested fix order

1. **C1** — one wrapper-level header validation plus a recovery interceptor closes both
   crash vectors. Smallest change, largest risk reduction.
2. **C2 / C5 / H5 / H7** — connection lifecycle. Sanitize on release, reap by idleness,
   single-releaser, fail rather than silently re-create. These are one coherent piece of work.
3. **C3 / C4** — session manager locking.
4. **H2 / M7 / L5** — make silent fail-open configurations fatal and log an explicit
   security posture line at startup.
5. **H3 / M2 / M3** — make resource limits actually apply and actually surface.
6. **H1** — session identity derivation (needs a small design decision, so it sequences after
   the mechanical fixes).
7. **H4 / M1 / M6 / L6** — secret handling in logs and boot SQL.
