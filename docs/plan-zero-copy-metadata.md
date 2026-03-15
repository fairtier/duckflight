# Plan: Zero-copy streaming for metadata endpoints

## Problem

Metadata endpoints (`DoGetCatalogs`, `DoGetDBSchemas`, `DoGetTables`,
`DoGetPrimaryKeys`, foreign key endpoints) currently:

1. Execute a DuckDB query via the Arrow interface (zero-copy internally).
2. Iterate every row, extract each value with `col.Value(i)`.
3. Clone strings (`strings.Clone`) so they outlive the Arrow batch.
4. Append each value to a new Arrow builder (allocates a second buffer).
5. Finalize the builder into a new Arrow array.
6. Wrap everything in a single `RecordBatch`.

This is **O(n) with two data copies** per response. DuckDB already produces
Arrow record batches — we throw them away and rebuild from scratch.

## Proposed approach

Stream DuckDB's Arrow batches directly to FlightSQL clients, only wrapping
them to fix schema-level metadata (nullability, field names). This is a
pattern used in production FlightSQL servers backed by DuckDB.

### Component 1: Nullability-fixing reader

DuckDB's Arrow interface marks all columns as nullable. The FlightSQL spec
(`schema_ref.*`) requires certain columns to be non-nullable (e.g.
`table_name`, `table_type` in `schema_ref.Tables`).

Build an `array.RecordReader` wrapper that:
- Takes an inner reader and a target `*arrow.Schema`.
- On each `Next()`, retains the inner batch's column arrays (refcount bump,
  no copy) and creates a new `RecordBatch` with the target schema.
- Validates field names match between inner and target schemas at
  construction time.

This is O(1) per batch — only the schema metadata object is new.

### Component 2: Schema-enriching reader for `GetTables` with `include_schema`

When `GetTables` is called with `include_schema=true`, each table's Arrow
schema must be serialized and appended as a binary column. Currently this is
done after collecting all rows.

Build an `array.RecordReader` wrapper that:
- Wraps the table-list reader from DuckDB.
- On each `Next()`, reads the `table_name` (and `catalog_name`,
  `db_schema_name`) columns from the inner batch.
- For each table, runs `SELECT * FROM <fqn> LIMIT 0` to get the schema.
- Serializes each schema via `flight.SerializeSchema`.
- Appends the serialized schemas as a new binary column alongside the
  retained original columns.
- Applies the `enrichSchemaWithTypeNames` metadata enrichment.

This requires a second DuckDB connection (for the `LIMIT 0` queries) since
the first connection is busy producing the table list.

### Component 3: Refactor metadata endpoints

Replace the manual builder pattern in each `DoGet*` method:

```
// Before (simplified):
rdr := duckdb.Query(query)
for rdr.Next() {
    for i := range rows {
        builder.Append(col.Value(i))  // copy
    }
}
batch := builder.Build()

// After:
rdr := duckdb.Query(query)
wrapped := newNullabilityReader(rdr, schema_ref.Tables)
go flight.StreamChunksFromReader(ctx, wrapped, ch)
```

Affected endpoints:
- `DoGetCatalogs` (metadata.go)
- `DoGetDBSchemas` (metadata.go)
- `DoGetTables` (metadata.go) — also needs schema-enriching reader
- `DoGetPrimaryKeys` (primarykeys.go)
- `DoGetTableTypes` (metadata.go) — static data, keep as-is
- `DoGetXdbcTypeInfo` (xdbctypeinfo.go) — static data, keep as-is
- Foreign key endpoints (foreignkeys.go) — uses `queryForeignKeys` which
  also manually rebuilds; could be refactored

## Tradeoffs

### Benefits
- True zero-copy for column data — only schema metadata is allocated.
- Streaming: batches flow to the client as DuckDB produces them, instead of
  buffering the entire result before sending.
- Lower memory footprint for large metadata result sets (many tables).
- Simpler code — less manual builder boilerplate.

### Costs and risks
- **Nullability wrapper adds complexity** at the reader level. Must handle
  `Retain`/`Release` correctly or risk use-after-free or memory leaks.
  Needs thorough testing.
- **Schema-enriching reader needs a second connection** from the pool for
  the `LIMIT 0` queries while the first streams the table list. Current
  code uses one connection sequentially. This increases pool pressure.
- **Column name mapping**: DuckDB's `information_schema` uses column names
  like `table_catalog`, `table_schema`, while FlightSQL's `schema_ref`
  uses `catalog_name`, `db_schema_name`. The nullability wrapper must
  handle this rename, or the SQL query must alias columns to match.
- **Streaming changes error semantics**: currently, errors are caught before
  sending anything. With streaming, a mid-stream error surfaces as a
  `StreamChunk{Err: err}` — clients must handle partial results.
- **`DoGetTables` with `include_schema`** is the most complex case: it
  combines streaming the table list, querying each table's schema on a
  separate connection, enriching with TYPE_NAME metadata, and serializing.
  The reader must coordinate two connections.
- **Metadata result sets are typically small** (tens to hundreds of rows).
  The performance gain may not be measurable without thousands of tables.
  Benchmarking should confirm whether this is worth the added complexity.

## Validation plan

1. ~~Add benchmarks for `DoGetTables` (with and without `include_schema`)
   using a database seeded with 100, 1000, and 5000 tables.~~ **Done.**
   `internal/server/bench_test.go` covers all metadata endpoints (`DoGetCatalogs`,
   `DoGetDBSchemas`, `DoGetTableTypes`, `DoGetTables` ± `include_schema`,
   `DoGetPrimaryKeys`, `DoGetImportedKeys`, `DoGetExportedKeys`,
   `DoGetCrossReference`) across 3 tiers (100, 1000, 5000 tables).
2. Compare current (manual build) vs streaming approach on:
   - Wall time
   - Allocations (`-benchmem`)
   - Memory high-water mark
   Use `benchstat` for comparison:
   ```bash
   go test -tags=duckdb_arrow -run='^$' -bench=Benchmark -benchmem -count=6 -timeout=30m ./internal/server/ | tee bench_baseline.txt
   # ... implement zero-copy ...
   go test -tags=duckdb_arrow -run='^$' -bench=Benchmark -benchmem -count=6 -timeout=30m ./internal/server/ | tee bench_zerocopy.txt
   benchstat bench_baseline.txt bench_zerocopy.txt
   ```
3. Run existing test suite — all `TestCommandGet*` and `TestADBC_*` tests
   must pass unchanged.
4. Verify with a real JDBC client (DataGrip) that metadata responses are
   accepted without errors.
