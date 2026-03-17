//go:build duckdb_arrow

package server_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prochac/duckflight/internal/config"
	"github.com/prochac/duckflight/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ---------------------------------------------------------------------------
// benchEnv — shared server/client for all metadata benchmarks
// ---------------------------------------------------------------------------

type benchEnv struct {
	srv    *server.DuckFlightSQLServer
	fs     flight.Server
	client *flightsql.Client
}

var (
	benchOnce    sync.Once
	sharedBench  *benchEnv
	benchInitErr error
)

func getBenchEnv(b *testing.B) *benchEnv {
	b.Helper()
	benchOnce.Do(func() {
		ensureTestMetrics()

		srv, err := server.New(&config.Config{
			MemoryLimit:  "512MB",
			MaxThreads:   2,
			QueryTimeout: "60s",
			PoolSize:     4,
		})
		if err != nil {
			benchInitErr = fmt.Errorf("server.New: %w", err)
			return
		}

		fs := flight.NewServerWithMiddleware(nil)
		fs.RegisterFlightService(flightsql.NewFlightServer(srv))
		if err := fs.Init("localhost:0"); err != nil {
			benchInitErr = fmt.Errorf("fs.Init: %w", err)
			return
		}
		go func() { _ = fs.Serve() }()

		cl, err := flightsql.NewClient(
			fs.Addr().String(),
			nil, nil,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			benchInitErr = fmt.Errorf("flightsql.NewClient: %w", err)
			return
		}

		// Use DefaultAllocator — no CheckedAllocator for benchmarks.
		cl.Alloc = memory.DefaultAllocator
		srv.Alloc = memory.DefaultAllocator

		sharedBench = &benchEnv{srv: srv, fs: fs, client: cl}

		// Seed all benchmark schemas.
		seedBenchSchemas(b)
	})
	if benchInitErr != nil {
		b.Fatal(benchInitErr)
	}
	return sharedBench
}

// ---------------------------------------------------------------------------
// Schema seeding
// ---------------------------------------------------------------------------

type benchTier struct {
	schema string
	total  int
	plain  int
	fkPairs int
}

var benchTiers = []benchTier{
	{schema: "bench_100", total: 100, plain: 80, fkPairs: 10},
	{schema: "bench_1000", total: 1000, plain: 960, fkPairs: 20},
	{schema: "bench_5000", total: 5000, plain: 4950, fkPairs: 25},
}

func seedBenchSchemas(b *testing.B) {
	b.Helper()
	ctx := context.Background()

	for _, tier := range benchTiers {
		// Create schema.
		if err := server.SeedSQL(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", tier.schema)); err != nil {
			b.Fatalf("create schema %s: %v", tier.schema, err)
		}

		// Plain tables with PK — batch in groups of ~200.
		var batch strings.Builder
		for i := 0; i < tier.plain; i++ {
			fmt.Fprintf(&batch, "CREATE TABLE %s.t_%d (id INTEGER PRIMARY KEY, name VARCHAR, value DOUBLE);\n", tier.schema, i)
			if (i+1)%200 == 0 || i == tier.plain-1 {
				if err := server.SeedSQL(ctx, batch.String()); err != nil {
					b.Fatalf("seed plain tables %s (batch ending at %d): %v", tier.schema, i, err)
				}
				batch.Reset()
			}
		}

		// FK parent tables — batch together.
		for i := 0; i < tier.fkPairs; i++ {
			fmt.Fprintf(&batch, "CREATE TABLE %s.pk_%d (id INTEGER PRIMARY KEY, label VARCHAR);\n", tier.schema, i)
			if (i+1)%200 == 0 || i == tier.fkPairs-1 {
				if err := server.SeedSQL(ctx, batch.String()); err != nil {
					b.Fatalf("seed FK parents %s (batch ending at %d): %v", tier.schema, i, err)
				}
				batch.Reset()
			}
		}

		// FK child tables — created individually (parent must exist first).
		for i := 0; i < tier.fkPairs; i++ {
			ddl := fmt.Sprintf("CREATE TABLE %s.fk_%d (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES %s.pk_%d(id))",
				tier.schema, i, tier.schema, i)
			if err := server.SeedSQL(ctx, ddl); err != nil {
				b.Fatalf("seed FK child %s.fk_%d: %v", tier.schema, i, err)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func drainFlightInfoB(b *testing.B, client *flightsql.Client, info *flight.FlightInfo) {
	b.Helper()
	ctx := context.Background()
	for _, ep := range info.Endpoint {
		rdr, err := client.DoGet(ctx, ep.Ticket)
		if err != nil {
			b.Fatal(err)
		}
		for rdr.Next() {
		}
		if err := rdr.Err(); err != nil {
			b.Fatal(err)
		}
		rdr.Release()
	}
}

// ---------------------------------------------------------------------------
// Benchmarks — constant-size metadata
// ---------------------------------------------------------------------------

func BenchmarkDoGetCatalogs(b *testing.B) {
	env := getBenchEnv(b)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		info, err := env.client.GetCatalogs(ctx)
		if err != nil {
			b.Fatal(err)
		}
		drainFlightInfoB(b, env.client, info)
	}
}

func BenchmarkDoGetDBSchemas(b *testing.B) {
	env := getBenchEnv(b)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		info, err := env.client.GetDBSchemas(ctx, &flightsql.GetDBSchemasOpts{})
		if err != nil {
			b.Fatal(err)
		}
		drainFlightInfoB(b, env.client, info)
	}
}

func BenchmarkDoGetTableTypes(b *testing.B) {
	env := getBenchEnv(b)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		info, err := env.client.GetTableTypes(ctx)
		if err != nil {
			b.Fatal(err)
		}
		drainFlightInfoB(b, env.client, info)
	}
}

// ---------------------------------------------------------------------------
// Benchmarks — table-count-dependent metadata
// ---------------------------------------------------------------------------

func BenchmarkDoGetTables(b *testing.B) {
	env := getBenchEnv(b)
	ctx := context.Background()

	for _, tier := range benchTiers {
		b.Run(fmt.Sprintf("tables=%d", tier.total), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				info, err := env.client.GetTables(ctx, &flightsql.GetTablesOpts{
					DbSchemaFilterPattern: new(tier.schema),
				})
				if err != nil {
					b.Fatal(err)
				}
				drainFlightInfoB(b, env.client, info)
			}
		})

		b.Run(fmt.Sprintf("tables=%d/include_schema", tier.total), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				info, err := env.client.GetTables(ctx, &flightsql.GetTablesOpts{
					DbSchemaFilterPattern: new(tier.schema),
					IncludeSchema:         true,
				})
				if err != nil {
					b.Fatal(err)
				}
				drainFlightInfoB(b, env.client, info)
			}
		})
	}
}

func BenchmarkDoGetPrimaryKeys(b *testing.B) {
	env := getBenchEnv(b)
	ctx := context.Background()

	for _, tier := range benchTiers {
		b.Run(fmt.Sprintf("tables=%d", tier.total), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				info, err := env.client.GetPrimaryKeys(ctx, flightsql.TableRef{
					DBSchema: new(tier.schema),
					Table:    "t_0",
				})
				if err != nil {
					b.Fatal(err)
				}
				drainFlightInfoB(b, env.client, info)
			}
		})
	}
}

func BenchmarkDoGetImportedKeys(b *testing.B) {
	env := getBenchEnv(b)
	ctx := context.Background()

	for _, tier := range benchTiers {
		b.Run(fmt.Sprintf("tables=%d", tier.total), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				info, err := env.client.GetImportedKeys(ctx, flightsql.TableRef{
					DBSchema: new(tier.schema),
					Table:    "fk_0",
				})
				if err != nil {
					b.Fatal(err)
				}
				drainFlightInfoB(b, env.client, info)
			}
		})
	}
}

func BenchmarkDoGetExportedKeys(b *testing.B) {
	env := getBenchEnv(b)
	ctx := context.Background()

	for _, tier := range benchTiers {
		b.Run(fmt.Sprintf("tables=%d", tier.total), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				info, err := env.client.GetExportedKeys(ctx, flightsql.TableRef{
					DBSchema: new(tier.schema),
					Table:    "pk_0",
				})
				if err != nil {
					b.Fatal(err)
				}
				drainFlightInfoB(b, env.client, info)
			}
		})
	}
}

func BenchmarkDoGetCrossReference(b *testing.B) {
	env := getBenchEnv(b)
	ctx := context.Background()

	for _, tier := range benchTiers {
		b.Run(fmt.Sprintf("tables=%d", tier.total), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				info, err := env.client.GetCrossReference(ctx,
					flightsql.TableRef{
						DBSchema: new(tier.schema),
						Table:    "pk_0",
					},
					flightsql.TableRef{
						DBSchema: new(tier.schema),
						Table:    "fk_0",
					},
				)
				if err != nil {
					b.Fatal(err)
				}
				drainFlightInfoB(b, env.client, info)
			}
		})
	}
}
