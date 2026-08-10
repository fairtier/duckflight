//go:build duckdb_arrow

package server_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/fairtier/duckflight/internal/config"
	"github.com/fairtier/duckflight/internal/server"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// spanByName returns the first recorded span with the given name.
func spanByName(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, s := range spans {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

func attrValue(s sdktrace.ReadOnlySpan, key string) (attribute.Value, bool) {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// TestQuerySpansAndRowMetrics checks that a plain query produces the spans and
// row accounting operators are meant to alert on. It is deliberately about the
// shape of the telemetry, not its contents: an instrumentation gap is silent by
// nature — nothing fails, the dashboards just go empty.
func TestQuerySpansAndRowMetrics(t *testing.T) {
	ensureTestMetrics()

	// The server takes its tracer from the global provider, so this has to be
	// installed before it is built. Both halves of the cleanup matter: the
	// shutdown stops the recorder from collecting every span the rest of the
	// package emits, and putting the old provider back keeps this test from
	// leaving a shut-down one behind for whatever runs next.
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})

	srv, err := server.New(&config.Config{
		MemoryLimit:  "256MB",
		MaxThreads:   2,
		QueryTimeout: "30s",
		PoolSize:     2,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })

	flightSrv := flight.NewServerWithMiddleware(nil)
	flightSrv.RegisterFlightService(flightsql.NewFlightServer(srv))
	require.NoError(t, flightSrv.Init("localhost:0"))
	go func() { _ = flightSrv.Serve() }()
	t.Cleanup(flightSrv.Shutdown)

	client, err := flightsql.NewClient(flightSrv.Addr().String(), nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	rowsBefore := getMetricValue("flightsql_rows_streamed_total", nil)

	ctx := context.Background()
	info, err := client.Execute(ctx, "SELECT * FROM range(5) t(id)")
	require.NoError(t, err)

	rdr, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	var rows int64
	for rdr.Next() {
		rows += rdr.RecordBatch().NumRows()
	}
	require.NoError(t, rdr.Err())
	rdr.Release()
	require.Equal(t, int64(5), rows)

	waitQuiescent(t, srv)
	spans := recorder.Ended()

	exec := spanByName(spans, "statement.execute")
	require.NotNil(t, exec, "query execution must be traced")
	system, ok := attrValue(exec, "db.system")
	require.True(t, ok, "execute span must carry db.system")
	require.Equal(t, "duckdb", system.AsString())

	// The stream span is the one that covers delivery, so it is the one that
	// has to know how much was delivered.
	stream := spanByName(spans, "statement.stream")
	require.NotNil(t, stream, "result streaming must be traced")
	streamed, ok := attrValue(stream, "flight.rows")
	require.True(t, ok, "stream span must report the row count")
	require.Equal(t, int64(5), streamed.AsInt64())

	require.NotNil(t, spanByName(spans, "pool.acquire"),
		"an anonymous request borrows from the pool, and that wait must be visible")

	require.Equal(t, rowsBefore+5, getMetricValue("flightsql_rows_streamed_total", nil))
}
