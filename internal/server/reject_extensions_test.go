//go:build duckdb_arrow

package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/fairtier/duckflight/internal/config"
	"github.com/fairtier/duckflight/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestRejectClientExtensions verifies that with RejectClientExtensions set,
// client-issued INSTALL/LOAD statements are refused with PermissionDenied on
// both the query and the update paths, while ordinary statements still run.
// DuckDB classifies INSTALL and LOAD identically (STATEMENT_TYPE_LOAD), so
// one policy check covers both.
func TestRejectClientExtensions(t *testing.T) {
	ensureTestMetrics()

	srv, err := server.New(&config.Config{
		MemoryLimit:            "256MB",
		MaxThreads:             2,
		QueryTimeout:           "10s",
		PoolSize:               2,
		RejectClientExtensions: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })

	fs := flight.NewServerWithMiddleware(nil)
	fs.RegisterFlightService(flightsql.NewFlightServer(srv))
	require.NoError(t, fs.Init("localhost:0"))
	go func() { _ = fs.Serve() }()
	t.Cleanup(fs.Shutdown)

	cl, err := flightsql.NewClient(
		fs.Addr().String(),
		nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cl.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	requireDenied := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok, "expected a gRPC status, got %v", err)
		require.Equal(t, codes.PermissionDenied, st.Code(), "unexpected code: %v", err)
	}

	t.Run("query path", func(t *testing.T) {
		for _, q := range []string{"INSTALL json", "LOAD json", "FORCE INSTALL json"} {
			info, err := cl.Execute(ctx, q)
			require.NoError(t, err, "rejection surfaces at DoGet, not GetFlightInfo")
			_, err = cl.DoGet(ctx, info.Endpoint[0].Ticket)
			requireDenied(t, err)
		}
	})

	t.Run("update path", func(t *testing.T) {
		_, err := cl.ExecuteUpdate(ctx, "LOAD json")
		requireDenied(t, err)
	})

	t.Run("ordinary statements still run", func(t *testing.T) {
		info, err := cl.Execute(ctx, "SELECT 1")
		require.NoError(t, err)
		rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
		require.NoError(t, err)
		defer rdr.Release()
		require.True(t, rdr.Next())
	})
}
