//go:build duckdb_arrow

package auth_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/fairtier/duckflight/internal/auth"
	"github.com/fairtier/duckflight/internal/config"
	"github.com/fairtier/duckflight/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// sharedTestToken is one API key handed to several independent clients.
const sharedTestToken = "shared-token"

// newSessionTestServer starts an authenticated server with a static token and
// returns its address plus the server handle.
func newSessionTestServer(t *testing.T) (string, *server.DuckFlightSQLServer) {
	t.Helper()

	srv, err := server.New(&config.Config{
		MemoryLimit:  "256MB",
		MaxThreads:   2,
		QueryTimeout: "10s",
		PoolSize:     4,
	})
	require.NoError(t, err)

	mw, err := auth.Middleware(auth.Config{StaticTokens: []string{sharedTestToken}})
	require.NoError(t, err)
	require.NotNil(t, mw)

	fs := flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		server.RecoveryMiddleware(),
		*mw,
	})
	fs.RegisterFlightService(flightsql.NewFlightServer(srv))
	require.NoError(t, fs.Init("localhost:0"))
	go func() { _ = fs.Serve() }()
	t.Cleanup(func() {
		fs.Shutdown()
		_ = srv.Close()
	})
	return fs.Addr().String(), srv
}

func dialToken(t *testing.T, addr string) *flightsql.Client {
	t.Helper()
	cl, err := flightsql.NewClient(addr, nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(bearerToken{token: sharedTestToken}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

func execQuery(ctx context.Context, cl *flightsql.Client, query string) error {
	info, err := cl.Execute(ctx, query)
	if err != nil {
		return err
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return err
	}
	defer rdr.Release()
	for rdr.Next() {
	}
	return rdr.Err()
}

// TestSharedTokenDoesNotShareSession is the regression test for session
// identity. Deriving the session id from the token alone collapses every
// client presenting that token onto a single pinned DuckDB connection: one
// analyst's temp tables and SET overrides become visible to another, one
// connection's COMMIT commits another's uncommitted writes, and all of them
// serialize behind a single session mutex.
//
// Two independent clients sharing one static token must get independent
// sessions, which shows up as one client's temp table being invisible to the
// other.
func TestSharedTokenDoesNotShareSession(t *testing.T) {
	addr, _ := newSessionTestServer(t)
	ctx := context.Background()

	clientA := dialToken(t, addr)
	clientB := dialToken(t, addr)

	require.NoError(t, execQuery(ctx, clientA, "CREATE TEMP TABLE scratch_a (id INTEGER)"))
	require.NoError(t, execQuery(ctx, clientA, "INSERT INTO scratch_a VALUES (1)"))

	// A's temp table lives on A's pinned connection only.
	require.NoError(t, execQuery(ctx, clientA, "SELECT * FROM scratch_a"))
	err := execQuery(ctx, clientB, "SELECT * FROM scratch_a")
	require.Error(t, err, "clients sharing a token must not share a session")
}

// TestCloseSessionConcurrentWithRPC hammers CloseSession against in-flight
// queries on the same session. The connection must never be recycled out from
// under a running query, and no call may take the process down.
func TestCloseSessionConcurrentWithRPC(t *testing.T) {
	addr, srv := newSessionTestServer(t)
	ctx := context.Background()

	cl := dialToken(t, addr)
	// Establish the session first.
	require.NoError(t, execQuery(ctx, cl, "SELECT 1"))

	var wg sync.WaitGroup
	const rounds = 40

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			// Errors are acceptable here (the session may have just been
			// closed); a crash or a hang is not.
			_ = execQuery(ctx, cl, fmt.Sprintf("SELECT %d", i))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			_, _ = cl.CloseSession(ctx, &flight.CloseSessionRequest{})
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()

	// The server is still healthy and every connection found its way home.
	require.NoError(t, execQuery(ctx, cl, "SELECT 1"))
	_, _ = cl.CloseSession(ctx, &flight.CloseSessionRequest{})

	pool := srv.Engine().Pool
	require.Eventually(t, func() bool { return pool.Len() == pool.Cap() },
		5*time.Second, 20*time.Millisecond,
		"pool connections not returned: %d of %d", pool.Len(), pool.Cap())
}

// TestSessionPinsConnectionAcrossCalls is the positive case: a single client's
// connection-local state has to survive across RPCs, which is the whole reason
// sessions exist.
func TestSessionPinsConnectionAcrossCalls(t *testing.T) {
	addr, _ := newSessionTestServer(t)
	ctx := context.Background()
	cl := dialToken(t, addr)

	require.NoError(t, execQuery(ctx, cl, "CREATE TEMP TABLE pinned (id INTEGER)"))
	require.NoError(t, execQuery(ctx, cl, "INSERT INTO pinned VALUES (42)"))
	require.NoError(t, execQuery(ctx, cl, "SELECT * FROM pinned"))
}

// dialTokenWithCookies dials like dialToken but with a client-side cookie jar,
// so the server-minted session cookie is echoed back on subsequent RPCs.
func dialTokenWithCookies(t *testing.T, addr string, jar flight.CookieMiddleware) *flightsql.Client {
	t.Helper()
	cl, err := flightsql.NewClient(addr, nil,
		[]flight.ClientMiddleware{flight.CreateClientMiddleware(jar)},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(bearerToken{token: sharedTestToken}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// TestCookieSharesSessionAcrossConnections proves the property peer-address
// derivation cannot provide: with a shared cookie jar, one logical client's
// session survives across two separate TCP connections (distinct peer
// addresses — exactly what an L7 proxy's connection pooling destroys).
func TestCookieSharesSessionAcrossConnections(t *testing.T) {
	addr, _ := newSessionTestServer(t)
	ctx := context.Background()

	jar := flight.NewCookieMiddleware()
	clientA := dialTokenWithCookies(t, addr, jar)

	// The first request runs on the peer-derived session and harvests the
	// cookie; everything after runs on the cookie session. This warm-up also
	// documents the accepted one-time transition.
	require.NoError(t, execQuery(ctx, clientA, "SELECT 1"))

	require.NoError(t, execQuery(ctx, clientA, "CREATE TEMP TABLE cookie_pinned (id INTEGER)"))
	require.NoError(t, execQuery(ctx, clientA, "INSERT INTO cookie_pinned VALUES (7)"))
	require.NoError(t, execQuery(ctx, clientA, "SELECT * FROM cookie_pinned"))

	// A second dial: new TCP connection, new peer address, same cookie jar.
	clientB := dialTokenWithCookies(t, addr, jar.Clone())
	require.NoError(t, execQuery(ctx, clientB, "SELECT * FROM cookie_pinned"),
		"cookie must pin the session across connections")

	// The original connection still sees its state too.
	require.NoError(t, execQuery(ctx, clientA, "SELECT * FROM cookie_pinned"))
}

// forgedCookie injects a fixed, never-server-minted session cookie into every
// outgoing RPC.
type forgedCookie struct{ value string }

func (f forgedCookie) StartCall(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "cookie", "arrow_flight_session_id="+f.value)
}

// TestForgedCookieFallsBackToPeerDerivation: a cookie the server did not mint
// must read as absent. Two clients presenting the same forged value keep their
// peer-derived isolation and no RPC fails because of it.
func TestForgedCookieFallsBackToPeerDerivation(t *testing.T) {
	addr, _ := newSessionTestServer(t)
	ctx := context.Background()

	forged := flight.CreateClientMiddleware(forgedCookie{value: "not-a-real-cookie"})
	dial := func() *flightsql.Client {
		cl, err := flightsql.NewClient(addr, nil, []flight.ClientMiddleware{forged},
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithPerRPCCredentials(bearerToken{token: sharedTestToken}),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = cl.Close() })
		return cl
	}
	clientA, clientB := dial(), dial()

	require.NoError(t, execQuery(ctx, clientA, "CREATE TEMP TABLE forged_scratch (id INTEGER)"))
	err := execQuery(ctx, clientB, "SELECT * FROM forged_scratch")
	require.Error(t, err, "a forged cookie must not join clients into one session")
}
