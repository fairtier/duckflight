//go:build duckdb_arrow

package auth_test

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/fairtier/duckflight/internal/auth"
	"github.com/fairtier/duckflight/internal/config"
	"github.com/fairtier/duckflight/internal/server"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const testJWTSecret = "test-jwt-secret-key-long-enough-for-hs256"

// newMalformedAuthServer starts a server whose auth middleware is the one
// under test, and returns its address.
func newMalformedAuthServer(t *testing.T) string {
	t.Helper()

	srv, err := server.New(&config.Config{
		MemoryLimit:  "256MB",
		MaxThreads:   2,
		QueryTimeout: "10s",
		PoolSize:     2,
	})
	require.NoError(t, err)

	mw, err := auth.Middleware(auth.Config{
		Users:        map[string]string{"alice": "secret123"},
		JWTSecret:    []byte(testJWTSecret),
		JWTTTL:       time.Hour,
		StaticTokens: []string{"test-secret"},
	})
	require.NoError(t, err)
	require.NotNil(t, mw)

	// The recovery middleware is deliberately NOT installed here: these tests
	// must show the auth layer itself rejecting malformed input, not a
	// recovery net papering over a panic.
	fs := flight.NewServerWithMiddleware([]flight.ServerMiddleware{*mw})
	fs.RegisterFlightService(flightsql.NewFlightServer(srv))
	require.NoError(t, fs.Init("localhost:0"))
	go func() { _ = fs.Serve() }()
	t.Cleanup(func() {
		fs.Shutdown()
		_ = srv.Close()
	})
	return fs.Addr().String()
}

// rawHeaderCreds sends an arbitrary Authorization header value, bypassing the
// well-formed token that the normal client credential helper produces.
type rawHeaderCreds struct{ value string }

func (r rawHeaderCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": r.value}, nil
}
func (rawHeaderCreds) RequireTransportSecurity() bool { return false }

// TestMalformedAuthorizationHeaderIsRejected covers the unauthenticated remote
// crash vector: arrow-go's basic-auth middleware slices the header before any
// validator runs, so a bare "Bearer" (no space, nothing after it) indexes past
// the end of the string and panics. gRPC does not recover handler panics, so
// one such call would take the whole process down — repeatably, from any peer
// that can reach the port.
//
// Every variant below must come back as Unauthenticated, and the server must
// still be serving afterwards.
func TestMalformedAuthorizationHeaderIsRejected(t *testing.T) {
	addr := newMalformedAuthServer(t)

	cases := []struct {
		name   string
		header string
	}{
		{"bare bearer, no separator", "Bearer"},
		{"bearer with trailing space only", "Bearer "},
		{"empty header", ""},
		{"scheme only", "Basic"},
		{"no scheme", "sometoken"},
		{"unknown scheme", "Digest abcdef"},
		{"basic on a non-handshake call", "Basic " + base64.StdEncoding.EncodeToString([]byte("alice:secret123"))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cl, err := flightsql.NewClient(addr, nil, nil,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithPerRPCCredentials(rawHeaderCreds{value: tc.header}),
			)
			require.NoError(t, err)
			defer func() { _ = cl.Close() }()

			_, err = cl.Execute(context.Background(), "SELECT 1")
			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, codes.Unauthenticated, st.Code(), "got %v", err)
		})
	}

	// The server survived every one of them.
	cl, err := flightsql.NewClient(addr, nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(bearerToken{token: "test-secret"}),
	)
	require.NoError(t, err)
	defer func() { _ = cl.Close() }()
	_, err = cl.Execute(context.Background(), "SELECT 1")
	require.NoError(t, err, "server should still be serving after malformed headers")
}

// TestMalformedHandshakeCredentialIsRejected covers the second crash vector: a
// Basic credential whose decoded value has no colon. Splitting on ":" yields a
// single element and the middleware indexes [1] unconditionally.
func TestMalformedHandshakeCredentialIsRejected(t *testing.T) {
	addr := newMalformedAuthServer(t)

	cases := []struct {
		name string
		raw  string
	}{
		{"no colon", "alice"},
		{"empty", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer func() { _ = conn.Close() }()

			cl := flight.NewFlightServiceClient(conn)
			ctx := metadata.AppendToOutgoingContext(context.Background(),
				"authorization", "Basic "+base64.RawStdEncoding.EncodeToString([]byte(tc.raw)))

			stream, err := cl.Handshake(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.CloseSend())
			_, err = stream.Recv()
			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, codes.Unauthenticated, st.Code(), "got %v", err)
		})
	}

	// Still serving: a valid handshake works after the malformed ones.
	cl, err := flightsql.NewClient(addr, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = cl.Close() }()
	_, err = cl.Client.AuthenticateBasicToken(context.Background(), "alice", "secret123")
	require.NoError(t, err)
}

// TestForeignSecretJWTRejected: a token with the right issuer but signed with
// somebody else's key must not authenticate.
func TestForeignSecretJWTRejected(t *testing.T) {
	addr := newMalformedAuthServer(t)

	claims := jwt.RegisteredClaims{
		Issuer:    "duckflight",
		Subject:   "alice",
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).
		SignedString([]byte("a-completely-different-secret-of-length"))
	require.NoError(t, err)

	cl, err := flightsql.NewClient(addr, nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(bearerToken{token: tok}),
	)
	require.NoError(t, err)
	defer func() { _ = cl.Close() }()

	_, err = cl.Execute(context.Background(), "SELECT 1")
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unauthenticated, st.Code())
}

// TestLocalJWTWithoutSidStillPins checks the compatibility path: a correctly
// signed token that predates the sid claim is accepted and still gets pinned
// to a session rather than being rejected or left session-less.
func TestLocalJWTWithoutSidStillPins(t *testing.T) {
	addr := newMalformedAuthServer(t)

	claims := jwt.RegisteredClaims{
		Issuer:    "duckflight",
		Subject:   "alice",
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(testJWTSecret))
	require.NoError(t, err)

	cl, err := flightsql.NewClient(addr, nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(bearerToken{token: tok}),
	)
	require.NoError(t, err)
	defer func() { _ = cl.Close() }()

	ctx := context.Background()
	info, err := cl.Execute(ctx, "SELECT 1")
	require.NoError(t, err)
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	defer rdr.Release()
	require.True(t, rdr.Next())
}

// TestShortJWTSecretRejected: an HS256 key short enough to brute-force offline
// must not start the server. Forging a token yields an attacker-chosen subject
// *and* session id, which is enough to take over another client's session.
func TestShortJWTSecretRejected(t *testing.T) {
	_, err := auth.Middleware(auth.Config{
		Users:     map[string]string{"alice": "secret123"},
		JWTSecret: []byte("dev"),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "AUTH_JWT_SECRET")
}
