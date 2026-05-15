//go:build duckdb_arrow

package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fairtier/duckflight/internal/auth"
	"github.com/fairtier/duckflight/internal/config"
	"github.com/fairtier/duckflight/internal/server"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// bearerToken implements credentials.PerRPCCredentials for test auth.
type bearerToken struct {
	token string
}

func (b bearerToken) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b bearerToken) RequireTransportSecurity() bool { return false }

var _ credentials.PerRPCCredentials = bearerToken{}

// ---------------------------------------------------------------------------
// AuthSuite — static token + basic-handshake JWT
// ---------------------------------------------------------------------------

type AuthSuite struct {
	suite.Suite

	server    flight.Server
	mem       *memory.CheckedAllocator
	jwtSecret []byte
}

func (s *AuthSuite) SetupSuite() {
	srv, err := server.New(&config.Config{
		MemoryLimit:  "256MB",
		MaxThreads:   2,
		QueryTimeout: "10s",
		PoolSize:     2,
	})
	s.Require().NoError(err)

	s.jwtSecret = []byte("test-jwt-secret-key-for-unit-tests")
	mw, err := auth.Middleware(auth.Config{
		Users:        map[string]string{"alice": "secret123"},
		JWTSecret:    s.jwtSecret,
		JWTTTL:       time.Hour,
		StaticTokens: []string{"test-secret"},
	})
	s.Require().NoError(err)
	s.Require().NotNil(mw)

	s.server = flight.NewServerWithMiddleware([]flight.ServerMiddleware{*mw})
	s.server.RegisterFlightService(flightsql.NewFlightServer(srv))
	s.Require().NoError(s.server.Init("localhost:0"))

	go func() { _ = s.server.Serve() }()
}

func (s *AuthSuite) TearDownSuite() {
	if s.server != nil {
		s.server.Shutdown()
	}
}

func (s *AuthSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
}

func (s *AuthSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
}

func (s *AuthSuite) dialClient(token string) *flightsql.Client {
	s.T().Helper()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(bearerToken{token: token}))
	}
	cl, err := flightsql.NewClient(s.server.Addr().String(), nil, nil, opts...)
	s.Require().NoError(err)
	cl.Alloc = s.mem
	return cl
}

// ---------------------------------------------------------------------------
// Static-token tests (preserved from the bearer-only middleware)
// ---------------------------------------------------------------------------

func (s *AuthSuite) TestStaticTokenAccepted() {
	cl := s.dialClient("test-secret")
	defer func() { _ = cl.Close() }()

	ctx := context.Background()
	info, err := cl.Execute(ctx, "SELECT 1 AS val")
	s.Require().NoError(err)

	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	s.True(rdr.Next())
	s.NotNil(rdr.RecordBatch())
}

func (s *AuthSuite) TestMissingTokenRejected() {
	cl := s.dialClient("")
	defer func() { _ = cl.Close() }()

	_, err := cl.Execute(context.Background(), "SELECT 1")
	s.Require().Error(err)

	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.Unauthenticated, st.Code())
}

func (s *AuthSuite) TestUnknownTokenRejected() {
	cl := s.dialClient("not-a-real-token")
	defer func() { _ = cl.Close() }()

	_, err := cl.Execute(context.Background(), "SELECT 1")
	s.Require().Error(err)

	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.Unauthenticated, st.Code())
}

func (s *AuthSuite) TestStreamRejectsNoAuth() {
	authCl := s.dialClient("test-secret")
	defer func() { _ = authCl.Close() }()

	ctx := context.Background()
	info, err := authCl.Execute(ctx, "SELECT 1")
	s.Require().NoError(err)

	noAuthCl := s.dialClient("")
	defer func() { _ = noAuthCl.Close() }()

	_, err = noAuthCl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().Error(err)

	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.Unauthenticated, st.Code())
}

// ---------------------------------------------------------------------------
// Handshake basic-auth → issued JWT
// ---------------------------------------------------------------------------

func (s *AuthSuite) TestHandshakeIssuesJWT() {
	cl := s.dialClient("")
	defer func() { _ = cl.Close() }()

	ctx, err := cl.Client.AuthenticateBasicToken(context.Background(), "alice", "secret123")
	s.Require().NoError(err)

	info, err := cl.Execute(ctx, "SELECT 1")
	s.Require().NoError(err)

	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	s.True(rdr.Next())
}

func (s *AuthSuite) TestHandshakeWrongPassword() {
	cl := s.dialClient("")
	defer func() { _ = cl.Close() }()

	_, err := cl.Client.AuthenticateBasicToken(context.Background(), "alice", "nope")
	s.Require().Error(err)

	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.Unauthenticated, st.Code())
}

// ---------------------------------------------------------------------------
// Expired locally minted JWT
// ---------------------------------------------------------------------------

func (s *AuthSuite) TestExpiredLocalJWTRejected() {
	// Mint a JWT directly with a past expiration using the same secret the
	// server is configured with.
	claims := jwt.RegisteredClaims{
		Issuer:    "duckflight",
		Subject:   "alice",
		IssuedAt:  jwt.NewNumericDate(time.Now().Add(-2 * time.Hour)),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.jwtSecret)
	s.Require().NoError(err)

	cl := s.dialClient(tok)
	defer func() { _ = cl.Close() }()

	_, err = cl.Execute(context.Background(), "SELECT 1")
	s.Require().Error(err)

	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.Unauthenticated, st.Code())
}

func TestAuthSuite(t *testing.T) {
	suite.Run(t, new(AuthSuite))
}

// ---------------------------------------------------------------------------
// OIDCSuite — JWKS-based JWT validation
// ---------------------------------------------------------------------------

type OIDCSuite struct {
	suite.Suite

	server flight.Server
	idp    *httptest.Server
	key    *rsa.PrivateKey
	mem    *memory.CheckedAllocator
}

func (s *OIDCSuite) SetupSuite() {
	var err error
	s.key, err = rsa.GenerateKey(rand.Reader, 2048)
	s.Require().NoError(err)

	mux := http.NewServeMux()
	s.idp = httptest.NewServer(mux)
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":   s.idp.URL,
			"jwks_uri": s.idp.URL + "/jwks.json",
		})
	})
	mux.HandleFunc("/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]any{{
				"kty": "RSA",
				"alg": "RS256",
				"use": "sig",
				"kid": "test-key",
				"n":   base64.RawURLEncoding.EncodeToString(s.key.N.Bytes()),
				"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(s.key.E)).Bytes()),
			}},
		})
	})

	verifier, err := auth.NewOIDCVerifier(context.Background(), s.idp.URL, "duckflight")
	s.Require().NoError(err)

	srv, err := server.New(&config.Config{
		MemoryLimit:  "256MB",
		MaxThreads:   2,
		QueryTimeout: "10s",
		PoolSize:     2,
	})
	s.Require().NoError(err)

	mw, err := auth.Middleware(auth.Config{OIDC: verifier})
	s.Require().NoError(err)
	s.Require().NotNil(mw)

	s.server = flight.NewServerWithMiddleware([]flight.ServerMiddleware{*mw})
	s.server.RegisterFlightService(flightsql.NewFlightServer(srv))
	s.Require().NoError(s.server.Init("localhost:0"))

	go func() { _ = s.server.Serve() }()
}

func (s *OIDCSuite) TearDownSuite() {
	if s.server != nil {
		s.server.Shutdown()
	}
	if s.idp != nil {
		s.idp.Close()
	}
}

func (s *OIDCSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
}

func (s *OIDCSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
}

func (s *OIDCSuite) mintToken(subject string, audience jwt.ClaimStrings, exp time.Duration) string {
	s.T().Helper()
	claims := jwt.RegisteredClaims{
		Issuer:    s.idp.URL,
		Subject:   subject,
		Audience:  audience,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(exp)),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "test-key"
	signed, err := tok.SignedString(s.key)
	s.Require().NoError(err)
	return signed
}

func (s *OIDCSuite) dialClient(token string) *flightsql.Client {
	s.T().Helper()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(bearerToken{token: token}),
	}
	cl, err := flightsql.NewClient(s.server.Addr().String(), nil, nil, opts...)
	s.Require().NoError(err)
	cl.Alloc = s.mem
	return cl
}

func (s *OIDCSuite) TestValidJWTAccepted() {
	tok := s.mintToken("user@example.com", jwt.ClaimStrings{"duckflight"}, time.Hour)
	cl := s.dialClient(tok)
	defer func() { _ = cl.Close() }()

	info, err := cl.Execute(context.Background(), "SELECT 1")
	s.Require().NoError(err)

	rdr, err := cl.DoGet(context.Background(), info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()
	s.True(rdr.Next())
}

func (s *OIDCSuite) TestWrongAudienceRejected() {
	tok := s.mintToken("user@example.com", jwt.ClaimStrings{"wrong-audience"}, time.Hour)
	cl := s.dialClient(tok)
	defer func() { _ = cl.Close() }()

	_, err := cl.Execute(context.Background(), "SELECT 1")
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.Unauthenticated, st.Code())
}

func TestOIDCSuite(t *testing.T) {
	suite.Run(t, new(OIDCSuite))
}
