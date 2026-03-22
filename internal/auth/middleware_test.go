//go:build duckdb_arrow

package auth_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fairtier/duckflight/internal/auth"
	"github.com/fairtier/duckflight/internal/config"
	"github.com/fairtier/duckflight/internal/server"
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
// AuthSuite
// ---------------------------------------------------------------------------

type AuthSuite struct {
	suite.Suite

	server flight.Server
	mem    *memory.CheckedAllocator
}

func (s *AuthSuite) SetupSuite() {
	srv, err := server.New(&config.Config{
		MemoryLimit:  "256MB",
		MaxThreads:   2,
		QueryTimeout: "10s",
		PoolSize:     2,
	})
	s.Require().NoError(err)

	var middlewares []flight.ServerMiddleware
	if m := auth.BearerTokenMiddleware([]string{"test-secret"}); m != nil {
		middlewares = append(middlewares, *m)
	}
	s.server = flight.NewServerWithMiddleware(middlewares)
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
// Tests
// ---------------------------------------------------------------------------

func (s *AuthSuite) TestAuthValidToken() {
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

func (s *AuthSuite) TestAuthMissingToken() {
	cl := s.dialClient("")
	defer func() { _ = cl.Close() }()

	ctx := context.Background()
	_, err := cl.Execute(ctx, "SELECT 1")
	s.Require().Error(err)

	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.Unauthenticated, st.Code())
	s.Contains(st.Message(), "missing authorization")
}

func (s *AuthSuite) TestAuthInvalidToken() {
	cl := s.dialClient("wrong-token")
	defer func() { _ = cl.Close() }()

	ctx := context.Background()
	_, err := cl.Execute(ctx, "SELECT 1")
	s.Require().Error(err)

	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.Unauthenticated, st.Code())
	s.Contains(st.Message(), "invalid bearer token")
}

func (s *AuthSuite) TestAuthStreamRejectsNoAuth() {
	// First get a valid ticket using an authenticated client.
	authCl := s.dialClient("test-secret")
	defer func() { _ = authCl.Close() }()

	ctx := context.Background()
	info, err := authCl.Execute(ctx, "SELECT 1")
	s.Require().NoError(err)

	// Now try DoGet with an unauthenticated client.
	noAuthCl := s.dialClient("")
	defer func() { _ = noAuthCl.Close() }()

	_, err = noAuthCl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().Error(err)

	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.Unauthenticated, st.Code())
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

func TestAuthSuite(t *testing.T) {
	suite.Run(t, new(AuthSuite))
}
