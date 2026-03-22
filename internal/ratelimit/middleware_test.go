//go:build duckdb_arrow

package ratelimit_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fairtier/duckflight/internal/config"
	"github.com/fairtier/duckflight/internal/ratelimit"
	"github.com/fairtier/duckflight/internal/server"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// RateLimitSuite — generous limit, single query succeeds
// ---------------------------------------------------------------------------

type RateLimitSuite struct {
	suite.Suite

	server flight.Server
	mem    *memory.CheckedAllocator
}

func (s *RateLimitSuite) SetupSuite() {
	srv, err := server.New(&config.Config{
		MemoryLimit:  "256MB",
		MaxThreads:   2,
		QueryTimeout: "10s",
		PoolSize:     2,
	})
	s.Require().NoError(err)

	var middlewares []flight.ServerMiddleware
	if m := ratelimit.Middleware(10000, 100); m != nil {
		middlewares = append(middlewares, *m)
	}
	s.server = flight.NewServerWithMiddleware(middlewares)
	s.server.RegisterFlightService(flightsql.NewFlightServer(srv))
	s.Require().NoError(s.server.Init("localhost:0"))

	go func() { _ = s.server.Serve() }()
}

func (s *RateLimitSuite) TearDownSuite() {
	if s.server != nil {
		s.server.Shutdown()
	}
}

func (s *RateLimitSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
}

func (s *RateLimitSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
}

func (s *RateLimitSuite) TestRateLimitAllowed() {
	cl, err := flightsql.NewClient(s.server.Addr().String(), nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	s.Require().NoError(err)
	cl.Alloc = s.mem
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

// ---------------------------------------------------------------------------
// ExceededSuite — tight limit (burst 1), rapid fire triggers rejection
// ---------------------------------------------------------------------------

type ExceededSuite struct {
	suite.Suite

	server flight.Server
	mem    *memory.CheckedAllocator
}

func (s *ExceededSuite) SetupSuite() {
	srv, err := server.New(&config.Config{
		MemoryLimit:  "256MB",
		MaxThreads:   2,
		QueryTimeout: "10s",
		PoolSize:     2,
	})
	s.Require().NoError(err)

	// Very low rate: 1 token/sec, burst of 1. Rapid-fire calls will exhaust it.
	var middlewares []flight.ServerMiddleware
	if m := ratelimit.Middleware(1, 1); m != nil {
		middlewares = append(middlewares, *m)
	}
	s.server = flight.NewServerWithMiddleware(middlewares)
	s.server.RegisterFlightService(flightsql.NewFlightServer(srv))
	s.Require().NoError(s.server.Init("localhost:0"))

	go func() { _ = s.server.Serve() }()
}

func (s *ExceededSuite) TearDownSuite() {
	if s.server != nil {
		s.server.Shutdown()
	}
}

func (s *ExceededSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
}

func (s *ExceededSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
}

func (s *ExceededSuite) TestRateLimitExceeded() {
	cl, err := flightsql.NewClient(s.server.Addr().String(), nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	s.Require().NoError(err)
	cl.Alloc = s.mem
	defer func() { _ = cl.Close() }()

	ctx := context.Background()

	// Fire many requests rapidly — at least one must be rejected.
	var gotExhausted bool
	for range 20 {
		_, err := cl.Execute(ctx, "SELECT 1")
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.ResourceExhausted {
				gotExhausted = true
				break
			}
		}
	}
	s.True(gotExhausted, "expected at least one ResourceExhausted error")
}

// ---------------------------------------------------------------------------
// BurstSuite — burst > 1, verify burst-many requests succeed then fail
// ---------------------------------------------------------------------------

type BurstSuite struct {
	suite.Suite

	server flight.Server
	mem    *memory.CheckedAllocator
}

func (s *BurstSuite) SetupSuite() {
	srv, err := server.New(&config.Config{
		MemoryLimit:  "256MB",
		MaxThreads:   2,
		QueryTimeout: "10s",
		PoolSize:     2,
	})
	s.Require().NoError(err)

	// Burst of 20, very slow refill (1/sec). We'll fire 20 rapid-fire unary
	// calls to consume the burst, then the next one should fail.
	var middlewares []flight.ServerMiddleware
	if m := ratelimit.Middleware(1, 20); m != nil {
		middlewares = append(middlewares, *m)
	}
	s.server = flight.NewServerWithMiddleware(middlewares)
	s.server.RegisterFlightService(flightsql.NewFlightServer(srv))
	s.Require().NoError(s.server.Init("localhost:0"))

	go func() { _ = s.server.Serve() }()
}

func (s *BurstSuite) TearDownSuite() {
	if s.server != nil {
		s.server.Shutdown()
	}
}

func (s *BurstSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
}

func (s *BurstSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
}

func (s *BurstSuite) TestRateLimitBurst() {
	cl, err := flightsql.NewClient(s.server.Addr().String(), nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	s.Require().NoError(err)
	cl.Alloc = s.mem
	defer func() { _ = cl.Close() }()

	ctx := context.Background()

	// Fire many Execute calls (unary RPCs) to consume the burst.
	var succeeded int
	for range 30 {
		_, err := cl.Execute(ctx, "SELECT 1")
		if err != nil {
			break
		}
		succeeded++
	}

	// Some requests should have succeeded (up to burst tokens).
	s.Greater(succeeded, 0, "at least one request should succeed")
	s.LessOrEqual(succeeded, 20, "no more than burst-count requests should succeed")

	// Verify the rejection is ResourceExhausted.
	_, err = cl.Execute(ctx, "SELECT 1")
	s.Require().Error(err)

	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.ResourceExhausted, st.Code())
}

// ---------------------------------------------------------------------------
// DisabledSuite — Middleware(0, 0) returns nil, server works normally
// ---------------------------------------------------------------------------

type DisabledSuite struct {
	suite.Suite

	server flight.Server
	mem    *memory.CheckedAllocator
}

func (s *DisabledSuite) SetupSuite() {
	srv, err := server.New(&config.Config{
		MemoryLimit:  "256MB",
		MaxThreads:   2,
		QueryTimeout: "10s",
		PoolSize:     2,
	})
	s.Require().NoError(err)

	m := ratelimit.Middleware(0, 0)
	s.Nil(m, "Middleware(0,0) should return nil")

	s.server = flight.NewServerWithMiddleware(nil)
	s.server.RegisterFlightService(flightsql.NewFlightServer(srv))
	s.Require().NoError(s.server.Init("localhost:0"))

	go func() { _ = s.server.Serve() }()
}

func (s *DisabledSuite) TearDownSuite() {
	if s.server != nil {
		s.server.Shutdown()
	}
}

func (s *DisabledSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
}

func (s *DisabledSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
}

func (s *DisabledSuite) TestRateLimitDisabled() {
	cl, err := flightsql.NewClient(s.server.Addr().String(), nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	s.Require().NoError(err)
	cl.Alloc = s.mem
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

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

func TestRateLimitSuite(t *testing.T) {
	suite.Run(t, new(RateLimitSuite))
}

func TestExceededSuite(t *testing.T) {
	suite.Run(t, new(ExceededSuite))
}

func TestBurstSuite(t *testing.T) {
	suite.Run(t, new(BurstSuite))
}

func TestDisabledSuite(t *testing.T) {
	suite.Run(t, new(DisabledSuite))
}
