//go:build duckdb_arrow

package server_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fairtier/duckflight/internal/config"
	"github.com/fairtier/duckflight/internal/server"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// Prometheus helpers
// ---------------------------------------------------------------------------

// getMetricValue reads a metric value from the test gatherer by name and
// optional label matching. Returns 0 if the metric is not found.
func getMetricValue(name string, labels map[string]string) float64 {
	families, err := testGatherer.Gather()
	if err != nil {
		return 0
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if matchLabels(m.GetLabel(), labels) {
				if c := m.GetCounter(); c != nil {
					return c.GetValue()
				}
				if g := m.GetGauge(); g != nil {
					return g.GetValue()
				}
				if h := m.GetHistogram(); h != nil {
					return float64(h.GetSampleCount())
				}
			}
		}
	}
	return 0
}

func matchLabels(pairs []*dto.LabelPair, want map[string]string) bool {
	if len(want) == 0 {
		return true
	}
	have := make(map[string]string, len(pairs))
	for _, p := range pairs {
		have[p.GetName()] = p.GetValue()
	}
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// MeteringSuite — tests MaxResultBytes
// ---------------------------------------------------------------------------

type MeteringSuite struct {
	suite.Suite

	server flight.Server
	srv    *server.DuckFlightSQLServer
	client *flightsql.Client
	mem    *memory.CheckedAllocator
}

func (s *MeteringSuite) SetupSuite() {
	ensureTestMetrics()

	srv, err := server.New(&config.Config{
		MemoryLimit:    "256MB",
		MaxThreads:     2,
		QueryTimeout:   "30s",
		PoolSize:       2,
		MaxResultBytes: 500_000,
	})
	s.Require().NoError(err)
	s.srv = srv

	s.server = flight.NewServerWithMiddleware(nil)
	s.server.RegisterFlightService(flightsql.NewFlightServer(srv))
	s.Require().NoError(s.server.Init("localhost:0"))

	go func() { _ = s.server.Serve() }()

	cl, err := flightsql.NewClient(
		s.server.Addr().String(),
		nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	s.Require().NoError(err)
	s.client = cl
}

func (s *MeteringSuite) TearDownSuite() {
	if s.client != nil {
		_ = s.client.Close()
	}
	if s.server != nil {
		s.server.Shutdown()
	}
}

func (s *MeteringSuite) SetupTest() {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	s.client.Alloc = s.mem
	s.srv.Alloc = s.mem
}

func (s *MeteringSuite) TearDownTest() {
	waitQuiescent(s.T(), s.srv)
	s.Assert().Equal(0, s.srv.OpenTransactionCount(), "leaked transactions")
	s.Assert().Equal(0, s.srv.PreparedStatementCount(), "leaked prepared statements")
	s.Assert().Equal(0, s.srv.ActiveQueryCount(), "leaked active queries")
	pool := s.srv.Engine().Pool
	s.Assert().Equal(pool.Cap(), pool.Len(), "pool connections not returned")

	s.srv.Alloc = memory.DefaultAllocator
	s.mem.AssertSize(s.T(), 0)
}

// ---------------------------------------------------------------------------
// MaxResultBytes tests
// ---------------------------------------------------------------------------

// TestMaxResultBytesFailsInsteadOfTruncating pins down that hitting the cap is
// an error, not a short read. Ending the stream cleanly would hand an ETL
// client a truncated dataset that is indistinguishable from a complete one,
// with a gRPC OK on top.
func (s *MeteringSuite) TestMaxResultBytesFailsInsteadOfTruncating() {
	ctx := context.Background()
	info, err := s.client.Execute(ctx, "SELECT * FROM range(100000) t(id)")
	s.Require().NoError(err)

	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	var total int64
	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()
		total += rec.NumRows()
		rec.Release()
	}
	s.Less(total, int64(100000), "MaxResultBytes should stop the stream early")

	s.Require().Error(rdr.Err(), "a truncated result must not be reported as success")
	st, ok := status.FromError(rdr.Err())
	s.Require().True(ok)
	s.Equal(codes.ResourceExhausted, st.Code())
}

// TestMaxResultBytesCountsNestedData covers byte accounting for nested types.
// A LIST column's top-level buffers are only validity bits and offsets, so
// counting those alone lets a single row stream unbounded data past a cap that
// never trips — and undercounts the billing signal by orders of magnitude.
func (s *MeteringSuite) TestMaxResultBytesCountsNestedData() {
	ctx := context.Background()
	// One row, one list of 200k int64s — ~1.6 MB of child data behind a
	// handful of top-level bytes.
	info, err := s.client.Execute(ctx, "SELECT list(id) AS payload FROM range(200000) t(id)")
	s.Require().NoError(err)

	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()
		rec.Release()
	}

	s.Require().Error(rdr.Err(), "nested child data must count toward MaxResultBytes")
	st, ok := status.FromError(rdr.Err())
	s.Require().True(ok)
	s.Equal(codes.ResourceExhausted, st.Code())
}

func (s *MeteringSuite) TestMaxResultBytesSmallQueryPasses() {
	ctx := context.Background()
	info, err := s.client.Execute(ctx, "SELECT 1 AS val")
	s.Require().NoError(err)

	rdr, err := s.client.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	defer rdr.Release()

	var total int64
	for rdr.Next() {
		total += rdr.RecordBatch().NumRows()
	}
	s.NoError(rdr.Err())
	s.Equal(int64(1), total)
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

func TestMeteringSuite(t *testing.T) {
	suite.Run(t, new(MeteringSuite))
}
