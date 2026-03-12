//go:build duckdb_arrow

package server_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prochac/duckflight/internal/server"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ---------------------------------------------------------------------------
// Prometheus helpers
// ---------------------------------------------------------------------------

// getMetricValue reads a metric value from the default gatherer by name and
// optional label matching. Returns 0 if the metric is not found.
func getMetricValue(name string, labels map[string]string) float64 {
	families, err := prometheus.DefaultGatherer.Gather()
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
	client *flightsql.Client
	mem    *memory.CheckedAllocator
}

func (s *MeteringSuite) SetupSuite() {
	srv, err := server.New(server.Config{
		MemoryLimit:    "256MB",
		MaxThreads:     2,
		QueryTimeout:   "30s",
		PoolSize:       2,
		MaxResultBytes: 500_000,
	})
	s.Require().NoError(err)

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
}

func (s *MeteringSuite) TearDownTest() {
	s.mem.AssertSize(s.T(), 0)
}

// ---------------------------------------------------------------------------
// MaxResultBytes tests
// ---------------------------------------------------------------------------

func (s *MeteringSuite) TestMaxResultBytesLimitsRows() {
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
	s.Less(total, int64(100000), "MaxResultBytes should limit the number of rows returned")
	s.Greater(total, int64(0), "should return at least some rows")
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
