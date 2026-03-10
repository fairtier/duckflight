//go:build duckdb_arrow

package server

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	queryCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flightsql_queries_total",
			Help: "Total number of queries executed.",
		},
		[]string{"status"},
	)
	queryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flightsql_query_duration_seconds",
			Help:    "Duration of query execution in seconds.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120},
		},
		[]string{},
	)
	queryBytesStreamed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "flightsql_bytes_streamed_total",
			Help: "Total bytes streamed to clients.",
		},
	)
	activeQueries = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "flightsql_active_queries",
			Help: "Number of currently active queries.",
		},
	)
)

func init() {
	prometheus.MustRegister(queryCount, queryDuration, queryBytesStreamed, activeQueries)
}

// meteredReader wraps an array.RecordReader, counting bytes as they stream.
type meteredReader struct {
	array.RecordReader
	bytes    int64
	maxBytes int64
	closed   bool
}

func newMeteredReader(rdr array.RecordReader, maxBytes int64) *meteredReader {
	return &meteredReader{
		RecordReader: rdr,
		maxBytes:     maxBytes,
	}
}

func (r *meteredReader) Next() bool {
	if !r.RecordReader.Next() {
		r.finish()
		return false
	}
	rec := r.RecordBatch()
	for i := 0; i < int(rec.NumCols()); i++ {
		r.bytes += r.arrayBytes(rec.Column(i))
	}
	if r.maxBytes > 0 && r.bytes > r.maxBytes {
		r.finish()
		return false
	}
	return true
}

func (r *meteredReader) arrayBytes(a arrow.Array) int64 {
	var n int64
	for _, buf := range a.Data().Buffers() {
		if buf != nil {
			n += int64(buf.Len())
		}
	}
	return n
}

func (r *meteredReader) finish() {
	if !r.closed {
		r.closed = true
		queryBytesStreamed.Add(float64(r.bytes))
	}
}

func (r *meteredReader) Release() {
	r.finish()
	r.RecordReader.Release()
}
