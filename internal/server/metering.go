//go:build duckdb_arrow

package server

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

var (
	queryCount    metric.Int64Counter
	queryDuration metric.Float64Histogram
	bytesStreamed metric.Int64Counter
	activeQueries metric.Int64UpDownCounter
)

func init() {
	// Initialize with noop meter so instruments are never nil.
	initMetrics(noop.Meter{})
}

// InitMetrics re-initializes metric instruments with the given meter.
func InitMetrics(meter metric.Meter) {
	initMetrics(meter)
}

func initMetrics(meter metric.Meter) {
	var err error

	queryCount, err = meter.Int64Counter("flightsql.queries",
		metric.WithDescription("Total number of queries executed."),
	)
	if err != nil {
		panic(err)
	}

	queryDuration, err = meter.Float64Histogram("flightsql.query.duration",
		metric.WithDescription("Duration of query execution in seconds."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120),
	)
	if err != nil {
		panic(err)
	}

	bytesStreamed, err = meter.Int64Counter("flightsql.bytes.streamed",
		metric.WithDescription("Total bytes streamed to clients."),
		metric.WithUnit("By"),
	)
	if err != nil {
		panic(err)
	}

	activeQueries, err = meter.Int64UpDownCounter("flightsql.active.queries",
		metric.WithDescription("Number of currently active queries."),
	)
	if err != nil {
		panic(err)
	}
}

func queryCountAdd(ctx context.Context, status string) {
	queryCount.Add(ctx, 1, metric.WithAttributes(attribute.String("status", status)))
}

// meteredReader wraps an array.RecordReader, counting bytes as they stream.
type meteredReader struct {
	array.RecordReader
	ctx      context.Context
	bytes    int64
	maxBytes int64
	closed   bool
}

func newMeteredReader(ctx context.Context, rdr array.RecordReader, maxBytes int64) *meteredReader {
	return &meteredReader{
		RecordReader: rdr,
		ctx:          ctx,
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
		bytesStreamed.Add(r.ctx, r.bytes)
	}
}

func (r *meteredReader) Release() {
	r.finish()
	r.RecordReader.Release()
}
