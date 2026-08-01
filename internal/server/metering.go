//go:build duckdb_arrow

package server

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	limitErr error
}

func newMeteredReader(ctx context.Context, rdr array.RecordReader, maxBytes int64) *meteredReader {
	return &meteredReader{
		RecordReader: rdr,
		ctx:          ctx,
		maxBytes:     maxBytes,
	}
}

func (r *meteredReader) Next() bool {
	if r.limitErr != nil {
		return false
	}
	if !r.RecordReader.Next() {
		r.finish()
		return false
	}
	rec := r.RecordBatch()
	for i := 0; i < int(rec.NumCols()); i++ {
		r.bytes += arrayBytes(rec.Column(i))
	}
	if r.maxBytes > 0 && r.bytes > r.maxBytes {
		// Stopping silently would hand the client a truncated result that is
		// indistinguishable from a complete one, with a gRPC OK on top. Fail
		// the stream instead; Err surfaces it to the caller.
		r.limitErr = status.Errorf(codes.ResourceExhausted,
			"result set exceeded MAX_RESULT_BYTES (%d bytes); result is incomplete", r.maxBytes)
		r.finish()
		return false
	}
	return true
}

// Err reports the limit breach ahead of the underlying reader's own error, so
// a truncated stream can never be reported as a successful one.
func (r *meteredReader) Err() error {
	if r.limitErr != nil {
		return r.limitErr
	}
	return r.RecordReader.Err()
}

// arrayBytes totals the memory backing an array, including the child arrays of
// nested types (LIST/STRUCT/MAP/union) and dictionary values. Counting only
// the top-level buffers would see just validity bitmaps and offsets for those
// types, letting a `SELECT list(payload)` stream gigabytes past a cap that
// never trips and bill for a few kilobytes.
func arrayBytes(a arrow.Array) int64 {
	if a == nil {
		return 0
	}
	return dataBytes(a.Data())
}

func dataBytes(d arrow.ArrayData) int64 {
	if isNilData(d) {
		return 0
	}
	var n int64
	for _, buf := range d.Buffers() {
		if buf != nil {
			n += int64(buf.Len())
		}
	}
	for _, child := range d.Children() {
		n += dataBytes(child)
	}
	n += dataBytes(d.Dictionary())
	return n
}

// isNilData also catches a non-nil arrow.ArrayData interface wrapping a nil
// *array.Data, which is what Dictionary() returns for a non-dictionary array.
func isNilData(d arrow.ArrayData) bool {
	if d == nil {
		return true
	}
	concrete, ok := d.(*array.Data)
	return ok && concrete == nil
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
