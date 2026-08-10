//go:build duckdb_arrow

package server

import (
	"context"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/fairtier/duckflight/internal/otelutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	queryCount    metric.Int64Counter
	queryDuration metric.Float64Histogram
	bytesStreamed metric.Int64Counter
	activeQueries metric.Int64UpDownCounter
	rowsStreamed  metric.Int64Counter
	rowsAffected  metric.Int64Counter
	resultSize    metric.Int64Histogram
	txnCount      metric.Int64Counter
)

// Values for the `operation` attribute on flightsql.rows.affected.
const (
	opUpdate         = "update"
	opPreparedUpdate = "prepared_update"
	opIngest         = "ingest"
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

	rowsStreamed, err = meter.Int64Counter("flightsql.rows.streamed",
		metric.WithDescription("Total rows returned to clients."),
	)
	if err != nil {
		panic(err)
	}

	rowsAffected, err = meter.Int64Counter("flightsql.rows.affected",
		metric.WithDescription("Total rows written by updates and ingestion."),
	)
	if err != nil {
		panic(err)
	}

	// Per-result size, where the byte counter only gives a total: a served
	// workload of many small results and one that occasionally ships a
	// gigabyte look identical in the counter and nothing alike here.
	resultSize, err = meter.Int64Histogram("flightsql.result.size",
		metric.WithDescription("Size of a single query result streamed to a client."),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9),
	)
	if err != nil {
		panic(err)
	}

	txnCount, err = meter.Int64Counter("flightsql.transactions",
		metric.WithDescription("Flight transactions by lifecycle action (begin/commit/rollback/reaped)."),
	)
	if err != nil {
		panic(err)
	}
}

// serverGauges holds the observable instruments that read live server state.
// They are per-server rather than package-level like the counters above,
// because their callbacks close over the server they report on.
type serverGauges struct {
	reg metric.Registration
}

// registerServerGauges publishes the open-resource counts of s. A telemetry
// failure is logged and otherwise ignored — it must never stop a server from
// starting.
func registerServerGauges(s *DuckFlightSQLServer) *serverGauges {
	g, err := buildServerGauges(otelutil.Meter(), s)
	if err != nil {
		slog.Error("server gauges unavailable", slog.String("error", err.Error()))
		return &serverGauges{}
	}
	return g
}

func buildServerGauges(meter metric.Meter, s *DuckFlightSQLServer) (*serverGauges, error) {
	txns, err := meter.Int64ObservableGauge("flightsql.transactions.active",
		metric.WithDescription("Flight transactions currently open."),
	)
	if err != nil {
		return nil, err
	}
	stmts, err := meter.Int64ObservableGauge("flightsql.prepared_statements.active",
		metric.WithDescription("Prepared statements currently open."),
	)
	if err != nil {
		return nil, err
	}

	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(txns, int64(s.OpenTransactionCount()))
		o.ObserveInt64(stmts, int64(s.PreparedStatementCount()))
		return nil
	}, txns, stmts)
	if err != nil {
		return nil, err
	}
	return &serverGauges{reg: reg}, nil
}

// stop drops the callback so a closed server stops being polled.
func (g *serverGauges) stop() {
	if g.reg != nil {
		_ = g.reg.Unregister()
	}
}

func queryCountAdd(ctx context.Context, status string) {
	queryCount.Add(ctx, 1, metric.WithAttributes(attribute.String("status", status)))
}

func rowsAffectedAdd(ctx context.Context, operation string, n int64) {
	if n <= 0 {
		return
	}
	rowsAffected.Add(ctx, n, metric.WithAttributes(attribute.String("operation", operation)))
}

func txnCountAdd(ctx context.Context, action string) {
	txnCount.Add(ctx, 1, metric.WithAttributes(attribute.String("action", action)))
}

// meteredReader wraps an array.RecordReader, counting bytes as they stream.
type meteredReader struct {
	array.RecordReader
	ctx      context.Context
	bytes    int64
	rows     int64
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
	r.rows += rec.NumRows()
	if r.maxBytes > 0 && r.bytes > r.maxBytes {
		// Stopping silently would hand the client a truncated result that is
		// indistinguishable from a complete one, with a gRPC OK on top. Fail
		// the stream instead; Err surfaces it to the caller.
		r.limitErr = status.Errorf(codes.ResourceExhausted,
			"result set exceeded MAX_RESULT_BYTES (%d bytes); result is incomplete", r.maxBytes)
		// An event rather than a span: the breach is instantaneous, and what
		// an operator needs is where in the stream it happened and how much
		// had already gone out — both of which the enclosing span dates.
		trace.SpanFromContext(r.ctx).AddEvent("result.limit_exceeded", trace.WithAttributes(
			attribute.Int64(attrBytes, r.bytes),
			attribute.Int64("flight.max_bytes", r.maxBytes),
			attribute.Int64(attrRows, r.rows),
		))
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
		rowsStreamed.Add(r.ctx, r.rows)
		resultSize.Record(r.ctx, r.bytes)
	}
}

func (r *meteredReader) Release() {
	r.finish()
	r.RecordReader.Release()
}
