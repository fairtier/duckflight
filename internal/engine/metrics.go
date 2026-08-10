//go:build duckdb_arrow

package engine

import (
	"context"
	"log/slog"

	"github.com/fairtier/duckflight/internal/otelutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Recycle reasons for [ArrowPool.Discard] / [ArrowPool.Release], reported as
// the `reason` attribute on duckflight.pool.connections.recycled. Distinguishes
// the cheap, expected churn (a client ran SET/BEGIN/DDL, so the connection is
// recycled on release) from connections torn down while still checked out.
const (
	recycleDirty     = "dirty"
	recycleDiscarded = "discarded"
)

// poolMetrics are per-pool instruments. Unlike the package-level instruments
// elsewhere in the tree, these are instance-scoped because the observable
// gauges read the pool they belong to; the callback registration is dropped
// when the pool closes so a dead pool stops reporting.
type poolMetrics struct {
	acquireDuration metric.Float64Histogram
	recycled        metric.Int64Counter
	lost            metric.Int64Counter
	reg             metric.Registration
}

// newPoolMetrics wires p's instruments to the global meter provider. Telemetry
// setup failure is never a reason to fail a pool: on error the pool keeps
// running with no-op instruments.
func newPoolMetrics(p *ArrowPool) *poolMetrics {
	pm, err := buildPoolMetrics(otelutil.Meter(), p)
	if err != nil {
		slog.Error("pool metrics unavailable", slog.String("error", err.Error()))
		pm, _ = buildPoolMetrics(noop.Meter{}, p)
	}
	return pm
}

func buildPoolMetrics(meter metric.Meter, p *ArrowPool) (*poolMetrics, error) {
	pm := &poolMetrics{}

	var err error
	pm.acquireDuration, err = meter.Float64Histogram("duckflight.pool.acquire.duration",
		metric.WithDescription("Time spent waiting for a connection from the pool."),
		metric.WithUnit("s"),
		// Buckets cluster near zero: an acquire that isn't instant means the
		// pool is saturated, which is the thing worth seeing early.
		metric.WithExplicitBucketBoundaries(0.0001, 0.001, 0.01, 0.1, 0.5, 1, 5, 30),
	)
	if err != nil {
		return nil, err
	}

	pm.recycled, err = meter.Int64Counter("duckflight.pool.connections.recycled",
		metric.WithDescription("Connections destroyed and replaced instead of being pooled again."),
	)
	if err != nil {
		return nil, err
	}

	pm.lost, err = meter.Int64Counter("duckflight.pool.connections.lost",
		metric.WithDescription("Connections whose replacement failed to boot, permanently shrinking the pool."),
	)
	if err != nil {
		return nil, err
	}

	idle, err := meter.Int64ObservableGauge("duckflight.pool.connections.idle",
		metric.WithDescription("Connections currently available in the pool."),
	)
	if err != nil {
		return nil, err
	}
	size, err := meter.Int64ObservableGauge("duckflight.pool.connections.max",
		metric.WithDescription("Pool capacity, i.e. the maximum number of concurrent connections."),
	)
	if err != nil {
		return nil, err
	}

	pm.reg, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(idle, int64(p.Len()))
		o.ObserveInt64(size, int64(p.Cap()))
		return nil
	}, idle, size)
	if err != nil {
		return nil, err
	}
	return pm, nil
}

func (pm *poolMetrics) recordRecycled(reason string) {
	pm.recycled.Add(context.Background(), 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// stop drops the gauge callback so a closed pool stops being polled.
func (pm *poolMetrics) stop() {
	if pm.reg != nil {
		_ = pm.reg.Unregister()
	}
}
