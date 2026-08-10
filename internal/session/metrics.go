//go:build duckdb_arrow

package session

import (
	"context"
	"log/slog"

	"github.com/fairtier/duckflight/internal/otelutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Eviction reasons, reported as the `reason` attribute on
// duckflight.sessions.evicted. Telling them apart matters operationally:
// `closed` is a client saying goodbye, `reaped` is normal idle collection, but
// a rising `reclaimed` means new clients are stealing connections from live
// sessions because the pool is too small for the client count.
const (
	evictClosed    = "closed"
	evictReaped    = "reaped"
	evictReclaimed = "reclaimed"
	evictShutdown  = "shutdown"
)

// sessionMetrics are per-manager instruments; the active-session gauge reads
// the manager it belongs to, so it cannot be a package-level instrument.
type sessionMetrics struct {
	created metric.Int64Counter
	evicted metric.Int64Counter
	reg     metric.Registration
}

// newSessionMetrics wires m's instruments to the global meter provider. A
// telemetry failure degrades to no-op instruments rather than failing startup.
func newSessionMetrics(m *Manager) *sessionMetrics {
	sm, err := buildSessionMetrics(otelutil.Meter(), m)
	if err != nil {
		slog.Error("session metrics unavailable", slog.String("error", err.Error()))
		sm, _ = buildSessionMetrics(noop.Meter{}, m)
	}
	return sm
}

func buildSessionMetrics(meter metric.Meter, m *Manager) (*sessionMetrics, error) {
	sm := &sessionMetrics{}

	var err error
	sm.created, err = meter.Int64Counter("duckflight.sessions.created",
		metric.WithDescription("Sessions opened, each pinning one DuckDB connection."),
	)
	if err != nil {
		return nil, err
	}

	sm.evicted, err = meter.Int64Counter("duckflight.sessions.evicted",
		metric.WithDescription("Sessions torn down, by reason."),
	)
	if err != nil {
		return nil, err
	}

	active, err := meter.Int64ObservableGauge("duckflight.sessions.active",
		metric.WithDescription("Sessions currently holding a pinned connection."),
	)
	if err != nil {
		return nil, err
	}

	sm.reg, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(active, int64(m.Count()))
		return nil
	}, active)
	if err != nil {
		return nil, err
	}
	return sm, nil
}

func (sm *sessionMetrics) recordEvicted(reason string) {
	sm.evicted.Add(context.Background(), 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// stop drops the gauge callback so a shut-down manager stops being polled.
func (sm *sessionMetrics) stop() {
	if sm.reg != nil {
		_ = sm.reg.Unregister()
	}
}
