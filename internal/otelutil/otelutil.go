// Package otelutil holds the OpenTelemetry wiring shared by every DuckFlight
// package: the instrumentation scope everything reports under, and the
// record-an-error-on-a-span idiom.
package otelutil

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName is the instrumentation scope name for every span and instrument
// DuckFlight emits, so its telemetry groups together in the collector.
const ScopeName = "duckflight"

// Tracer returns the DuckFlight tracer from the global provider. It is safe to
// call before the SDK is installed: the global provider delegates once
// [telemetry.Setup] registers the real one.
func Tracer() trace.Tracer { return otel.Tracer(ScopeName) }

// Meter returns the DuckFlight meter from the global provider, with the same
// pre-registration guarantee as [Tracer].
func Meter() metric.Meter { return otel.Meter(ScopeName) }

// RecordError attaches err to span and marks the span failed. A nil err is a
// no-op.
//
// Both halves matter: span.RecordError alone leaves the status Unset, and a
// trace UI shows an unset span as successful no matter what exception events
// hang off it.
func RecordError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// Failed is [RecordError] for the common case of failing out of a function,
// returning err so the call fits on the return statement:
//
//	return nil, otelutil.Failed(span, err)
func Failed(span trace.Span, err error) error {
	RecordError(span, err)
	return err
}
