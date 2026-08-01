//go:build duckdb_arrow

package ratelimit

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/fairtier/duckflight/internal/grpcutil"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var rejected metric.Int64Counter

func init() {
	initMetrics(noop.Meter{})
}

// InitMetrics re-initializes metric instruments with the given meter.
func InitMetrics(meter metric.Meter) {
	initMetrics(meter)
}

func initMetrics(meter metric.Meter) {
	var err error
	rejected, err = meter.Int64Counter("flightsql.ratelimit.rejected",
		metric.WithDescription("Total number of requests rejected by rate limiting."),
	)
	if err != nil {
		panic(err)
	}
}

// Middleware returns a flight.ServerMiddleware that enforces a global
// token-bucket rate limit. Returns nil if rps <= 0 (disabled).
//
// It is installed *outside* the auth middleware so that the expensive part of
// authentication — JWT parsing, JWKS lookup, RSA signature verification —
// is itself rate limited. Behind auth, a flood of syntactically valid but
// bogus RS256 tokens would each cost a full signature verification (and an
// attacker-chosen `kid` would drive a JWKS refresh against the IdP) while
// consuming no tokens at all.
func Middleware(rps float64, burst int) *flight.ServerMiddleware {
	if rps <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = max(1, int(rps))
	}

	limiter := rate.NewLimiter(rate.Limit(rps), burst)

	check := func(ctx context.Context, fullMethod string) error {
		// Health probes are exempt: shedding them turns a load spike into a
		// failed liveness probe and a restarted-but-healthy pod.
		if grpcutil.IsHealthMethod(fullMethod) {
			return nil
		}
		if !limiter.Allow() {
			rejected.Add(ctx, 1)
			return status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}
		return nil
	}

	return &flight.ServerMiddleware{
		Unary: func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := check(ctx, info.FullMethod); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		},
		Stream: func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := check(ss.Context(), info.FullMethod); err != nil {
				return err
			}
			return handler(srv, ss)
		},
	}
}
