//go:build duckdb_arrow

package server

import (
	"context"
	"log/slog"
	"runtime/debug"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoveryMiddleware returns a flight.ServerMiddleware that turns a panic in
// any handler into an Internal error for that one call.
//
// grpc-go does not recover handler panics: without this, a single panic
// unwinds the serving goroutine and takes the whole process with it, dropping
// every in-flight query and every pinned DuckDB session. Install it as the
// outermost middleware so it also covers the auth and rate-limit layers.
//
// The panic value is logged with its stack but never returned to the client.
func RecoveryMiddleware() flight.ServerMiddleware {
	logPanic := func(ctx context.Context, method string, p any) error {
		slog.ErrorContext(ctx, "recovered panic in gRPC handler",
			slog.String("grpc_method", method),
			slog.Any("panic", p),
			slog.String("stack", string(debug.Stack())),
		)
		return status.Error(codes.Internal, "internal server error")
	}

	return flight.ServerMiddleware{
		Unary: func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
			defer func() {
				if p := recover(); p != nil {
					resp, err = nil, logPanic(ctx, info.FullMethod, p)
				}
			}()
			return handler(ctx, req)
		},
		Stream: func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
			defer func() {
				if p := recover(); p != nil {
					err = logPanic(ss.Context(), info.FullMethod, p)
				}
			}()
			return handler(srv, ss)
		},
	}
}
