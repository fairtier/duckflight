package auth

import (
	"context"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// BearerTokenMiddleware returns a flight.ServerMiddleware that validates bearer
// tokens against the provided set of valid tokens.
// If tokens is empty, returns nil (auth disabled).
func BearerTokenMiddleware(tokens []string) *flight.ServerMiddleware {
	if len(tokens) == 0 {
		return nil
	}
	valid := make(map[string]struct{}, len(tokens))
	for _, t := range tokens {
		valid[t] = struct{}{}
	}

	validate := func(ctx context.Context) error {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return status.Error(codes.Unauthenticated, "missing metadata")
		}
		vals := md.Get("authorization")
		if len(vals) == 0 {
			return status.Error(codes.Unauthenticated, "missing authorization header")
		}
		token := vals[0]
		token = strings.TrimPrefix(token, "Bearer ")
		if _, ok := valid[token]; !ok {
			return status.Error(codes.Unauthenticated, "invalid bearer token")
		}
		return nil
	}

	return &flight.ServerMiddleware{
		Unary: func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := validate(ctx); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		},
		Stream: func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := validate(ss.Context()); err != nil {
				return err
			}
			return handler(srv, ss)
		},
	}
}
