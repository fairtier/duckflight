//go:build duckdb_arrow

package server

import (
	"context"
	"errors"

	"github.com/fairtier/duckflight/internal/auth"
	"github.com/fairtier/duckflight/internal/engine"
	"github.com/fairtier/duckflight/internal/session"
	"github.com/fairtier/duckflight/internal/sqlguard"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// plane is one DuckDB instance with its pool and session manager. A server
// has the main, read-write plane and, when read-only sessions are enabled, a
// second plane on the hardened read-only instance. A request is served by
// exactly one of them, chosen by its identity: a session, transaction or
// pooled connection never crosses from one to the other.
type plane struct {
	engine   *engine.Engine
	sessions *session.Manager
	readOnly bool
}

// errReadOnlyDisabled answers a read-only request on a server that has no
// read-only instance. Refusing is the only safe answer: running the request
// read-write would ignore what the caller asked for.
var errReadOnlyDisabled = status.Error(codes.FailedPrecondition,
	"read-only sessions are not enabled on this server")

// planeFor picks the plane that serves the request in ctx.
func (s *DuckFlightSQLServer) planeFor(ctx context.Context) (*plane, error) {
	if !auth.ReadOnlyFromContext(ctx) {
		return s.rw, nil
	}
	if s.ro == nil {
		return nil, errReadOnlyDisabled
	}
	return s.ro, nil
}

// checkReadOnlySQL applies the read-only statement policy to query when the
// request is read-only, and is a no-op otherwise. It runs before a
// connection is touched, on the guard's own instance, so a refused statement
// never reaches the serving engine at all.
func (s *DuckFlightSQLServer) checkReadOnlySQL(ctx context.Context, query string) error {
	if !auth.ReadOnlyFromContext(ctx) {
		return nil
	}
	if s.ro == nil {
		return errReadOnlyDisabled
	}
	err := s.guard.CheckReadOnly(ctx, query)
	if err == nil {
		return nil
	}
	// An event on the request span, so a refusal is visible without logging
	// the SQL. The reason names the construct, never the statement text.
	var v *sqlguard.Violation
	var pe *sqlguard.ParseError
	switch {
	case errors.As(err, &v):
		trace.SpanFromContext(ctx).AddEvent("read_only.refused",
			trace.WithAttributes(attribute.String("reason", v.Reason)))
		return status.Error(codes.PermissionDenied, v.Error())
	case errors.As(err, &pe):
		return status.Errorf(codes.InvalidArgument, "syntax error: %s", pe.Msg)
	}
	return status.Errorf(codes.Internal, "classify statement: %s", err)
}

// refuseReadOnly is for RPCs that only write (bulk ingestion): nothing a
// read-only caller sends through them can be allowed.
func refuseReadOnly(ctx context.Context, what string) error {
	if auth.ReadOnlyFromContext(ctx) {
		return status.Errorf(codes.PermissionDenied, "read-only session: %s is not allowed", what)
	}
	return nil
}
