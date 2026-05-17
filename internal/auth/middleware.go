package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// healthMethodPrefix matches grpc.health.v1.Health/Check and /Watch.
// Kubernetes gRPC probes can't send bearer tokens, so the auth middleware
// must let health RPCs through unconditionally.
const healthMethodPrefix = "/grpc.health.v1.Health/"

// Identity is the value stored on the gRPC context by the auth middleware.
// Retrieve it with [flight.AuthFromContext] or [IdentityFromContext]; pull
// just the session id with [SessionIDFromContext].
type Identity struct {
	// Subject is the authenticated principal (username, OIDC sub, or a static
	// marker like "static").
	Subject string
	// SessionID pins the client to a server-side session backed by a dedicated
	// DuckDB connection. For Handshake-issued JWTs this is a UUID generated at
	// issue time; for OIDC and static tokens it's a deterministic hash of the
	// raw bearer token so the same client gets a stable session for the
	// token's lifetime.
	SessionID string
}

// Config bundles every auth backend the server can be wired with. Any nil/zero
// field disables that backend; if all are zero, Middleware returns nil and
// auth is effectively off.
type Config struct {
	// Users maps username → password for the Handshake basic-auth flow.
	Users map[string]string
	// JWTSecret signs/verifies HS256 tokens issued during Handshake.
	// Required iff Users is non-empty.
	JWTSecret []byte
	// JWTTTL is the lifetime of locally issued JWTs.
	JWTTTL time.Duration
	// OIDC, if non-nil, accepts tokens minted by an external IdP.
	OIDC *oidcVerifier
	// StaticTokens is the opaque-token allowlist (legacy AUTH_TOKENS).
	StaticTokens []string
}

// Middleware builds the Flight ServerMiddleware. Returns nil when no backend
// is configured (auth disabled, today's default behavior).
func Middleware(cfg Config) (*flight.ServerMiddleware, error) {
	if len(cfg.Users) == 0 && len(cfg.StaticTokens) == 0 && cfg.OIDC == nil {
		return nil, nil
	}

	v := &validator{
		users:        cfg.Users,
		staticTokens: make(map[string]struct{}, len(cfg.StaticTokens)),
		oidc:         cfg.OIDC,
	}
	for _, t := range cfg.StaticTokens {
		v.staticTokens[t] = struct{}{}
	}
	if len(cfg.Users) > 0 {
		secret := cfg.JWTSecret
		if len(secret) == 0 {
			buf := make([]byte, 32)
			if _, err := rand.Read(buf); err != nil {
				return nil, err
			}
			secret = buf
			slog.Warn("AUTH_JWT_SECRET not set; generated ephemeral secret — handshake-issued tokens will not survive restart or work across replicas")
		}
		ttl := cfg.JWTTTL
		if ttl <= 0 {
			ttl = time.Hour
		}
		v.local = newLocalJWT(secret, ttl)
	}

	inner := flight.CreateServerBasicAuthMiddleware(v)
	m := flight.ServerMiddleware{
		Unary: func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if strings.HasPrefix(info.FullMethod, healthMethodPrefix) {
				return handler(ctx, req)
			}
			return inner.Unary(ctx, req, info, handler)
		},
		Stream: func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if strings.HasPrefix(info.FullMethod, healthMethodPrefix) {
				return handler(srv, ss)
			}
			return inner.Stream(srv, ss, info, handler)
		},
	}
	return &m, nil
}

// validator implements flight.BasicAuthValidator across all configured backends.
type validator struct {
	users        map[string]string
	local        *localJWT
	oidc         *oidcVerifier
	staticTokens map[string]struct{}
}

// Validate is called by Flight Handshake with Basic credentials.
func (v *validator) Validate(username, password string) (string, error) {
	if v.local == nil {
		return "", status.Error(codes.Unauthenticated, "basic auth not enabled")
	}
	expected, ok := v.users[username]
	if !ok || subtle.ConstantTimeCompare([]byte(expected), []byte(password)) != 1 {
		return "", status.Error(codes.Unauthenticated, "invalid username or password")
	}
	tok, err := v.local.issue(username)
	if err != nil {
		return "", status.Errorf(codes.Internal, "issue jwt: %s", err)
	}
	return tok, nil
}

// IsValid is called by every other RPC with the bearer token from
// `Authorization: Bearer …`. The returned [Identity] is plumbed into the
// gRPC context via flight's authCtxKey; handlers may pull it back out with
// [flight.AuthFromContext] (or the typed helpers in this package).
func (v *validator) IsValid(token string) (any, error) {
	if token == "" {
		return nil, status.Error(codes.Unauthenticated, "missing authorization header")
	}
	if _, ok := v.staticTokens[token]; ok {
		return Identity{Subject: "static", SessionID: deriveSessionID(token)}, nil
	}

	iss, err := tokenIssuer(token)
	if err != nil {
		// Not a JWT and not in the static allowlist.
		return nil, status.Error(codes.Unauthenticated, "invalid bearer token")
	}

	switch {
	case v.oidc != nil && iss == v.oidc.issuer:
		sub, err := v.oidc.verify(token)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "oidc: %s", err)
		}
		// External issuer can't stamp our session ids; derive a stable one
		// from the token so the same bearer always lands on the same session.
		return Identity{Subject: sub, SessionID: deriveSessionID(token)}, nil
	case v.local != nil && iss == localJWTIssuer:
		sub, sid, err := v.local.verify(token)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "jwt: %s", err)
		}
		// Fallback: an old token minted before the sid claim existed gets a
		// derived id so it still pins to a session.
		if sid == "" {
			sid = deriveSessionID(token)
		}
		return Identity{Subject: sub, SessionID: sid}, nil
	default:
		return nil, status.Error(codes.Unauthenticated, "unknown token issuer")
	}
}

// deriveSessionID hashes a bearer token into a stable, opaque session id.
func deriveSessionID(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:8]) // 16 hex chars — collision-resistant per token
}

// IdentityFromContext returns the authenticated [Identity] attached by the
// auth middleware, or (zero, false) if none is present (auth disabled).
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	v := flight.AuthFromContext(ctx)
	if v == nil {
		return Identity{}, false
	}
	id, ok := v.(Identity)
	return id, ok
}

// SessionIDFromContext returns the session id of the authenticated request,
// or "" if none is present.
func SessionIDFromContext(ctx context.Context) string {
	id, _ := IdentityFromContext(ctx)
	return id.SessionID
}

// NewOIDCVerifier exposes oidc construction so cmd/server can build it during
// startup. Errors here should fail-fast.
func NewOIDCVerifier(ctx context.Context, issuer, audience string) (*oidcVerifier, error) {
	if issuer == "" {
		return nil, errors.New("issuer required")
	}
	return newOIDCVerifier(ctx, issuer, audience)
}
