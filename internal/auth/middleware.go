package auth

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"log/slog"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

	m := flight.CreateServerBasicAuthMiddleware(v)
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
// `Authorization: Bearer …`. The identity it returns is plumbed into the
// gRPC context via flight's authCtxKey, available to handlers if they care.
func (v *validator) IsValid(token string) (any, error) {
	if token == "" {
		return nil, status.Error(codes.Unauthenticated, "missing authorization header")
	}
	if _, ok := v.staticTokens[token]; ok {
		return "static", nil
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
		return sub, nil
	case v.local != nil && iss == localJWTIssuer:
		sub, err := v.local.verify(token)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "jwt: %s", err)
		}
		return sub, nil
	default:
		return nil, status.Error(codes.Unauthenticated, "unknown token issuer")
	}
}

// NewOIDCVerifier exposes oidc construction so cmd/server can build it during
// startup. Errors here should fail-fast.
func NewOIDCVerifier(ctx context.Context, issuer, audience string) (*oidcVerifier, error) {
	if issuer == "" {
		return nil, errors.New("issuer required")
	}
	return newOIDCVerifier(ctx, issuer, audience)
}
