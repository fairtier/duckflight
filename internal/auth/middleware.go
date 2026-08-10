package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/fairtier/duckflight/internal/grpcutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	authHeader   = "authorization"
	basicScheme  = "Basic"
	bearerScheme = "Bearer"
)

// minJWTSecretLen is the shortest accepted AUTH_JWT_SECRET. HS256 tokens are
// only as strong as their key: a short secret is brute-forceable offline, and
// forging one yields a token with an attacker-chosen subject *and* session id,
// which is enough to take over another client's DuckDB session.
const minJWTSecretLen = 32

// identityCtxKey is the context key under which the authenticated identity is
// stored. Auth is handled here rather than via arrow-go's basic-auth
// middleware: that middleware parses the Authorization header before any
// validator runs and panics on malformed input (a bare `Bearer` slices past
// the end of the string; a `Basic` credential without a colon indexes past the
// end of the split), which an unauthenticated peer can trigger at will. gRPC
// does not recover handler panics, so each one takes the process down.
type identityCtxKey struct{}

// Identity is the value stored on the gRPC context by the auth middleware.
// Retrieve it with [IdentityFromContext], or pull just the session id with
// [SessionIDFromContext].
type Identity struct {
	// Subject is the authenticated principal (username, OIDC sub, or a static
	// marker like "static").
	Subject string
	// SessionID pins the client to a server-side session backed by a dedicated
	// DuckDB connection. For Handshake-issued JWTs this is a UUID generated at
	// issue time; for OIDC and static tokens it is derived per client
	// connection so that two clients sharing a token don't share a session.
	SessionID string
	// Method is which backend accepted the credential, and SessionSource which
	// rung of the session-id precedence produced SessionID. Both are recorded
	// on the request span: when a client reports that its temp tables vanished
	// or that two clients collided on one session, these two labels are the
	// answer, and they are cheap, bounded and free of user identifiers.
	Method        string
	SessionSource string
}

// Values for [Identity.Method].
const (
	MethodStatic = "static"
	MethodJWT    = "jwt"
	MethodOIDC   = "oidc"
)

// Values for [Identity.SessionSource], in precedence order.
const (
	// SessionSourceJWT is a sid claim stamped into a handshake-issued JWT.
	SessionSourceJWT = "jwt"
	// SessionSourceCookie is a valid echoed session cookie — the rung that
	// survives an L7 proxy pooling upstream connections.
	SessionSourceCookie = "cookie"
	// SessionSourcePeer is the fallback derivation from token + peer address,
	// which is what clients that do not echo cookies get.
	SessionSourcePeer = "peer"
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

// Backends names the auth backends a Config enables, for the startup log line.
func (c Config) Backends() []string {
	var out []string
	if len(c.Users) > 0 {
		out = append(out, "basic+jwt")
	}
	if len(c.StaticTokens) > 0 {
		out = append(out, "static-tokens")
	}
	if c.OIDC != nil {
		out = append(out, "oidc")
	}
	return out
}

// Middleware builds the Flight ServerMiddleware. Returns nil when no backend
// is configured (auth disabled).
func Middleware(cfg Config) (*flight.ServerMiddleware, error) {
	if len(cfg.Users) == 0 && len(cfg.StaticTokens) == 0 && cfg.OIDC == nil {
		return nil, nil
	}

	v := &validator{
		users:        cfg.Users,
		staticTokens: make(map[[sha256.Size]byte]struct{}, len(cfg.StaticTokens)),
		oidc:         cfg.OIDC,
	}
	for _, t := range cfg.StaticTokens {
		v.staticTokens[sha256.Sum256([]byte(t))] = struct{}{}
	}
	if len(cfg.Users) > 0 {
		secret := cfg.JWTSecret
		switch {
		case len(secret) == 0:
			buf := make([]byte, minJWTSecretLen)
			if _, err := rand.Read(buf); err != nil {
				return nil, err
			}
			secret = buf
			slog.Warn("AUTH_JWT_SECRET not set; generated ephemeral secret — handshake-issued tokens will not survive restart or work across replicas")
		case len(secret) < minJWTSecretLen:
			return nil, fmt.Errorf("AUTH_JWT_SECRET must be at least %d bytes, got %d", minJWTSecretLen, len(secret))
		}
		ttl := cfg.JWTTTL
		if ttl <= 0 {
			ttl = time.Hour
		}
		v.local = newLocalJWT(secret, ttl)
	}

	// The cookie key follows AUTH_JWT_SECRET even when the Handshake flow is
	// off, so a static/OIDC-only multi-replica deployment can keep cookies
	// valid across replicas by setting the secret.
	ca, err := newCookieAuthority(cfg.JWTSecret)
	if err != nil {
		return nil, err
	}
	v.cookies = ca

	m := flight.ServerMiddleware{
		Unary: func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if grpcutil.IsHealthMethod(info.FullMethod) {
				return handler(ctx, req)
			}
			authCtx, err := v.authenticate(ctx)
			if err != nil {
				return nil, err
			}
			return handler(authCtx, req)
		},
		Stream: func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if grpcutil.IsHealthMethod(info.FullMethod) {
				return handler(srv, ss)
			}
			if strings.HasSuffix(info.FullMethod, "/Handshake") {
				return v.handshake(srv, ss, handler)
			}
			authCtx, err := v.authenticate(ss.Context())
			if err != nil {
				return err
			}
			return handler(srv, &wrappedStream{ServerStream: ss, ctx: authCtx})
		},
	}
	return &m, nil
}

// wrappedStream overrides a server stream's context so downstream handlers see
// the authenticated identity.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// validator implements the auth backends.
type validator struct {
	users        map[string]string
	local        *localJWT
	oidc         *oidcVerifier
	staticTokens map[[sha256.Size]byte]struct{}
	// cookies mints/verifies the session cookie that gives static/OIDC
	// clients a per-client session id independent of the transport. Always
	// non-nil when the middleware is enabled.
	cookies *cookieAuthority
}

// credentialFromContext pulls `Authorization: <scheme> <credential>` out of the
// request metadata. It is deliberately strict about the header's shape —
// every malformed variant is rejected here, before any parsing that assumes
// well-formed input.
func credentialFromContext(ctx context.Context) (scheme, credential string, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", status.Error(codes.Unauthenticated, "missing authorization header")
	}
	vals := md.Get(authHeader)
	if len(vals) == 0 {
		return "", "", status.Error(codes.Unauthenticated, "missing authorization header")
	}
	scheme, credential, found := strings.Cut(strings.TrimSpace(vals[0]), " ")
	if !found {
		return "", "", status.Error(codes.Unauthenticated, "malformed authorization header: expected '<scheme> <credential>'")
	}
	// Only the separating space is trimmed, per the HTTP auth grammar;
	// trailing whitespace can be significant in a credential.
	credential = strings.TrimLeft(credential, " ")
	if scheme == "" || credential == "" {
		return "", "", status.Error(codes.Unauthenticated, "malformed authorization header: empty scheme or credential")
	}
	return scheme, credential, nil
}

// authenticate validates the bearer token on a non-Handshake call and returns
// a context carrying the resulting [Identity].
func (v *validator) authenticate(ctx context.Context) (context.Context, error) {
	scheme, credential, err := credentialFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if scheme != bearerScheme {
		return nil, status.Errorf(codes.Unauthenticated, "unsupported authorization scheme %q", scheme)
	}
	id, setCookie, err := v.identify(ctx, credential)
	if err != nil {
		return nil, err
	}

	// Attributes on the RPC's existing span rather than a span of their own:
	// authentication is a step inside the call, not a unit of work worth its
	// own timing. Deliberately absent is the subject — a username or an OIDC
	// `sub` is a user identifier, and spans travel to the collector.
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("auth.method", id.Method),
		attribute.String("session.source", id.SessionSource),
		attribute.String("session.id", id.SessionID),
	)
	if setCookie != "" {
		// Best effort: a client without a cookie jar ignores it, and a
		// SetHeader failure only means this request keeps its peer-derived
		// session id — never a reason to fail the RPC.
		if err := grpc.SetHeader(ctx, metadata.Pairs("set-cookie", setCookie)); err != nil {
			slog.Debug("session set-cookie not sent", slog.String("error", err.Error()))
		}
	}
	return context.WithValue(ctx, identityCtxKey{}, id), nil
}

// handshake implements the Flight Handshake exchange: Basic credentials in,
// a freshly minted bearer token back on the response trailer.
func (v *validator) handshake(srv any, ss grpc.ServerStream, handler grpc.StreamHandler) error {
	scheme, credential, err := credentialFromContext(ss.Context())
	if err != nil {
		return err
	}
	if scheme != basicScheme {
		return status.Error(codes.Unauthenticated, "only Basic auth is implemented for Handshake")
	}

	raw, err := base64.RawStdEncoding.DecodeString(credential)
	if err != nil {
		if raw, err = base64.StdEncoding.DecodeString(credential); err != nil {
			return status.Errorf(codes.Unauthenticated, "invalid basic auth encoding: %s", err)
		}
	}
	// A credential without a colon has no password field at all. Splitting and
	// indexing blindly is what panics arrow-go's own middleware.
	username, password, found := strings.Cut(string(raw), ":")
	if !found {
		return status.Error(codes.Unauthenticated, "malformed basic auth credential: expected 'username:password'")
	}

	token, err := v.issueToken(username, password)
	if err != nil {
		return err
	}

	ss.SetTrailer(metadata.New(map[string]string{authHeader: bearerScheme + " " + token}))
	return handler(srv, ss)
}

// issueToken validates Basic credentials and mints a local JWT.
func (v *validator) issueToken(username, password string) (string, error) {
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

// matchesStaticToken reports whether token is in the allowlist, comparing
// digests in constant time and without short-circuiting on the first match.
// The digest already destroys any prefix correlation an attacker could time,
// so this is consistency with the password path rather than a live fix.
func (v *validator) matchesStaticToken(token string) bool {
	sum := sha256.Sum256([]byte(token))
	var match int
	for candidate := range v.staticTokens {
		match |= subtle.ConstantTimeCompare(sum[:], candidate[:])
	}
	return match == 1
}

// identify resolves a bearer token to an [Identity]. The second return value
// is a Set-Cookie header value to send back, or "" — returned rather than set
// here so identify stays free of gRPC side effects.
func (v *validator) identify(ctx context.Context, token string) (Identity, string, error) {
	if v.matchesStaticToken(token) {
		// Bind to the raw token: every static token shares the "static"
		// subject, so binding to the subject would merge distinct tokens
		// that happen to present the same cookie.
		sid, setCookie, src := v.sessionForToken(ctx, token, token)
		return Identity{
			Subject:       "static",
			SessionID:     sid,
			Method:        MethodStatic,
			SessionSource: src,
		}, setCookie, nil
	}

	iss, err := tokenIssuer(token)
	if err != nil {
		// Not a JWT and not in the static allowlist.
		return Identity{}, "", status.Error(codes.Unauthenticated, "invalid bearer token")
	}

	switch {
	case v.oidc != nil && iss == v.oidc.issuer:
		sub, err := v.oidc.verify(token)
		if err != nil {
			return Identity{}, "", status.Errorf(codes.Unauthenticated, "oidc: %s", err)
		}
		// An external issuer can't stamp our session ids, so use the cookie.
		// Bind to issuer+subject rather than the raw token: OAuth refresh
		// rotates the token mid-session, and rebinding on every rotation
		// would orphan the client's temp tables and open transactions.
		sid, setCookie, src := v.sessionForToken(ctx, token, "oidc\x00"+v.oidc.issuer+"\x00"+sub)
		return Identity{
			Subject:       sub,
			SessionID:     sid,
			Method:        MethodOIDC,
			SessionSource: src,
		}, setCookie, nil
	case v.local != nil && iss == localJWTIssuer:
		sub, sid, err := v.local.verify(token)
		if err != nil {
			return Identity{}, "", status.Errorf(codes.Unauthenticated, "jwt: %s", err)
		}
		// A token with a server-stamped sid needs no cookie. An old token
		// minted before the sid claim existed is exactly the "no stamped
		// sid" case the cookie exists for.
		if sid != "" {
			return Identity{
				Subject:       sub,
				SessionID:     sid,
				Method:        MethodJWT,
				SessionSource: SessionSourceJWT,
			}, "", nil
		}
		sid, setCookie, src := v.sessionForToken(ctx, token, token)
		return Identity{
			Subject:       sub,
			SessionID:     sid,
			Method:        MethodJWT,
			SessionSource: src,
		}, setCookie, nil
	default:
		return Identity{}, "", status.Error(codes.Unauthenticated, "unknown token issuer")
	}
}

// sessionForToken resolves the session id for a bearer token that carries no
// server-stamped session id of its own. A valid echoed cookie wins — it
// identifies the client regardless of which socket (or L7 proxy connection)
// the request arrived on. Otherwise this request keeps the peer-derived id
// and a fresh cookie is minted for the next one; clients that never echo
// cookies therefore keep the peer-derived behavior exactly.
func (v *validator) sessionForToken(ctx context.Context, token, binding string) (sid, setCookie, source string) {
	if val, ok := incomingSessionCookie(ctx); ok {
		if id, ok := v.cookies.verify(val); ok {
			return cookieSessionID(id, binding), "", SessionSourceCookie
		}
	}
	sid = deriveSessionID(ctx, token)
	mv, err := v.cookies.mint()
	if err != nil {
		slog.Warn("session cookie mint failed; using peer-derived session id", slog.String("error", err.Error()))
		return sid, "", SessionSourcePeer
	}
	return sid, (&http.Cookie{Name: sessionCookieName, Value: mv}).String(), SessionSourcePeer
}

// deriveSessionID builds a stable, opaque session id for a bearer token that
// carries no session id of its own (static tokens, OIDC tokens) and whose
// request echoed no valid session cookie — it is the fallback rung under
// [validator.sessionForToken]'s cookie lookup.
//
// The peer address is mixed in so the id identifies a *client connection*
// rather than the token. Keying on the token alone would collapse every client
// presenting the same token onto one session and therefore one pinned DuckDB
// connection: temp tables and SET overrides would be visible across
// principals, one connection's COMMIT would commit another's uncommitted
// writes, and every such client would execute strictly serially behind the
// session mutex.
func deriveSessionID(ctx context.Context, token string) string {
	h := sha256.New()
	h.Write([]byte(token))
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		h.Write([]byte{0})
		h.Write([]byte(p.Addr.String()))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// IdentityFromContext returns the authenticated [Identity] attached by the
// auth middleware, or (zero, false) if none is present (auth disabled).
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(identityCtxKey{}).(Identity)
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
