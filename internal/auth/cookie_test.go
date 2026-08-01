package auth

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// cookieCtx fabricates the incoming context the middleware sees: a peer
// address plus any number of raw "cookie" metadata values.
func cookieCtx(peerAddr string, cookieVals ...string) context.Context {
	ctx := context.Background()
	if peerAddr != "" {
		host, port, _ := strings.Cut(peerAddr, ":")
		_ = port
		ctx = peer.NewContext(ctx, &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP(host), Port: 12345}})
	}
	md := metadata.MD{}
	for _, v := range cookieVals {
		md.Append("cookie", v)
	}
	return metadata.NewIncomingContext(ctx, md)
}

func TestCookieMintVerifyRoundTrip(t *testing.T) {
	ca, err := newCookieAuthority(nil)
	require.NoError(t, err)

	val, err := ca.mint()
	require.NoError(t, err)
	id, ok := ca.verify(val)
	require.True(t, ok)
	require.Len(t, id, cookieIDLen)

	// Tampering anywhere in the value must read as "no cookie".
	for _, bad := range []string{
		"",
		"not-base64!!",
		val[:len(val)-4],                   // truncated
		"A" + val[1:],                      // flipped id byte
		val[:len(val)-1] + flip(val[len(val)-1]), // flipped tag byte
		val + val,                          // wrong length
	} {
		_, ok := ca.verify(bad)
		require.False(t, ok, "verify(%q) must fail", bad)
	}
}

// flip returns a different base64url character than the input.
func flip(c byte) string {
	if c == 'A' {
		return "B"
	}
	return "A"
}

func TestCookieKeyFollowsJWTSecret(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")

	a, err := newCookieAuthority(secret)
	require.NoError(t, err)
	b, err := newCookieAuthority(secret)
	require.NoError(t, err)
	other, err := newCookieAuthority([]byte("fedcba9876543210fedcba9876543210"))
	require.NoError(t, err)
	random, err := newCookieAuthority(nil)
	require.NoError(t, err)

	// The key is derived, never the secret itself.
	require.NotEqual(t, secret, a.key)

	val, err := a.mint()
	require.NoError(t, err)

	// Same secret ⇒ replicas honor each other's cookies.
	_, ok := b.verify(val)
	require.True(t, ok)
	// Different or absent secret ⇒ foreign cookies read as absent.
	_, ok = other.verify(val)
	require.False(t, ok)
	_, ok = random.verify(val)
	require.False(t, ok)
}

func TestSessionForTokenPrecedence(t *testing.T) {
	ca, err := newCookieAuthority(nil)
	require.NoError(t, err)
	v := &validator{cookies: ca}
	const token = "tok"

	cookieVal, err := ca.mint()
	require.NoError(t, err)
	cookie := sessionCookieName + "=" + cookieVal

	t.Run("no cookie falls back to peer and mints", func(t *testing.T) {
		ctx := cookieCtx("10.0.0.1:1")
		sid, setCookie := v.sessionForToken(ctx, token, token)
		require.Equal(t, deriveSessionID(ctx, token), sid)
		require.NotEmpty(t, setCookie)

		sc, err := http.ParseSetCookie(setCookie)
		require.NoError(t, err)
		require.Equal(t, sessionCookieName, sc.Name)
		_, ok := ca.verify(sc.Value)
		require.True(t, ok, "minted cookie must verify")
	})

	t.Run("valid cookie wins over peer address", func(t *testing.T) {
		sidA, scA := v.sessionForToken(cookieCtx("10.0.0.1:1", cookie), token, token)
		sidB, scB := v.sessionForToken(cookieCtx("10.9.9.9:1", cookie), token, token)
		require.Equal(t, sidA, sidB, "same cookie must map to one session regardless of peer")
		require.Empty(t, scA)
		require.Empty(t, scB)
	})

	t.Run("forged cookie reads as absent", func(t *testing.T) {
		ctx := cookieCtx("10.0.0.1:1", sessionCookieName+"=forged-garbage")
		sid, setCookie := v.sessionForToken(ctx, token, token)
		require.Equal(t, deriveSessionID(ctx, token), sid)
		require.NotEmpty(t, setCookie)
	})

	t.Run("same cookie with different bindings diverges", func(t *testing.T) {
		sidA, _ := v.sessionForToken(cookieCtx("10.0.0.1:1", cookie), token, "binding-a")
		sidB, _ := v.sessionForToken(cookieCtx("10.0.0.1:1", cookie), token, "binding-b")
		require.NotEqual(t, sidA, sidB)
	})

	t.Run("packed and split cookie headers parse", func(t *testing.T) {
		want, _ := v.sessionForToken(cookieCtx("10.0.0.1:1", cookie), token, token)

		packed, _ := v.sessionForToken(cookieCtx("10.0.0.1:1", "foo=bar; "+cookie), token, token)
		require.Equal(t, want, packed)

		split, _ := v.sessionForToken(cookieCtx("10.0.0.1:1", "foo=bar", cookie), token, token)
		require.Equal(t, want, split)
	})
}

func TestIdentifyLocalJWTSidBeatsCookie(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	ca, err := newCookieAuthority(secret)
	require.NoError(t, err)
	v := &validator{local: newLocalJWT(secret, time.Hour), cookies: ca}

	cookieVal, err := ca.mint()
	require.NoError(t, err)
	cookie := sessionCookieName + "=" + cookieVal

	t.Run("stamped sid wins", func(t *testing.T) {
		token, err := v.local.issue("alice")
		require.NoError(t, err)
		_, wantSid, err := v.local.verify(token)
		require.NoError(t, err)

		id, setCookie, err := v.identify(cookieCtx("10.0.0.1:1", cookie), token)
		require.NoError(t, err)
		require.Equal(t, wantSid, id.SessionID)
		require.Empty(t, setCookie)
	})

	t.Run("legacy token without sid uses the cookie", func(t *testing.T) {
		// A token minted before the sid claim existed.
		now := time.Now()
		legacy, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
			Issuer:    localJWTIssuer,
			Subject:   "alice",
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
		}).SignedString(secret)
		require.NoError(t, err)

		idA, setCookie, err := v.identify(cookieCtx("10.0.0.1:1", cookie), legacy)
		require.NoError(t, err)
		require.Empty(t, setCookie)
		idB, _, err := v.identify(cookieCtx("10.9.9.9:1", cookie), legacy)
		require.NoError(t, err)
		require.Equal(t, idA.SessionID, idB.SessionID, "cookie must pin the legacy token across peers")
	})
}
