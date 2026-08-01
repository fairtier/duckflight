package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"

	"google.golang.org/grpc/metadata"
)

const (
	// sessionCookieName is the Flight session spec's stateful cookie name.
	// Client cookie jars are name-agnostic, but using the standard name means
	// anyone inspecting metadata recognizes what it is.
	sessionCookieName = "arrow_flight_session_id"
	// cookieIDLen is the length of the random session id inside the cookie.
	cookieIDLen = 16
	// cookieKeyLabel domain-separates the cookie HMAC key derived from
	// AUTH_JWT_SECRET, so the cookie key is never the JWT signing key itself.
	cookieKeyLabel = "duckflight cookie key v1"
)

// cookieAuthority mints and verifies session cookies. A cookie is
// base64url(id ‖ HMAC-SHA256(key, id)): stateless to verify, so there is no
// server-side cookie store to grow or reap — the session itself is created
// lazily by session.Manager and reaped by its idle TTL.
//
// The HMAC keeps the honored-id space server-controlled, but it is
// defense-in-depth rather than load-bearing: the session id mixes the cookie
// id with the caller's token/subject, so forging a cookie only lets an
// attacker choose which of *their own* sessions they land in. That is why
// every failure path here fails open to peer-address derivation instead of
// failing the RPC.
type cookieAuthority struct {
	key []byte
}

// newCookieAuthority derives the HMAC key from jwtSecret when set — cookies
// then stay valid across replicas and restarts exactly as far as
// handshake-issued JWTs do — and otherwise generates a per-process key, under
// which a restart merely falls clients back to peer derivation until they
// pick up a fresh cookie.
func newCookieAuthority(jwtSecret []byte) (*cookieAuthority, error) {
	if len(jwtSecret) > 0 {
		mac := hmac.New(sha256.New, jwtSecret)
		mac.Write([]byte(cookieKeyLabel))
		return &cookieAuthority{key: mac.Sum(nil)}, nil
	}
	key := make([]byte, sha256.Size)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return &cookieAuthority{key: key}, nil
}

// mint returns a fresh cookie value. RawURLEncoding contains no '=', ';' or
// ',', so the value is valid cookie-octets without quoting.
func (ca *cookieAuthority) mint() (string, error) {
	id := make([]byte, cookieIDLen)
	if _, err := rand.Read(id); err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, ca.key)
	mac.Write(id)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(id)), nil
}

// verify returns the embedded session id if value is a cookie this server (or
// a replica sharing its key) minted. Anything else — wrong length, bad
// encoding, bad tag — reads as "no cookie".
func (ca *cookieAuthority) verify(value string) (id []byte, ok bool) {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(raw) != cookieIDLen+sha256.Size {
		return nil, false
	}
	id, tag := raw[:cookieIDLen], raw[cookieIDLen:]
	mac := hmac.New(sha256.New, ca.key)
	mac.Write(id)
	if !hmac.Equal(tag, mac.Sum(nil)) {
		return nil, false
	}
	return id, true
}

// incomingSessionCookie extracts the session cookie from the request
// metadata. Clients send cookies the HTTP way — any number of "cookie"
// metadata values, each possibly packing several "k=v" pairs — so let
// net/http do the parsing rather than reimplementing the grammar.
func incomingSessionCookie(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	h := http.Header{}
	for _, v := range md.Get("cookie") {
		h.Add("Cookie", v)
	}
	c, err := (&http.Request{Header: h}).Cookie(sessionCookieName)
	if err != nil {
		return "", false
	}
	return c.Value, true
}

// cookieSessionID binds a verified cookie id to the caller's identity
// material: sid = hex(sha256(id ‖ 0x00 ‖ binding)). A leaked cookie alone is
// useless, and two identities presenting the same cookie land in different
// sessions.
func cookieSessionID(id []byte, binding string) string {
	h := sha256.New()
	h.Write(id)
	h.Write([]byte{0})
	h.Write([]byte(binding))
	return hex.EncodeToString(h.Sum(nil))
}
