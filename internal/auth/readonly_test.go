//go:build duckdb_arrow

package auth

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestReadOnlyUsersMustBeConfiguredUsers(t *testing.T) {
	_, err := Middleware(Config{
		Users:         map[string]string{"alice": "pw"},
		ReadOnlyUsers: []string{"bob"},
		JWTSecret:     []byte(strings.Repeat("k", 32)),
	})
	if err == nil || !strings.Contains(err.Error(), `"bob"`) {
		t.Fatalf("expected an error naming the unknown read-only user, got %v", err)
	}
}

func TestReadOnlyTokensAloneEnableAuth(t *testing.T) {
	mw, err := Middleware(Config{ReadOnlyStaticTokens: []string{"ro"}})
	if err != nil || mw == nil {
		t.Fatalf("read-only tokens alone must enable the middleware: mw=%v err=%v", mw, err)
	}
}

func TestStaticTokenReadOnlyMatch(t *testing.T) {
	// Built the way Middleware builds it: read-write tokens first, then the
	// read-only ones, so a token listed in both ends up read-only.
	v := &validator{staticTokens: map[[sha256.Size]byte]bool{}}
	for _, tok := range []string{"rw", "both"} {
		v.staticTokens[sha256.Sum256([]byte(tok))] = false
	}
	for _, tok := range []string{"ro", "both"} {
		v.staticTokens[sha256.Sum256([]byte(tok))] = true
	}
	for _, tc := range []struct {
		token        string
		match, roTok bool
	}{
		{"rw", true, false},
		{"ro", true, true},
		{"both", true, true},
		{"nope", false, false},
	} {
		m, ro := v.matchesStaticToken(tc.token)
		if m != tc.match || ro != tc.roTok {
			t.Errorf("token %q: match=%v ro=%v, want %v %v", tc.token, m, ro, tc.match, tc.roTok)
		}
	}
}

func TestReadOnlyHeaderParsing(t *testing.T) {
	for _, tc := range []struct {
		vals []string
		want bool
		code codes.Code
	}{
		{nil, false, codes.OK},
		{[]string{"true"}, true, codes.OK},
		{[]string{" TRUE "}, true, codes.OK},
		{[]string{"1"}, true, codes.OK},
		{[]string{"false"}, false, codes.OK},
		{[]string{"0"}, false, codes.OK},
		{[]string{""}, false, codes.OK},
		// Repeated headers: any true wins; a later false cannot undo it.
		{[]string{"true", "false"}, true, codes.OK},
		{[]string{"yes"}, false, codes.InvalidArgument},
		{[]string{"false", "nope"}, false, codes.InvalidArgument},
	} {
		md := metadata.MD{}
		for _, v := range tc.vals {
			md.Append(ReadOnlyHeader, v)
		}
		got, err := readOnlyRequested(metadata.NewIncomingContext(context.Background(), md))
		if status.Code(err) != tc.code || got != tc.want {
			t.Errorf("%q: got %v, %v; want %v, %v", tc.vals, got, err, tc.want, tc.code)
		}
	}
}
