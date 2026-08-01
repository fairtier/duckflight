//go:build duckdb_arrow

package main

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestParseUsersRejectsMalformedEntries is the regression test for auth
// disabling itself on a typo. parseUsers used to skip entries without a colon,
// so `AUTH_USERS="alice"` parsed to zero users, Middleware returned nil, and
// the server started fully open — with one WARN line as the only signal.
func TestParseUsersRejectsMalformedEntries(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"missing colon", "alice"},
		{"missing colon among valid entries", "alice:secret,bob"},
		{"empty username", ":hunter2"},
		{"only separators", ",,,"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			users, err := parseUsers(tc.value)
			if err == nil {
				t.Fatalf("parseUsers(%q) = %v, want error — a malformed value must not silently disable auth", tc.value, users)
			}
			if users != nil {
				t.Fatalf("parseUsers(%q) returned users alongside an error: %v", tc.value, users)
			}
			// The value may contain a password; it must never be echoed.
			if strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "secret") {
				t.Fatalf("error message leaks credential material: %v", err)
			}
		})
	}
}

func TestParseUsersAcceptsValidEntries(t *testing.T) {
	users, err := parseUsers("alice:secret123, bob:hunter2")
	if err != nil {
		t.Fatalf("parseUsers: %v", err)
	}
	if len(users) != 2 || users["alice"] != "secret123" || users["bob"] != "hunter2" {
		t.Fatalf("unexpected users: %v", users)
	}

	// Unset means "no basic-auth backend", not an error.
	users, err = parseUsers("")
	if err != nil || users != nil {
		t.Fatalf("parseUsers(\"\") = %v, %v; want nil, nil", users, err)
	}
}

// TestEnvParsersFailOnMalformedInput covers the silent fail-open on typos:
// RATE_LIMIT_RPS="100/s" used to parse as 0, which disables rate limiting, and
// MAX_RESULT_BYTES="1e9" as 0, which removes the result cap — both with no
// signal at all.
func TestEnvParsersFailOnMalformedInput(t *testing.T) {
	t.Setenv("TEST_ENV_NUM", "100/s")

	if _, err := envInt("TEST_ENV_NUM", 7); err == nil {
		t.Error("envInt accepted a malformed value")
	}
	if _, err := envInt64("TEST_ENV_NUM", 7); err == nil {
		t.Error("envInt64 accepted a malformed value")
	}
	if _, err := envFloat64("TEST_ENV_NUM", 7); err == nil {
		t.Error("envFloat64 accepted a malformed value")
	}
	if _, err := envDuration("TEST_ENV_NUM", time.Second); err == nil {
		t.Error("envDuration accepted a malformed value")
	}
}

func TestEnvParsersFallBackWhenUnset(t *testing.T) {
	if err := os.Unsetenv("TEST_ENV_UNSET"); err != nil {
		t.Fatal(err)
	}

	if n, err := envInt("TEST_ENV_UNSET", 7); err != nil || n != 7 {
		t.Errorf("envInt = %d, %v; want 7, nil", n, err)
	}
	if n, err := envInt64("TEST_ENV_UNSET", 7); err != nil || n != 7 {
		t.Errorf("envInt64 = %d, %v; want 7, nil", n, err)
	}
	if f, err := envFloat64("TEST_ENV_UNSET", 1.5); err != nil || f != 1.5 {
		t.Errorf("envFloat64 = %v, %v; want 1.5, nil", f, err)
	}
	if d, err := envDuration("TEST_ENV_UNSET", time.Minute); err != nil || d != time.Minute {
		t.Errorf("envDuration = %v, %v; want 1m, nil", d, err)
	}
}
