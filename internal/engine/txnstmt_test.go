//go:build duckdb_arrow

package engine

import (
	"context"
	"testing"

	"github.com/fairtier/duckflight/internal/config"
)

// TestIsTransactionStatement verifies that DuckDB's parser correctly classifies
// transaction-control statements (used by [server.isRedundantTxnCollision] to
// avoid substring matching on SQL or error messages).
func TestIsTransactionStatement(t *testing.T) {
	eng, err := NewEngine(&config.Config{MemoryLimit: "256MB", MaxThreads: 1, PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()

	ac, err := eng.Pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Pool.Release(ac)

	cases := []struct {
		query string
		want  bool
	}{
		{"BEGIN", true},
		{"BEGIN TRANSACTION", true},
		{"COMMIT", true},
		{"ROLLBACK", true},
		{"ABORT", true},
		{"START TRANSACTION", true},
		{"SELECT 1", false},
		{"CREATE TABLE x (id INT)", false},
		{"", false},
		{"NOT VALID SQL", false},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			got := ac.IsTransactionStatement(context.Background(), tc.query)
			if got != tc.want {
				t.Errorf("IsTransactionStatement(%q) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}
