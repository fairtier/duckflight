//go:build duckdb_arrow

package engine

import (
	"context"
	"crypto/sha256"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"time"
)

// reconcileInterval is how often the reconciler re-reads the watched file.
// Kubelet propagates Secret updates to mounted volumes on its own sync period
// (~1 min), so polling faster than this buys nothing.
const reconcileInterval = 10 * time.Second

// Reconciler watches a SQL file and executes it instance-wide whenever its
// content changes. It is the operator's runtime channel into the shared
// engine: `CREATE OR REPLACE SECRET` and `LOAD` are instance-wide in DuckDB,
// so a rotated credential or a newly enabled extension reaches every pooled
// connection without a restart and without dropping client sessions.
//
// The file is typically a Kubernetes Secret mounted as a volume; content
// comparison (not mtime) makes the watcher robust to the kubelet's
// atomic-symlink update mechanism. The file's content is never logged — it
// carries live credentials.
type Reconciler struct {
	engine   *Engine
	path     string
	interval time.Duration
	// lastAttempt is the hash of the most recently executed content. Failed
	// content is not retried until it changes: reconcile SQL errors are
	// deterministic (bad statement, unknown extension), and the operator's
	// refresh cadence re-delivers rotated content anyway.
	lastAttempt [sha256.Size]byte
	attempted   bool
}

// NewReconciler creates a Reconciler for the given engine and file path.
func NewReconciler(eng *Engine, path string) *Reconciler {
	return &Reconciler{engine: eng, path: path, interval: reconcileInterval}
}

// Start applies the file once synchronously (so secrets are in place before
// the first query when the file already exists), then watches it on ctx until
// canceled. A missing file is a normal state — the mount may be created later
// — and an execution error is logged but never fatal: the lake must keep
// serving even when a federated source's setup SQL is broken.
func (r *Reconciler) Start(ctx context.Context) {
	r.apply(ctx)
	go func() {
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.apply(ctx)
			}
		}
	}()
}

// apply reads the file and executes it when its content differs from the last
// attempt.
func (r *Reconciler) apply(ctx context.Context) {
	content, err := os.ReadFile(r.path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			slog.Warn("reconcile sql: read failed", slog.String("path", r.path), slog.String("error", err.Error()))
		}
		return
	}
	if len(content) == 0 {
		return
	}

	hash := sha256.Sum256(content)
	if r.attempted && hash == r.lastAttempt {
		return
	}
	r.lastAttempt = hash
	r.attempted = true

	if err := r.engine.ExecSQL(ctx, "reconcile sql", string(content)); err != nil {
		// The error from ExecSQL carries only the label, never the SQL text.
		slog.Error("reconcile sql: apply failed", slog.String("path", r.path), slog.String("error", err.Error()))
		return
	}
	slog.Info("reconcile sql: applied", slog.String("path", r.path), slog.Int("bytes", len(content)))
}
