//go:build duckdb_arrow

// Package session pins one DuckDB connection per authenticated client so
// connection-local state (BEGIN/COMMIT/ROLLBACK, CREATE TEMP TABLE, SET,
// PRAGMA, ATTACH) behaves the way clients expect it to. Sessions are keyed
// by the [auth.Identity.SessionID] plumbed through the gRPC context.
package session

import (
	"cmp"
	"context"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fairtier/duckflight/internal/ctxlock"
	"github.com/fairtier/duckflight/internal/engine"
	"github.com/fairtier/duckflight/internal/otelutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Session owns one [engine.ArrowConn] for its lifetime. Callers acquire it
// through [Manager.Acquire] or [Session.Lock], both of which serialize
// concurrent RPCs on the same session (DuckDB connections are not safe to
// share across goroutines).
type Session struct {
	ID        string
	createdAt time.Time
	lastUsed  atomic.Int64 // unix nanos; touched on every lock and unlock

	mu sync.Mutex
	// conn and evicted are guarded by mu. evicted marks a session that has
	// been removed from the manager: its connection is gone for good and the
	// caller must not keep using it.
	conn    *engine.ArrowConn
	evicted bool
}

// Conn returns the pinned connection. Caller must hold the session lock,
// which [Manager.Acquire] and [Session.Lock] hand out.
func (s *Session) Conn() *engine.ArrowConn { return s.conn }

// Lock takes the session lock for the duration of one call, failing if the
// session has since been evicted (reaped for idleness or explicitly closed).
// The returned release function must always be called on success.
//
// Callers holding a long-lived reference to a Session — an open Flight
// transaction, for instance — use this instead of [Manager.Acquire] so that a
// reaped session surfaces as an error rather than silently resurrecting as a
// fresh connection with none of the transaction's state.
func (s *Session) Lock(ctx context.Context) (func(), error) {
	if err := lockWithCtx(ctx, &s.mu); err != nil {
		return nil, err
	}
	if s.evicted || s.conn == nil {
		s.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition,
			"session expired; its connection was released and any open transaction rolled back")
	}
	s.lastUsed.Store(time.Now().UnixNano())
	return func() {
		s.lastUsed.Store(time.Now().UnixNano())
		s.mu.Unlock()
	}, nil
}

// Manager is the lookup table for sessions. It's keyed by the opaque id
// supplied by the auth layer (e.g. a JWT sid claim).
type Manager struct {
	pool    *engine.ArrowPool
	idleTTL time.Duration
	tracer  trace.Tracer
	metrics *sessionMetrics

	mu       sync.Mutex
	sessions map[string]*Session
}

// NewManager creates a Manager that lazily pins connections from pool. A
// session that has been idle longer than idleTTL is reaped and its connection
// returned to the pool.
func NewManager(pool *engine.ArrowPool, idleTTL time.Duration) *Manager {
	m := &Manager{
		pool:     pool,
		idleTTL:  idleTTL,
		tracer:   otelutil.Tracer(),
		sessions: make(map[string]*Session),
	}
	m.metrics = newSessionMetrics(m)
	return m
}

// Acquire returns the session for sid, lazily pinning a pool connection on
// first use. The returned release function unlocks the session — callers must
// always call it, even on the error path. The session is *not* closed by
// release; that's the reaper's or [Manager.Close]'s job.
func (m *Manager) Acquire(ctx context.Context, sid string) (*Session, func(), error) {
	if sid == "" {
		return nil, nil, status.Error(codes.Internal, "session.Acquire called with empty sid")
	}

	// The span covers the wait for the session lock as well as the connection
	// pinning: every RPC of one session runs strictly serially, so time spent
	// here is another request of the same client still holding the connection —
	// a latency source that is invisible in the RPC span alone.
	ctx, span := m.tracer.Start(ctx, "session.acquire",
		trace.WithAttributes(attribute.String("session.id", sid)))
	defer span.End()

	for {
		now := time.Now().UnixNano()

		m.mu.Lock()
		sess, ok := m.sessions[sid]
		if !ok {
			sess = &Session{ID: sid, createdAt: time.Now()}
			// Stamp before publishing so the reaper can't treat a session
			// that has never been used as infinitely idle.
			sess.lastUsed.Store(now)
			m.sessions[sid] = sess
		}
		m.mu.Unlock()

		// The pool is deliberately not touched while m.mu is held: Acquire
		// blocks when the pool is exhausted, and holding the manager lock
		// across that would stall the reaper — the only thing that could free
		// a connection — along with every other session.
		if err := lockWithCtx(ctx, &sess.mu); err != nil {
			return nil, nil, otelutil.Failed(span, err)
		}
		if sess.evicted {
			// Reaped between our map lookup and taking the lock. Drop it and
			// look the sid up again; the map now holds a live session or none.
			sess.mu.Unlock()
			continue
		}
		sess.lastUsed.Store(time.Now().UnixNano())

		fresh := sess.conn == nil
		if fresh {
			ac, err := m.pinConn(ctx, span)
			if err != nil {
				sess.mu.Unlock()
				m.dropIfEmpty(sess)
				return nil, nil, otelutil.Failed(span, status.Errorf(codes.ResourceExhausted,
					"no pool connection available for new session: %s", err))
			}
			sess.conn = ac
			m.metrics.created.Add(ctx, 1)
		}
		span.SetAttributes(attribute.Bool("session.new", fresh))

		release := func() {
			sess.lastUsed.Store(time.Now().UnixNano())
			sess.mu.Unlock()
		}
		return sess, release, nil
	}
}

// pinConn takes a connection for a new session, reclaiming an idle session's
// connection first if the pool has none free.
//
// Sessions hold their connection for the whole idle TTL, and there is one per
// client connection, so a burst of new clients can find the pool empty while
// several sessions sit idle. Blocking there would make new clients wait out
// their deadline behind sessions nobody is using; taking the least recently
// used one instead costs that session its connection-local state but keeps the
// server answering.
func (m *Manager) pinConn(ctx context.Context, span trace.Span) (*engine.ArrowConn, error) {
	if ac, ok := m.pool.TryAcquire(); ok {
		return ac, nil
	}
	if m.reclaimIdle() {
		// An event, not a span: it is a single instant, but it explains why
		// some other client's temp tables just vanished.
		span.AddEvent("session.reclaimed_idle")
		slog.Warn("pool exhausted; reclaimed the least recently used idle session")
	}
	return m.pool.Acquire(ctx)
}

// reclaimIdle evicts the least recently used session that isn't currently
// serving a request, returning its connection to the pool. Reports whether it
// evicted anything.
func (m *Manager) reclaimIdle() bool {
	m.mu.Lock()
	candidates := make([]*Session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		candidates = append(candidates, sess)
	}
	m.mu.Unlock()

	slices.SortFunc(candidates, func(a, b *Session) int {
		return cmp.Compare(a.lastUsed.Load(), b.lastUsed.Load())
	})

	for _, sess := range candidates {
		// TryLock: a session serving a request is not idle, and must not have
		// its connection pulled out from under it.
		if !sess.mu.TryLock() {
			continue
		}
		if sess.evicted || sess.conn == nil {
			sess.mu.Unlock()
			continue
		}
		conn := m.evict(sess, evictReclaimed)
		sess.mu.Unlock()
		m.pool.Discard(conn)
		return true
	}
	return false
}

// dropIfEmpty removes a session that never managed to pin a connection, so a
// failed Acquire doesn't leave an empty placeholder behind.
func (m *Manager) dropIfEmpty(sess *Session) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.conn != nil || sess.evicted {
		return
	}
	sess.evicted = true
	m.unlink(sess)
}

// unlink removes sess from the map, but only if the map still points at this
// exact session — a replacement created under the same sid must survive.
func (m *Manager) unlink(sess *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.sessions[sess.ID]; ok && cur == sess {
		delete(m.sessions, sess.ID)
	}
}

// evict takes sess out of service and hands its connection back to the pool.
// Caller must hold sess.mu. The map entry is removed before the session lock
// is dropped, so a concurrent Acquire can never adopt a session that is on its
// way out and pin a second connection to it.
//
// The eviction is counted here, on the transition rather than at each call
// site: Close can race a reaper that already took the session, and counting at
// the call site would report that one session twice.
func (m *Manager) evict(sess *Session, reason string) *engine.ArrowConn {
	if sess.evicted {
		return nil
	}
	m.unlink(sess)
	conn := sess.conn
	sess.conn = nil
	sess.evicted = true
	m.metrics.recordEvicted(reason)
	return conn
}

// Close removes a session and returns its pinned connection to the pool.
// Safe to call on an unknown sid. Blocks until any in-flight RPC on the
// session has finished, so the connection is never recycled underneath a
// running query.
func (m *Manager) Close(sid string) {
	m.mu.Lock()
	sess, ok := m.sessions[sid]
	m.mu.Unlock()
	if !ok {
		return
	}

	sess.mu.Lock()
	conn := m.evict(sess, evictClosed)
	sess.mu.Unlock()

	if conn != nil {
		// Discard rather than Release: the client is gone and the connection
		// may still carry temp tables, SET overrides or attached databases.
		m.pool.Discard(conn)
	}
}

// CloseAll evicts every session, returning all pinned connections to the pool.
// Used during shutdown, so it also drops the metric callback — a manager that
// has been shut down should stop reporting an active-session count.
func (m *Manager) CloseAll() {
	m.metrics.stop()

	m.mu.Lock()
	sessions := make([]*Session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		sessions = append(sessions, sess)
	}
	m.mu.Unlock()

	for _, sess := range sessions {
		sess.mu.Lock()
		conn := m.evict(sess, evictShutdown)
		sess.mu.Unlock()
		if conn != nil {
			m.pool.Discard(conn)
		}
	}
}

// StartReaper evicts idle sessions every minute. Stops when ctx is canceled.
func (m *Manager) StartReaper(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				m.reap(now)
			}
		}
	}()
}

func (m *Manager) reap(now time.Time) {
	cutoff := now.Add(-m.idleTTL).UnixNano()

	m.mu.Lock()
	candidates := make([]*Session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		if sess.lastUsed.Load() < cutoff {
			candidates = append(candidates, sess)
		}
	}
	m.mu.Unlock()

	for _, sess := range candidates {
		// Try-lock so we don't block on an in-flight long query. If the
		// session is busy, skip it this tick — we'll get it on the next pass
		// once it actually goes idle.
		if !sess.mu.TryLock() {
			continue
		}
		// Re-check under the session lock: a request may have arrived (and
		// stamped lastUsed) between building the candidate list and winning
		// the lock. Evicting then would pull the connection out from under a
		// caller that is about to use it.
		if sess.lastUsed.Load() >= cutoff || sess.evicted {
			sess.mu.Unlock()
			continue
		}
		conn := m.evict(sess, evictReaped)
		sess.mu.Unlock()

		if conn != nil {
			m.pool.Discard(conn)
		}
		slog.Info("reaped idle session", "sid", sess.ID, "age", time.Since(sess.createdAt))
	}
}

// Count returns the number of currently active sessions (exposed for tests).
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

// lockWithCtx waits to acquire mu, but returns early if ctx is canceled.
func lockWithCtx(ctx context.Context, mu *sync.Mutex) error {
	return ctxlock.Lock(ctx, mu)
}
