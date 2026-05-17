//go:build duckdb_arrow

// Package session pins one DuckDB connection per authenticated client so
// connection-local state (BEGIN/COMMIT/ROLLBACK, CREATE TEMP TABLE, SET,
// PRAGMA, ATTACH) behaves the way clients expect it to. Sessions are keyed
// by the [auth.Identity.SessionID] plumbed through the gRPC context.
package session

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fairtier/duckflight/internal/engine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Session owns one [engine.ArrowConn] for its lifetime. Callers acquire it
// with [Session.Lock], which serializes concurrent RPCs on the same session
// (DuckDB connections are not safe to share across goroutines).
type Session struct {
	ID        string
	conn      *engine.ArrowConn
	createdAt time.Time
	lastUsed  atomic.Int64 // unix nanos; touched on every Lock call
	mu        sync.Mutex
}

// Conn returns the pinned connection. Caller must hold the session lock.
func (s *Session) Conn() *engine.ArrowConn { return s.conn }

// Manager is the lookup table for sessions. It's keyed by the opaque id
// supplied by the auth layer (e.g. a JWT sid claim).
type Manager struct {
	pool    *engine.ArrowPool
	idleTTL time.Duration

	mu       sync.Mutex
	sessions map[string]*Session
}

// NewManager creates a Manager that lazily pins connections from pool. A
// session whose lock has been idle longer than idleTTL is reaped and its
// connection returned to the pool.
func NewManager(pool *engine.ArrowPool, idleTTL time.Duration) *Manager {
	return &Manager{
		pool:     pool,
		idleTTL:  idleTTL,
		sessions: make(map[string]*Session),
	}
}

// Acquire returns the session for sid, lazily pinning a pool connection on
// first use. The returned release function unlocks the session — callers must
// always call it, even on the error path. The session is *not* closed by
// release; that's the reaper's or [Manager.Close]'s job.
func (m *Manager) Acquire(ctx context.Context, sid string) (*Session, func(), error) {
	if sid == "" {
		return nil, nil, status.Error(codes.Internal, "session.Acquire called with empty sid")
	}

	m.mu.Lock()
	sess, ok := m.sessions[sid]
	if !ok {
		// Pin a pool connection for this session. If the pool is exhausted
		// the caller gets ResourceExhausted instead of hanging silently.
		ac, err := m.pool.Acquire(ctx)
		if err != nil {
			m.mu.Unlock()
			return nil, nil, status.Errorf(codes.ResourceExhausted, "no pool connection available for new session: %s", err)
		}
		sess = &Session{
			ID:        sid,
			conn:      ac,
			createdAt: time.Now(),
		}
		m.sessions[sid] = sess
	}
	m.mu.Unlock()

	// Acquire the per-session lock — serializes concurrent RPCs on the same
	// session. Use a select so a canceled ctx doesn't wedge here forever.
	if err := lockWithCtx(ctx, &sess.mu); err != nil {
		return nil, nil, err
	}
	sess.lastUsed.Store(time.Now().UnixNano())

	release := func() {
		sess.lastUsed.Store(time.Now().UnixNano())
		sess.mu.Unlock()
	}
	return sess, release, nil
}

// Close removes a session and returns its pinned connection to the pool.
// Safe to call on an unknown sid.
func (m *Manager) Close(sid string) {
	m.mu.Lock()
	sess, ok := m.sessions[sid]
	if ok {
		delete(m.sessions, sid)
	}
	m.mu.Unlock()
	if !ok {
		return
	}
	// Wait for any in-flight RPC on this session to finish before recycling
	// the connection.
	sess.mu.Lock()
	conn := sess.conn
	sess.conn = nil
	sess.mu.Unlock()
	if conn != nil {
		m.pool.Release(conn)
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
	var victims []*Session
	m.mu.Lock()
	for sid, sess := range m.sessions {
		if sess.lastUsed.Load() < cutoff {
			victims = append(victims, sess)
			delete(m.sessions, sid)
		}
	}
	m.mu.Unlock()
	for _, sess := range victims {
		// Try-lock so we don't block on an in-flight long query. If the
		// session is busy, skip it this tick — we'll get it on the next pass
		// once it actually goes idle.
		if !tryLock(&sess.mu) {
			m.mu.Lock()
			m.sessions[sess.ID] = sess
			m.mu.Unlock()
			continue
		}
		conn := sess.conn
		sess.conn = nil
		sess.mu.Unlock()
		if conn != nil {
			m.pool.Release(conn)
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
	got := make(chan struct{})
	go func() {
		mu.Lock()
		close(got)
	}()
	select {
	case <-got:
		return nil
	case <-ctx.Done():
		// We still have to drain the lock once the goroutine acquires it,
		// otherwise we'd leak a held mutex. Unlock once the slow path resolves.
		go func() {
			<-got
			mu.Unlock()
		}()
		return status.FromContextError(ctx.Err()).Err()
	}
}

// tryLock is sync.Mutex.TryLock; thin wrapper so callers don't repeat the
// boolean dance.
func tryLock(mu *sync.Mutex) bool { return mu.TryLock() }
