//go:build duckdb_arrow

package server

import (
	"context"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
)

type queryState struct {
	query         string
	transactionID string
	cancel        context.CancelFunc
	done          chan struct{}
	createdAt     time.Time
	mu            sync.Mutex
}

type queryTracker struct {
	queries sync.Map // handle string -> *queryState
	ttl     time.Duration
}

// Register creates a new query state entry.
func (t *queryTracker) Register(handle, query, txnID string) {
	t.queries.Store(handle, &queryState{
		query:         query,
		transactionID: txnID,
		done:          make(chan struct{}),
		createdAt:     time.Now(),
	})
}

// Load reads query data for the given handle.
func (t *queryTracker) Load(handle string) (query, txnID string, ok bool) {
	val, loaded := t.queries.Load(handle)
	if !loaded {
		return "", "", false
	}
	qs := val.(*queryState)
	return qs.query, qs.transactionID, true
}

// SetCancel stores the cancel function, returns false if handle not found.
func (t *queryTracker) SetCancel(handle string, cancel context.CancelFunc) bool {
	val, ok := t.queries.Load(handle)
	if !ok {
		return false
	}
	qs := val.(*queryState)
	qs.mu.Lock()
	qs.cancel = cancel
	qs.mu.Unlock()
	return true
}

// Complete marks a query as done and allows cleanup.
func (t *queryTracker) Complete(handle string) {
	val, ok := t.queries.Load(handle)
	if !ok {
		return
	}
	qs := val.(*queryState)
	qs.mu.Lock()
	select {
	case <-qs.done:
	default:
		close(qs.done)
	}
	qs.mu.Unlock()
}

// Cancel cancels a query. Returns the appropriate cancel status.
func (t *queryTracker) Cancel(handle string) flight.CancelStatus {
	val, ok := t.queries.Load(handle)
	if !ok {
		return flight.CancelStatusNotCancellable
	}
	qs := val.(*queryState)
	qs.mu.Lock()
	defer qs.mu.Unlock()

	// Already completed.
	select {
	case <-qs.done:
		return flight.CancelStatusNotCancellable
	default:
	}

	// Running (has cancel func) — cancel it.
	if qs.cancel != nil {
		qs.cancel()
		return flight.CancelStatusCancelling
	}

	// Pending (not yet picked up by DoGet) — remove it.
	t.queries.Delete(handle)
	return flight.CancelStatusCancelled
}

// Remove deletes an entry.
func (t *queryTracker) Remove(handle string) {
	t.queries.Delete(handle)
}

// StartCleanup runs a background goroutine that reaps expired entries.
func (t *queryTracker) StartCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				t.queries.Range(func(key, value any) bool {
					qs := value.(*queryState)
					if now.Sub(qs.createdAt) > t.ttl {
						t.queries.Delete(key)
					}
					return true
				})
			}
		}
	}()
}
