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
	createdAt     time.Time

	mu       sync.Mutex
	cancel   context.CancelFunc
	started  bool // a DoGet has claimed this handle and is executing it
	canceled bool // CancelFlightInfo arrived, possibly before execution started
	done     chan struct{}
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

// Start claims a handle for execution and registers its cancel function.
// It reports false when the handle is unknown, when a cancellation already
// arrived for it, or when another DoGet is already executing it — in all three
// cases the caller must not run the query.
//
// Claiming and canceling under the same lock is what keeps a CancelFlightInfo
// that lands between Load and Start from being reported as "canceled" while
// the query runs happily to completion.
func (t *queryTracker) Start(handle string, cancel context.CancelFunc) bool {
	val, ok := t.queries.Load(handle)
	if !ok {
		return false
	}
	qs := val.(*queryState)
	qs.mu.Lock()
	defer qs.mu.Unlock()
	if qs.canceled || qs.started {
		return false
	}
	qs.started = true
	qs.cancel = cancel
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

// Cancel cancels a running or pending query and returns the resulting status.
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

	qs.canceled = true

	// Running — cancel its context.
	if qs.cancel != nil {
		qs.cancel()
		return flight.CancelStatusCancelling
	}

	// Pending: no DoGet has claimed it yet, so drop the handle and a later
	// DoGet gets NotFound. A DoGet that already loaded this entry and is on
	// its way to Start still sees canceled and backs out, so the query can't
	// run behind a cancellation the client was already told succeeded.
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
				t.reap(time.Now())
			}
		}
	}()
}

func (t *queryTracker) reap(now time.Time) {
	t.queries.Range(func(key, value any) bool {
		qs := value.(*queryState)
		if now.Sub(qs.createdAt) <= t.ttl {
			return true
		}
		// Never drop a query that is still executing: removing it would make
		// its handle uncancellable and lose the entry a live DoGet still
		// reports completion against.
		qs.mu.Lock()
		running := qs.started
		if running {
			select {
			case <-qs.done:
				running = false
			default:
			}
		}
		qs.mu.Unlock()
		if !running {
			t.queries.Delete(key)
		}
		return true
	})
}
