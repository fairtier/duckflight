// Package ctxlock provides a mutex acquisition that honors context
// cancellation, so a client that goes away doesn't leave an RPC blocked on a
// lock forever.
package ctxlock

import (
	"context"
	"sync"

	"google.golang.org/grpc/status"
)

// Lock waits to acquire mu, returning a gRPC status error if ctx is canceled
// or times out first. On success the caller owns mu and must unlock it.
func Lock(ctx context.Context, mu *sync.Mutex) error {
	// Fast path: uncontended locks are the common case and skip the goroutine.
	if mu.TryLock() {
		return nil
	}
	got := make(chan struct{})
	go func() {
		mu.Lock()
		close(got)
	}()
	select {
	case <-got:
		return nil
	case <-ctx.Done():
		// The goroutine will still acquire the lock; hand it straight back so
		// we don't leak a permanently held mutex.
		go func() {
			<-got
			mu.Unlock()
		}()
		return status.FromContextError(ctx.Err()).Err()
	}
}
