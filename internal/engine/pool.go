//go:build duckdb_arrow

package engine

import (
	"context"
	"database/sql/driver"

	"github.com/marcboeker/go-duckdb/v2"
)

// ArrowConn wraps a raw DuckDB connection and its Arrow interface.
type ArrowConn struct {
	conn  driver.Conn
	Arrow *duckdb.Arrow
}

// ArrowPool is a bounded pool of Arrow-enabled DuckDB connections.
type ArrowPool struct {
	pool chan *ArrowConn
}

// NewArrowPool creates a pool of size Arrow connections from the given connector.
func NewArrowPool(connector *duckdb.Connector, size int) (*ArrowPool, error) {
	p := &ArrowPool{pool: make(chan *ArrowConn, size)}
	for i := 0; i < size; i++ {
		conn, err := connector.Connect(context.Background())
		if err != nil {
			p.Close()
			return nil, err
		}
		ar, err := duckdb.NewArrowFromConn(conn)
		if err != nil {
			conn.Close()
			p.Close()
			return nil, err
		}
		p.pool <- &ArrowConn{conn: conn, Arrow: ar}
	}
	return p, nil
}

// Acquire blocks until an ArrowConn is available or ctx is cancelled.
func (p *ArrowPool) Acquire(ctx context.Context) (*ArrowConn, error) {
	select {
	case ac := <-p.pool:
		return ac, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Release returns an ArrowConn to the pool.
func (p *ArrowPool) Release(ac *ArrowConn) {
	p.pool <- ac
}

// Close drains and closes all connections in the pool.
func (p *ArrowPool) Close() {
	for {
		select {
		case ac := <-p.pool:
			ac.conn.Close()
		default:
			return
		}
	}
}
