package pgsql

import (
	"io"
	"net"
	"sync"
)

// bufferedConn wraps a net.Conn so the underlying socket is read in one
// large syscall per fill while callers keep receiving exactly the slice
// size they asked for. See openPgxPool's KINE_PGSQL_READ_BUFFER block for
// why this exists.
//
// All reads funnel through the single mutex-protected buffer, so it is
// safe for pgconn's two potential readers (the foreground protocol reader
// and the background prefetch reader started on slow writes): they
// serialize on the mutex and jointly consume the same byte stream, which
// is exactly the semantics of reading the raw conn. The buffer is
// allocated lazily so connections that never carry a large response do
// not hold the memory.
type bufferedConn struct {
	net.Conn
	bufSize int

	mu         sync.Mutex
	buf        []byte
	rp, wp     int
	pendingErr error
}

func newBufferedConn(c net.Conn, size int) *bufferedConn {
	return &bufferedConn{Conn: c, bufSize: size}
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.rp == c.wp {
		if c.pendingErr != nil {
			err := c.pendingErr
			c.pendingErr = nil
			return 0, err
		}
		// Direct read when the caller wants at least a buffer's worth:
		// buffering would only add a copy.
		if len(p) >= c.bufSize {
			return c.Conn.Read(p)
		}
		if c.buf == nil {
			c.buf = make([]byte, c.bufSize)
		}
		n, err := c.Conn.Read(c.buf)
		c.rp = 0
		c.wp = n
		if err != nil {
			if n > 0 {
				// Deliver the data first; surface the error on the
				// next call (standard buffered-reader behavior).
				c.pendingErr = err
				break
			}
			return 0, err
		}
		if n == 0 {
			return 0, io.EOF
		}
	}
	n := copy(p, c.buf[c.rp:c.wp])
	c.rp += n
	return n, nil
}
