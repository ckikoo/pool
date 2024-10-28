package tcpPool

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// channelPool 实现Pool接口并且带有缓冲的连接池
type channelPool struct {
	mu      sync.Mutex
	conns   chan net.Conn
	factory Factory
}

// Factory 获取创建一个连接
type Factory func() (net.Conn, error)

// NewChannelPool 创建一个新的连接池
func NewChannelPool(initialCap, maxCap int, factory Factory) (Pool, error) {
	if initialCap < 0 || maxCap <= 0 || initialCap > maxCap {
		return nil, errors.New("invalid capacity settings")
	}

	c := &channelPool{
		conns:   make(chan net.Conn, maxCap),
		factory: factory,
	}

	for i := 0; i < initialCap; i++ {
		conn, err := factory()
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		pconn := PoolConn{Conn: conn, CreateTime: time.Now(), c: c}
		c.conns <- &pconn
	}

	return c, nil
}

func (c *channelPool) wrapConn(conn net.Conn) net.Conn {
	p := &PoolConn{Conn: conn}
	p.CreateTime = time.Now() // 记录创建时间
	return p
}

func (c *channelPool) Get() (net.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns == nil {
		return nil, ErrClosed
	}

	select {
	case conn := <-c.conns:
		if conn == nil {
			return nil, ErrClosed
		}
		return c.wrapConn(conn), nil
	default:
		conn, err := c.factory()
		if err != nil {
			return nil, err
		}
		pconn := PoolConn{Conn: conn, CreateTime: time.Now(), c: c}
		return c.wrapConn(&pconn), nil
	}
}

func (c *channelPool) put(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns == nil {
		return conn.Close()
	}

	select {
	case c.conns <- conn:
		return nil
	default:
		return conn.Close()
	}
}

func (c *channelPool) Close() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)

	for conn := range conns {
		conn.Close()
	}
}

func (c *channelPool) Len() int {
	return len(c.getConns())
}
