package tcpPool

import (
	"net"
	"time"
)

type PoolConn struct {
	net.Conn
	c          *channelPool
	unusable   bool
	CreateTime time.Time
}

func (p *PoolConn) Close() error {

	if p.unusable {

		if p.Conn != nil {

			return p.Conn.Close()

		}

		return nil

	}

	return p.c.put(p.Conn)

}

func (p *PoolConn) MarkUnusable() {
	p.unusable = true
}
