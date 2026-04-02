package connectpool

import (
	"context"
	"errors"
	"sync"
	"time"
	"unsafe"
)

var (
	ErrClosed  = errors.New("pool is closed")
	ErrTimeout = errors.New("get connection timeout")
	ErrMaxWait = errors.New("exceeded connection pool max wait")
)

type Config struct {
	InitialCap  int
	MaxCap      int
	MaxWait     int
	WaitTimeout time.Duration
	Factory     func() (unsafe.Pointer, error)
	Close       func(pointer unsafe.Pointer)
}

type connReq struct {
	idleConn unsafe.Pointer
}

type ConnectPool struct {
	mu           sync.RWMutex
	conns        chan unsafe.Pointer
	factory      func() (unsafe.Pointer, error)
	close        func(pointer unsafe.Pointer)
	maxActive    int
	openingConns int
	maxWait      int
	waitTimeout  time.Duration
	connReqs     []chan connReq
	released     bool
	releasedOnce sync.Once
}

func NewConnectPool(poolConfig *Config) (*ConnectPool, error) {
	if poolConfig.MaxCap <= 0 {
		return nil, errors.New("MaxCap must larger than 0")
	}
	if poolConfig.Factory == nil {
		return nil, errors.New("invalid factory func settings")
	}
	if poolConfig.Close == nil {
		return nil, errors.New("invalid close func settings")
	}

	c := &ConnectPool{
		conns:        make(chan unsafe.Pointer, poolConfig.MaxCap),
		factory:      poolConfig.Factory,
		close:        poolConfig.Close,
		maxActive:    poolConfig.MaxCap,
		maxWait:      poolConfig.MaxWait,
		waitTimeout:  poolConfig.WaitTimeout,
		openingConns: 0,
		released:     false,
	}

	for i := 0; i < poolConfig.InitialCap; i++ {
		conn, err := c.factory()
		if err != nil {
			c.Release()
			return nil, err
		}
		c.openingConns++
		c.conns <- conn
	}

	return c, nil
}

func (c *ConnectPool) getConns() chan unsafe.Pointer {
	c.mu.Lock()
	if c.released {
		c.mu.Unlock()
		return nil
	}
	conns := c.conns
	c.mu.Unlock()
	return conns
}

func (c *ConnectPool) Get() (unsafe.Pointer, error) {
	if c.waitTimeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), c.waitTimeout)
		defer cancel()
		return c.GetWithContext(ctx)
	}
	return c.GetWithContext(context.Background())
}

func (c *ConnectPool) GetWithContext(ctx context.Context) (unsafe.Pointer, error) {
	conns := c.getConns()
	if conns == nil {
		return nil, ErrClosed
	}
	for {
		select {
		case wrapConn := <-conns:
			if wrapConn == nil { // conn closed
				return nil, ErrClosed
			}
			return wrapConn, nil
		default:
			c.mu.Lock()
			if c.released {
				c.mu.Unlock()
				return nil, ErrClosed
			}
			if c.openingConns >= c.maxActive {
				if c.maxWait > 0 && len(c.connReqs) >= c.maxWait {
					c.mu.Unlock()
					return nil, ErrMaxWait
				}
				req := make(chan connReq, 1)
				c.connReqs = append(c.connReqs, req)
				c.mu.Unlock()
				select {
				case ret, ok := <-req:
					if !ok {
						return nil, ErrClosed
					}
					return ret.idleConn, nil
				case <-ctx.Done():
					c.mu.Lock()
					if c.released {
						c.mu.Unlock()
						return nil, ErrClosed
					}
					gotReq := false
					for i, r := range c.connReqs {
						// remove the request from the queue
						if r == req {
							gotReq = true
							copy(c.connReqs[i:], c.connReqs[i+1:])
							c.connReqs = c.connReqs[:len(c.connReqs)-1]
							c.mu.Unlock()
							break
						}
					}
					if !gotReq {
						c.mu.Unlock()
						ret, ok := <-req
						if !ok {
							return nil, ErrClosed
						}
						return ret.idleConn, nil
					}
					return nil, ErrTimeout
				}

			}
			if c.factory == nil {
				c.mu.Unlock()
				return nil, ErrClosed
			}
			conn, err := c.factory()
			if err != nil {
				c.mu.Unlock()
				return nil, err
			}
			c.openingConns++
			c.mu.Unlock()
			return conn, nil
		}
	}
}

func (c *ConnectPool) Put(conn unsafe.Pointer) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.Lock()

	if c.released {
		c.mu.Unlock()
		_ = c.Close(conn)
		if c.openingConns == 0 {
			c.close = nil
		}
		return nil
	}

	if l := len(c.connReqs); l > 0 {
		req := c.connReqs[0]
		copy(c.connReqs, c.connReqs[1:])
		c.connReqs = c.connReqs[:l-1]
		req <- connReq{
			idleConn: conn,
		}
		c.mu.Unlock()
		return nil
	}
	select {
	case c.conns <- conn:
		c.mu.Unlock()
		return nil
	default:
		c.mu.Unlock()
		return c.Close(conn)
	}

}

func (c *ConnectPool) Close(conn unsafe.Pointer) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.close == nil {
		return nil
	}
	c.openingConns--
	c.close(conn)
	return nil
}

func (c *ConnectPool) Release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.released {
		return
	}
	c.released = true
	for i := 0; i < len(c.connReqs); i++ {
		close(c.connReqs[i])
	}
	c.connReqs = nil
	c.doRelease()
}

func (c *ConnectPool) doRelease() {
	c.releasedOnce.Do(func() {
		c.factory = nil
		close(c.conns)
		for conn := range c.conns {
			c.openingConns--
			c.close(conn)
		}
		c.conns = nil
	})
}
