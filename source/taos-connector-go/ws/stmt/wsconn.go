package stmt

import (
	"time"

	"github.com/taosdata/driver-go/v3/ws/client"
)

// Deprecated: use unified.Client internals from package ws/unified instead.
type WSConn struct {
}

// Deprecated: use unified.NewClient from package ws/unified instead.
func NewWSConn(_ *client.Client, _ time.Duration, _ time.Duration) *WSConn {
	return &WSConn{}
}

// Deprecated: use unified.Client internals from package ws/unified instead.
func (c *WSConn) Close() {
}
