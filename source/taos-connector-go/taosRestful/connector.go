package taosRestful

import (
	"context"
	"database/sql/driver"

	"github.com/taosdata/driver-go/v3/common"
)

type connector struct {
	cfg *Config
}

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	// Connect to Server
	if len(c.cfg.User) == 0 {
		c.cfg.User = common.DefaultUser
	}
	if len(c.cfg.Passwd) == 0 {
		c.cfg.Passwd = common.DefaultPassword
	}
	if c.cfg.Port == 0 {
		c.cfg.Port = common.DefaultHttpPort
	}
	if len(c.cfg.Net) == 0 {
		c.cfg.Net = "http"
	}
	if len(c.cfg.Addr) == 0 {
		c.cfg.Addr = "127.0.0.1"
	}
	tc, err := newTaosConn(c.cfg)
	return tc, err
}

// Driver implements driver.Connector interface.
// Driver returns &TDengineDriver{}.
func (c *connector) Driver() driver.Driver {
	return &TDengineDriver{}
}
