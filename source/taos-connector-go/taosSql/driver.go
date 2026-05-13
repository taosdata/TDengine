package taosSql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"

	"github.com/taosdata/driver-go/v3/wrapper/handler"
	"github.com/taosdata/driver-go/v3/wrapper/thread"
)

var locker *thread.Locker
var onceInitLock = sync.Once{}
var asyncHandlerPool *handler.HandlerPool
var onceInitHandlerPool = sync.Once{}

// TDengineDriver is exported to make the driver directly accessible.
// In general the driver is used via the database/sql package.
type TDengineDriver struct{}

// Open new Connection.
// the DSN string is formatted
func (d TDengineDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &connector{
		cfg: cfg,
	}

	return c.Connect(context.Background())
}

func init() {
	sql.Register("taosSql", &TDengineDriver{})
}

// NewConnector returns new driver.Connector.
func NewConnector(cfg *Config) (driver.Connector, error) {
	return &connector{cfg: cfg}, nil
}

// OpenConnector implements driver.DriverContext.
func (d TDengineDriver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &connector{
		cfg: cfg,
	}, nil
}
