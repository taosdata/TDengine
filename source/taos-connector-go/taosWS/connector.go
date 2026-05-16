package taosWS

import (
	"context"
	"database/sql/driver"

	"github.com/taosdata/driver-go/v3/ws/unified"
)

type connector struct {
	cfg *Config
}

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	_ = ctx
	if c == nil || c.cfg == nil {
		return nil, unified.ErrNilConfig
	}
	normalizedCfg := unified.BuildConnectionConfig(c.cfg, unified.TaosWSConnectionDefaults)
	unifiedConnector, err := unified.NewConnector(normalizedCfg, "/ws")
	if err != nil {
		return nil, err
	}
	unifiedClient, err := unifiedConnector.Connect()
	if err != nil {
		return nil, NewBadConnError(err)
	}
	return newTaosConnWithClient(normalizedCfg, unifiedClient), nil
}

// Driver implements driver.Connector interface.
// Driver returns &TDengineDriver{}.
func (c *connector) Driver() driver.Driver {
	return &TDengineDriver{}
}
