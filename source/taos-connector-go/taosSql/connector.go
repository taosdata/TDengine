package taosSql

import (
	"context"
	"database/sql/driver"
	"runtime"
	"sync"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/handler"
	"github.com/taosdata/driver-go/v3/wrapper/thread"
)

type connector struct {
	cfg *Config
}

var once = sync.Once{}

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	onceInitLock.Do(func() {
		threads := c.cfg.CgoThread
		if threads <= 0 {
			threads = runtime.NumCPU()
		}
		locker = thread.NewLocker(threads)
	})
	onceInitHandlerPool.Do(func() {
		poolSize := c.cfg.CgoAsyncHandlerPoolSize
		if poolSize <= 0 {
			poolSize = 10000
		}
		asyncHandlerPool = handler.NewHandlerPool(poolSize)
	})
	var err error
	tc := newTaosConn(c.cfg)
	if c.cfg.Net == "cfg" && len(c.cfg.ConfigPath) > 0 {
		once.Do(func() {
			locker.Lock()
			code := wrapper.TaosOptions(common.TSDB_OPTION_CONFIGDIR, c.cfg.ConfigPath)
			locker.Unlock()
			if code != 0 {
				err = errors.NewError(code, wrapper.TaosErrorStr(nil))
			}
		})
	}
	if err != nil {
		return nil, err
	}
	// Connect to Server
	if len(tc.cfg.User) == 0 {
		tc.cfg.User = common.DefaultUser
	}
	if len(tc.cfg.Passwd) == 0 {
		tc.cfg.Passwd = common.DefaultPassword
	}
	locker.Lock()
	err = wrapper.TaosSetConfig(tc.cfg.Params)
	locker.Unlock()
	if err != nil {
		return nil, err
	}
	locker.Lock()
	taos, err := wrapper.TaosConnect(tc.cfg.Addr, tc.cfg.User, tc.cfg.Passwd, tc.cfg.DbName, tc.cfg.Port)
	locker.Unlock()
	if err != nil {
		return nil, err
	}
	if tc.timezoneStr != "" {
		code := wrapper.TaosOptionsConnection(taos, common.TSDB_OPTION_CONNECTION_TIMEZONE, &tc.timezoneStr)
		if code != 0 {
			err = errors.NewError(code, wrapper.TaosErrorStr(nil))
			wrapper.TaosClose(taos)
			return nil, err
		}
	}
	tc.taos = taos
	return tc, nil
}

// Driver implements driver.Connector interface.
// Driver returns &TDengineDriver{}.
func (c *connector) Driver() driver.Driver {
	return &TDengineDriver{}
}
