/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package taosSql

import (
	"context"
	"database/sql/driver"
)

type connector struct {
	cfg *config
}

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	var err error
 	// New taosConn
	mc := &taosConn{
		cfg:              c.cfg,
		parseTime:        c.cfg.parseTime,
	}

	// Connect to Server
	mc.taos, err = mc.taosConnect(mc.cfg.addr, mc.cfg.user, mc.cfg.passwd, mc.cfg.dbName, mc.cfg.port)
    if err != nil {
        return nil, err
    }

	return mc, nil
}

// Driver implements driver.Connector interface.
// Driver returns &taosSQLDriver{}.
func (c *connector) Driver() driver.Driver {
	return &taosSQLDriver{}
}
