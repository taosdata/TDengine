// Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package process

import (
	"context"
	"fmt"

	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/infrastructure/log"
	"github.com/taosdata/taoskeeper/util"
)

var builderLogger = log.GetLogger("BLD")

func ExpandMetricsFromConfig(ctx context.Context, conn *db.Connector, cfg *config.Metrics) (tables map[string]struct{}, err error) {
	tables = make(map[string]struct{})
	for _, name := range cfg.Tables {
		builderLogger.Debug("normal table: ", name)

		_, exist := tables[name]
		if exist {
			builderLogger.Debug(name, "is exist in config")
			continue
		}
		tables[name] = struct{}{}
	}

	sql := fmt.Sprintf(GetStableNameListSql(), cfg.Database.Name)
	data, err := conn.QueryWithRetryForever(ctx, sql, util.GetQidOwn(config.Conf.InstanceID))
	if err != nil {
		return nil, err
	}
	builderLogger.Debugf("show stables:%s", sql)

	for _, info := range data.Data {
		name := info[0].(string)
		builderLogger.Debug("stable:", info)

		_, exist := tables[name]
		if exist {
			builderLogger.Debug(name, "is exist in config")
			continue
		}
		tables[name] = struct{}{}
	}
	return
}

func GetStableNameListSql() string {
	return "select stable_name from information_schema.ins_stables " +
		" where db_name = '%s' " +
		" and (stable_name not like 'taosx\\_%%')" +
		" and (stable_name not like 'taosadapter%%')" +
		" and (stable_name != 'temp_dir' and stable_name != 'data_dir')"
}
