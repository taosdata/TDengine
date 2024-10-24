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

func ExpandMetricsFromConfig(ctx context.Context, conn *db.Connector, cfg *config.MetricsConfig) (tables map[string]struct{}, err error) {
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
	data, err := conn.Query(ctx, sql, util.GetQidOwn())
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
