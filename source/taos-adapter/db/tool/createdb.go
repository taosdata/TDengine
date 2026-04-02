package tool

import (
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/tools/pool"
)

func CreateDBWithConnection(connection unsafe.Pointer, logger *logrus.Entry, isDebug bool, db string, reqID int64) error {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("create database if not exists ")
	b.WriteString(db)
	b.WriteString(" precision 'ns' schemaless 1")
	err := async.GlobalAsync.TaosExecWithoutResult(connection, logger, isDebug, b.String(), reqID)
	if err != nil {
		return err
	}
	return nil
}

func SchemalessSelectDB(taosConnect unsafe.Pointer, logger *logrus.Entry, isDebug bool, db string, reqID int64) error {
	err := async.GlobalAsync.TaosExecWithoutResult(taosConnect, logger, isDebug, "use "+db, reqID)
	if err != nil {
		e, is := err.(*errors.TaosError)
		if is && e.Code == httperror.TSDB_CODE_MND_DB_NOT_EXIST && config.Conf.SMLAutoCreateDB {
			err := CreateDBWithConnection(taosConnect, logger, isDebug, db, reqID)
			if err != nil {
				return err
			}
			logger.Tracef("use db %s", db)
			code := syncinterface.TaosSelectDB(taosConnect, db, logger, isDebug)
			if code != 0 {
				return errors.NewError(code, syncinterface.TaosErrorStr(nil, logger, isDebug))
			}
		} else {
			return err
		}
	}
	return nil
}
