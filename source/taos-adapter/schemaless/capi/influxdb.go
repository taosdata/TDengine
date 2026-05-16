package capi

import (
	"strings"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/generator"
)

func InsertInfluxdb(conn unsafe.Pointer, data []byte, db, precision string, ttl int, reqID int64, tableNameKey string, logger *logrus.Entry) error {
	if reqID == 0 {
		reqID = generator.GetReqID()
	}
	err := tool.SchemalessSelectDB(conn, logger, log.IsDebug(), db, reqID)
	if err != nil {
		return err
	}

	d := strings.TrimSpace(string(data))

	var result unsafe.Pointer

	_, result = syncinterface.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn, d, wrapper.InfluxDBLineProtocol, precision, ttl, reqID, tableNameKey, logger, log.IsDebug())

	defer func() {
		syncinterface.TaosSchemalessFree(result, logger, log.IsDebug())
	}()

	if code := syncinterface.TaosError(result, logger, log.IsDebug()); code != 0 {
		return tErrors.NewError(code, syncinterface.TaosErrorStr(result, logger, log.IsDebug()))
	}
	return nil
}
