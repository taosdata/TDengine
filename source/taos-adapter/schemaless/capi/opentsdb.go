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

func InsertOpentsdbJson(conn unsafe.Pointer, data []byte, db string, ttl int, reqID int64, tableNameKey string, logger *logrus.Entry) error {
	if len(data) == 0 {
		return nil
	}
	isDebug := log.IsDebug()
	if err := tool.SchemalessSelectDB(conn, logger, isDebug, db, reqID); err != nil {
		return err
	}

	var result unsafe.Pointer
	_, result = syncinterface.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn, string(data), wrapper.OpenTSDBJsonFormatProtocol,
		"", ttl, getReqID(reqID), tableNameKey, logger, isDebug)

	defer func() {
		syncinterface.TaosSchemalessFree(result, logger, isDebug)
	}()
	if code := syncinterface.TaosError(result, logger, isDebug); code != 0 {
		return tErrors.NewError(code, syncinterface.TaosErrorStr(result, logger, isDebug))
	}
	return nil
}

func InsertOpentsdbTelnet(conn unsafe.Pointer, data []string, db string, ttl int, reqID int64, tableNameKey string, logger *logrus.Entry) error {
	isDebug := log.IsDebug()
	trimData := make([]string, 0, len(data))
	for i := 0; i < len(data); i++ {
		if len(data[i]) == 0 {
			continue
		}
		trimData = append(trimData, strings.TrimPrefix(strings.TrimSpace(data[i]), "put "))
	}
	if len(trimData) == 0 {
		return nil
	}
	if err := tool.SchemalessSelectDB(conn, logger, isDebug, db, reqID); err != nil {
		return err
	}

	var result unsafe.Pointer
	_, result = syncinterface.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn, strings.Join(trimData, "\n"),
		wrapper.OpenTSDBTelnetLineProtocol, "", ttl, getReqID(reqID), tableNameKey, logger, isDebug)
	defer func() {
		syncinterface.TaosSchemalessFree(result, logger, isDebug)
	}()

	code := syncinterface.TaosError(result, logger, isDebug)
	if code != 0 {
		return tErrors.NewError(code, syncinterface.TaosErrorStr(result, logger, isDebug))
	}
	return nil
}

func getReqID(id int64) int64 {
	if id == 0 {
		return generator.GetReqID()
	}
	return id
}
