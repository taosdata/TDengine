package inserter

import (
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/schemaless/capi"
	"github.com/taosdata/taosadapter/v3/tools/generator"
)

func InsertInfluxdb(taosConnect unsafe.Pointer, data []byte, db, precision string, ttl int, reqID uint64, tableNameKey string, logger *logrus.Entry) error {
	return capi.InsertInfluxdb(taosConnect, data, db, precision, ttl, getReqID(reqID), tableNameKey, logger)
}

func InsertOpentsdbJson(taosConnect unsafe.Pointer, data []byte, db string, ttl int, reqID uint64, tableNameKey string, logger *logrus.Entry) error {
	return capi.InsertOpentsdbJson(taosConnect, data, db, ttl, getReqID(reqID), tableNameKey, logger)
}

func InsertOpentsdbTelnetBatch(taosConnect unsafe.Pointer, data []string, db string, ttl int, reqID uint64, tableNameKey string, logger *logrus.Entry) error {
	return capi.InsertOpentsdbTelnet(taosConnect, data, db, ttl, getReqID(reqID), tableNameKey, logger)
}

func getReqID(id uint64) int64 {
	if id == 0 {
		return generator.GetReqID()
	}
	return int64(id)
}
