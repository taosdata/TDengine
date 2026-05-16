package syncinterface

import (
	"database/sql/driver"
	"strings"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/types"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/monitor/metrics"
	"github.com/taosdata/taosadapter/v3/thread"
)

func doTaosFreeResult(res unsafe.Pointer, logger *logrus.Entry, isDebug bool, startMetric, successMetric *metrics.Gauge) {
	if res == nil {
		logger.Trace("result is nil")
		return
	}
	logger.Trace("sync semaphore acquire for taos_free_result")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_free_result")
	}()
	logger.Debugf("call taos_free_result, res:%p", res)
	startMetric.Inc()
	s := log.GetLogNow(isDebug)
	wrapper.TaosFreeResult(res)
	successMetric.Inc()
	logger.Debugf("taos_free_result finish, cost:%s", log.GetLogDuration(isDebug, s))
}

func TaosSchemalessFree(res unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	doTaosFreeResult(res, logger, isDebug, monitor.TaosSchemalessFreeResultCounter, monitor.TaosSchemalessFreeResultSuccessCounter)
}

func TaosSyncQueryFree(res unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	doTaosFreeResult(res, logger, isDebug, monitor.TaosSyncQueryFreeResultCounter, monitor.TaosSyncQueryFreeResultSuccessCounter)
}

func TaosClose(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	if conn == nil {
		logger.Trace("connection is nil")
		return
	}
	logger.Trace("sync semaphore acquire for taos_close")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_close")
	}()
	logger.Debugf("call taos_close, conn:%p", conn)
	monitor.TaosCloseCounter.Inc()
	s := log.GetLogNow(isDebug)
	wrapper.TaosClose(conn)
	monitor.TaosCloseSuccessCounter.Inc()
	logger.Debugf("taos_close finish, cost:%s", log.GetLogDuration(isDebug, s))
}

func TaosSelectDB(conn unsafe.Pointer, db string, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_select_db")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_select_db")
	}()
	logger.Debugf("call taos_select_db, conn:%p, db:%s", conn, db)
	monitor.TaosSelectDBCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosSelectDB(conn, db)
	if code != 0 {
		monitor.TaosSelectDBFailCounter.Inc()
	} else {
		monitor.TaosSelectDBSuccessCounter.Inc()
	}
	logger.Debugf("taos_select_db finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosConnect(host, user, pass, db string, port int, logger *logrus.Entry, isDebug bool) (unsafe.Pointer, error) {
	logger.Trace("sync semaphore acquire for taos_connect")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_connect")
	}()
	logger.Debugf("call taos_connect, host:%s, user:%s, db:%s, port:%d", host, user, db, port)
	monitor.TaosConnectCounter.Inc()
	s := log.GetLogNow(isDebug)
	conn, err := wrapper.TaosConnect(host, user, pass, db, port)
	logger.Debugf("taos_connect finish, conn:%p, err:%v, cost:%s", conn, err, log.GetLogDuration(isDebug, s))
	if err != nil {
		monitor.TaosConnectFailCounter.Inc()
	} else {
		monitor.TaosConnectSuccessCounter.Inc()
	}
	return conn, err
}

func TaosGetTablesVgID(conn unsafe.Pointer, db string, tables []string, logger *logrus.Entry, isDebug bool) ([]int, int) {
	logger.Trace("sync semaphore acquire for taos_get_tables_vgId")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_get_tables_vgId")
	}()
	logger.Debugf("call taos_get_tables_vgId, conn:%p, db:%s, tables:%s", conn, db, strings.Join(tables, ", "))
	monitor.TaosGetTablesVgIDCounter.Inc()
	s := log.GetLogNow(isDebug)
	vgIDs, code := wrapper.TaosGetTablesVgID(conn, db, tables)
	logger.Debugf("taos_get_tables_vgId finish, vgid:%v, code:%d, cost:%s", vgIDs, code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosGetTablesVgIDFailCounter.Inc()
	} else {
		monitor.TaosGetTablesVgIDSuccessCounter.Inc()
	}
	return vgIDs, code
}

func TaosStmtInitWithReqID(conn unsafe.Pointer, reqID int64, logger *logrus.Entry, isDebug bool) unsafe.Pointer {
	logger.Trace("sync semaphore acquire for taos_stmt_init_with_reqid")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt_init_with_reqid")
	}()
	logger.Debugf("call taos_stmt_init_with_reqid, conn:%p, QID:0x%x", conn, reqID)
	monitor.TaosStmtInitWithReqIDCounter.Inc()
	s := log.GetLogNow(isDebug)
	stmtInit := wrapper.TaosStmtInitWithReqID(conn, reqID)
	logger.Debugf("taos_stmt_init_with_reqid finish, result:%p, cost:%s", stmtInit, log.GetLogDuration(isDebug, s))
	if stmtInit == nil {
		monitor.TaosStmtInitWithReqIDFailCounter.Inc()
	} else {
		monitor.TaosStmtInitWithReqIDSuccessCounter.Inc()
	}
	return stmtInit
}

func TaosStmtPrepare(stmt unsafe.Pointer, sql string, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt_prepare")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt_prepare")
	}()
	logger.Debugf("call taos_stmt_prepare, stmt:%p,  sql:%s", stmt, log.GetLogSql(sql))
	monitor.TaosStmtPrepareCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmtPrepare(stmt, sql)
	logger.Debugf("taos_stmt_prepare finish, code:%d cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmtPrepareFailCounter.Inc()
	} else {
		monitor.TaosStmtPrepareSuccessCounter.Inc()
	}
	return code
}

func TaosStmtIsInsert(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) (bool, int) {
	logger.Trace("sync semaphore acquire for taos_stmt_is_insert")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt_is_insert")
	}()
	logger.Debugf("call taos_stmt_is_insert, stmt:%p", stmt)
	monitor.TaosStmtIsInsertCounter.Inc()
	s := log.GetLogNow(isDebug)
	isInsert, code := wrapper.TaosStmtIsInsert(stmt)
	logger.Debugf("taos_stmt_is_insert isInsert finish, insert:%t, code:%d, cost:%s", isInsert, code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmtIsInsertFailCounter.Inc()
	} else {
		monitor.TaosStmtIsInsertSuccessCounter.Inc()
	}
	return isInsert, code
}

func TaosStmtSetTBName(stmt unsafe.Pointer, tbname string, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt_set_tbname")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt_set_tbname")
	}()
	logger.Debugf("call taos_stmt_set_tbname, stmt:%p, tbname:%s", stmt, tbname)
	monitor.TaosStmtSetTBNameCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmtSetTBName(stmt, tbname)
	logger.Debugf("taos_stmt_set_tbname finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmtSetTBNameFailCounter.Inc()
	} else {
		monitor.TaosStmtSetTBNameSuccessCounter.Inc()
	}
	return code
}

func TaosStmtGetTagFields(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) (int, int, unsafe.Pointer) {
	logger.Trace("sync semaphore acquire for taos_stmt_get_tag_fields")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt_get_tag_fields")
	}()
	logger.Debugf("call taos_stmt_get_tag_fields, stmt:%p", stmt)
	monitor.TaosStmtGetTagFieldsCounter.Inc()
	s := log.GetLogNow(isDebug)
	code, num, fields := wrapper.TaosStmtGetTagFields(stmt)
	logger.Debugf("taos_stmt_get_tag_fields finish, code:%d, num:%d, fields:%p, cost:%s", code, num, fields, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmtGetTagFieldsFailCounter.Inc()
	} else {
		monitor.TaosStmtGetTagFieldsSuccessCounter.Inc()
	}
	return code, num, fields
}

func TaosStmtReclaimFields(stmt unsafe.Pointer, fields unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	logger.Trace("sync semaphore acquire for taos_stmt_reclaim_fields")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt_reclaim_fields")
	}()
	logger.Debugf("call taos_stmt_reclaim_fields, stmt:%p, fields:%p", stmt, fields)
	monitor.TaosStmtReclaimFieldsCounter.Inc()
	s := log.GetLogNow(isDebug)
	wrapper.TaosStmtReclaimFields(stmt, fields)
	logger.Debugf("taos_stmt_reclaim_fields finish, cost:%s", log.GetLogDuration(isDebug, s))
	monitor.TaosStmtReclaimFieldsSuccessCounter.Inc()
}

func TaosStmtSetTags(stmt unsafe.Pointer, tags []driver.Value, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt_set_tags")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt_set_tags")
	}()
	logger.Debugf("call taos_stmt_set_tags, stmt:%p, tags:%v", stmt, tags)
	monitor.TaosStmtSetTagsCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmtSetTags(stmt, tags)
	logger.Debugf("taos_stmt_set_tags finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmtSetTagsFailCounter.Inc()
	} else {
		monitor.TaosStmtSetTagsSuccessCounter.Inc()
	}
	return code
}

func TaosStmtGetColFields(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) (int, int, unsafe.Pointer) {
	logger.Trace("sync semaphore acquire for taos_stmt_get_col_fields")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt_get_col_fields")
	}()
	logger.Debugf("call taos_stmt_get_col_fields, stmt:%p", stmt)
	monitor.TaosStmtGetColFieldsCounter.Inc()
	s := log.GetLogNow(isDebug)
	code, num, fields := wrapper.TaosStmtGetColFields(stmt)
	logger.Debugf("taos_stmt_get_col_fields finish, code:%d, num:%d, fields:%p, cost:%s", code, num, fields, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmtGetColFieldsFailCounter.Inc()
	} else {
		monitor.TaosStmtGetColFieldsSuccessCounter.Inc()
	}
	return code, num, fields
}

func TaosStmtBindParamBatch(stmt unsafe.Pointer, multiBind [][]driver.Value, bindType []*types.ColumnType, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt_bind_param_batch")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt_bind_param_batch")
	}()
	logger.Debugf("call taos_stmt_bind_param_batch, stmt:%p, multiBind:%v, bindType:%v", stmt, multiBind, bindType)
	monitor.TaosStmtBindParamBatchCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmtBindParamBatch(stmt, multiBind, bindType)
	logger.Debugf("taos_stmt_bind_param_batch finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmtBindParamBatchFailCounter.Inc()
	} else {
		monitor.TaosStmtBindParamBatchSuccessCounter.Inc()
	}
	return code
}

func TaosStmtAddBatch(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt_add_batch")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt_add_batch")
	}()
	logger.Debugf("call taos_stmt_add_batch, stmt:%p", stmt)
	monitor.TaosStmtAddBatchCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmtAddBatch(stmt)
	logger.Debugf("taos_stmt_add_batch finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmtAddBatchFailCounter.Inc()
	} else {
		monitor.TaosStmtAddBatchSuccessCounter.Inc()
	}
	return code
}

func TaosStmtExecute(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt_execute")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt_execute")
	}()
	logger.Debugf("call taos_stmt_execute, stmt:%p", stmt)
	monitor.TaosStmtExecuteCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmtExecute(stmt)
	logger.Debugf("taos_stmt_execute finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmtExecuteFailCounter.Inc()
	} else {
		monitor.TaosStmtExecuteSuccessCounter.Inc()
	}
	return code
}

func TaosStmtClose(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt_close")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt_close")
	}()
	logger.Debugf("call taos_stmt_close, stmt:%p", stmt)
	monitor.TaosStmtCloseCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmtClose(stmt)
	logger.Debugf("taos_stmt_close finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmtCloseFailCounter.Inc()
	} else {
		monitor.TaosStmtCloseSuccessCounter.Inc()
	}
	return code
}

func TMQWriteRaw(conn unsafe.Pointer, length uint32, metaType uint16, data unsafe.Pointer, logger *logrus.Entry, isDebug bool) int32 {
	logger.Trace("sync semaphore acquire for tmq_write_raw")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for tmq_write_raw")
	}()
	logger.Debugf("call tmq_write_raw, conn:%p, length:%d, metaType:%d, data:%p", conn, length, metaType, data)
	monitor.TMQWriteRawCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TMQWriteRaw(conn, length, metaType, data)
	logger.Debugf("tmq_write_raw finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TMQWriteRawFailCounter.Inc()
	} else {
		monitor.TMQWriteRawSuccessCounter.Inc()
	}
	return code
}

func TaosWriteRawBlockWithReqID(conn unsafe.Pointer, numOfRows int, pData unsafe.Pointer, tableName string, reqID int64, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_write_raw_block_with_reqid")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_write_raw_block_with_reqid")
	}()
	logger.Debugf("call taos_write_raw_block_with_reqid, conn:%p, numOfRows:%d, pData:%p, tableName:%s, reqID:%d", conn, numOfRows, pData, tableName, reqID)
	monitor.TaosWriteRawBlockWithReqIDCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosWriteRawBlockWithReqID(conn, numOfRows, pData, tableName, reqID)
	logger.Debugf("taos_write_raw_block_with_reqid finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosWriteRawBlockWithReqIDFailCounter.Inc()
	} else {
		monitor.TaosWriteRawBlockWithReqIDSuccessCounter.Inc()
	}
	logger.Debugf("taos_write_raw_block_with_reqid finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	return code
}

func TaosWriteRawBlockWithFieldsWithReqID(
	conn unsafe.Pointer,
	numOfRows int,
	pData unsafe.Pointer,
	tableName string,
	fields unsafe.Pointer,
	numFields int,
	reqID int64,
	logger *logrus.Entry,
	isDebug bool,
) int {
	logger.Trace("sync semaphore acquire for taos_write_raw_block_with_fields_with_reqid")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_write_raw_block_with_fields_with_reqid")
	}()
	logger.Debugf(
		"call taos_write_raw_block_with_fields_with_reqid, conn:%p, numOfRows:%d, pData:%p, tableName:%s, fields:%p, numFields:%d, reqID:%d",
		conn,
		numOfRows,
		pData,
		tableName,
		fields,
		numFields,
		reqID,
	)
	monitor.TaosWriteRawBlockWithFieldsWithReqIDCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosWriteRawBlockWithFieldsWithReqID(conn, numOfRows, pData, tableName, fields, numFields, reqID)
	logger.Debugf("taos_write_raw_block_with_fields_with_reqid finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosWriteRawBlockWithFieldsWithReqIDFailCounter.Inc()
	} else {
		monitor.TaosWriteRawBlockWithFieldsWithReqIDSuccessCounter.Inc()
	}
	return code
}

func TaosGetCurrentDB(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) (string, error) {
	logger.Trace("sync semaphore acquire for taos_get_current_db")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_get_current_db")
	}()
	logger.Debugf("call taos_get_current_db, conn:%p", conn)
	monitor.TaosGetCurrentDBCounter.Inc()
	s := log.GetLogNow(isDebug)
	db, err := wrapper.TaosGetCurrentDB(conn)
	logger.Debugf("taos_get_current_db finish, db:%s, err:%v, cost:%s", db, err, log.GetLogDuration(isDebug, s))
	if err != nil {
		monitor.TaosGetCurrentDBFailCounter.Inc()
	} else {
		monitor.TaosGetCurrentDBSuccessCounter.Inc()
	}
	return db, err
}

func TaosGetServerInfo(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) string {
	logger.Trace("sync semaphore acquire for taos_get_server_info")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_get_server_info")
	}()
	logger.Debugf("call taos_get_server_info, conn:%p", conn)
	monitor.TaosGetServerInfoCounter.Inc()
	s := log.GetLogNow(isDebug)
	info := wrapper.TaosGetServerInfo(conn)
	logger.Debugf("taos_get_server_info finish, info:%s, cost:%s", info, log.GetLogDuration(isDebug, s))
	monitor.TaosGetServerInfoSuccessCounter.Inc()
	return info
}

func TaosStmtNumParams(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) (int, int) {
	logger.Trace("sync semaphore acquire for taos_stmt_num_params")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt_num_params")
	}()
	logger.Debugf("call taos_stmt_num_params, stmt:%p", stmt)
	monitor.TaosStmtNumParamsCounter.Inc()
	s := log.GetLogNow(isDebug)
	num, errCode := wrapper.TaosStmtNumParams(stmt)
	logger.Debugf("taos_stmt_num_params finish, num:%d, code:%d, cost:%s", num, errCode, log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		monitor.TaosStmtNumParamsFailCounter.Inc()
	} else {
		monitor.TaosStmtNumParamsSuccessCounter.Inc()
	}
	return num, errCode
}

func TaosStmtGetParam(stmt unsafe.Pointer, index int, logger *logrus.Entry, isDebug bool) (int, int, error) {
	logger.Trace("sync semaphore acquire for taos_stmt_get_param")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt_get_param")
	}()
	logger.Debugf("call taos_stmt_get_param, stmt:%p, index:%d", stmt, index)
	monitor.TaosStmtGetParamCounter.Inc()
	s := log.GetLogNow(isDebug)
	dataType, dataLength, err := wrapper.TaosStmtGetParam(stmt, index)
	logger.Debugf("taos_stmt_get_param finish, type:%d, len:%d, err:%v, cost:%s", dataType, dataLength, err, log.GetLogDuration(isDebug, s))
	if err != nil {
		monitor.TaosStmtGetParamFailCounter.Inc()
	} else {
		monitor.TaosStmtGetParamSuccessCounter.Inc()
	}
	return dataType, dataLength, err
}

func TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn unsafe.Pointer, lines string, protocol int, precision string, ttl int, reqID int64, tbNameKey string, logger *logrus.Entry, isDebug bool) (int32, unsafe.Pointer) {
	logger.Trace("sync semaphore acquire for taos_schemaless_insert_raw_ttl_with_reqid_tbname_key")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_schemaless_insert_raw_ttl_with_reqid_tbname_key")
	}()
	logger.Debugf("call taos_schemaless_insert_raw_ttl_with_reqid_tbname_key, conn:%p, lines:%s, protocol:%d, precision:%s, ttl:%d, reqID:%d, tbnameKey:%s", conn, lines, protocol, precision, ttl, reqID, tbNameKey)
	monitor.TaosSchemalessInsertCounter.Inc()
	s := log.GetLogNow(isDebug)
	rows, result := wrapper.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn, lines, protocol, precision, ttl, reqID, tbNameKey)
	logger.Debugf("taos_schemaless_insert_raw_ttl_with_reqid_tbname_key finish, rows:%d, result:%p, cost:%s", rows, result, log.GetLogDuration(isDebug, s))
	code := TaosError(result, logger, isDebug)
	if code != 0 {
		monitor.TaosSchemalessInsertFailCounter.Inc()
	} else {
		monitor.TaosSchemalessInsertSuccessCounter.Inc()
	}
	return rows, result
}

func TaosStmt2Init(taosConnect unsafe.Pointer, reqID int64, singleStbInsert bool, singleTableBindOnce bool, handle cgo.Handle, logger *logrus.Entry, isDebug bool) unsafe.Pointer {
	logger.Trace("sync semaphore acquire for taos_stmt2_init")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt2_init")
	}()
	logger.Debugf("call taos_stmt2_init, taosConnect:%p, reqID:%d, singleStbInsert:%t, singleTableBindOnce:%t, handle:%p", taosConnect, reqID, singleStbInsert, singleTableBindOnce, handle.Pointer())
	monitor.TaosStmt2InitCounter.Inc()
	s := log.GetLogNow(isDebug)
	stmt2 := wrapper.TaosStmt2Init(taosConnect, reqID, singleStbInsert, singleTableBindOnce, handle)
	logger.Debugf("taos_stmt2_init finish, stmt2:%p, cost:%s", stmt2, log.GetLogDuration(isDebug, s))
	if stmt2 == nil {
		monitor.TaosStmt2InitFailCounter.Inc()
	} else {
		monitor.TaosStmt2InitSuccessCounter.Inc()
	}
	return stmt2
}

func TaosStmt2Prepare(stmt2 unsafe.Pointer, sql string, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt2_prepare")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt2_prepare")
	}()
	logger.Debugf("call taos_stmt2_prepare, stmt2:%p, sql:%s", stmt2, log.GetLogSql(sql))
	monitor.TaosStmt2PrepareCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmt2Prepare(stmt2, sql)
	logger.Debugf("taos_stmt2_prepare finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmt2PrepareFailCounter.Inc()
	} else {
		monitor.TaosStmt2PrepareSuccessCounter.Inc()
	}
	return code
}

func TaosStmt2IsInsert(stmt2 unsafe.Pointer, logger *logrus.Entry, isDebug bool) (bool, int) {
	logger.Trace("sync semaphore acquire for taos_stmt2_is_insert")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt2_is_insert")
	}()
	logger.Debugf("call taos_stmt2_is_insert, stmt2:%p", stmt2)
	monitor.TaosStmt2IsInsertCounter.Inc()
	s := log.GetLogNow(isDebug)
	isInsert, code := wrapper.TaosStmt2IsInsert(stmt2)
	logger.Debugf("taos_stmt2_is_insert finish, isInsert:%t, code:%d, cost:%s", isInsert, code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmt2IsInsertFailCounter.Inc()
	} else {
		monitor.TaosStmt2IsInsertSuccessCounter.Inc()
	}
	return isInsert, code
}

func TaosStmt2GetFields(stmt2 unsafe.Pointer, logger *logrus.Entry, isDebug bool) (code, count int, fields unsafe.Pointer) {
	logger.Trace("sync semaphore acquire for taos_stmt2_get_fields")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt2_get_fields")
	}()
	logger.Debugf("call taos_stmt2_get_fields, stmt2:%p", stmt2)
	monitor.TaosStmt2GetFieldsCounter.Inc()
	s := log.GetLogNow(isDebug)
	code, count, fields = wrapper.TaosStmt2GetFields(stmt2)
	logger.Debugf("taos_stmt2_get_fields finish, code:%d, count:%d, fields:%p, cost:%s", code, count, fields, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmt2GetFieldsFailCounter.Inc()
	} else {
		monitor.TaosStmt2GetFieldsSuccessCounter.Inc()
	}
	return code, count, fields
}

func TaosStmt2Exec(stmt2 unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt2_exec")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt2_exec")
	}()
	logger.Debugf("call taos_stmt2_exec, stmt2:%p", stmt2)
	monitor.TaosStmt2ExecCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmt2Exec(stmt2)
	logger.Debugf("taos_stmt2_exec finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmt2ExecFailCounter.Inc()
	} else {
		monitor.TaosStmt2ExecSuccessCounter.Inc()
	}
	return code
}

func TaosStmt2Close(stmt2 unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_stmt2_close")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt2_close")
	}()
	logger.Debugf("call taos_stmt2_close, stmt2:%p", stmt2)
	monitor.TaosStmt2CloseCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosStmt2Close(stmt2)
	logger.Debugf("taos_stmt2_close finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosStmt2CloseFailCounter.Inc()
	} else {
		monitor.TaosStmt2CloseSuccessCounter.Inc()
	}
	return code
}

func TaosStmt2BindBinary(stmt2 unsafe.Pointer, data []byte, colIdx int32, logger *logrus.Entry, isDebug bool) error {
	logger.Trace("sync semaphore acquire for taos_stmt2_bind_binary")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt2_bind_binary")
	}()
	logger.Debugf("call taos_stmt2_bind_binary, stmt2:%p, colIdx:%d, data:%v", stmt2, colIdx, data)
	monitor.TaosStmt2BindBinaryCounter.Inc()
	s := log.GetLogNow(isDebug)
	err := wrapper.TaosStmt2BindBinary(stmt2, data, colIdx)
	logger.Debugf("taos_stmt2_bind_binary finish, err:%v, cost:%s", err, log.GetLogDuration(isDebug, s))
	if err != nil {
		monitor.TaosStmt2BindBinaryFailCounter.Inc()
	} else {
		monitor.TaosStmt2BindBinarySuccessCounter.Inc()
	}
	return err
}

func TaosStmt2Result(stmt2 unsafe.Pointer, logger *logrus.Entry, isDebug bool) unsafe.Pointer {
	logger.Trace("sync semaphore acquire for taos_stmt2_result")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_stmt2_result")
	}()
	logger.Debugf("call taos_stmt2_result, stmt2:%p", stmt2)
	s := log.GetLogNow(isDebug)
	result := wrapper.TaosStmt2Result(stmt2)
	logger.Debugf("taos_stmt2_result finish, result:%p, cost:%s", result, log.GetLogDuration(isDebug, s))
	return result
}

func TaosOptionsConnection(conn unsafe.Pointer, option int, value *string, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_options_connection")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_options_connection")
	}()
	if value == nil {
		logger.Debugf("call taos_options_connection, conn:%p, option:%d, value:<nil>", conn, option)
	} else {
		logger.Debugf("call taos_options_connection, conn:%p, option:%d, value:%s", conn, option, *value)
	}
	monitor.TaosOptionsConnectionCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosOptionsConnection(conn, option, value)
	logger.Debugf("taos_options_connection finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosOptionsConnectionFailCounter.Inc()
	} else {
		monitor.TaosOptionsConnectionSuccessCounter.Inc()
	}
	return code
}

func TaosValidateSql(taosConnect unsafe.Pointer, sql string, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_validate_sql")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_validate_sql")
	}()
	logger.Debugf("call taos_validate_sql, taosConnect:%p, sql:%s", taosConnect, sql)
	monitor.TaosValidateSqlCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosValidateSql(taosConnect, sql)
	logger.Debugf("taos_validate_sql finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosValidateSqlFailCounter.Inc()
	} else {
		monitor.TaosValidateSqlSuccessCounter.Inc()
	}
	return code
}

func TaosCheckServerStatus(fqdn *string, port int32, logger *logrus.Entry, isDebug bool) (int32, string) {
	logger.Trace("sync semaphore acquire for taos_check_server_status")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_check_server_status")
	}()
	if fqdn == nil {
		logger.Debugf("call taos_check_server_status, fqdn: nil, port:%d", port)
	} else {
		logger.Debugf("call taos_check_server_status, fqdn:%s, port:%d", *fqdn, port)
	}
	monitor.TaosCheckServerStatusCounter.Inc()
	s := log.GetLogNow(isDebug)
	status, details := wrapper.TaosCheckServerStatus(fqdn, port)
	logger.Debugf("taos_check_server_status finish, status:%d, detail:%s, cost:%s", status, details, log.GetLogDuration(isDebug, s))
	monitor.TaosCheckServerStatusSuccessCounter.Inc()
	return status, details
}

func TMQSubscription(consumer unsafe.Pointer, logger *logrus.Entry, isDebug bool) (int32, unsafe.Pointer) {
	logger.Trace("sync semaphore acquire for tmq_subscription")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for tmq_subscription")
	}()
	logger.Debugf("call tmq_subscription, consumer:%p", consumer)
	monitor.TMQSubscriptionCounter.Inc()
	s := log.GetLogNow(isDebug)
	code, result := wrapper.TMQSubscription(consumer)
	logger.Debugf("tmq_subscription finish, code:%d, result:%p, cost:%s", code, result, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TMQSubscriptionFailCounter.Inc()
	} else {
		monitor.TMQSubscriptionSuccessCounter.Inc()
	}
	return code, result
}

func TaosErrorStr(res unsafe.Pointer, logger *logrus.Entry, isDebug bool) string {
	logger.Tracef("call taos_error_str, res:%p", res)
	monitor.TaosErrorStrCounter.Inc()
	errStr := wrapper.TaosErrorStr(res)
	logger.Debugf("taos_error_str finish, errStr:%s", errStr)
	monitor.TaosErrorStrSuccessCounter.Inc()
	return errStr
}

func TaosIsUpdateQuery(res unsafe.Pointer, logger *logrus.Entry, isDebug bool) bool {
	logger.Tracef("call taos_is_update_query, res:%p", res)
	monitor.TaosIsUpdateQueryCounter.Inc()
	isUpdate := wrapper.TaosIsUpdateQuery(res)
	logger.Debugf("taos_is_update_query finish, isUpdate:%t", isUpdate)
	monitor.TaosIsUpdateQuerySuccessCounter.Inc()
	return isUpdate
}

func TaosError(res unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_error, res:%p", res)
	monitor.TaosErrorCounter.Inc()
	errCode := wrapper.TaosError(res)
	logger.Debugf("taos_error finish, errCode:%d", errCode)
	monitor.TaosErrorSuccessCounter.Inc()
	return errCode
}

func TaosAffectedRows(res unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_affected_rows, res:%p", res)
	monitor.TaosAffectedRowsCounter.Inc()
	affectedRows := wrapper.TaosAffectedRows(res)
	logger.Debugf("taos_affected_rows finish, affectedRows:%d", affectedRows)
	monitor.TaosAffectedRowsSuccessCounter.Inc()
	return affectedRows
}

func TaosNumFields(res unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_num_fields, res:%p", res)
	monitor.TaosNumFieldsCounter.Inc()
	numFields := wrapper.TaosNumFields(res)
	logger.Debugf("taos_num_fields finish, numFields:%d", numFields)
	monitor.TaosNumFieldsSuccessCounter.Inc()
	return numFields
}

func ReadColumn(res unsafe.Pointer, fieldsCount int, logger *logrus.Entry, isDebug bool) (*wrapper.RowsHeader, error) {
	logger.Tracef("call read_column, res:%p, fieldsCount:%d", res, fieldsCount)
	monitor.TaosFetchFieldsECounter.Inc()
	s := log.GetLogNow(isDebug)
	rh, err := wrapper.ReadColumn(res, fieldsCount)
	logger.Debugf("read_column finish, rh:%p, err:%v, cost:%s", rh, err, log.GetLogDuration(isDebug, s))
	if err != nil {
		monitor.TaosFetchFieldsEFailCounter.Inc()
	} else {
		monitor.TaosFetchFieldsESuccessCounter.Inc()
	}
	return rh, err
}

func TaosResultPrecision(res unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_result_precision, res:%p", res)
	precision := wrapper.TaosResultPrecision(res)
	logger.Debugf("taos_result_precision finish, precision:%d", precision)
	monitor.TaosResultPrecisionCounter.Inc()
	monitor.TaosResultPrecisionSuccessCounter.Inc()
	return precision
}

func TaosGetRawBlock(res unsafe.Pointer, logger *logrus.Entry, isDebug bool) unsafe.Pointer {
	logger.Tracef("call taos_get_raw_block, res:%p", res)
	monitor.TaosGetRawBlockCounter.Inc()
	rawBlock := wrapper.TaosGetRawBlock(res)
	logger.Debugf("taos_get_raw_block finish, rawBlock:%p", rawBlock)
	monitor.TaosGetRawBlockSuccessCounter.Inc()
	return rawBlock
}

func TaosQuery(taosConnect unsafe.Pointer, sql string, logger *logrus.Entry, isDebug bool) unsafe.Pointer {
	logger.Trace("sync semaphore acquire for taos_query")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_query")
	}()
	logger.Debugf("call taos_query, taosConnect:%p, sql:%s", taosConnect, log.GetLogSql(sql))
	monitor.TaosQueryCounter.Inc()
	s := log.GetLogNow(isDebug)
	res := wrapper.TaosQuery(taosConnect, sql)
	logger.Debugf("taos_query finish, res:%p, cost:%s", res, log.GetLogDuration(isDebug, s))
	code := TaosError(res, logger, isDebug)
	if code != 0 {
		monitor.TaosQueryFailCounter.Inc()
	} else {
		monitor.TaosQuerySuccessCounter.Inc()
	}
	return res
}

func TMQErr2Str(code int32, logger *logrus.Entry, isDebug bool) string {
	logger.Tracef("call tmq_err2str, code:%d", code)
	monitor.TMQErr2StrCounter.Inc()
	errStr := wrapper.TMQErr2Str(code)
	logger.Debugf("tmq_err2str finish, errStr:%s", errStr)
	monitor.TMQErr2StrSuccessCounter.Inc()
	return errStr
}

func TMQConfSet(conf unsafe.Pointer, key string, value string, logger *logrus.Entry, isDebug bool) int32 {
	if key != "td.connect.pass" && key != "td.connect.token" {
		logger.Tracef("call tmq_conf_set, conf:%p, key:%s, value:%s", conf, key, value)
	} else {
		logger.Tracef("call tmq_conf_set, conf:%p, key:%s", conf, key)
	}
	monitor.TMQConfSetCounter.Inc()
	code := wrapper.TMQConfSet(conf, key, value)
	logger.Debugf("tmq_conf_set finish, key:%s, code:%d", key, code)
	if code != 0 {
		monitor.TMQConfSetFailCounter.Inc()
	} else {
		monitor.TMQConfSetSuccessCounter.Inc()
	}
	return code
}

func TaosFetchLengths(res unsafe.Pointer, count int, logger *logrus.Entry, isDebug bool) []int {
	logger.Tracef("call taos_fetch_lengths, res:%p", res)
	monitor.TaosFetchLengthsCounter.Inc()
	s := log.GetLogNow(isDebug)
	lengths := wrapper.FetchLengths(res, count)
	logger.Debugf("taos_fetch_lengths finish, lengths:%p, cost:%s", lengths, log.GetLogDuration(isDebug, s))
	monitor.TaosFetchLengthsSuccessCounter.Inc()
	return lengths
}

func TaosStmtErrStr(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) string {
	logger.Tracef("call taos_stmt_err_str, stmt:%p", stmt)
	monitor.TaosStmtErrStrCounter.Inc()
	errStr := wrapper.TaosStmtErrStr(stmt)
	logger.Debugf("taos_stmt_err_str finish, errStr:%s", errStr)
	monitor.TaosStmtErrStrSuccessCounter.Inc()
	return errStr
}

func TaosStmtAffectedRowsOnce(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_stmt_affected_rows_once, stmt:%p", stmt)
	monitor.TaosStmtAffectedRowsOnceCounter.Inc()
	affectedRows := wrapper.TaosStmtAffectedRowsOnce(stmt)
	logger.Debugf("taos_stmt_affected_rows_once finish, affectedRows:%d", affectedRows)
	monitor.TaosStmtAffectedRowsOnceSuccessCounter.Inc()
	return affectedRows
}

func TaosStmt2FreeFields(stmt2 unsafe.Pointer, fields unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("call taos_stmt2_free_fields, stmt2:%p, fields:%p", stmt2, fields)
	monitor.TaosStmt2FreeFieldsCounter.Inc()
	s := log.GetLogNow(isDebug)
	wrapper.TaosStmt2FreeFields(stmt2, fields)
	logger.Debugf("taos_stmt2_free_fields finish, cost:%s", log.GetLogDuration(isDebug, s))
	monitor.TaosStmt2FreeFieldsSuccessCounter.Inc()
}

func TaosStmt2Error(stmt2 unsafe.Pointer, logger *logrus.Entry, isDebug bool) string {
	logger.Tracef("call taos_stmt2_error, stmt2:%p", stmt2)
	monitor.TaosStmt2ErrorCounter.Inc()
	errStr := wrapper.TaosStmt2Error(stmt2)
	logger.Debugf("taos_stmt2_error finish, errStr:%s", errStr)
	monitor.TaosStmt2ErrorSuccessCounter.Inc()
	return errStr
}

func TaosSetNotifyCB(conn unsafe.Pointer, handle cgo.Handle, notifyType int, logger *logrus.Entry, isDebug bool) int32 {
	logger.Tracef("call taos_set_notify_cb, conn:%p, handle:%p", conn, handle.Pointer())
	monitor.TaosSetNotifyCBCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosSetNotifyCB(conn, handle, notifyType)
	logger.Debugf("taos_set_notify_cb finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosSetNotifyCBFailCounter.Inc()
	} else {
		monitor.TaosSetNotifyCBSuccessCounter.Inc()
	}
	return code
}

func TMQListNew(logger *logrus.Entry, isDebug bool) unsafe.Pointer {
	logger.Trace("call tmq_list_new")
	monitor.TMQListNewCounter.Inc()
	list := wrapper.TMQListNew()
	logger.Debugf("tmq_list_new finish, list:%p", list)
	if list == nil {
		monitor.TMQListNewFailCounter.Inc()
	} else {
		monitor.TMQListNewSuccessCounter.Inc()
	}
	return list
}

func TMQListDestroy(list unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("call tmq_list_destroy, list:%p", list)
	monitor.TMQListDestroyCounter.Inc()
	s := log.GetLogNow(isDebug)
	wrapper.TMQListDestroy(list)
	logger.Debugf("tmq_list_destroy finish, cost:%s", log.GetLogDuration(isDebug, s))
	monitor.TMQListDestroySuccessCounter.Inc()
}

func TMQListAppend(list unsafe.Pointer, item string, logger *logrus.Entry, isDebug bool) int32 {
	logger.Tracef("call tmq_list_append, list:%p, item:%s", list, item)
	monitor.TMQListAppendCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TMQListAppend(list, item)
	logger.Debugf("tmq_list_append finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TMQListAppendFailCounter.Inc()
	} else {
		monitor.TMQListAppendSuccessCounter.Inc()
	}
	return code
}

func TMQGetResType(message unsafe.Pointer, logger *logrus.Entry, isDebug bool) int32 {
	logger.Tracef("call tmq_get_res_type, message:%p", message)
	monitor.TMQGetResTypeCounter.Inc()
	resType := wrapper.TMQGetResType(message)
	logger.Debugf("tmq_get_res_type finish, resType:%d", resType)
	monitor.TMQGetResTypeSuccessCounter.Inc()
	return resType
}

func TMQGetTopicName(message unsafe.Pointer, logger *logrus.Entry, isDebug bool) string {
	logger.Tracef("call tmq_get_topic_name, message:%p", message)
	monitor.TMQGetTopicNameCounter.Inc()
	topicName := wrapper.TMQGetTopicName(message)
	logger.Debugf("tmq_get_topic_name finish, topicName:%s", topicName)
	monitor.TMQGetTopicNameSuccessCounter.Inc()
	return topicName
}

func TMQGetVgroupID(message unsafe.Pointer, logger *logrus.Entry, isDebug bool) int32 {
	logger.Tracef("call tmq_get_vgroup_id, message:%p", message)
	monitor.TMQGetVgroupIDCounter.Inc()
	vgroupID := wrapper.TMQGetVgroupID(message)
	logger.Debugf("tmq_get_vgroup_id finish, vgroupID:%d", vgroupID)
	monitor.TMQGetVgroupIDSuccessCounter.Inc()
	return vgroupID
}

func TMQGetVgroupOffset(message unsafe.Pointer, logger *logrus.Entry, isDebug bool) int64 {
	logger.Tracef("call tmq_get_vgroup_offset, message:%p", message)
	monitor.TMQGetVgroupOffsetCounter.Inc()
	vgroupOffset := wrapper.TMQGetVgroupOffset(message)
	logger.Debugf("tmq_get_vgroup_offset finish, vgroupOffset:%d", vgroupOffset)
	monitor.TMQGetVgroupOffsetSuccessCounter.Inc()
	return vgroupOffset
}

func TMQGetDBName(message unsafe.Pointer, logger *logrus.Entry, isDebug bool) string {
	logger.Tracef("call tmq_get_db_name, message:%p", message)
	monitor.TMQGetDBNameCounter.Inc()
	dbName := wrapper.TMQGetDBName(message)
	logger.Debugf("tmq_get_db_name finish, dbName:%s", dbName)
	monitor.TMQGetDBNameSuccessCounter.Inc()
	return dbName
}

func TMQGetTableName(message unsafe.Pointer, logger *logrus.Entry, isDebug bool) string {
	logger.Tracef("call tmq_get_table_name, message:%p", message)
	monitor.TMQGetTableNameCounter.Inc()
	tableName := wrapper.TMQGetTableName(message)
	logger.Debugf("tmq_get_table_name finish, tableName:%s", tableName)
	monitor.TMQGetTableNameSuccessCounter.Inc()
	return tableName
}

func TMQListGetSize(message unsafe.Pointer, logger *logrus.Entry, isDebug bool) int32 {
	logger.Tracef("call tmq_list_get_size, message:%p", message)
	monitor.TMQListGetSizeCounter.Inc()
	size := wrapper.TMQListGetSize(message)
	logger.Debugf("tmq_list_get_size finish, size:%d", size)
	monitor.TMQListGetSizeSuccessCounter.Inc()
	return size
}

func TMQConfNew(logger *logrus.Entry, isDebug bool) unsafe.Pointer {
	logger.Trace("call tmq_conf_new")
	monitor.TMQConfNewCounter.Inc()
	conf := wrapper.TMQConfNew()
	logger.Debugf("tmq_conf_new finish, conf:%p", conf)
	if conf == nil {
		monitor.TMQConfNewFailCounter.Inc()
	} else {
		monitor.TMQConfNewSuccessCounter.Inc()
	}
	return conf
}

func TMQConfDestroy(conf unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("call tmq_conf_destroy, conf:%p", conf)
	monitor.TMQConfDestroyCounter.Inc()
	s := log.GetLogNow(isDebug)
	wrapper.TMQConfDestroy(conf)
	logger.Debugf("tmq_conf_destroy finish, cost:%s", log.GetLogDuration(isDebug, s))
	monitor.TMQConfDestroySuccessCounter.Inc()
}

func TMQGetConnect(conf unsafe.Pointer, logger *logrus.Entry, isDebug bool) unsafe.Pointer {
	logger.Tracef("call tmq_get_connect, conf:%p", conf)
	monitor.TMQGetConnectCounter.Inc()
	connect := wrapper.TMQGetConnect(conf)
	logger.Debugf("tmq_get_connect finish, connect:%p", connect)
	monitor.TMQGetConnectSuccessCounter.Inc()
	return connect
}

func TMQFreeRaw(raw unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("call tmq_free_raw, raw:%p", raw)
	monitor.TMQFreeRawCounter.Inc()
	s := log.GetLogNow(isDebug)
	wrapper.TMQFreeRaw(raw)
	logger.Debugf("tmq_free_raw finish, cost:%s", log.GetLogDuration(isDebug, s))
	monitor.TMQFreeRawSuccessCounter.Inc()
}

func TMQFreeJsonMeta(jsonMeta unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("call tmq_free_json_meta, jsonMeta:%p", jsonMeta)
	monitor.TMQFreeJsonMetaCounter.Inc()
	s := log.GetLogNow(isDebug)
	wrapper.TMQFreeJsonMeta(jsonMeta)
	logger.Debugf("tmq_free_json_meta finish, cost:%s", log.GetLogDuration(isDebug, s))
	monitor.TMQFreeJsonMetaSuccessCounter.Inc()
}

func TaosStmtUseResult(stmt unsafe.Pointer, logger *logrus.Entry, isDebug bool) unsafe.Pointer {
	logger.Tracef("call taos_stmt_use_result, stmt:%p", stmt)
	monitor.TaosStmtUseResultCounter.Inc()
	s := log.GetLogNow(isDebug)
	res := wrapper.TaosStmtUseResult(stmt)
	logger.Debugf("taos_stmt_use_result finish, res:%p, cost:%s", res, log.GetLogDuration(isDebug, s))
	if res == nil {
		monitor.TaosStmtUseResultFailCounter.Inc()
	} else {
		monitor.TaosStmtUseResultSuccessCounter.Inc()
	}
	return res
}

func TaosSetConnMode(conn unsafe.Pointer, mode int, value int, logger *logrus.Entry, isDebug bool) int {
	logger.Tracef("call taos_set_conn_mode, conn:%p, mode:%d", conn, mode)
	monitor.TaosSetConnModeCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosSetConnMode(conn, mode, value)
	logger.Debugf("taos_set_conn_mode finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosSetConnModeFailCounter.Inc()
	} else {
		monitor.TaosSetConnModeSuccessCounter.Inc()
	}
	return code
}

func TaosOptions(option int, value string, logger *logrus.Entry, isDebug bool) int {
	logger.Trace("sync semaphore acquire for taos_options")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_options")
	}()

	logger.Debugf("call taos_options, option:%d, value:%s", option, value)
	monitor.TaosOptionsCounter.Inc()
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosOptions(option, value)
	logger.Debugf("taos_options finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TaosOptionsFailCounter.Inc()
	} else {
		monitor.TaosOptionsSuccessCounter.Inc()
	}
	return code
}

func TaosResetCurrentDB(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	logger.Trace("sync semaphore acquire for taos_reset_current_db")
	logger.Debugf("call taos_reset_current_db, conn:%p", conn)
	monitor.TaosResetCurrentDBCounter.Inc()
	s := log.GetLogNow(isDebug)
	wrapper.TaosResetCurrentDB(conn)
	logger.Debugf("taos_reset_current_db finish, cost:%s", log.GetLogDuration(isDebug, s))
	monitor.TaosResetCurrentDBSuccessCounter.Inc()
}

func TaosFetchWhitelistA(conn unsafe.Pointer, caller cgo.Handle, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("call taos_fetch_ip_whitelist_a, conn:%p", conn)
	monitor.TaosFetchWhitelistACounter.Inc()
	s := log.GetLogNow(false)
	wrapper.TaosFetchIPWhitelistA(conn, caller)
	logger.Debugf("taos_fetch_ip_whitelist_a finish, cost:%s", log.GetLogDuration(false, s))
	monitor.TaosFetchWhitelistASuccessCounter.Inc()
}

// TaosRegisterInstance register an instance to TDengine server.
// This function should not be blocked by semaphore.
func TaosRegisterInstance(id, registerType, desc string, expire int32, logger *logrus.Entry, isDebug bool) int32 {
	logger.Debugf("call taos_register_instance, id:%s, type:%s, desc:%s, expire:%d", id, registerType, desc, expire)
	s := log.GetLogNow(isDebug)
	code := wrapper.TaosRegisterInstance(id, registerType, desc, expire)
	logger.Debugf("taos_register_instance finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))

	return code
}

func TaosConnectTOTP(host, user, pass, totp, db string, port int, logger *logrus.Entry, isDebug bool) (unsafe.Pointer, error) {
	logger.Trace("sync semaphore acquire for taos_connect_totp")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_connect_totp")
	}()
	logger.Debugf("call taos_connect_totp, host:%s, user:%s, db:%s, port:%d", host, user, db, port)
	monitor.TaosConnectTOTPCounter.Inc()
	s := log.GetLogNow(isDebug)
	conn, err := wrapper.TaosConnectTOTP(host, user, pass, totp, db, port)
	logger.Debugf("taos_connect finish, conn:%p, err:%v, cost:%s", conn, err, log.GetLogDuration(isDebug, s))
	if err != nil {
		monitor.TaosConnectTOTPFailCounter.Inc()
	} else {
		monitor.TaosConnectTOTPSuccessCounter.Inc()
	}
	return conn, err
}

func TaosConnectToken(host, token, db string, port int, logger *logrus.Entry, isDebug bool) (unsafe.Pointer, error) {
	logger.Trace("sync semaphore acquire for taos_connect_token")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_connect_token")
	}()
	logger.Debugf("call taos_connect_token, host:%s, db:%s, port:%d", host, db, port)
	monitor.TaosConnectTokenCounter.Inc()
	s := log.GetLogNow(isDebug)
	conn, err := wrapper.TaosConnectToken(host, token, db, port)
	logger.Debugf("taos_connect_token finish, conn:%p, err:%v, cost:%s", conn, err, log.GetLogDuration(isDebug, s))
	if err != nil {
		monitor.TaosConnectTokenFailCounter.Inc()
	} else {
		monitor.TaosConnectTokenSuccessCounter.Inc()
	}
	return conn, err
}

func TaosGetConnectionUserName(conn unsafe.Pointer, logger *logrus.Entry, isDebug bool) (string, int) {
	return TaosGetConnectionInfo(conn, common.TSDB_CONNECTION_INFO_USER, logger, isDebug)
}

func TaosGetConnectionInfo(conn unsafe.Pointer, infoType int, logger *logrus.Entry, isDebug bool) (string, int) {
	logger.Trace("sync semaphore acquire for taos_get_connection_info")
	thread.SyncSemaphore.Acquire()
	defer func() {
		thread.SyncSemaphore.Release()
		logger.Trace("sync semaphore release for taos_get_connection_info")
	}()
	logger.Debugf("call taos_get_connection_info, conn:%p, infoType:%d", conn, infoType)
	monitor.TaosGetConnectionInfoCounter.Inc()
	s := log.GetLogNow(isDebug)
	info, codeCode := wrapper.TaosGetConnectionInfo(conn, infoType)
	logger.Debugf("taos_get_connection_info finish, info:%s, cost:%s", info, log.GetLogDuration(isDebug, s))
	if codeCode != 0 {
		monitor.TaosGetConnectInfoFailCounter.Inc()
	} else {
		monitor.TaosGetConnectionInfoSuccessCounter.Inc()
	}
	return info, codeCode
}
