package async

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools/generator"
)

var ErrFetchRowError = errors.New("fetch row error")
var GlobalAsync *Async

type Async struct {
	HandlerPool *HandlerPool
}

func NewAsync(handlerPool *HandlerPool) *Async {
	return &Async{HandlerPool: handlerPool}
}

func (a *Async) TaosExec(taosConnect unsafe.Pointer, logger *logrus.Entry, isDebug bool, sql string, timeFormat wrapper.FormatTimeFunc, reqID int64) (*ExecResult, error) {
	handler := a.HandlerPool.Get()
	defer a.HandlerPool.Put(handler)
	result := a.TaosQuery(taosConnect, logger, isDebug, sql, handler, reqID)
	defer func() {
		if result != nil && result.Res != nil {
			FreeResultAsync(result.Res, logger, isDebug)
		}
	}()
	res := result.Res
	code := syncinterface.TaosError(res, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosErrorStr(res, logger, isDebug)
		logger.Tracef("taos query error, code:%d, msg:%s", code, errStr)
		return nil, tErrors.NewError(code, errStr)
	}
	logger.Tracef("get query result, res:%p", res)
	isUpdate := syncinterface.TaosIsUpdateQuery(res, logger, isDebug)
	logger.Tracef("get isUpdate:%t", isUpdate)
	execResult := &ExecResult{}
	if isUpdate {
		affectRows := syncinterface.TaosAffectedRows(res, logger, isDebug)
		logger.Tracef("get affectRows:%d", affectRows)
		execResult.AffectedRows = affectRows
		return execResult, nil
	}
	fieldsCount := syncinterface.TaosNumFields(res, logger, isDebug)
	logger.Tracef("get fieldsCount:%d", fieldsCount)
	execResult.FieldCount = fieldsCount
	rowsHeader, err := syncinterface.ReadColumn(res, fieldsCount, logger, isDebug)
	if err != nil {
		logger.Errorf("read column error, error:%s", err)
		return nil, err
	}
	execResult.Header = rowsHeader
	precision := syncinterface.TaosResultPrecision(res, logger, isDebug)
	logger.Tracef("get precision:%d", precision)
	for {
		result = a.TaosFetchRowsA(res, logger, isDebug, handler)
		logger.Tracef("get fetch result, rows:%d", result.N)
		if result.N == 0 {
			logger.Trace("fetch finished")
			return execResult, nil
		}
		res = result.Res
		for i := 0; i < result.N; i++ {
			row := taosFetchRow(res, logger, isDebug)
			lengths := syncinterface.TaosFetchLengths(res, len(rowsHeader.ColNames), logger, isDebug)
			logger.Tracef("fetch lengths:%d", lengths)
			values := make([]driver.Value, len(rowsHeader.ColNames))
			for j := range rowsHeader.ColTypes {
				if row == nil {
					logger.Error("fetch row error, row is nil")
					return nil, ErrFetchRowError
				}
				v := wrapper.FetchRow(row, j, rowsHeader.ColTypes[j], lengths[j], precision, timeFormat)
				if vv, is := v.([]byte); is {
					v = json.RawMessage(vv)
				}
				values[j] = v
			}
			logger.Tracef("get data, %v", values)
			execResult.Data = append(execResult.Data, values)
		}
	}
}

func taosFetchRow(result unsafe.Pointer, logger *logrus.Entry, isDebug bool) unsafe.Pointer {
	logger.Tracef("async semaphore acquire for taos_fetch_row")
	thread.AsyncSemaphore.Acquire()
	defer func() {
		thread.AsyncSemaphore.Release()
		logger.Trace("async semaphore release for taos_fetch_row")
	}()
	logger.Debugf("call taos_fetch_row, result:%p", result)
	monitor.TaosFetchRowCounter.Inc()
	s := log.GetLogNow(isDebug)
	row := wrapper.TaosFetchRow(result)
	logger.Debugf("taos_fetch_row finish, row:%p, cost:%s", row, log.GetLogDuration(isDebug, s))
	monitor.TaosFetchRowSuccessCounter.Inc()
	return row
}

func (a *Async) TaosQuery(taosConnect unsafe.Pointer, logger *logrus.Entry, isDebug bool, sql string, handler *Handler, reqID int64) *Result {
	if reqID == 0 {
		reqID = generator.GetReqID()
		logger.Debugf("reqID is 0, generate a new one:0x%x", reqID)
		logger = logger.WithField(config.ReqIDKey, reqID)
	}
	taosQueryAWithReqID(taosConnect, sql, handler.Handler, reqID, logger, isDebug)
	logger.Debugf("wait for query result")
	monitor.TaosQueryAWithReqIDCallBackCounter.Inc()
	s := log.GetLogNow(isDebug)
	r := <-handler.Caller.QueryResult
	logger.Debugf("get query result, res:%p, n:%d, cost:%s", r.Res, r.N, log.GetLogDuration(isDebug, s))
	if r.N != 0 {
		monitor.TaosQueryAWithReqIDCallBackFailCounter.Inc()
	} else {
		monitor.TaosQueryAWithReqIDCallBackSuccessCounter.Inc()
	}
	return r
}

func taosQueryAWithReqID(taosConn unsafe.Pointer, sql string, handler cgo.Handle, reqID int64, logger *logrus.Entry, isDebug bool) {
	logger.Trace("async semaphore acquire for taos_query_a")
	thread.AsyncSemaphore.Acquire()
	defer func() {
		thread.AsyncSemaphore.Release()
		logger.Trace("async semaphore release for taos_query_a")
	}()
	logger.Debugf("call taos_query_a, conn:%p, QID:0x%x, sql:%s", taosConn, reqID, log.GetLogSql(sql))
	monitor.TaosQueryAWithReqIDCounter.Inc()
	s := log.GetLogNow(isDebug)
	wrapper.TaosQueryAWithReqID(taosConn, sql, handler, reqID)
	logger.Debugf("taos_query_a finish, cost:%s", log.GetLogDuration(isDebug, s))
	monitor.TaosQueryAWithReqIDSuccessCounter.Inc()
}

func (a *Async) TaosFetchRowsA(res unsafe.Pointer, logger *logrus.Entry, isDebug bool, handler *Handler) *Result {
	taosFetchRowsA(res, handler.Handler, logger, isDebug)
	logger.Debug("wait for fetch rows result")
	s := log.GetLogNow(isDebug)
	monitor.TaosFetchRowsACallBackCounter.Inc()
	r := <-handler.Caller.FetchResult
	logger.Debugf("get fetch rows result finish, res:%p, n:%d, cost:%s", r.Res, r.N, log.GetLogDuration(isDebug, s))
	if r.N >= 0 {
		monitor.TaosFetchRowsACallBackSuccessCounter.Inc()
	} else {
		monitor.TaosFetchRowsACallBackFailCounter.Inc()
	}
	return r
}

func taosFetchRowsA(res unsafe.Pointer, handler cgo.Handle, logger *logrus.Entry, isDebug bool) {
	logger.Trace("async semaphore acquire for taos_fetch_rows_a")
	thread.AsyncSemaphore.Acquire()
	defer func() {
		thread.AsyncSemaphore.Release()
		logger.Trace("async semaphore release for taos_fetch_rows_a")
	}()
	logger.Debugf("call taos_fetch_rows_a, res:%p", res)
	monitor.TaosFetchRowsACounter.Inc()
	s := log.GetLogNow(isDebug)
	wrapper.TaosFetchRowsA(res, handler)
	logger.Debugf("taos_fetch_rows_a finish, cost:%s", log.GetLogDuration(isDebug, s))
	monitor.TaosFetchRowsASuccessCounter.Inc()
}

func (a *Async) TaosFetchRawBlockA(res unsafe.Pointer, logger *logrus.Entry, isDebug bool, handler *Handler) *Result {
	taosFetchRawBlockA(res, handler.Handler, logger, isDebug)
	logger.Debug("wait for fetch raw block result")
	monitor.TaosFetchRawBlockACallBackCounter.Inc()
	s := log.GetLogNow(isDebug)
	r := <-handler.Caller.FetchResult
	logger.Debugf("get fetch raw block result, res:%p, n:%d, cost:%s", r.Res, r.N, log.GetLogDuration(isDebug, s))
	if r.N >= 0 {
		monitor.TaosFetchRawBlockACallBackSuccessCounter.Inc()
	} else {
		monitor.TaosFetchRawBlockACallBackFailCounter.Inc()
	}
	return r
}

func taosFetchRawBlockA(res unsafe.Pointer, handler cgo.Handle, logger *logrus.Entry, isDebug bool) {
	logger.Trace("async semaphore acquire for taos_fetch_raw_block_a")
	thread.AsyncSemaphore.Acquire()
	defer func() {
		thread.AsyncSemaphore.Release()
		logger.Trace("async semaphore release for taos_fetch_raw_block_a")
	}()
	logger.Debugf("call taos_fetch_raw_block_a, res:%p", res)
	monitor.TaosFetchRawBlockACounter.Inc()
	s := log.GetLogNow(isDebug)
	wrapper.TaosFetchRawBlockA(res, handler)
	logger.Debugf("taos_fetch_raw_block_a finish, cost:%s", log.GetLogDuration(isDebug, s))
	monitor.TaosFetchRawBlockASuccessCounter.Inc()
}

type ExecResult struct {
	AffectedRows int
	FieldCount   int
	Header       *wrapper.RowsHeader
	Data         [][]driver.Value
}

func (a *Async) TaosExecWithoutResult(taosConnect unsafe.Pointer, logger *logrus.Entry, isDebug bool, sql string, reqID int64) error {
	_, err := a.doExec(taosConnect, logger, isDebug, sql, reqID, false)
	return err
}

func (a *Async) TaosExecWithAffectedRows(taosConnect unsafe.Pointer, logger *logrus.Entry, isDebug bool, sql string, reqID int64) (int, error) {
	return a.doExec(taosConnect, logger, isDebug, sql, reqID, true)
}

func (a *Async) doExec(taosConnect unsafe.Pointer, logger *logrus.Entry, isDebug bool, sql string, reqID int64, getAffectedRows bool) (int, error) {
	logger.Trace("get handler from pool")
	handler := a.HandlerPool.Get()
	defer func() {
		a.HandlerPool.Put(handler)
		logger.Trace("put handler back to pool")
	}()
	result := a.TaosQuery(taosConnect, logger, isDebug, sql, handler, reqID)
	defer func() {
		if result != nil && result.Res != nil {
			FreeResultAsync(result.Res, logger, isDebug)
		}
	}()
	res := result.Res
	code := syncinterface.TaosError(res, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosErrorStr(res, logger, isDebug)
		logger.Errorf("taos query error, code:%d, msg:%s", code, errStr)
		return 0, tErrors.NewError(code, errStr)
	}
	if getAffectedRows {
		affectRows := syncinterface.TaosAffectedRows(res, logger, isDebug)
		logger.Tracef("get affectRows:%d", affectRows)
		return affectRows, nil
	}
	return 0, nil
}

func FreeResultAsync(res unsafe.Pointer, logger *logrus.Entry, isDebug bool) {
	if res == nil {
		logger.Trace("async free result, result is nil")
		return
	}
	logger.Trace("async semaphore acquire for taos_free_result")
	thread.AsyncSemaphore.Acquire()
	defer func() {
		thread.AsyncSemaphore.Release()
		logger.Trace("async semaphore release for taos_free_result")
	}()
	logger.Debugf("call taos_free_result async, res:%p", res)
	monitor.TaosAsyncQueryFreeResultCounter.Inc()
	s := log.GetLogNow(isDebug)
	wrapper.TaosFreeResult(res)
	logger.Debugf("taos_free_result finish, cost:%s", log.GetLogDuration(isDebug, s))
	monitor.TaosAsyncQueryFreeResultSuccessCounter.Inc()
}

func init() {
	GlobalAsync = NewAsync(NewHandlerPool(10000))
}
