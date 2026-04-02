package ws

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/common/stmt"
	errors2 "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor/recordsql"
	"github.com/taosdata/taosadapter/v3/tools/jsontype"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

type stmt2InitRequest struct {
	ReqID               uint64 `json:"req_id"`
	SingleStbInsert     bool   `json:"single_stb_insert"`
	SingleTableBindOnce bool   `json:"single_table_bind_once"`
}

type stmt2InitResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (h *messageHandler) stmt2Init(ctx context.Context, session *melody.Session, action string, req stmt2InitRequest, innerReqID uint64, logger *logrus.Entry, isDebug bool) {
	handle, caller := async.GlobalStmt2CallBackCallerPool.Get()
	stmtInit := syncinterface.TaosStmt2Init(h.conn, int64(innerReqID), req.SingleStbInsert, req.SingleTableBindOnce, handle, logger, isDebug)
	if stmtInit == nil {
		async.GlobalStmt2CallBackCallerPool.Put(handle)
		errStr := syncinterface.TaosStmt2Error(stmtInit, logger, isDebug)
		logger.Errorf("stmt2 init error, err:%s", errStr)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, errStr)
		return
	}
	stmtItem := &StmtItem{stmt: stmtInit, handler: handle, caller: caller, isStmt2: true}
	h.stmts.Add(stmtItem)
	logger.Tracef("stmt2 init sucess, stmt_id:%d, stmt pointer:%p", stmtItem.index, stmtInit)
	resp := &stmt2InitResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: stmtItem.index,
	}
	wstool.WSWriteJson(session, logger, resp)
}

func (h *messageHandler) stmt2ValidateAndLock(ctx context.Context, session *melody.Session, action string, reqID uint64, stmtID uint64, logger *logrus.Entry, isDebug bool) (stmtItem *StmtItem, locked bool) {
	stmtItem = h.stmts.GetStmt2(stmtID)
	if stmtItem == nil {
		logger.Errorf("stmt2 is nil, stmt_id:%d", stmtID)
		stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, "stmt2 is nil", stmtID)
		return nil, false
	}
	s := log.GetLogNow(isDebug)
	logger.Trace("get stmt2 lock")
	stmtItem.Lock()
	logger.Debugf("get stmt2 lock cost:%s", log.GetLogDuration(isDebug, s))
	if stmtItem.stmt == nil {
		stmtItem.Unlock()
		logger.Errorf("stmt2 has been freed, stmt_id:%d", stmtID)
		stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, "stmt has been freed", stmtID)
		return nil, false
	}
	return stmtItem, true
}

type stmt2PrepareRequest struct {
	ReqID     uint64 `json:"req_id"`
	StmtID    uint64 `json:"stmt_id"`
	SQL       string `json:"sql"`
	GetFields bool   `json:"get_fields"`
}

type stmt2PrepareResponse struct {
	Code        int                   `json:"code"`
	Message     string                `json:"message"`
	Action      string                `json:"action"`
	ReqID       uint64                `json:"req_id"`
	Timing      int64                 `json:"timing"`
	StmtID      uint64                `json:"stmt_id"`
	IsInsert    bool                  `json:"is_insert"`
	Fields      []*stmt.Stmt2AllField `json:"fields"`
	FieldsCount int                   `json:"fields_count"`
}

func (h *messageHandler) stmt2Prepare(ctx context.Context, session *melody.Session, action string, req stmt2PrepareRequest, logger *logrus.Entry, isDebug bool) {
	logger.Debugf("stmt2 prepare, stmt_id:%d, sql:%s", req.StmtID, req.SQL)
	stmtItem, locked := h.stmt2ValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	stmt2 := stmtItem.stmt
	record, recordStmt := recordsql.GetStmtRecord()
	if recordStmt {
		defer func() {
			recordsql.PutStmtRecord(record)
		}()
		record.InitPrepare(uintptr(stmt2), h.ipStr, h.port, h.appName, h.user, recordsql.WSType, req.ReqID, time.Now(), req.SQL)
	}
	code := syncinterface.TaosStmt2Prepare(stmt2, req.SQL, logger, isDebug)
	if recordStmt {
		record.SetPrepareEnd(code)
	}
	if code != 0 {
		errStr := syncinterface.TaosStmt2Error(stmt2, logger, isDebug)
		logger.Errorf("stmt2 prepare error, err:%s", errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	logger.Tracef("stmt2 prepare success, stmt_id:%d", req.StmtID)
	isInsert, code := syncinterface.TaosStmt2IsInsert(stmt2, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmt2Error(stmt2, logger, isDebug)
		logger.Errorf("check stmt2 is insert error, err:%s", errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	logger.Tracef("stmt2 is insert:%t", isInsert)
	stmtItem.isInsert = isInsert
	prepareResp := &stmt2PrepareResponse{StmtID: req.StmtID, IsInsert: isInsert}
	if req.GetFields {
		code, count, fields := syncinterface.TaosStmt2GetFields(stmt2, logger, isDebug)
		if code != 0 {
			errStr := syncinterface.TaosStmt2Error(stmt2, logger, isDebug)
			logger.Errorf("stmt2 get fields error, code:%d, err:%s", code, errStr)
			stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
			return
		}
		defer syncinterface.TaosStmt2FreeFields(stmt2, fields, logger, isDebug)
		stbFields := wrapper.Stmt2ParseAllFields(count, fields)
		prepareResp.Fields = stbFields
		prepareResp.FieldsCount = count

	}
	prepareResp.ReqID = req.ReqID
	prepareResp.Action = action
	prepareResp.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, logger, prepareResp)
}

type stmt2ExecRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type stmt2ExecResponse struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	Action   string `json:"action"`
	ReqID    uint64 `json:"req_id"`
	Timing   int64  `json:"timing"`
	StmtID   uint64 `json:"stmt_id"`
	Affected int    `json:"affected"`
}

func (h *messageHandler) stmt2Exec(ctx context.Context, session *melody.Session, action string, req stmt2ExecRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("stmt2 execute, stmt_id:%d", req.StmtID)
	stmtItem, locked := h.stmt2ValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	record, recordStmt := recordsql.GetStmtRecord()
	if recordStmt {
		defer func() {
			recordsql.PutStmtRecord(record)
		}()
		record.InitExecute(uintptr(stmtItem.stmt), h.ipStr, h.port, h.appName, h.user, recordsql.WSType, req.ReqID, time.Now())
	}
	code := syncinterface.TaosStmt2Exec(stmtItem.stmt, logger, isDebug)
	if code != 0 {
		if recordStmt {
			record.SetExecuteEnd(code, 0)
		}
		errStr := syncinterface.TaosStmt2Error(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt2 execute error,code:%d, err:%s", code, errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	s := log.GetLogNow(isDebug)
	logger.Tracef("stmt2 execute wait callback, stmt_id:%d", req.StmtID)
	result := <-stmtItem.caller.ExecResult
	logger.Debugf("stmt2 execute wait callback finish, affected:%d, res:%p, n:%d, cost:%s", result.Affected, result.Res, result.N, log.GetLogDuration(isDebug, s))
	if result.N < 0 {
		if recordStmt {
			record.SetExecuteEnd(result.N, 0)
		}
		errStr := syncinterface.TaosStmt2Error(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt2 execute callback error, code:%d, err:%s", result.N, errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, result.N, errStr, req.StmtID)
		return
	}
	if recordStmt {
		record.SetExecuteEnd(result.N, result.Affected)
	}
	resp := &stmt2ExecResponse{
		Action:   action,
		ReqID:    req.ReqID,
		Timing:   wstool.GetDuration(ctx),
		StmtID:   req.StmtID,
		Affected: result.Affected,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmt2UseResultRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type stmt2UseResultResponse struct {
	Code             int                `json:"code"`
	Message          string             `json:"message"`
	Action           string             `json:"action"`
	ReqID            uint64             `json:"req_id"`
	Timing           int64              `json:"timing"`
	StmtID           uint64             `json:"stmt_id"`
	ID               uint64             `json:"id"`
	FieldsCount      int                `json:"fields_count"`
	FieldsNames      []string           `json:"fields_names"`
	FieldsTypes      jsontype.JsonUint8 `json:"fields_types"`
	FieldsLengths    []int64            `json:"fields_lengths"`
	Precision        int                `json:"precision"`
	FieldsPrecisions []int64            `json:"fields_precisions"`
	FieldsScales     []int64            `json:"fields_scales"`
}

func (h *messageHandler) stmt2UseResult(ctx context.Context, session *melody.Session, action string, req stmt2UseResultRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("stmt2 use result, stmt_id:%d", req.StmtID)
	stmtItem, locked := h.stmt2ValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	if stmtItem.isInsert {
		logger.Errorf("stmt2 use result error, stmt is insert, stmt_id:%d", req.StmtID)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, "stmt is insert, no result set", req.StmtID)
		return
	}
	result := syncinterface.TaosStmt2Result(stmtItem.stmt, logger, isDebug)
	if result == nil {
		logger.Errorf("stmt2 use result error, result is nil, stmt_id:%d", req.StmtID)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, "result is nil", req.StmtID)
		return
	}
	fieldsCount := syncinterface.TaosNumFields(result, logger, isDebug)
	rowsHeader, _ := syncinterface.ReadColumn(result, fieldsCount, logger, isDebug)
	precision := syncinterface.TaosResultPrecision(result, logger, isDebug)
	logger.Tracef("stmt use result success, stmt_id:%d, fields_count:%d, precision:%d", req.StmtID, fieldsCount, precision)
	queryResult := NewStmt2Result(result, fieldsCount, rowsHeader, precision)
	idx := h.queryResults.Add(queryResult)
	resp := &stmt2UseResultResponse{
		Action:           action,
		ReqID:            req.ReqID,
		Timing:           wstool.GetDuration(ctx),
		StmtID:           req.StmtID,
		ID:               idx,
		FieldsCount:      fieldsCount,
		FieldsNames:      rowsHeader.ColNames,
		FieldsTypes:      rowsHeader.ColTypes,
		FieldsLengths:    rowsHeader.ColLength,
		Precision:        precision,
		FieldsPrecisions: rowsHeader.Precisions,
		FieldsScales:     rowsHeader.Scales,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmt2CloseRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type stmt2CloseResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (h *messageHandler) stmt2Close(ctx context.Context, session *melody.Session, action string, req stmt2CloseRequest, logger *logrus.Entry) {
	logger.Tracef("stmt2 close, stmt_id:%d", req.StmtID)
	err := h.stmts.FreeStmtByID(req.StmtID, true, logger)
	if err != nil {
		logger.Errorf("stmt2 close error, err:%s", err.Error())
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, err.Error(), req.StmtID)
		return
	}
	logger.Tracef("stmt2 close success, stmt_id:%d", req.StmtID)
	resp := &stmt2CloseResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: req.StmtID,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmt2BindResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (h *messageHandler) stmt2BinaryBind(ctx context.Context, session *melody.Session, action string, reqID uint64, stmtID uint64, message []byte, logger *logrus.Entry, isDebug bool) {
	if len(message) < 30 {
		logger.Errorf("message length is too short, len:%d, stmt_id:%d", len(message), stmtID)
		stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, "message length is too short", stmtID)
		return
	}
	v := binary.LittleEndian.Uint16(message[24:])
	if v != Stmt2BindProtocolVersion1 {
		logger.Errorf("unknown stmt2 bind version, version:%d, stmt_id:%d", v, stmtID)
		stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, "unknown stmt2 bind version", stmtID)
		return
	}
	colIndex := int32(binary.LittleEndian.Uint32(message[26:]))
	stmtItem, locked := h.stmt2ValidateAndLock(ctx, session, action, reqID, stmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	bindData := message[30:]
	record, recordStmt := recordsql.GetStmtRecord()
	if recordStmt {
		defer func() {
			recordsql.PutStmtRecord(record)
		}()
		record.InitBind(uintptr(stmtItem.stmt), h.ipStr, h.port, h.appName, h.user, recordsql.WSType, reqID, time.Now(), bindData)
	}
	err := syncinterface.TaosStmt2BindBinary(stmtItem.stmt, bindData, colIndex, logger, isDebug)
	if err != nil {
		logger.Errorf("stmt2 bind error, err:%s", err.Error())
		var tError *errors2.TaosError
		if errors.As(err, &tError) {
			if recordStmt {
				record.SetBindEnd(int(tError.Code))
			}
			stmtErrorResponse(ctx, session, logger, action, reqID, int(tError.Code), tError.ErrStr, stmtID)
			return
		}
		if recordStmt {
			record.SetBindEnd(0xffff)
		}
		stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, err.Error(), stmtID)
		return
	}
	if recordStmt {
		record.SetBindEnd(0)
	}
	logger.Trace("stmt2 bind success")
	resp := &stmt2BindResponse{
		Action: action,
		ReqID:  reqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: stmtID,
	}
	wstool.WSWriteJson(session, logger, resp)
}
