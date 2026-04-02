package ws

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/controller/ws/stmt"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	stmtCommon "github.com/taosdata/taosadapter/v3/driver/common/stmt"
	errors2 "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/types"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/jsontype"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

type stmtInitRequest struct {
	ReqID uint64 `json:"req_id"`
}

type stmtInitResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (h *messageHandler) stmtInit(ctx context.Context, session *melody.Session, action string, req stmtInitRequest, innerReqID uint64, logger *logrus.Entry, isDebug bool) {
	stmtInit := syncinterface.TaosStmtInitWithReqID(h.conn, int64(innerReqID), logger, isDebug)
	if stmtInit == nil {
		errStr := syncinterface.TaosStmtErrStr(stmtInit, logger, isDebug)
		logger.Errorf("stmt init error, err:%s", errStr)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, errStr)
		return
	}
	stmtItem := &StmtItem{stmt: stmtInit}
	h.stmts.Add(stmtItem)
	logger.Tracef("stmt init sucess, stmt_id:%d, stmt pointer:%p", stmtItem.index, stmtInit)
	resp := &stmtInitResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: stmtItem.index,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmtPrepareRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
	SQL    string `json:"sql"`
}

type stmtPrepareResponse struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	Action   string `json:"action"`
	ReqID    uint64 `json:"req_id"`
	Timing   int64  `json:"timing"`
	StmtID   uint64 `json:"stmt_id"`
	IsInsert bool   `json:"is_insert"`
}

func (h *messageHandler) stmtValidateAndLock(ctx context.Context, session *melody.Session, action string, reqID uint64, stmtID uint64, logger *logrus.Entry, isDebug bool) (stmtItem *StmtItem, locked bool) {
	stmtItem = h.stmts.Get(stmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", stmtID)
		stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, "stmt is nil", stmtID)
		return nil, false
	}
	s := log.GetLogNow(isDebug)
	logger.Trace("get stmt lock")
	stmtItem.Lock()
	logger.Debugf("get stmt lock cost:%s", log.GetLogDuration(isDebug, s))
	if stmtItem.stmt == nil {
		stmtItem.Unlock()
		logger.Errorf("stmt has been freed, stmt_id:%d", stmtID)
		stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, "stmt has been freed", stmtID)
		return nil, false
	}
	return stmtItem, true
}

func (h *messageHandler) stmtPrepare(ctx context.Context, session *melody.Session, action string, req stmtPrepareRequest, logger *logrus.Entry, isDebug bool) {
	logger.Debugf("stmt prepare, stmt_id:%d, sql:%s", req.StmtID, req.SQL)
	stmtItem, locked := h.stmtValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	code := syncinterface.TaosStmtPrepare(stmtItem.stmt, req.SQL, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt prepare error, code:%d, err:%s", code, errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	logger.Tracef("stmt prepare success, stmt_id:%d", req.StmtID)
	isInsert, code := syncinterface.TaosStmtIsInsert(stmtItem.stmt, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("check stmt is insert error, code:%d, err:%s", code, errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	logger.Tracef("stmt is insert:%t", isInsert)
	stmtItem.isInsert = isInsert
	resp := &stmtPrepareResponse{
		Action:   action,
		ReqID:    req.ReqID,
		Timing:   wstool.GetDuration(ctx),
		StmtID:   stmtItem.index,
		IsInsert: isInsert,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmtSetTableNameRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
	Name   string `json:"name"`
}

type stmtSetTableNameResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (h *messageHandler) stmtSetTableName(ctx context.Context, session *melody.Session, action string, req stmtSetTableNameRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("stmt set table name, stmt_id:%d, name:%s", req.StmtID, req.Name)
	stmtItem, locked := h.stmtValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	code := syncinterface.TaosStmtSetTBName(stmtItem.stmt, req.Name, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt set table name error, err:%s", errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	logger.Tracef("stmt set table name success, stmt_id:%d", req.StmtID)
	resp := &stmtSetTableNameResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: req.StmtID,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmtSetTagsRequest struct {
	ReqID  uint64          `json:"req_id"`
	StmtID uint64          `json:"stmt_id"`
	Tags   json.RawMessage `json:"tags"`
}

type stmtSetTagsResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (h *messageHandler) stmtSetTags(ctx context.Context, session *melody.Session, action string, req stmtSetTagsRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("stmt set tags, stmt_id:%d, tags:%s", req.StmtID, req.Tags)
	stmtItem, locked := h.stmtValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	code, tagNums, tagFields := syncinterface.TaosStmtGetTagFields(stmtItem.stmt, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt get tag fields error, err:%s", errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	defer func() {
		syncinterface.TaosStmtReclaimFields(stmtItem.stmt, tagFields, logger, isDebug)
	}()
	logger.Tracef("stmt tag nums:%d", tagNums)
	if tagNums == 0 {
		logger.Trace("no tags")
		resp := &stmtSetTagsResponse{
			Action: action,
			ReqID:  req.ReqID,
			Timing: wstool.GetDuration(ctx),
			StmtID: req.StmtID,
		}
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	s := log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	data, err := stmt.StmtParseTag(req.Tags, fields)
	logger.Debugf("stmt parse tag json cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("stmt parse tag json error, err:%s", err.Error())
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, fmt.Sprintf("stmt parse tag json:%s", err.Error()), req.StmtID)
		return
	}
	code = syncinterface.TaosStmtSetTags(stmtItem.stmt, data, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt set tags error, code:%d, err:%s", code, errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	logger.Trace("stmt set tags success")
	resp := &stmtSetTagsResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: req.StmtID,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmtBindRequest struct {
	ReqID   uint64          `json:"req_id"`
	StmtID  uint64          `json:"stmt_id"`
	Columns json.RawMessage `json:"columns"`
}

type stmtBindResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (h *messageHandler) stmtBind(ctx context.Context, session *melody.Session, action string, req stmtBindRequest, logger *logrus.Entry, isDebug bool) {
	stmtItem, locked := h.stmtValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	code, colNums, colFields := syncinterface.TaosStmtGetColFields(stmtItem.stmt, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt get col fields error,code:%d, err:%s", code, errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	defer func() {
		syncinterface.TaosStmtReclaimFields(stmtItem.stmt, colFields, logger, isDebug)
	}()
	if colNums == 0 {
		logger.Trace("no columns")
		resp := &stmtBindResponse{
			Action: action,
			ReqID:  req.ReqID,
			Timing: wstool.GetDuration(ctx),
			StmtID: req.StmtID,
		}
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	s := log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(colNums, colFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	fieldTypes := make([]*types.ColumnType, colNums)

	var err error
	for i := 0; i < colNums; i++ {
		if fieldTypes[i], err = fields[i].GetType(); err != nil {
			logger.Errorf("stmt get column type error, err:%s", err.Error())
			stmtErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, fmt.Sprintf("stmt get column type error, err:%s", err.Error()), req.StmtID)
			return
		}
	}
	s = log.GetLogNow(isDebug)
	data, err := stmt.StmtParseColumn(req.Columns, fields, fieldTypes)
	logger.Debugf("stmt parse column json cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("stmt parse column json error, err:%s", err.Error())
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, fmt.Sprintf("stmt parse column json:%s", err.Error()), req.StmtID)
		return
	}
	code = syncinterface.TaosStmtBindParamBatch(stmtItem.stmt, data, fieldTypes, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt bind param error, err:%s", errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	logger.Trace("stmt bind success")
	resp := &stmtBindResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: req.StmtID,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmtAddBatchRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type stmtAddBatchResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (h *messageHandler) stmtAddBatch(ctx context.Context, session *melody.Session, action string, req stmtAddBatchRequest, logger *logrus.Entry, isDebug bool) {
	stmtItem, locked := h.stmtValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	code := syncinterface.TaosStmtAddBatch(stmtItem.stmt, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt add batch error, code:%d, err:%s", code, errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	logger.Trace("stmt add batch success")
	resp := &stmtAddBatchResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: req.StmtID,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmtExecRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type stmtExecResponse struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	Action   string `json:"action"`
	ReqID    uint64 `json:"req_id"`
	Timing   int64  `json:"timing"`
	StmtID   uint64 `json:"stmt_id"`
	Affected int    `json:"affected"`
}

func (h *messageHandler) stmtExec(ctx context.Context, session *melody.Session, action string, req stmtExecRequest, logger *logrus.Entry, isDebug bool) {
	stmtItem, locked := h.stmtValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	code := syncinterface.TaosStmtExecute(stmtItem.stmt, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt execute error, code:%d, err:%s", code, errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	s := log.GetLogNow(isDebug)
	affected := syncinterface.TaosStmtAffectedRowsOnce(stmtItem.stmt, logger, isDebug)
	logger.Debugf("stmt_affected_rows_once, affected:%d, cost:%s", affected, log.GetLogDuration(isDebug, s))
	resp := &stmtExecResponse{
		Action:   action,
		ReqID:    req.ReqID,
		Timing:   wstool.GetDuration(ctx),
		StmtID:   req.StmtID,
		Affected: affected,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmtCloseRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

func (h *messageHandler) stmtClose(ctx context.Context, session *melody.Session, action string, req stmtCloseRequest, logger *logrus.Entry) {
	logger.Tracef("stmt close, stmt_id:%d", req.StmtID)
	err := h.stmts.FreeStmtByID(req.StmtID, false, logger)
	if err != nil {
		logger.Errorf("stmt close error, err:%s", err.Error())
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, err.Error(), req.StmtID)
		return
	}
	logger.Tracef("stmt close success, stmt_id:%d", req.StmtID)
}

type stmtGetTagFieldsRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type stmtGetTagFieldsResponse struct {
	Code    int                     `json:"code"`
	Message string                  `json:"message"`
	Action  string                  `json:"action"`
	ReqID   uint64                  `json:"req_id"`
	Timing  int64                   `json:"timing"`
	StmtID  uint64                  `json:"stmt_id"`
	Fields  []*stmtCommon.StmtField `json:"fields,omitempty"`
}

func (h *messageHandler) stmtGetTagFields(ctx context.Context, session *melody.Session, action string, req stmtGetTagFieldsRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("stmt get tag fields, stmt_id:%d", req.StmtID)
	stmtItem, locked := h.stmtValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	code, tagNums, tagFields := syncinterface.TaosStmtGetTagFields(stmtItem.stmt, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt get tag fields error, err:%s", errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	defer func() {
		syncinterface.TaosStmtReclaimFields(stmtItem.stmt, tagFields, logger, isDebug)
	}()
	if tagNums == 0 {
		logger.Trace("no tags")
		resp := &stmtGetTagFieldsResponse{
			Action: action,
			ReqID:  req.ReqID,
			Timing: wstool.GetDuration(ctx),
			StmtID: req.StmtID,
		}
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	s := log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	resp := &stmtGetTagFieldsResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: req.StmtID,
		Fields: fields,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmtGetColFieldsRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type stmtGetColFieldsResponse struct {
	Code    int                     `json:"code"`
	Message string                  `json:"message"`
	Action  string                  `json:"action"`
	ReqID   uint64                  `json:"req_id"`
	Timing  int64                   `json:"timing"`
	StmtID  uint64                  `json:"stmt_id"`
	Fields  []*stmtCommon.StmtField `json:"fields"`
}

func (h *messageHandler) stmtGetColFields(ctx context.Context, session *melody.Session, action string, req stmtGetColFieldsRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("stmt get col fields, stmt_id:%d", req.StmtID)
	stmtItem, locked := h.stmtValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	code, colNums, colFields := syncinterface.TaosStmtGetColFields(stmtItem.stmt, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt get col fields error, err:%s", errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	defer func() {
		syncinterface.TaosStmtReclaimFields(stmtItem.stmt, colFields, logger, isDebug)
	}()
	if colNums == 0 {
		logger.Trace("no columns")
		resp := &stmtGetColFieldsResponse{
			Action: action,
			ReqID:  req.ReqID,
			Timing: wstool.GetDuration(ctx),
			StmtID: req.StmtID,
		}
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	s := log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(colNums, colFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	resp := &stmtGetColFieldsResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: req.StmtID,
		Fields: fields,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmtUseResultRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type stmtUseResultResponse struct {
	Code             int                `json:"code"`
	Message          string             `json:"message"`
	Action           string             `json:"action"`
	ReqID            uint64             `json:"req_id"`
	Timing           int64              `json:"timing"`
	StmtID           uint64             `json:"stmt_id"`
	ResultID         uint64             `json:"result_id"`
	FieldsCount      int                `json:"fields_count"`
	FieldsNames      []string           `json:"fields_names"`
	FieldsTypes      jsontype.JsonUint8 `json:"fields_types"`
	FieldsLengths    []int64            `json:"fields_lengths"`
	Precision        int                `json:"precision"`
	FieldsPrecisions []int64            `json:"fields_precisions"`
	FieldsScales     []int64            `json:"fields_scales"`
}

func (h *messageHandler) stmtUseResult(ctx context.Context, session *melody.Session, action string, req stmtUseResultRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("stmt use result, stmt_id:%d", req.StmtID)
	stmtItem, locked := h.stmtValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	logger.Trace("call stmt use result")
	result := syncinterface.TaosStmtUseResult(stmtItem.stmt, logger, isDebug)
	if result == nil {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt use result error, err:%s", errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, errStr, req.StmtID)
		return
	}

	fieldsCount := syncinterface.TaosNumFields(result, logger, isDebug)
	rowsHeader, _ := syncinterface.ReadColumn(result, fieldsCount, logger, isDebug)
	precision := syncinterface.TaosResultPrecision(result, logger, isDebug)
	logger.Tracef("stmt use result success, stmt_id:%d, fields_count:%d, precision:%d", req.StmtID, fieldsCount, precision)
	queryResult := NewStmt1Result(result, fieldsCount, rowsHeader, precision)
	idx := h.queryResults.Add(queryResult)
	logger.Tracef("add query result, result_id:%d", idx)
	resp := &stmtUseResultResponse{
		Action:           action,
		ReqID:            req.ReqID,
		Timing:           wstool.GetDuration(ctx),
		StmtID:           req.StmtID,
		ResultID:         idx,
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

type stmtNumParamsRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type stmtNumParamsResponse struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Action    string `json:"action"`
	ReqID     uint64 `json:"req_id"`
	Timing    int64  `json:"timing"`
	StmtID    uint64 `json:"stmt_id"`
	NumParams int    `json:"num_params"`
}

func (h *messageHandler) stmtNumParams(ctx context.Context, session *melody.Session, action string, req stmtNumParamsRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("stmt num params, stmt_id:%d", req.StmtID)
	stmtItem, locked := h.stmtValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	count, code := syncinterface.TaosStmtNumParams(stmtItem.stmt, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt get col fields error, err:%s", errStr)
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr, req.StmtID)
		return
	}
	logger.Tracef("stmt num params success, stmt_id:%d, num_params:%d", req.StmtID, count)
	resp := &stmtNumParamsResponse{
		Action:    action,
		ReqID:     req.ReqID,
		Timing:    wstool.GetDuration(ctx),
		StmtID:    req.StmtID,
		NumParams: count,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type stmtGetParamRequest struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
	Index  int    `json:"index"`
}

type stmtGetParamResponse struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	Action   string `json:"action"`
	ReqID    uint64 `json:"req_id"`
	Timing   int64  `json:"timing"`
	StmtID   uint64 `json:"stmt_id"`
	Index    int    `json:"index"`
	DataType int    `json:"data_type"`
	Length   int    `json:"length"`
}

func (h *messageHandler) stmtGetParam(ctx context.Context, session *melody.Session, action string, req stmtGetParamRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("stmt get param, stmt_id:%d, index:%d", req.StmtID, req.Index)
	stmtItem, locked := h.stmtValidateAndLock(ctx, session, action, req.ReqID, req.StmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	dataType, length, err := syncinterface.TaosStmtGetParam(stmtItem.stmt, req.Index, logger, isDebug)
	if err != nil {
		taosErr := err.(*errors2.TaosError)
		logger.Errorf("stmt get param error, err:%s", taosErr.Error())
		stmtErrorResponse(ctx, session, logger, action, req.ReqID, int(taosErr.Code), taosErr.ErrStr, req.StmtID)
		return
	}
	logger.Tracef("stmt get param success, data_type:%d, length:%d", dataType, length)
	resp := &stmtGetParamResponse{
		Action:   action,
		ReqID:    req.ReqID,
		Timing:   wstool.GetDuration(ctx),
		StmtID:   req.StmtID,
		Index:    req.Index,
		DataType: dataType,
		Length:   length,
	}
	wstool.WSWriteJson(session, logger, resp)
}

func (h *messageHandler) stmtBinarySetTags(ctx context.Context, session *melody.Session, action string, reqID uint64, stmtID uint64, message []byte, logger *logrus.Entry, isDebug bool) {
	p0 := unsafe.Pointer(&message[0])
	block := tools.AddPointer(p0, uintptr(24))
	columns := parser.RawBlockGetNumOfCols(block)
	rows := parser.RawBlockGetNumOfRows(block)
	logger.Tracef("set tags message, stmt_id:%d, columns:%d, rows:%d", stmtID, columns, rows)
	if rows != 1 {
		logger.Errorf("rows not equal 1, rows:%d", rows)
		stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, "rows not equal 1", stmtID)
		return
	}
	stmtItem, locked := h.stmtValidateAndLock(ctx, session, action, reqID, stmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	code, tagNums, tagFields := syncinterface.TaosStmtGetTagFields(stmtItem.stmt, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt get tag fields error, code:%d, err:%s", code, errStr)
		stmtErrorResponse(ctx, session, logger, action, reqID, code, errStr, stmtID)
		return
	}
	defer func() {
		syncinterface.TaosStmtReclaimFields(stmtItem.stmt, tagFields, logger, isDebug)
	}()
	if tagNums == 0 {
		logger.Trace("no tags")
		resp := &stmtSetTagsResponse{
			Action: action,
			ReqID:  reqID,
			Timing: wstool.GetDuration(ctx),
			StmtID: stmtID,
		}
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	if int(columns) != tagNums {
		logger.Errorf("stmt tags count not match %d != %d", columns, tagNums)
		stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, "stmt tags count not match", stmtID)
		return
	}
	s := log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	tags := stmt.BlockConvert(block, int(rows), fields, nil)
	logger.Debugf("block concert cost:%s", log.GetLogDuration(isDebug, s))
	reTags := make([]driver.Value, tagNums)
	for i := 0; i < tagNums; i++ {
		reTags[i] = tags[i][0]
	}
	code = syncinterface.TaosStmtSetTags(stmtItem.stmt, reTags, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt set tags error, code:%d, msg:%s", code, errStr)
		stmtErrorResponse(ctx, session, logger, action, reqID, code, errStr, stmtID)
		return
	}
	resp := &stmtSetTagsResponse{
		Action: action,
		ReqID:  reqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: stmtID,
	}
	wstool.WSWriteJson(session, logger, resp)
}

func (h *messageHandler) stmtBinaryBind(ctx context.Context, session *melody.Session, action string, reqID uint64, stmtID uint64, message []byte, logger *logrus.Entry, isDebug bool) {
	p0 := unsafe.Pointer(&message[0])
	block := tools.AddPointer(p0, uintptr(24))
	columns := parser.RawBlockGetNumOfCols(block)
	rows := parser.RawBlockGetNumOfRows(block)
	logger.Tracef("bind message, stmt_id:%d columns:%d, rows:%d", stmtID, columns, rows)
	stmtItem, locked := h.stmtValidateAndLock(ctx, session, action, reqID, stmtID, logger, isDebug)
	if !locked {
		return
	}
	defer stmtItem.Unlock()
	var data [][]driver.Value
	var fieldTypes []*types.ColumnType
	if stmtItem.isInsert {
		code, colNums, colFields := syncinterface.TaosStmtGetColFields(stmtItem.stmt, logger, isDebug)
		if code != 0 {
			errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
			logger.Errorf("stmt get col fields error, code:%d, err:%s", code, errStr)
			stmtErrorResponse(ctx, session, logger, action, reqID, code, errStr, stmtID)
			return
		}
		defer func() {
			syncinterface.TaosStmtReclaimFields(stmtItem.stmt, colFields, logger, isDebug)
		}()
		if colNums == 0 {
			logger.Trace("no columns")
			resp := &stmtBindResponse{
				Action: action,
				ReqID:  reqID,
				Timing: wstool.GetDuration(ctx),
				StmtID: stmtID,
			}
			wstool.WSWriteJson(session, logger, resp)
			return
		}
		s := log.GetLogNow(isDebug)
		fields := wrapper.StmtParseFields(colNums, colFields)
		logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
		fieldTypes = make([]*types.ColumnType, colNums)
		var err error
		for i := 0; i < colNums; i++ {
			fieldTypes[i], err = fields[i].GetType()
			if err != nil {
				logger.Errorf("stmt get column type error, err:%s", err.Error())
				stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, fmt.Sprintf("stmt get column type error, err:%s", err.Error()), stmtID)
				return
			}
		}
		if int(columns) != colNums {
			logger.Errorf("stmt column count not match %d != %d", columns, colNums)
			stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, "stmt column count not match", stmtID)
			return
		}
		s = log.GetLogNow(isDebug)
		data = stmt.BlockConvert(block, int(rows), fields, fieldTypes)
		logger.Debugf("block convert cost:%s", log.GetLogDuration(isDebug, s))
	} else {
		var fields []*stmtCommon.StmtField
		var err error
		logger.Trace("parse row block info")
		fields, fieldTypes, err = parseRowBlockInfo(block, int(columns))
		if err != nil {
			logger.Errorf("parse row block info error, err:%s", err.Error())
			stmtErrorResponse(ctx, session, logger, action, reqID, 0xffff, fmt.Sprintf("parse row block info error, err:%s", err.Error()), stmtID)
			return
		}
		logger.Trace("convert block to data")
		data = stmt.BlockConvert(block, int(rows), fields, fieldTypes)
		logger.Trace("convert block to data finish")
	}

	code := syncinterface.TaosStmtBindParamBatch(stmtItem.stmt, data, fieldTypes, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosStmtErrStr(stmtItem.stmt, logger, isDebug)
		logger.Errorf("stmt bind param error, code:%d, err:%s", code, errStr)
		stmtErrorResponse(ctx, session, logger, action, reqID, code, errStr, stmtID)
		return
	}
	logger.Trace("stmt bind param success")
	resp := &stmtBindResponse{
		Action: action,
		ReqID:  reqID,
		Timing: wstool.GetDuration(ctx),
		StmtID: stmtID,
	}
	wstool.WSWriteJson(session, logger, resp)
}

func parseRowBlockInfo(block unsafe.Pointer, columns int) (fields []*stmtCommon.StmtField, fieldTypes []*types.ColumnType, err error) {
	infos := make([]parser.RawBlockColInfo, columns)
	parser.RawBlockGetColInfo(block, infos)

	fields = make([]*stmtCommon.StmtField, len(infos))
	fieldTypes = make([]*types.ColumnType, len(infos))

	for i, info := range infos {
		switch info.ColType {
		case common.TSDB_DATA_TYPE_BOOL:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_BOOL}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosBoolType}
		case common.TSDB_DATA_TYPE_TINYINT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_TINYINT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosTinyintType}
		case common.TSDB_DATA_TYPE_SMALLINT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_SMALLINT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosSmallintType}
		case common.TSDB_DATA_TYPE_INT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_INT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosIntType}
		case common.TSDB_DATA_TYPE_BIGINT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_BIGINT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosBigintType}
		case common.TSDB_DATA_TYPE_FLOAT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_FLOAT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosFloatType}
		case common.TSDB_DATA_TYPE_DOUBLE:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_DOUBLE}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosDoubleType}
		case common.TSDB_DATA_TYPE_BINARY:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_BINARY}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosBinaryType}
		//case common.TSDB_DATA_TYPE_TIMESTAMP:// todo precision
		//	fields[i] = &stmtCommon.StmtField{FieldType:common.TSDB_DATA_TYPE_TIMESTAMP}
		//	fieldTypes[i] = &types.ColumnType{Type:types.TaosTimestampType}
		case common.TSDB_DATA_TYPE_NCHAR:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_NCHAR}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosNcharType}
		case common.TSDB_DATA_TYPE_UTINYINT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_UTINYINT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosUTinyintType}
		case common.TSDB_DATA_TYPE_USMALLINT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_USMALLINT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosUSmallintType}
		case common.TSDB_DATA_TYPE_UINT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_UINT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosUIntType}
		case common.TSDB_DATA_TYPE_UBIGINT:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_UBIGINT}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosUBigintType}
		case common.TSDB_DATA_TYPE_JSON:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_JSON}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosJsonType}
		case common.TSDB_DATA_TYPE_VARBINARY:
			fields[i] = &stmtCommon.StmtField{FieldType: common.TSDB_DATA_TYPE_VARBINARY}
			fieldTypes[i] = &types.ColumnType{Type: types.TaosBinaryType}
		default:
			err = fmt.Errorf("unsupported data type %d", info.ColType)
		}
	}

	return
}
