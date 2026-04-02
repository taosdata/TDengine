package ws

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/bytesutil"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

func (h *messageHandler) resultValidateAndLock(ctx context.Context, session *melody.Session, action string, reqID uint64, resultID uint64, logger *logrus.Entry) (item *QueryResult, locked bool) {
	item = h.queryResults.Get(resultID)
	if item == nil {
		logger.Debugf("result is nil, result_id:%d", resultID)
		commonErrorResponse(ctx, session, logger, action, reqID, 0xffff, "result is nil")
		return nil, false
	}
	item.Lock()
	if item.TaosResult == nil {
		item.Unlock()
		logger.Errorf("result has been freed, result_id:%d", resultID)
		commonErrorResponse(ctx, session, logger, action, reqID, 0xffff, "result has been freed")
		return nil, false
	}
	return item, true
}

type fetchRequest struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

type fetchResponse struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Action    string `json:"action"`
	ReqID     uint64 `json:"req_id"`
	Timing    int64  `json:"timing"`
	ID        uint64 `json:"id"`
	Completed bool   `json:"completed"`
	Lengths   []int  `json:"lengths"`
	Rows      int    `json:"rows"`
}

func (h *messageHandler) fetch(ctx context.Context, session *melody.Session, action string, req fetchRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("get result by id, id:%d", req.ID)
	item := h.queryResults.Get(req.ID)
	if item == nil {
		logger.Debug("result is nil")
		commonErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, "result is nil")
		return
	}
	item.Lock()
	if item.TaosResult == nil {
		item.Unlock()
		logger.Errorf("result has been freed")
		commonErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, "result has been freed")
		return
	}
	s := log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	logger.Debugf("get handler, cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	var recordTime time.Time
	if item.record != nil {
		recordTime = time.Now()
	}
	result := async.GlobalAsync.TaosFetchRawBlockA(item.TaosResult, logger, isDebug, handler)
	if item.record != nil {
		item.record.AddFetchDuration(time.Since(recordTime))
	}
	logger.Debugf("fetch_raw_block_a, cost:%s", log.GetLogDuration(isDebug, s))
	if result.N == 0 {
		item.Unlock()
		logger.Trace("fetch raw block completed")
		h.queryResults.FreeResultByID(req.ID, logger)
		resp := &fetchResponse{
			Action:    action,
			ReqID:     req.ReqID,
			Timing:    wstool.GetDuration(ctx),
			ID:        req.ID,
			Completed: true,
		}
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	if result.N < 0 {
		item.Unlock()
		errStr := syncinterface.TaosErrorStr(result.Res, logger, isDebug)
		logger.Errorf("fetch raw block error, code:%d, message:%s", result.N, errStr)
		h.queryResults.FreeResultByID(req.ID, logger)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, result.N, errStr)
		return
	}
	s = log.GetLogNow(isDebug)
	length := syncinterface.TaosFetchLengths(item.TaosResult, item.FieldsCount, logger, isDebug)
	logger.Debugf("fetch_lengths result:%d, cost:%s", length, log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Trace("get raw block")
	item.Block = syncinterface.TaosGetRawBlock(item.TaosResult, logger, isDebug)
	logger.Debugf("get_raw_block result:%p, cost:%s", item.Block, log.GetLogDuration(isDebug, s))
	item.Size = result.N
	item.Unlock()
	resp := &fetchResponse{
		Action:  action,
		ReqID:   req.ReqID,
		Timing:  wstool.GetDuration(ctx),
		ID:      req.ID,
		Lengths: length,
		Rows:    result.N,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type fetchBlockRequest struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

func (h *messageHandler) fetchBlock(ctx context.Context, session *melody.Session, action string, req fetchBlockRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("fetch block, id:%d", req.ID)
	item, locked := h.resultValidateAndLock(ctx, session, action, req.ReqID, req.ID, logger)
	if !locked {
		return
	}
	defer item.Unlock()
	if item.Block == nil {
		logger.Trace("block is nil")
		commonErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, "block is nil")
		return
	}

	blockLength := int(parser.RawBlockGetLength(item.Block))
	if blockLength <= 0 {
		logger.Errorf("block length illegal:%d", blockLength)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, "block length illegal")
		return
	}
	s := log.GetLogNow(isDebug)
	if cap(item.buf) < blockLength+16 {
		item.buf = make([]byte, 0, blockLength+16)
	}
	item.buf = item.buf[:blockLength+16]
	binary.LittleEndian.PutUint64(item.buf, uint64(wstool.GetDuration(ctx)))
	binary.LittleEndian.PutUint64(item.buf[8:], req.ID)
	bytesutil.Copy(item.Block, item.buf, 16, blockLength)
	logger.Debugf("handle binary content cost:%s", log.GetLogDuration(isDebug, s))
	wstool.WSWriteBinary(session, item.buf, logger)
}

func (h *messageHandler) fetchRawBlock(ctx context.Context, session *melody.Session, reqID uint64, resultID uint64, message []byte, logger *logrus.Entry, isDebug bool) {
	if len(message) < 26 {
		logger.Errorf("message length is too short")
		fetchRawBlockErrorResponse(session, logger, 0xffff, "message length is too short", reqID, resultID, uint64(wstool.GetDuration(ctx)))
		return
	}
	v := binary.LittleEndian.Uint16(message[24:])
	if v != BinaryProtocolVersion1 {
		logger.Errorf("unknown fetch raw block version:%d", v)
		fetchRawBlockErrorResponse(session, logger, 0xffff, "unknown fetch raw block version", reqID, resultID, uint64(wstool.GetDuration(ctx)))
		return
	}
	item := h.queryResults.Get(resultID)
	logger.Tracef("fetch raw block, result_id:%d", resultID)
	if item == nil {
		logger.Debugf("result is nil, result_id:%d", resultID)
		fetchRawBlockErrorResponse(session, logger, 0xffff, "result is nil", reqID, resultID, uint64(wstool.GetDuration(ctx)))
		return
	}
	item.Lock()
	if item.TaosResult == nil {
		item.Unlock()
		logger.Errorf("result has been freed, result_id:%d", resultID)
		fetchRawBlockErrorResponse(session, logger, 0xffff, "result has been freed", reqID, resultID, uint64(wstool.GetDuration(ctx)))
		return
	}
	s := log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	logger.Tracef("get handler cost:%s", log.GetLogDuration(isDebug, s))
	var recordTime time.Time
	if item.record != nil {
		recordTime = time.Now()
	}
	result := async.GlobalAsync.TaosFetchRawBlockA(item.TaosResult, logger, isDebug, handler)
	if item.record != nil {
		item.record.AddFetchDuration(time.Since(recordTime))
	}
	if result.N == 0 {
		item.Unlock()
		logger.Trace("fetch raw block success")
		h.queryResults.FreeResultByID(resultID, logger)
		fetchRawBlockFinishResponse(session, logger, reqID, resultID, uint64(wstool.GetDuration(ctx)))
		return
	}
	if result.N < 0 {
		item.Unlock()
		errStr := syncinterface.TaosErrorStr(result.Res, logger, isDebug)
		logger.Errorf("fetch raw block error:%d %s", result.N, errStr)
		h.queryResults.FreeResultByID(resultID, logger)
		fetchRawBlockErrorResponse(session, logger, result.N, errStr, reqID, resultID, uint64(wstool.GetDuration(ctx)))
		return
	}
	logger.Trace("call taos_get_raw_block")
	s = log.GetLogNow(isDebug)
	item.Block = syncinterface.TaosGetRawBlock(item.TaosResult, logger, isDebug)
	logger.Debugf("get_raw_block cost:%s", log.GetLogDuration(isDebug, s))
	item.Size = result.N
	s = log.GetLogNow(isDebug)
	blockLength := int(parser.RawBlockGetLength(item.Block))
	if blockLength <= 0 {
		item.Unlock()
		logger.Errorf("block length illegal:%d", blockLength)
		fetchRawBlockErrorResponse(session, logger, 0xffff, "block length illegal", reqID, resultID, uint64(wstool.GetDuration(ctx)))
		return
	}
	item.buf = fetchRawBlockMessage(item.buf, reqID, resultID, uint64(wstool.GetDuration(ctx)), int32(blockLength), item.Block)
	logger.Debugf("handle binary content cost:%s", log.GetLogDuration(isDebug, s))
	item.Unlock()
	wstool.WSWriteBinary(session, item.buf, logger)
}

type numFieldsRequest struct {
	ReqID    uint64 `json:"req_id"`
	ResultID uint64 `json:"result_id"`
}

type numFieldsResponse struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Action    string `json:"action"`
	ReqID     uint64 `json:"req_id"`
	Timing    int64  `json:"timing"`
	NumFields int    `json:"num_fields"`
}

func (h *messageHandler) numFields(ctx context.Context, session *melody.Session, action string, req numFieldsRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("num fields, result_id:%d", req.ResultID)
	item, locked := h.resultValidateAndLock(ctx, session, action, req.ReqID, req.ResultID, logger)
	if !locked {
		return
	}
	defer item.Unlock()
	num := syncinterface.TaosNumFields(item.TaosResult, logger, isDebug)
	resp := &numFieldsResponse{
		Action:    action,
		ReqID:     req.ReqID,
		Timing:    wstool.GetDuration(ctx),
		NumFields: num,
	}
	wstool.WSWriteJson(session, logger, resp)
}
