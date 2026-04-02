package ws

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

type schemalessWriteRequest struct {
	ReqID        uint64 `json:"req_id"`
	Protocol     int    `json:"protocol"`
	Precision    string `json:"precision"`
	TTL          int    `json:"ttl"`
	Data         string `json:"data"`
	TableNameKey string `json:"table_name_key"`
}

type schemalessWriteResponse struct {
	Code         int    `json:"code"`
	Message      string `json:"message"`
	Action       string `json:"action"`
	ReqID        uint64 `json:"req_id"`
	Timing       int64  `json:"timing"`
	AffectedRows int    `json:"affected_rows"`
	TotalRows    int32  `json:"total_rows"`
}

func (h *messageHandler) schemalessWrite(ctx context.Context, session *melody.Session, action string, req schemalessWriteRequest, innReqID uint64, logger *logrus.Entry, isDebug bool) {
	if req.Protocol == 0 {
		logger.Error("schemaless write request error. protocol is null")
		commonErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, "schemaless write protocol is null")
		return
	}
	var affectedRows int
	totalRows, result := syncinterface.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(h.conn, req.Data, req.Protocol, req.Precision, req.TTL, int64(innReqID), req.TableNameKey, logger, isDebug)
	defer syncinterface.TaosSchemalessFree(result, logger, isDebug)
	if code := syncinterface.TaosError(result, logger, isDebug); code != 0 {
		errStr := syncinterface.TaosErrorStr(result, logger, isDebug)
		logger.Errorf("schemaless write error, code:%d, err:%s", code, errStr)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr)
		return
	}
	affectedRows = syncinterface.TaosAffectedRows(result, logger, isDebug)
	logger.Debugf("schemaless write finish, total rows:%d, affected rows:%d", totalRows, affectedRows)
	resp := &schemalessWriteResponse{
		Action:       action,
		ReqID:        req.ReqID,
		Timing:       wstool.GetDuration(ctx),
		AffectedRows: affectedRows,
		TotalRows:    totalRows,
	}
	wstool.WSWriteJson(session, logger, resp)
}
