package ws

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	errors2 "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/bytesutil"
	"github.com/taosdata/taosadapter/v3/tools/melody"
	"github.com/taosdata/taosadapter/v3/version"
)

type getCurrentDBRequest struct {
	ReqID uint64 `json:"req_id"`
}

type getCurrentDBResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	DB      string `json:"db"`
}

func (h *messageHandler) getCurrentDB(ctx context.Context, session *melody.Session, action string, req getCurrentDBRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("get current db")
	db, err := syncinterface.TaosGetCurrentDB(h.conn, logger, isDebug)
	if err != nil {
		logger.Errorf("get current db error, err:%s", err)
		taosErr := err.(*errors2.TaosError)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, int(taosErr.Code), taosErr.Error())
		return
	}
	resp := &getCurrentDBResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		DB:     db,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type getServerInfoRequest struct {
	ReqID uint64 `json:"req_id"`
}

type getServerInfoResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	Info    string `json:"info"`
}

func (h *messageHandler) getServerInfo(ctx context.Context, session *melody.Session, action string, req getServerInfoRequest, logger *logrus.Entry, isDebug bool) {
	logger.Trace("get server info")
	serverInfo := syncinterface.TaosGetServerInfo(h.conn, logger, isDebug)
	resp := &getServerInfoResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		Info:   serverInfo,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type versionRequest struct {
	ReqID uint64 `json:"req_id"`
}

type versionResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	Version string `json:"version"`
}

func (h *messageHandler) version(ctx context.Context, session *melody.Session, action string, req versionRequest, logger *logrus.Entry, isDebug bool) {
	logger.Trace("get version")
	resp := &versionResp{
		Action:  action,
		ReqID:   req.ReqID,
		Timing:  wstool.GetDuration(ctx),
		Version: version.TaosClientVersion,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type optionsConnectionRequest struct {
	ReqID   uint64    `json:"req_id"`
	Options []*option `json:"options"`
}
type option struct {
	Option int     `json:"option"`
	Value  *string `json:"value"`
}

func (h *messageHandler) optionsConnection(ctx context.Context, session *melody.Session, action string, req optionsConnectionRequest, logger *logrus.Entry, isDebug bool) {
	logger.Trace("options connection")
	for i := 0; i < len(req.Options); i++ {
		code := syncinterface.TaosOptionsConnection(h.conn, req.Options[i].Option, req.Options[i].Value, logger, isDebug)
		if code != 0 {
			errStr := syncinterface.TaosErrorStr(nil, logger, isDebug)
			val := "<nil>"
			if req.Options[i].Value != nil {
				val = *req.Options[i].Value
			}
			logger.Errorf("options connection error, option:%d, value:%s, code:%d, err:%s", req.Options[i].Option, val, code, errStr)
			commonErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr)
			return
		}
	}
	commonSuccessResponse(ctx, session, logger, action, req.ReqID)
}

type validateSqlResponse struct {
	Code       int    `json:"code"`
	Message    string `json:"message"`
	Action     string `json:"action"`
	ReqID      uint64 `json:"req_id"`
	Timing     int64  `json:"timing"`
	ResultCode int64  `json:"result_code"`
}

func (h *messageHandler) validateSQL(ctx context.Context, session *melody.Session, action string, reqID uint64, message []byte, logger *logrus.Entry, isDebug bool) {
	if len(message) < 31 {
		commonErrorResponse(ctx, session, logger, action, reqID, 0xffff, "message length is too short")
		return
	}
	v := binary.LittleEndian.Uint16(message[24:])
	var sql []byte
	if v == BinaryProtocolVersion1 {
		sqlLen := binary.LittleEndian.Uint32(message[26:])
		remainMessageLength := len(message) - 30
		if remainMessageLength < int(sqlLen) {
			commonErrorResponse(ctx, session, logger, action, reqID, 0xffff, fmt.Sprintf("uncompleted message, sql length:%d, remainMessageLength:%d", sqlLen, remainMessageLength))
			return
		}
		sql = message[30 : 30+sqlLen]
	} else {
		logger.Errorf("unknown binary query version:%d", v)
		commonErrorResponse(ctx, session, logger, action, reqID, 0xffff, fmt.Sprintf("unknown binary query version:%d", v))
		return
	}
	logger.Debugf("validate sql, sql:%s", log.GetLogSql(bytesutil.ToUnsafeString(sql)))
	code := syncinterface.TaosValidateSql(h.conn, bytesutil.ToUnsafeString(sql), logger, isDebug)
	resp := &validateSqlResponse{
		Action:     action,
		ReqID:      reqID,
		Timing:     wstool.GetDuration(ctx),
		ResultCode: int64(code),
	}
	wstool.WSWriteJson(session, logger, resp)
}

type checkServerStatusRequest struct {
	ReqID uint64  `json:"req_id"`
	FQDN  *string `json:"fqdn"`
	Port  int32   `json:"port"`
}

type checkServerStatusResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	Status  int32  `json:"status"`
	Details string `json:"details"`
}

func (h *messageHandler) checkServerStatus(ctx context.Context, session *melody.Session, action string, req checkServerStatusRequest, logger *logrus.Entry, isDebug bool) {
	if req.FQDN == nil {
		logger.Tracef("check server status, fqdn: nil, port:%d", req.Port)
	} else {
		logger.Tracef("check server status, fqdn:%s, port:%d", *req.FQDN, req.Port)
	}
	status, details := syncinterface.TaosCheckServerStatus(req.FQDN, req.Port, logger, isDebug)
	resp := &checkServerStatusResponse{
		Action:  action,
		ReqID:   req.ReqID,
		Timing:  wstool.GetDuration(ctx),
		Status:  status,
		Details: details,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type getConnectionInfoRequest struct {
	ReqID uint64 `json:"req_id"`
	Info  int    `json:"info"`
}

type getConnectionInfoResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	Info    string `json:"info"`
}

func (h *messageHandler) getConnectionInfo(ctx context.Context, session *melody.Session, action string, req getConnectionInfoRequest, logger *logrus.Entry, isDebug bool) {
	logger.Trace("get connection info")
	info, code := syncinterface.TaosGetConnectionInfo(h.conn, req.Info, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosErrorStr(nil, logger, isDebug)
		logger.Errorf("get connection info error, info:%d, code:%d, err:%s", req.Info, code, errStr)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, code, errStr)
		return
	}
	resp := &getConnectionInfoResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		Info:   info,
	}
	wstool.WSWriteJson(session, logger, resp)
}
