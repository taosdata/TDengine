package ws

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/common"
	taoserrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/melody"
	"github.com/taosdata/taosadapter/v3/version"
)

type connRequest struct {
	ReqID       uint64 `json:"req_id"`
	User        string `json:"user"`
	Password    string `json:"password"`
	DB          string `json:"db"`
	Mode        *int   `json:"mode"`
	TZ          string `json:"tz"`
	App         string `json:"app"`
	IP          string `json:"ip"`
	Connector   string `json:"connector"`
	TOTPCode    string `json:"totp_code"`
	BearerToken string `json:"bearer_token"`
}

func (r *connRequest) String() string {
	builder := &strings.Builder{}

	builder.WriteString("{")
	_, _ = fmt.Fprintf(builder, "req_id: %d,", r.ReqID)
	_, _ = fmt.Fprintf(builder, "user: %q,", r.User)
	builder.WriteString("password: \"[HIDDEN]\",")
	_, _ = fmt.Fprintf(builder, "db: %q,", r.DB)
	if r.Mode != nil {
		_, _ = fmt.Fprintf(builder, "mode: %d,", *r.Mode)
	} else {
		builder.WriteString("mode: nil,")
	}

	_, _ = fmt.Fprintf(builder, "tz: %q,", r.TZ)
	_, _ = fmt.Fprintf(builder, "app: %q,", r.App)
	_, _ = fmt.Fprintf(builder, "ip: %q,", r.IP)
	_, _ = fmt.Fprintf(builder, "connector: %q,", r.Connector)

	builder.WriteString("totp_code: \"[HIDDEN]\",")
	builder.WriteString("bearer_token: \"[HIDDEN]\"")
	builder.WriteString("}")
	return builder.String()
}

type connResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	Version string `json:"version"`
}

func (h *messageHandler) connect(ctx context.Context, session *melody.Session, action string, req connRequest, innerReqID uint64, logger *logrus.Entry, isDebug bool) {
	h.Lock(logger, isDebug)
	defer h.Unlock()
	if h.IsClosed() {
		logger.Trace("server closed")
		return
	}
	if h.conn != nil {
		logger.Trace("duplicate connections")
		commonErrorResponse(ctx, session, logger, action, req.ReqID, 0xffff, "duplicate connections")
		return
	}

	conn, err := connect(&req, h.cloudTokenExists, logger, isDebug)

	if err != nil {
		handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, err, "connect to TDengine error")
		return
	}
	logger.Trace("get whitelist")
	s := log.GetLogNow(isDebug)
	allowlist, blocklist, err := tool.GetWhitelist(conn, logger, isDebug)
	logger.Debugf("get whitelist cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, err, "get whitelist error")
		return
	}
	allowlistStr := tool.IpNetSliceToString(allowlist)
	blocklistStr := tool.IpNetSliceToString(blocklist)
	logger.Tracef("check whitelist, ip:%s, allowlist:%s, blocklist:%s", h.ipStr, allowlistStr, blocklistStr)
	valid := tool.CheckWhitelist(allowlist, blocklist, h.ip)
	if !valid {
		err = errors.New("ip not in whitelist")
		handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, err, "ip not in whitelist")
		return
	}
	h.InitNotifyHandles()
	notifyRegistered := false
	defer func() {
		if !notifyRegistered {
			h.PutNotifyHandles()
		}
	}()
	s = log.GetLogNow(isDebug)
	logger.Trace("register whitelist change")
	err = tool.RegisterChangeWhitelist(conn, h.WhitelistChangeHandle, logger, isDebug)
	logger.Debugf("register whitelist change cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, err, "register whitelist change error")
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("register drop user")
	err = tool.RegisterDropUser(conn, h.DropUserHandle, logger, isDebug)
	logger.Debugf("register drop user cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, err, "register drop user error")
		return
	}
	if req.Mode != nil {
		switch *req.Mode {
		case common.TAOS_CONN_MODE_BI:
			// BI mode
			logger.Trace("set connection mode to BI")
			code := syncinterface.TaosSetConnMode(conn, common.TAOS_CONN_MODE_BI, 1, logger, isDebug)
			logger.Trace("set connection mode to BI done")
			if code != 0 {
				handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, taoserrors.NewError(code, syncinterface.TaosErrorStr(nil, logger, isDebug)), "set connection mode to BI error")
				return
			}
		default:
			err = fmt.Errorf("unexpected mode:%d", *req.Mode)
			handleConnectError(ctx, conn, session, logger, isDebug, action, req.ReqID, err, err.Error())
			return
		}
	}
	// set connection ip
	clientIP := h.ipStr
	if req.IP != "" {
		clientIP = req.IP
	}
	if !setConnectOption(
		ctx,
		conn,
		session,
		logger,
		isDebug,
		action,
		req.ReqID,
		common.TSDB_OPTION_CONNECTION_USER_IP,
		clientIP,
		"connection ip",
	) {
		return
	}

	// set timezone
	if req.TZ != "" {
		if !setConnectOption(
			ctx,
			conn,
			session,
			logger,
			isDebug,
			action,
			req.ReqID,
			common.TSDB_OPTION_CONNECTION_TIMEZONE,
			req.TZ,
			"connection timezone",
		) {
			return
		}
	}
	// set connection app
	if req.App != "" {
		if !setConnectOption(
			ctx,
			conn,
			session,
			logger,
			isDebug,
			action,
			req.ReqID,
			common.TSDB_OPTION_CONNECTION_USER_APP,
			req.App,
			"app",
		) {
			return
		}
	}
	// set connector info
	if req.Connector != "" {
		if !setConnectOption(
			ctx,
			conn,
			session,
			logger,
			isDebug,
			action,
			req.ReqID,
			common.TSDB_OPTION_CONNECTION_CONNECTOR_INFO,
			req.Connector,
			"connector info",
		) {
			return
		}
	}
	h.conn = conn
	// save user for record
	h.user = req.User
	h.appName = req.App
	logger.Trace("start wait signal goroutine")
	notifyRegistered = true
	go wstool.WaitSignal(h, conn, h.ip, h.ipStr, h.WhitelistChangeHandle, h.DropUserHandle, h.WhitelistChangeChan, h.DropUserChan, h.exit, h.logger)
	resp := &connResponse{
		Action:  action,
		ReqID:   req.ReqID,
		Timing:  wstool.GetDuration(ctx),
		Version: version.TaosClientVersion,
	}
	wstool.WSWriteJson(session, logger, resp)
}

func connect(req *connRequest, cloudTokenExists bool, logger *logrus.Entry, isDebug bool) (unsafe.Pointer, error) {
	if !cloudTokenExists && req.BearerToken != "" {
		logger.Debugf("use bearer token to connect")
		return syncinterface.TaosConnectToken("", req.BearerToken, req.DB, 0, logger, isDebug)
	}
	if req.TOTPCode != "" {
		logger.Debugf("use TOTP code to connect")
		return syncinterface.TaosConnectTOTP("", req.User, req.Password, req.TOTPCode, req.DB, 0, logger, isDebug)
	}
	logger.Debugf("use password to connect")
	return syncinterface.TaosConnect("", req.User, req.Password, req.DB, 0, logger, isDebug)
}

func setConnectOption(ctx context.Context, conn unsafe.Pointer, session *melody.Session, logger *logrus.Entry, isDebug bool, action string, reqID uint64, option int, value string, optionName string) bool {
	logger.Tracef("set %s, value: %s", optionName, value)
	code := syncinterface.TaosOptionsConnection(conn, option, &value, logger, isDebug)
	logger.Tracef("set %s done", optionName)
	if code != 0 {
		handleConnectError(ctx, conn, session, logger, isDebug, action, reqID, taoserrors.NewError(code, syncinterface.TaosErrorStr(nil, logger, isDebug)), fmt.Sprintf("set %s error", optionName))
		return false
	}
	return true
}
