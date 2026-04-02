package schemaless

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

type SchemalessController struct {
	schemaless *melody.Melody
}

func NewSchemalessController() *SchemalessController {
	schemaless := melody.New()
	schemaless.Upgrader.EnableCompression = true
	schemaless.Config.MaxMessageSize = 0

	schemaless.HandleConnect(func(session *melody.Session) {
		monitor.RecordWSSMLConn()
		logger := wstool.GetLogger(session)
		logger.Debug("ws connect")
		session.Set(taosSchemalessKey, NewTaosSchemaless(session))
	})

	schemaless.HandleMessage(func(session *melody.Session, bytes []byte) {
		t := session.MustGet(taosSchemalessKey).(*TaosSchemaless)
		if t.IsClosed() {
			return
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			if t.IsClosed() {
				return
			}
			ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now())
			logger := wstool.GetLogger(session)
			var action wstool.WSAction
			err := json.Unmarshal(bytes, &action)
			if err != nil {
				logger.Errorf("unmarshal ws request error, err:%s", err)
				wstool.WSError(ctx, session, logger, err, action.Action, 0)
				return
			}
			if action.Action != SchemalessConn {
				logger.Debugf("get ws message data:%s", bytes)
			}
			switch action.Action {
			case wstool.ClientVersion:
				wstool.WSWriteVersion(session, logger)
			case SchemalessConn:
				var req schemalessConnReq
				if err = json.Unmarshal(action.Args, &req); err != nil {
					logger.Errorf("unmarshal connect args, err:%s, args:%s", err, action.Args)
					wstool.WSError(ctx, session, logger, err, SchemalessConn, req.ReqID)
					return
				}
				logger.Debugf("get ws message, connect action:%s", &req)
				t.connect(ctx, session, req)
			case SchemalessWrite:
				var req schemalessWriteReq
				if err = json.Unmarshal(action.Args, &req); err != nil {
					logger.Errorf("unmarshal schemaless insert args, err:%s, args:%s", err, action.Args)
					wstool.WSError(ctx, session, logger, err, SchemalessWrite, req.ReqID)
					return
				}
				t.insert(ctx, session, req)
			default:
				logger.Errorf("unknown action:%s", action.Action)
				return
			}
		}()
	})

	schemaless.HandleClose(func(session *melody.Session, i int, s string) error {
		logger := wstool.GetLogger(session)
		logger.Debugf("ws close, code:%d, msg %s", i, s)
		CloseWs(session)
		return nil
	})

	schemaless.HandleError(func(session *melody.Session, err error) {
		wstool.LogWSError(session, err)
		CloseWs(session)
	})

	schemaless.HandleDisconnect(func(session *melody.Session) {
		monitor.RecordWSSMLDisconnect()
		logger := wstool.GetLogger(session)
		logger.Debug("ws disconnect")
		CloseWs(session)
	})
	return &SchemalessController{schemaless: schemaless}
}

func CloseWs(session *melody.Session) {
	t, exist := session.Get(taosSchemalessKey)
	if exist && t != nil {
		t.(*TaosSchemaless).Close()
	}
}

func (s *SchemalessController) Init(ctl gin.IRouter) {
	ctl.GET("rest/schemaless", func(c *gin.Context) {
		sessionID := generator.GetSessionID()
		logger := log.GetLogger("SML").WithFields(logrus.Fields{
			config.SessionIDKey: sessionID})
		_ = s.schemaless.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": logger})
	})
}

type TaosSchemaless struct {
	conn   unsafe.Pointer
	logger *logrus.Entry
	closed uint32
	exit   chan struct{}
	wstool.NotifyHandles
	session *melody.Session
	ip      net.IP
	ipStr   string
	wg      sync.WaitGroup
	mutex   sync.Mutex
	once    sync.Once
}

func NewTaosSchemaless(session *melody.Session) *TaosSchemaless {
	logger := wstool.GetLogger(session)
	ipAddr := iptool.GetRealIP(session.Request)
	return &TaosSchemaless{
		exit:    make(chan struct{}),
		session: session,
		ip:      ipAddr,
		ipStr:   ipAddr.String(),
		logger:  logger,
	}
}

func (t *TaosSchemaless) UnlockAndExit(logger *logrus.Entry, isDebug bool) {
	logger.Trace("close session")
	s := log.GetLogNow(isDebug)
	_ = t.session.Close()
	logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
	t.Unlock()
	s = log.GetLogNow(isDebug)
	t.Close()
	logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
}

func (t *TaosSchemaless) Lock(logger *logrus.Entry, isDebug bool) {
	s := log.GetLogNow(isDebug)
	logger.Trace("get handler lock")
	t.mutex.Lock()
	logger.Debugf("get handler lock cost:%s", log.GetLogDuration(isDebug, s))
}

func (t *TaosSchemaless) Unlock() {
	t.mutex.Unlock()
}

func (t *TaosSchemaless) IsClosed() bool {
	return atomic.LoadUint32(&t.closed) == 1
}

func (t *TaosSchemaless) setClosed() {
	atomic.StoreUint32(&t.closed, 1)
}

func (t *TaosSchemaless) Close() {
	t.Lock(t.logger, log.IsDebug())
	defer t.Unlock()
	if t.IsClosed() {
		return
	}
	t.logger.Trace("schemaless close")
	t.setClosed()
	t.stop()
}

func (t *TaosSchemaless) stop() {
	t.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		done := make(chan struct{})
		go func() {
			t.wg.Wait()
			close(done)
		}()
		select {
		case <-ctx.Done():
			t.logger.Warn("wait stop over 1 minute")
			<-done
		case <-done:
			t.logger.Debug("all goroutines exit")
		}
		t.logger.Debug("wait stop done")
		if t.conn != nil {
			syncinterface.TaosClose(t.conn, t.logger, log.IsDebug())
			t.conn = nil
		}
		close(t.exit)
	})
}

type schemalessConnReq struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
}

func (r *schemalessConnReq) String() string {
	builder := &strings.Builder{}

	builder.WriteString("{")
	_, _ = fmt.Fprintf(builder, "req_id: %d,", r.ReqID)
	_, _ = fmt.Fprintf(builder, "user: %q,", r.User)
	builder.WriteString("password: \"[HIDDEN]\",")
	_, _ = fmt.Fprintf(builder, "db: %q,", r.DB)
	builder.WriteString("}")

	return builder.String()
}

type schemalessConnResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *TaosSchemaless) connect(ctx context.Context, session *melody.Session, req schemalessConnReq) {
	action := SchemalessConn
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("connect request:%+v", req)
	isDebug := log.IsDebug()
	t.Lock(logger, isDebug)
	defer t.Unlock()
	if t.IsClosed() {
		logger.Trace("server closed")
		return
	}
	if t.conn != nil {
		logger.Errorf("duplicate connections")
		wsSchemalessErrorMsg(ctx, session, logger, 0xffff, "duplicate connections", action, req.ReqID)
		return
	}
	conn, err := syncinterface.TaosConnect("", req.User, req.Password, req.DB, 0, logger, isDebug)
	if err != nil {
		logger.Errorf("connect error, err:%s", err)
		wstool.WSError(ctx, session, logger, err, action, req.ReqID)
		return
	}
	s := log.GetLogNow(isDebug)
	allowlist, blocklist, err := tool.GetWhitelist(conn, logger, isDebug)
	logger.Debugf("get whitelist cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("get whitelist error, close connection, err:%s", err)
		syncinterface.TaosClose(conn, logger, isDebug)
		wstool.WSError(ctx, session, logger, err, action, req.ReqID)
		return
	}
	allowlistStr := tool.IpNetSliceToString(allowlist)
	blocklistStr := tool.IpNetSliceToString(blocklist)
	logger.Tracef("check whitelist, ip: %s, allowlist: %s, blocklist: %s", t.ipStr, allowlistStr, blocklistStr)
	valid := tool.CheckWhitelist(allowlist, blocklist, t.ip)
	if !valid {
		logger.Errorf("ip not in whitelist, ip: %s, allowlist: %s, blocklist: %s", t.ipStr, allowlistStr, blocklistStr)
		syncinterface.TaosClose(conn, logger, isDebug)
		wstool.WSErrorMsg(ctx, session, logger, 0xffff, "whitelist prohibits current IP access", action, req.ReqID)
		return
	}
	t.InitNotifyHandles()
	notifyRegistered := false
	defer func() {
		if !notifyRegistered {
			t.PutNotifyHandles()
		}
	}()
	logger.Trace("register change whitelist")
	err = tool.RegisterChangeWhitelist(conn, t.WhitelistChangeHandle, logger, isDebug)
	if err != nil {
		logger.Errorf("register change whitelist error:%s", err)
		syncinterface.TaosClose(conn, t.logger, isDebug)
		wstool.WSError(ctx, session, logger, err, action, req.ReqID)
		return
	}
	logger.Trace("register drop user")
	err = tool.RegisterDropUser(conn, t.DropUserHandle, logger, isDebug)
	if err != nil {
		logger.Errorf("register drop user error:%s", err)
		syncinterface.TaosClose(conn, t.logger, isDebug)
		wstool.WSError(ctx, session, logger, err, action, req.ReqID)
		return
	}
	t.conn = conn
	logger.Trace("start to wait signal")
	notifyRegistered = true
	go wstool.WaitSignal(t, conn, t.ip, t.ipStr, t.WhitelistChangeHandle, t.DropUserHandle, t.WhitelistChangeChan, t.DropUserChan, t.exit, t.logger)
	wstool.WSWriteJson(session, logger, &schemalessConnResp{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
	})
}

type schemalessWriteReq struct {
	ReqID        uint64 `json:"req_id"`
	Protocol     int    `json:"protocol"`
	Precision    string `json:"precision"`
	TTL          int    `json:"ttl"`
	Data         string `json:"data"`
	TableNameKey string `json:"table_name_key"`
}

type schemalessResp struct {
	Code         int    `json:"code"`
	Message      string `json:"message"`
	ReqID        uint64 `json:"req_id"`
	Action       string `json:"action"`
	Timing       int64  `json:"timing"`
	AffectedRows int    `json:"affected_rows"`
	TotalRows    int32  `json:"total_rows"`
}

func (t *TaosSchemaless) insert(ctx context.Context, session *melody.Session, req schemalessWriteReq) {
	action := SchemalessWrite
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("schemaless insert request:%+v", req)
	isDebug := log.IsDebug()
	if req.Protocol == 0 {
		logger.Errorf("args error, protocol is 0")
		wsSchemalessErrorMsg(ctx, session, logger, 0xffff, "args error", action, req.ReqID)
		return
	}
	if t.conn == nil {
		logger.Errorf("server not connected")
		wsSchemalessErrorMsg(ctx, session, logger, 0xffff, "server not connected", action, req.ReqID)
		return
	}
	var result unsafe.Pointer
	defer func() {
		if result != nil {
			syncinterface.TaosSchemalessFree(result, logger, isDebug)
		}
	}()
	var err error
	var totalRows int32
	var affectedRows int
	totalRows, result = syncinterface.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(
		t.conn,
		req.Data,
		req.Protocol,
		req.Precision,
		req.TTL,
		int64(req.ReqID),
		req.TableNameKey,
		logger,
		isDebug,
	)

	if code := syncinterface.TaosError(result, logger, isDebug); code != 0 {
		err = tErrors.NewError(code, syncinterface.TaosErrorStr(result, logger, isDebug))
	}
	if err != nil {
		logger.Errorf("insert error, err:%s", err)
		wstool.WSError(ctx, session, logger, err, action, req.ReqID)
		return
	}
	affectedRows = syncinterface.TaosAffectedRows(result, logger, isDebug)
	logger.Tracef("insert success, affected rows:%d, total rows:%d", affectedRows, totalRows)
	resp := &schemalessResp{
		ReqID:        req.ReqID,
		Action:       action,
		Timing:       wstool.GetDuration(ctx),
		AffectedRows: affectedRows,
		TotalRows:    totalRows,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type WSSchemalessErrorResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func wsSchemalessErrorMsg(ctx context.Context, session *melody.Session, logger *logrus.Entry, code int, message string, action string, reqID uint64) {
	data := &WSSchemalessErrorResp{
		Code:    code & 0xffff,
		Message: message,
		Action:  action,
		ReqID:   reqID,
		Timing:  wstool.GetDuration(ctx),
	}
	wstool.WSWriteJson(session, logger, data)
}

func init() {
	c := NewSchemalessController()
	controller.AddController(c)
}
