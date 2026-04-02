package stmt

import (
	"container/list"
	"context"
	"database/sql/driver"
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
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	stmtCommon "github.com/taosdata/taosadapter/v3/driver/common/stmt"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/types"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

type STMTController struct {
	stmtM *melody.Melody
}

func NewSTMTController() *STMTController {
	stmtM := melody.New()
	stmtM.Upgrader.EnableCompression = true
	stmtM.Config.MaxMessageSize = 0

	stmtM.HandleConnect(func(session *melody.Session) {
		monitor.RecordWSStmtConn()
		logger := wstool.GetLogger(session)
		logger.Debug("ws connect")
		session.Set(TaosStmtKey, NewTaosStmt(session, logger))
	})

	stmtM.HandleMessage(func(session *melody.Session, data []byte) {
		t := session.MustGet(TaosStmtKey).(*TaosStmt)
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
			err := json.Unmarshal(data, &action)
			if err != nil {
				logger.Errorf("unmarshal ws request error, err:%s", err)
				logger.WithError(err).Errorln("unmarshal ws request")
				return
			}
			if action.Action != STMTConnect {
				logger.Debugf("get ws message data:%s", data)
			}
			switch action.Action {
			case wstool.ClientVersion:
				wstool.WSWriteVersion(session, logger)
			case STMTConnect:
				var req StmtConnectReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal connect args, err:%s, args:%s", err, action.Args)
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal connect request args")
					return
				}
				logger.Debugf("get ws message, connect action:%s", &req)
				t.connect(ctx, session, &req)
			case STMTInit:
				var req StmtInitReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal init args, err:%s, args:%s", err, action.Args)
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal init args")
					return
				}
				t.init(ctx, session, &req)
			case STMTPrepare:
				var req StmtPrepareReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal prepare args, err:%s, args:%s", err, action.Args)
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal prepare args")
					return
				}
				t.prepare(ctx, session, &req)
			case STMTSetTableName:
				var req StmtSetTableNameReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal set_table_name args, err:%s, args:%s", err, action.Args)
					logger.WithField(config.ReqIDKey, req.ReqID).WithError(err).Errorln("unmarshal set table name args")
					return
				}
				t.setTableName(ctx, session, &req)
			case STMTSetTags:
				var req StmtSetTagsReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal set_tags args, err:%s, args:%s", err, action.Args)
					return
				}
				t.setTags(ctx, session, &req)
			case STMTBind:
				var req StmtBindReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal bind args, err:%s, args:%s", err, action.Args)
					return
				}
				t.bind(ctx, session, &req)
			case STMTAddBatch:
				var req StmtAddBatchReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal add_batch args, err:%s, args:%s", err, action.Args)
					return
				}
				t.addBatch(ctx, session, &req)
			case STMTExec:
				var req StmtExecReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal exec args, err:%s, args:%s", err, action.Args)
					return
				}
				t.exec(ctx, session, &req)
			case STMTClose:
				var req StmtClose
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal close args, err:%s, args:%s", err, action.Args)
					return
				}
				t.close(ctx, session, &req)
			case STMTGetColFields:
				var req StmtGetColFieldsReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal close args, err:%s, args:%s", err, action.Args)
					return
				}
				t.getColFields(ctx, session, &req)
			case STMTGetTagFields:
				var req StmtGetTagFieldsReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.WithError(err).Errorln("unmarshal get_tag_fields args")
					return
				}
				t.getTagFields(ctx, session, &req)
			default:
				logger.Errorf("unknown action:%s", action.Action)
				return
			}
		}()
	})

	stmtM.HandleMessageBinary(func(session *melody.Session, data []byte) {
		t := session.MustGet(TaosStmtKey).(*TaosStmt)
		if t.IsClosed() {
			return
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			if t.IsClosed() {
				return
			}
			logger := wstool.GetLogger(session)
			logger.Tracef("get ws block message data:%+v", data)
			ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now())
			//p0 uin64  req_id
			//p0+8 uint64  stmt_id
			//p0+16 uint64 (1 (set tag) 2 (bind))
			//p0+24 raw block
			p0 := unsafe.Pointer(&data[0])
			reqID := *(*uint64)(p0)
			stmtID := *(*uint64)(tools.AddPointer(p0, uintptr(8)))
			action := *(*uint64)(tools.AddPointer(p0, uintptr(16)))
			logger.Debugf("get ws message binary QID:0x%x, stmtID:%d, action:%d", reqID, stmtID, action)
			block := tools.AddPointer(p0, uintptr(24))
			columns := parser.RawBlockGetNumOfCols(block)
			rows := parser.RawBlockGetNumOfRows(block)
			switch action {
			case BindMessage:
				t.bindBlock(ctx, session, reqID, stmtID, int(rows), int(columns), block)
			case SetTagsMessage:
				t.setTagsBlock(ctx, session, reqID, stmtID, int(rows), int(columns), block)
			}
		}()
	})

	stmtM.HandleClose(func(session *melody.Session, i int, s string) error {
		logger := wstool.GetLogger(session)
		logger.Debugf("ws close, code:%d, msg %s", i, s)
		CloseWs(session)
		return nil
	})

	stmtM.HandleError(func(session *melody.Session, err error) {
		wstool.LogWSError(session, err)
		CloseWs(session)
	})

	stmtM.HandleDisconnect(func(session *melody.Session) {
		monitor.RecordWSStmtDisconnect()
		logger := wstool.GetLogger(session)
		logger.Debug("ws disconnect")
		CloseWs(session)
	})
	return &STMTController{stmtM: stmtM}
}

func CloseWs(session *melody.Session) {
	t, exist := session.Get(TaosStmtKey)
	if exist && t != nil {
		t.(*TaosStmt).Close()
	}
}

func (s *STMTController) Init(ctl gin.IRouter) {
	ctl.GET("rest/stmt", func(c *gin.Context) {
		sessionID := generator.GetSessionID()
		logger := log.GetLogger("STM").WithFields(logrus.Fields{
			config.SessionIDKey: sessionID})
		_ = s.stmtM.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": logger})
	})
}

type TaosStmt struct {
	conn            unsafe.Pointer
	stmtIndexLocker sync.RWMutex
	StmtList        *list.List
	logger          *logrus.Entry
	stmtIndex       uint64
	closed          uint32
	exit            chan struct{}
	wstool.NotifyHandles
	session *melody.Session
	ip      net.IP
	ipStr   string
	wg      sync.WaitGroup
	mutex   sync.Mutex
	once    sync.Once
}

func NewTaosStmt(session *melody.Session, logger *logrus.Entry) *TaosStmt {
	ipAddr := iptool.GetRealIP(session.Request)
	return &TaosStmt{
		StmtList: list.New(),
		exit:     make(chan struct{}),
		session:  session,
		ip:       ipAddr,
		ipStr:    ipAddr.String(),
		logger:   logger,
	}
}

func (t *TaosStmt) UnlockAndExit(logger *logrus.Entry, isDebug bool) {
	logger.Trace("close session")
	s := log.GetLogNow(isDebug)
	_ = t.session.Close()
	logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
	t.Unlock()
	s = log.GetLogNow(isDebug)
	t.Close()
	logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
}

func (t *TaosStmt) Lock(logger *logrus.Entry, isDebug bool) {
	s := log.GetLogNow(isDebug)
	logger.Trace("get handler lock")
	t.mutex.Lock()
	logger.Debugf("get handler lock cost:%s", log.GetLogDuration(isDebug, s))
}

func (t *TaosStmt) Unlock() {
	t.mutex.Unlock()
}

func (t *TaosStmt) IsClosed() bool {
	return atomic.LoadUint32(&t.closed) == 1
}

func (t *TaosStmt) setClosed() {
	atomic.StoreUint32(&t.closed, 1)
}

type StmtItem struct {
	index uint64
	stmt  unsafe.Pointer
	sync.Mutex
}

func (s *StmtItem) clean(logger *logrus.Entry) {
	s.Lock()
	defer s.Unlock()
	if s.stmt != nil {
		syncinterface.TaosStmtClose(s.stmt, logger, log.IsDebug())
	}
}

func (t *TaosStmt) addStmtItem(stmt *StmtItem) {
	index := atomic.AddUint64(&t.stmtIndex, 1)
	stmt.index = index
	t.stmtIndexLocker.Lock()
	defer t.stmtIndexLocker.Unlock()
	t.StmtList.PushBack(stmt)
	monitor.WSStmtStmtCount.Inc()
}

func (t *TaosStmt) getStmtItem(index uint64) *list.Element {
	t.stmtIndexLocker.RLock()
	defer t.stmtIndexLocker.RUnlock()
	root := t.StmtList.Front()
	if root == nil {
		return nil
	}
	rootIndex := root.Value.(*StmtItem).index
	if rootIndex == index {
		return root
	}
	item := root.Next()
	for {
		if item == nil || item == root {
			return nil
		}
		if item.Value.(*StmtItem).index == index {
			return item
		}
		item = item.Next()
	}
}

func (t *TaosStmt) removeStmtItem(item *list.Element) {
	t.stmtIndexLocker.Lock()
	t.StmtList.Remove(item)
	t.stmtIndexLocker.Unlock()
	monitor.WSStmtStmtCount.Dec()
}

type StmtConnectReq struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
}

func (r *StmtConnectReq) String() string {
	builder := &strings.Builder{}

	builder.WriteString("{")
	_, _ = fmt.Fprintf(builder, "req_id: %d,", r.ReqID)
	_, _ = fmt.Fprintf(builder, "user: %q,", r.User)
	builder.WriteString("password: \"[HIDDEN]\",")
	_, _ = fmt.Fprintf(builder, "db: %q,", r.DB)
	builder.WriteString("}")

	return builder.String()
}

type StmtConnectResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *TaosStmt) connect(ctx context.Context, session *melody.Session, req *StmtConnectReq) {
	action := STMTConnect
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
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "duplicate connections", action, req.ReqID, nil)
		return
	}
	conn, err := syncinterface.TaosConnect("", req.User, req.Password, req.DB, 0, logger, isDebug)
	if err != nil {
		logger.Errorf("connect error, err:%s", err)
		wsStmtError(ctx, session, logger, err, action, req.ReqID, nil)
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
		logger.Errorf("register change whitelist error, err:%s", err)
		syncinterface.TaosClose(conn, logger, isDebug)
		wstool.WSError(ctx, session, logger, err, action, req.ReqID)
		return
	}
	logger.Trace("register drop user")
	err = tool.RegisterDropUser(conn, t.DropUserHandle, logger, isDebug)
	if err != nil {
		logger.Errorf("register drop user error, err:%s", err)
		syncinterface.TaosClose(conn, logger, isDebug)
		wstool.WSError(ctx, session, logger, err, action, req.ReqID)
		return
	}
	t.conn = conn
	notifyRegistered = true
	go wstool.WaitSignal(t, conn, t.ip, t.ipStr, t.WhitelistChangeHandle, t.DropUserHandle, t.WhitelistChangeChan, t.DropUserChan, t.exit, t.logger)
	wstool.WSWriteJson(session, logger, &StmtConnectResp{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
	})
}

type StmtInitReq struct {
	ReqID uint64 `json:"req_id"`
}
type StmtInitResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) init(ctx context.Context, session *melody.Session, req *StmtInitReq) {
	action := STMTInit
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("stmt init request:%+v", req)
	if t.conn == nil {
		logger.Error("server not connected")
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "server not connected", action, req.ReqID, nil)
		return
	}
	isDebug := log.IsDebug()
	stmt := syncinterface.TaosStmtInitWithReqID(t.conn, int64(req.ReqID), logger, isDebug)
	if stmt == nil {
		errStr := syncinterface.TaosStmtErrStr(stmt, logger, isDebug)
		logger.Errorf("stmt init error, err:%s", errStr)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, errStr, action, req.ReqID, nil)
		return
	}
	stmtItem := &StmtItem{
		stmt: stmt,
	}
	t.addStmtItem(stmtItem)
	logger.Tracef("stmt init sucess, stmt_id:%d, stmt pointer:%p", stmtItem.index, stmt)
	resp := &StmtInitResp{Action: action, ReqID: req.ReqID, StmtID: stmtItem.index, Timing: wstool.GetDuration(ctx)}
	wstool.WSWriteJson(session, logger, resp)
}

type StmtPrepareReq struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
	SQL    string `json:"sql"`
}
type StmtPrepareResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) prepare(ctx context.Context, session *melody.Session, req *StmtPrepareReq) {
	action := STMTPrepare
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("stmt prepare, stmt_id:%d, sql:%s", req.StmtID, req.SQL)

	if t.conn == nil {
		logger.Error("server not connected")
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "server not connected", action, req.ReqID, &req.StmtID)
		return
	}

	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "stmt is nil", action, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	isDebug := log.IsDebug()
	code := syncinterface.TaosStmtPrepare(stmt.stmt, req.SQL, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosStmtErrStr(stmt.stmt, logger, isDebug)
		logger.Errorf("stmt prepare error, code:%d, msg:%s", code, errStr)
		wsStmtErrorMsg(ctx, session, logger, code, errStr, action, req.ReqID, &req.StmtID)
		return
	}
	logger.Tracef("stmt prepare success, stmt_id:%d", req.StmtID)
	resp := &StmtPrepareResp{
		Action: action,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
		Timing: wstool.GetDuration(ctx),
	}
	wstool.WSWriteJson(session, logger, resp)
}

type StmtSetTableNameReq struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
	Name   string `json:"name"`
}
type StmtSetTableNameResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) setTableName(ctx context.Context, session *melody.Session, req *StmtSetTableNameReq) {
	action := STMTSetTableName
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("stmt set table name, stmt_id:%d, name:%s", req.StmtID, req.Name)
	if t.conn == nil {
		logger.Error("server not connected")
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "server not connected", action, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "stmt is nil", action, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	isDebug := log.IsDebug()
	code := syncinterface.TaosStmtSetTBName(stmt.stmt, req.Name, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosStmtErrStr(stmt.stmt, logger, isDebug)
		logger.Errorf("stmt set table name error, code:%d, msg:%s", code, errStr)
		wsStmtErrorMsg(ctx, session, logger, code, errStr, action, req.ReqID, &req.StmtID)
		return
	}
	resp := &StmtSetTableNameResp{
		Action: action,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
		Timing: wstool.GetDuration(ctx),
	}
	logger.Tracef("stmt set table name success, stmt_id:%d", req.StmtID)
	wstool.WSWriteJson(session, logger, resp)
}

type StmtSetTagsReq struct {
	ReqID  uint64          `json:"req_id"`
	StmtID uint64          `json:"stmt_id"`
	Tags   json.RawMessage `json:"tags"`
}

type StmtSetTagsResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) setTags(ctx context.Context, session *melody.Session, req *StmtSetTagsReq) {
	action := STMTSetTags
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("stmt set tags, stmt_id:%d, tags:%+v", req.StmtID, req.Tags)
	if t.conn == nil {
		logger.Error("server not connected")
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "server not connected", action, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "stmt is nil", action, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	isDebug := log.IsDebug()
	code, tagNums, tagFields := syncinterface.TaosStmtGetTagFields(stmt.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosStmtErrStr(stmt.stmt, logger, isDebug)
		logger.Errorf("stmt get tag fields error, code:%d, msg:%s", code, errStr)
		wsStmtErrorMsg(ctx, session, logger, code, errStr, action, req.ReqID, &req.StmtID)
		return
	}
	defer func() {
		syncinterface.TaosStmtReclaimFields(stmt.stmt, tagFields, logger, isDebug)
	}()
	resp := &StmtSetTagsResp{
		Action: action,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
	}
	if tagNums == 0 {
		logger.Trace("no tags")
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	s := log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	//tags := make([][]driver.Value, tagNums)
	//for i := 0; i < tagNums; i++ {
	//	tags[i] = []driver.Value{req.Tags[i]}
	//}
	s = log.GetLogNow(isDebug)
	data, err := StmtParseTag(req.Tags, fields)
	logger.Debugf("stmt parse tag json cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("stmt parse tag json error, err:%s", err)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, fmt.Sprintf("stmt parse tag json:%s", err.Error()), action, req.ReqID, &req.StmtID)
		return
	}
	code = syncinterface.TaosStmtSetTags(stmt.stmt, data, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosStmtErrStr(stmt.stmt, logger, isDebug)
		logger.Errorf("stmt set tags error, code:%d, msg:%s", code, errStr)
		wsStmtErrorMsg(ctx, session, logger, code, errStr, action, req.ReqID, &req.StmtID)
		return
	}
	resp.Timing = wstool.GetDuration(ctx)
	logger.Trace("stmt set tags success")
	wstool.WSWriteJson(session, logger, resp)
}

type StmtGetTagFieldsReq struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtGetTagFieldsResp struct {
	Code    int                     `json:"code"`
	Message string                  `json:"message"`
	Action  string                  `json:"action"`
	ReqID   uint64                  `json:"req_id"`
	Timing  int64                   `json:"timing"`
	StmtID  uint64                  `json:"stmt_id"`
	Fields  []*stmtCommon.StmtField `json:"fields"`
}

func (t *TaosStmt) getTagFields(ctx context.Context, session *melody.Session, req *StmtGetTagFieldsReq) {
	action := STMTGetTagFields
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("stmt get tag fields, stmt_id:%d", req.StmtID)
	if t.conn == nil {
		logger.Error("server not connected")
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "server not connected", action, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "stmt is nil", action, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	isDebug := log.IsDebug()
	code, tagNums, tagFields := syncinterface.TaosStmtGetTagFields(stmt.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosStmtErrStr(stmt.stmt, logger, isDebug)
		logger.Errorf("stmt get tag fields error, code:%d, msg:%s", code, errStr)
		wsStmtErrorMsg(ctx, session, logger, code, errStr, action, req.ReqID, &req.StmtID)
		return
	}
	defer func() {
		syncinterface.TaosStmtReclaimFields(stmt.stmt, tagFields, logger, isDebug)
	}()
	resp := &StmtGetTagFieldsResp{
		Action: action,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
	}
	if tagNums == 0 {
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	s := log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	resp.Fields = fields
	resp.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, logger, resp)
}

type StmtGetColFieldsReq struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

type StmtGetColFieldsResp struct {
	Code    int                     `json:"code"`
	Message string                  `json:"message"`
	Action  string                  `json:"action"`
	ReqID   uint64                  `json:"req_id"`
	Timing  int64                   `json:"timing"`
	StmtID  uint64                  `json:"stmt_id"`
	Fields  []*stmtCommon.StmtField `json:"fields"`
}

func (t *TaosStmt) getColFields(ctx context.Context, session *melody.Session, req *StmtGetColFieldsReq) {
	action := STMTGetColFields
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("stmt get tag fields, stmt_id:%d", req.StmtID)
	if t.conn == nil {
		logger.Error("server not connected")
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "server not connected", action, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "stmt is nil", action, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	isDebug := log.IsDebug()
	code, colNums, colFields := syncinterface.TaosStmtGetColFields(stmt.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosStmtErrStr(stmt.stmt, logger, isDebug)
		logger.Errorf("stmt get col fields error, code:%d, msg:%s", code, errStr)
		wsStmtErrorMsg(ctx, session, logger, code, errStr, action, req.ReqID, &req.StmtID)
		return
	}
	defer func() {
		syncinterface.TaosStmtReclaimFields(stmt.stmt, colFields, logger, isDebug)
	}()
	resp := &StmtGetColFieldsResp{
		Action: action,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
	}
	if colNums == 0 {
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	s := log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(colNums, colFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	resp.Fields = fields
	resp.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, logger, resp)
}

type StmtBindReq struct {
	ReqID   uint64          `json:"req_id"`
	StmtID  uint64          `json:"stmt_id"`
	Columns json.RawMessage `json:"columns"`
}
type StmtBindResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) bind(ctx context.Context, session *melody.Session, req *StmtBindReq) {
	action := STMTBind
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("stmt bind, stmt_id:%d, cols:%s", req.StmtID, req.Columns)

	if t.conn == nil {
		logger.Error("server not connected")
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "server not connected", action, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "stmt is nil", action, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	isDebug := log.IsDebug()
	code, colNums, colFields := syncinterface.TaosStmtGetColFields(stmt.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosStmtErrStr(stmt.stmt, logger, isDebug)
		logger.Errorf("stmt get col fields error, code:%d, msg:%s", code, errStr)
		wsStmtErrorMsg(ctx, session, logger, code, errStr, action, req.ReqID, &req.StmtID)
		return
	}
	defer func() {
		syncinterface.TaosStmtReclaimFields(stmt.stmt, colFields, logger, isDebug)
	}()
	resp := &StmtBindResp{
		Action: action,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
	}
	if colNums == 0 {
		resp.Timing = wstool.GetDuration(ctx)
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	s := log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(colNums, colFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	fieldTypes := make([]*types.ColumnType, colNums)
	var err error

	for i := 0; i < colNums; i++ {
		fieldTypes[i], err = fields[i].GetType()
		if err != nil {
			logger.Errorf("stmt get column type error, err:%s", err)
			wsStmtErrorMsg(ctx, session, logger, 0xffff, fmt.Sprintf("stmt get column type error, err:%s", err.Error()), action, req.ReqID, &req.StmtID)
			return
		}
	}
	s = log.GetLogNow(isDebug)
	data, err := StmtParseColumn(req.Columns, fields, fieldTypes)
	logger.Debugf("stmt parse column json cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("stmt parse column json error, err:%s", err)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, fmt.Sprintf("stmt parse column json:%s", err.Error()), action, req.ReqID, &req.StmtID)
		return
	}
	code = syncinterface.TaosStmtBindParamBatch(stmt.stmt, data, fieldTypes, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosStmtErrStr(stmt.stmt, logger, isDebug)
		logger.Errorf("stmt bind error, code:%d, msg:%s", code, errStr)
		wsStmtErrorMsg(ctx, session, logger, code, errStr, action, req.ReqID, &req.StmtID)
		return
	}
	resp.Timing = wstool.GetDuration(ctx)
	logger.Trace("stmt bind success")
	wstool.WSWriteJson(session, logger, resp)
}

type StmtAddBatchReq struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}
type StmtAddBatchResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func (t *TaosStmt) addBatch(ctx context.Context, session *melody.Session, req *StmtAddBatchReq) {
	action := STMTAddBatch
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("stmt add batch, stmt_id:%d", req.StmtID)
	if t.conn == nil {
		logger.Error("server not connected")
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "server not connected", action, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "stmt is nil", action, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	isDebug := log.IsDebug()
	code := syncinterface.TaosStmtAddBatch(stmt.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosStmtErrStr(stmt.stmt, logger, isDebug)
		logger.Errorf("stmt add batch error, code:%d, msg:%s", code, errStr)
		wsStmtErrorMsg(ctx, session, logger, code, errStr, action, req.ReqID, &req.StmtID)
		return
	}
	resp := &StmtAddBatchResp{
		Action: action,
		ReqID:  req.ReqID,
		StmtID: req.StmtID,
		Timing: wstool.GetDuration(ctx),
	}
	logger.Trace("stmt add batch success")
	wstool.WSWriteJson(session, logger, resp)
}

type StmtExecReq struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}
type StmtExecResp struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	Action   string `json:"action"`
	ReqID    uint64 `json:"req_id"`
	Timing   int64  `json:"timing"`
	StmtID   uint64 `json:"stmt_id"`
	Affected int    `json:"affected"`
}

func (t *TaosStmt) exec(ctx context.Context, session *melody.Session, req *StmtExecReq) {
	action := STMTExec
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("stmt exec, stmt_id:%d", req.StmtID)
	if t.conn == nil {
		logger.Error("server not connected")
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "server not connected", action, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "stmt is nil", action, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	isDebug := log.IsDebug()
	code := syncinterface.TaosStmtExecute(stmt.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosStmtErrStr(stmt.stmt, logger, isDebug)
		logger.Errorf("stmt exec error, code:%d, msg:%s", code, errStr)
		wsStmtErrorMsg(ctx, session, logger, code, errStr, action, req.ReqID, &req.StmtID)
		return
	}
	s := log.GetLogNow(isDebug)
	affected := syncinterface.TaosStmtAffectedRowsOnce(stmt.stmt, logger, isDebug)
	logger.Debugf("stmt_affected_rows_once cost:%s", log.GetLogDuration(isDebug, s))
	resp := &StmtExecResp{
		Action:   action,
		ReqID:    req.ReqID,
		StmtID:   req.StmtID,
		Timing:   wstool.GetDuration(ctx),
		Affected: affected,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type StmtClose struct {
	ReqID  uint64 `json:"req_id"`
	StmtID uint64 `json:"stmt_id"`
}

func (t *TaosStmt) close(ctx context.Context, session *melody.Session, req *StmtClose) {
	action := STMTClose
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("stmt close, stmt_id:%d", req.StmtID)
	if t.conn == nil {
		logger.Error("server not connected")
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "server not connected", action, req.ReqID, &req.StmtID)
		return
	}
	stmtItem := t.getStmtItem(req.StmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", req.StmtID)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "stmt is nil", action, req.ReqID, &req.StmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	t.removeStmtItem(stmtItem)
	stmt.clean(logger)
}

func (t *TaosStmt) setTagsBlock(ctx context.Context, session *melody.Session, reqID, stmtID uint64, rows, columns int, block unsafe.Pointer) {
	action := STMTSetTags
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, reqID)
	logger.Tracef("stmt set tags with block, stmt_id:%d", stmtID)
	if rows != 1 {
		logger.Errorf("rows not equal 1")
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "rows not equal 1", action, reqID, &stmtID)
		return
	}
	if t.conn == nil {
		logger.Error("server not connected")
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "server not connected", action, reqID, &stmtID)
		return
	}
	stmtItem := t.getStmtItem(stmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", stmtID)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "stmt is nil", action, reqID, &stmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	isDebug := log.IsDebug()
	code, tagNums, tagFields := syncinterface.TaosStmtGetTagFields(stmt.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosStmtErrStr(stmt.stmt, logger, isDebug)
		logger.Errorf("stmt get tag fields error, code:%d, msg:%s", code, errStr)
		wsStmtErrorMsg(ctx, session, logger, code, errStr, action, reqID, &stmtID)
		return
	}
	defer func() {
		syncinterface.TaosStmtReclaimFields(stmt.stmt, tagFields, logger, isDebug)
	}()
	resp := &StmtSetTagsResp{
		Action: action,
		ReqID:  reqID,
		StmtID: stmtID,
	}
	if tagNums == 0 {
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	if columns != tagNums {
		logger.Errorf("stmt tags count not match, columns:%d, tagNums:%d", columns, tagNums)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "stmt tags count not match", action, reqID, &stmtID)
		return
	}
	s := log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(tagNums, tagFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	tags := BlockConvert(block, rows, fields, nil)
	logger.Debugf("block concert cost:%s", log.GetLogDuration(isDebug, s))
	reTags := make([]driver.Value, tagNums)
	for i := 0; i < tagNums; i++ {
		reTags[i] = tags[i][0]
	}
	code = syncinterface.TaosStmtSetTags(stmt.stmt, reTags, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosStmtErrStr(stmt.stmt, logger, isDebug)
		logger.Errorf("stmt set tags error, code:%d, msg:%s", code, errStr)
		wsStmtErrorMsg(ctx, session, logger, code, errStr, action, reqID, &stmtID)
		return
	}
	resp.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, logger, resp)
}

func (t *TaosStmt) bindBlock(ctx context.Context, session *melody.Session, reqID, stmtID uint64, rows, columns int, block unsafe.Pointer) {
	action := STMTBind
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, reqID)
	logger.Tracef("stmt bind with block, stmt_id:%d", stmtID)
	if t.conn == nil {
		logger.Error("server not connected")
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "server not connected", action, reqID, &stmtID)
		return
	}
	stmtItem := t.getStmtItem(stmtID)
	if stmtItem == nil {
		logger.Errorf("stmt is nil, stmt_id:%d", stmtID)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "stmt is nil", action, reqID, &stmtID)
		return
	}
	stmt := stmtItem.Value.(*StmtItem)
	isDebug := log.IsDebug()
	code, colNums, colFields := syncinterface.TaosStmtGetColFields(stmt.stmt, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosStmtErrStr(stmt.stmt, logger, isDebug)
		logger.Errorf("stmt get col fields error, code:%d, msg:%s", code, errStr)
		wsStmtErrorMsg(ctx, session, logger, code, errStr, action, reqID, &stmtID)
		return
	}
	defer func() {
		syncinterface.TaosStmtReclaimFields(stmt.stmt, colFields, logger, isDebug)
	}()
	resp := &StmtBindResp{
		Action: action,
		ReqID:  reqID,
		StmtID: stmtID,
	}
	if colNums == 0 {
		resp.Timing = wstool.GetDuration(ctx)
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	s := log.GetLogNow(isDebug)
	fields := wrapper.StmtParseFields(colNums, colFields)
	logger.Debugf("stmt parse fields cost:%s", log.GetLogDuration(isDebug, s))
	fieldTypes := make([]*types.ColumnType, colNums)
	var err error
	for i := 0; i < colNums; i++ {
		fieldTypes[i], err = fields[i].GetType()
		if err != nil {
			logger.Errorf("stmt get column type error, err:%s", err)
			wsStmtErrorMsg(ctx, session, logger, 0xffff, fmt.Sprintf("stmt get column type error, err:%s", err.Error()), action, reqID, &stmtID)
			return
		}
	}
	if columns != colNums {
		logger.Errorf("stmt column count not match, columns:%d, colNums:%d", columns, colNums)
		wsStmtErrorMsg(ctx, session, logger, 0xffff, "stmt column count not match", action, reqID, &stmtID)
		return
	}
	s = log.GetLogNow(isDebug)
	data := BlockConvert(block, rows, fields, fieldTypes)
	logger.Debugf("block convert cost:%s", log.GetLogDuration(isDebug, s))
	code = syncinterface.TaosStmtBindParamBatch(stmt.stmt, data, fieldTypes, logger, isDebug)
	if code != httperror.SUCCESS {
		errStr := syncinterface.TaosStmtErrStr(stmt.stmt, logger, isDebug)
		logger.Errorf("stmt bind error, code:%d, msg:%s", code, errStr)
		wsStmtErrorMsg(ctx, session, logger, code, errStr, action, reqID, &stmtID)
		return
	}
	resp.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, logger, resp)
}

func (t *TaosStmt) Close() {
	t.Lock(t.logger, log.IsDebug())
	defer t.Unlock()
	if t.IsClosed() {
		return
	}
	t.logger.Trace("stmt connection close")
	t.setClosed()
	t.stop()
}

func (t *TaosStmt) stop() {
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
		t.cleanUp(t.logger)
		if t.conn != nil {
			syncinterface.TaosClose(t.conn, t.logger, log.IsDebug())
			t.conn = nil
		}
		close(t.exit)
	})
}

func (t *TaosStmt) cleanUp(logger *logrus.Entry) {
	t.stmtIndexLocker.Lock()
	defer t.stmtIndexLocker.Unlock()
	defer func() {
		t.StmtList = t.StmtList.Init()
	}()
	root := t.StmtList.Front()
	if root == nil {
		return
	}
	root.Value.(*StmtItem).clean(logger)
	monitor.WSStmtStmtCount.Dec()
	item := root.Next()
	for {
		if item == nil || item == root {
			return
		}
		item.Value.(*StmtItem).clean(logger)
		monitor.WSStmtStmtCount.Dec()
		item = item.Next()
	}
}

type WSStmtErrorResp struct {
	Code    int     `json:"code"`
	Message string  `json:"message"`
	Action  string  `json:"action"`
	ReqID   uint64  `json:"req_id"`
	Timing  int64   `json:"timing"`
	StmtID  *uint64 `json:"stmt_id,omitempty"`
}

func wsStmtErrorMsg(ctx context.Context, session *melody.Session, logger *logrus.Entry, code int, message string, action string, reqID uint64, stmtID *uint64) {
	data := &WSStmtErrorResp{
		Code:    code & 0xffff,
		Message: message,
		Action:  action,
		ReqID:   reqID,
		Timing:  wstool.GetDuration(ctx),
		StmtID:  stmtID,
	}
	wstool.WSWriteJson(session, logger, data)
}
func wsStmtError(ctx context.Context, session *melody.Session, logger *logrus.Entry, err error, action string, reqID uint64, stmtID *uint64) {
	e, is := err.(*tErrors.TaosError)
	if is {
		wsStmtErrorMsg(ctx, session, logger, int(e.Code)&0xffff, e.ErrStr, action, reqID, stmtID)
	} else {
		wsStmtErrorMsg(ctx, session, logger, 0xffff, err.Error(), action, reqID, stmtID)
	}
}

func init() {
	c := NewSTMTController()
	controller.AddController(c)
}
