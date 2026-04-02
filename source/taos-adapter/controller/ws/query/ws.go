package query

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/tools/generator"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/jsontype"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

type QueryController struct {
	queryM *melody.Melody
}

func NewQueryController() *QueryController {
	queryM := melody.New()
	queryM.Upgrader.EnableCompression = true
	queryM.Config.MaxMessageSize = 0

	queryM.HandleConnect(func(session *melody.Session) {
		monitor.RecordWSQueryConn()
		logger := wstool.GetLogger(session)
		ipAddr := iptool.GetRealIP(session.Request)
		logger.WithField("ip", ipAddr.String()).Debug("ws connect")
		session.Set(TaosSessionKey, NewTaos(session, logger))
	})

	queryM.HandleMessage(func(session *melody.Session, data []byte) {
		t := session.MustGet(TaosSessionKey).(*Taos)
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
			var action WSAction
			err := json.Unmarshal(data, &action)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal ws request")
				return
			}
			if action.Action != WSConnect {
				logger.Debugf("get ws message data: %s", data)
			}
			switch action.Action {
			case wstool.ClientVersion:
				wstool.WSWriteVersion(session, logger)
			case WSConnect:
				var wsConnect WSConnectReq
				err = json.Unmarshal(action.Args, &wsConnect)
				if err != nil {
					logger.WithError(err).Errorln("unmarshal connect request args")
					return
				}
				logger.Debugf("get ws message, connect action:%s", &wsConnect)
				t.connect(ctx, session, &wsConnect)
			case WSQuery:
				var wsQuery WSQueryReq
				err = json.Unmarshal(action.Args, &wsQuery)
				if err != nil {
					logger.WithError(err).WithField(config.ReqIDKey, wsQuery.ReqID).Errorln("unmarshal query args")
					return
				}
				t.query(ctx, session, &wsQuery)
			case WSFetch:
				var wsFetch WSFetchReq
				err = json.Unmarshal(action.Args, &wsFetch)
				if err != nil {
					logger.WithError(err).WithField(config.ReqIDKey, wsFetch.ReqID).Errorln("unmarshal fetch args")
					return
				}
				t.fetch(ctx, session, &wsFetch)
			case WSFetchBlock:
				var fetchBlock WSFetchBlockReq
				err = json.Unmarshal(action.Args, &fetchBlock)
				if err != nil {
					logger.WithError(err).WithField(config.ReqIDKey, fetchBlock.ReqID).Errorln("unmarshal fetch_block args")
					return
				}
				t.fetchBlock(ctx, session, &fetchBlock)
			case WSFreeResult:
				var fetchJson WSFreeResultReq
				err = json.Unmarshal(action.Args, &fetchJson)
				if err != nil {
					logger.WithError(err).WithField(config.ReqIDKey, fetchJson.ReqID).Errorln("unmarshal fetch_json args")
					return
				}
				t.freeResult(&fetchJson)
			default:
				logger.WithError(err).Errorln("unknown action :" + action.Action)
				return
			}
		}()

	})

	queryM.HandleMessageBinary(func(session *melody.Session, data []byte) {
		t := session.MustGet(TaosSessionKey).(*Taos)
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
			logger.Tracef("get ws block message data:%+v", data)
			p0 := unsafe.Pointer(&data[0])
			reqID := *(*uint64)(p0)
			messageID := *(*uint64)(tools.AddPointer(p0, uintptr(8)))
			action := *(*uint64)(tools.AddPointer(p0, uintptr(16)))
			logger.Tracef("get ws message binary QID:0x%x, messageID:%d, action:%d", reqID, messageID, action)
			switch action {
			case TMQRawMessage:
				length := *(*uint32)(tools.AddPointer(p0, uintptr(24)))
				metaType := *(*uint16)(tools.AddPointer(p0, uintptr(28)))
				b := tools.AddPointer(p0, uintptr(30))
				t.writeRaw(ctx, session, reqID, messageID, length, metaType, b)
			case RawBlockMessage:
				numOfRows := *(*int32)(tools.AddPointer(p0, uintptr(24)))
				tableNameLength := *(*uint16)(tools.AddPointer(p0, uintptr(28)))
				tableName := make([]byte, tableNameLength)
				for i := 0; i < int(tableNameLength); i++ {
					tableName[i] = *(*byte)(tools.AddPointer(p0, uintptr(30+i)))
				}
				b := tools.AddPointer(p0, uintptr(30+tableNameLength))
				t.writeRawBlock(ctx, session, reqID, int(numOfRows), string(tableName), b)
			case RawBlockMessageWithFields:
				numOfRows := *(*int32)(tools.AddPointer(p0, uintptr(24)))
				tableNameLength := int(*(*uint16)(tools.AddPointer(p0, uintptr(28))))
				tableName := make([]byte, tableNameLength)
				for i := 0; i < tableNameLength; i++ {
					tableName[i] = *(*byte)(tools.AddPointer(p0, uintptr(30+i)))
				}
				b := tools.AddPointer(p0, uintptr(30+tableNameLength))
				blockLength := int(parser.RawBlockGetLength(b))
				numOfColumn := int(parser.RawBlockGetNumOfCols(b))
				fieldsBlock := tools.AddPointer(p0, uintptr(30+tableNameLength+blockLength))
				t.writeRawBlockWithFields(ctx, session, reqID, int(numOfRows), string(tableName), b, fieldsBlock, numOfColumn)
			}
		}()
	})

	queryM.HandleClose(func(session *melody.Session, i int, s string) error {
		//message := melody.FormatCloseMessage(i, "")
		//session.WriteControl(websocket.CloseMessage, message, time.Now().Add(time.Second))
		logger := wstool.GetLogger(session)
		logger.Debugf("ws close, code:%d, msg %s", i, s)
		CloseWs(session)
		return nil
	})

	queryM.HandleError(func(session *melody.Session, err error) {
		wstool.LogWSError(session, err)
		CloseWs(session)
	})

	queryM.HandleDisconnect(func(session *melody.Session) {
		monitor.RecordWSQueryDisconnect()
		logger := wstool.GetLogger(session)
		logger.Debug("ws disconnect")
		CloseWs(session)
	})
	return &QueryController{queryM: queryM}
}

func CloseWs(session *melody.Session) {
	t, exist := session.Get(TaosSessionKey)
	if exist && t != nil {
		t.(*Taos).Close()
	}
}

func (s *QueryController) Init(ctl gin.IRouter) {
	ctl.GET("rest/ws", func(c *gin.Context) {
		// generate session id
		sessionID := generator.GetSessionID()
		logger := log.GetLogger("QRY").WithFields(logrus.Fields{
			config.SessionIDKey: sessionID})
		_ = s.queryM.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": logger})
	})
}

type Taos struct {
	conn         unsafe.Pointer
	resultLocker sync.RWMutex
	Results      *list.List
	resultIndex  uint64
	logger       *logrus.Entry
	closed       uint32
	exit         chan struct{}
	wstool.NotifyHandles
	session *melody.Session
	ip      net.IP
	wg      sync.WaitGroup
	ipStr   string
	mutex   sync.Mutex
	once    sync.Once
}

func (t *Taos) Lock(logger *logrus.Entry, isDebug bool) {
	logger.Trace("get handler lock")
	s := log.GetLogNow(isDebug)
	t.mutex.Lock()
	logger.Debugf("get handler lock cost:%s", log.GetLogDuration(isDebug, s))
}

func (t *Taos) Unlock() {
	t.mutex.Unlock()
}

func NewTaos(session *melody.Session, logger *logrus.Entry) *Taos {
	ipAddr := iptool.GetRealIP(session.Request)
	return &Taos{
		Results: list.New(),
		exit:    make(chan struct{}, 1),
		session: session,
		ip:      ipAddr,
		ipStr:   ipAddr.String(),
		logger:  logger,
	}
}

func (t *Taos) IsClosed() bool {
	return atomic.LoadUint32(&t.closed) == 1
}

func (t *Taos) setClosed() {
	atomic.StoreUint32(&t.closed, 1)
}

func (t *Taos) UnlockAndExit(logger *logrus.Entry, isDebug bool) {
	logger.Trace("close session")
	s := log.GetLogNow(isDebug)
	_ = t.session.Close()
	logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
	t.Unlock()
	logger.Trace("close handler")
	s = log.GetLogNow(isDebug)
	t.Close()
	logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
}

type Result struct {
	index       uint64
	TaosResult  unsafe.Pointer
	FieldsCount int
	Header      *wrapper.RowsHeader
	Lengths     []int
	Size        int
	Block       unsafe.Pointer
	precision   int
	buffer      *bytes.Buffer
	logger      *logrus.Entry
	sync.Mutex
}

func (r *Result) FreeResult(logger *logrus.Entry) {
	r.Lock()
	defer r.Unlock()
	r.FieldsCount = 0
	r.Header = nil
	r.Lengths = nil
	r.Size = 0
	r.precision = 0
	r.Block = nil
	if logger == nil {
		logger = r.logger
	}
	if r.TaosResult != nil {
		async.FreeResultAsync(r.TaosResult, logger, log.IsDebug())
		r.TaosResult = nil
	}
}

func (t *Taos) addResult(result *Result) {
	index := atomic.AddUint64(&t.resultIndex, 1)
	result.index = index
	result.logger = t.logger.WithField("resultID", index)
	t.logger.Trace("get result locker")
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.resultLocker.Lock()
	t.logger.Debugf("get result locker cost:%s", log.GetLogDuration(isDebug, s))
	t.Results.PushBack(result)
	t.logger.Trace("add result to list finished")
	t.resultLocker.Unlock()
	monitor.WSQuerySqlResultCount.Inc()
}

func (t *Taos) getResult(index uint64) *list.Element {
	t.resultLocker.RLock()
	defer t.resultLocker.RUnlock()
	root := t.Results.Front()
	if root == nil {
		return nil
	}
	rootIndex := root.Value.(*Result).index
	if rootIndex == index {
		return root
	}
	item := root.Next()
	for {
		if item == nil || item == root {
			return nil
		}
		if item.Value.(*Result).index == index {
			return item
		}
		item = item.Next()
	}
}

func (t *Taos) removeResult(item *list.Element) {
	t.resultLocker.Lock()
	defer t.resultLocker.Unlock()
	t.Results.Remove(item)
	monitor.WSQuerySqlResultCount.Dec()
}

type WSConnectReq struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
}

func (r *WSConnectReq) String() string {
	builder := &strings.Builder{}

	builder.WriteString("{")
	_, _ = fmt.Fprintf(builder, "req_id: %d,", r.ReqID)
	_, _ = fmt.Fprintf(builder, "user: %q,", r.User)
	builder.WriteString("password: \"[HIDDEN]\",")
	_, _ = fmt.Fprintf(builder, "db: %q,", r.DB)
	builder.WriteString("}")

	return builder.String()
}

type WSConnectResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *Taos) connect(ctx context.Context, session *melody.Session, req *WSConnectReq) {
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSConnect, config.ReqIDKey: req.ReqID},
	)
	isDebug := log.IsDebug()
	t.Lock(logger, isDebug)
	defer t.Unlock()
	if t.IsClosed() {
		logger.Trace("server closed")
		return
	}
	if t.conn != nil {
		logger.Trace("duplicate connections")
		wsErrorMsg(ctx, session, logger, 0xffff, "duplicate connections", WSConnect, req.ReqID)
		return
	}
	conn, err := syncinterface.TaosConnect("", req.User, req.Password, req.DB, 0, logger, isDebug)
	if err != nil {
		logger.WithError(err).Errorln("connect to TDengine error")
		wstool.WSError(ctx, session, logger, err, WSConnect, req.ReqID)
		return
	}
	logger.Trace("get whitelist")
	s := log.GetLogNow(isDebug)
	allowlist, blocklist, err := tool.GetWhitelist(conn, logger, isDebug)
	logger.Debugf("get whitelist cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.WithError(err).Errorln("get whitelist error")
		syncinterface.TaosClose(conn, logger, isDebug)
		wstool.WSError(ctx, session, logger, err, WSConnect, req.ReqID)
		return
	}
	allowlistStr := tool.IpNetSliceToString(allowlist)
	blocklistStr := tool.IpNetSliceToString(blocklist)
	logger.Tracef("check whitelist, ip: %s, allowlist: %s, blocklist: %s", t.ipStr, allowlistStr, blocklistStr)
	valid := tool.CheckWhitelist(allowlist, blocklist, t.ip)
	if !valid {
		logger.Errorf("ip not in whitelist, ip: %s, allowlist: %s, blocklist: %s", t.ipStr, allowlistStr, blocklistStr)
		syncinterface.TaosClose(conn, logger, isDebug)
		wstool.WSErrorMsg(ctx, session, logger, 0xffff, "whitelist prohibits current IP access", WSConnect, req.ReqID)
		return
	}
	t.InitNotifyHandles()
	notifyRegistered := false
	defer func() {
		if !notifyRegistered {
			t.PutNotifyHandles()
		}
	}()
	s = log.GetLogNow(isDebug)
	logger.Trace("register whitelist change")
	err = tool.RegisterChangeWhitelist(conn, t.WhitelistChangeHandle, logger, isDebug)
	logger.Debugf("register whitelist change cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.WithError(err).Errorln("register whitelist change error")
		syncinterface.TaosClose(conn, logger, isDebug)
		wstool.WSError(ctx, session, logger, err, WSConnect, req.ReqID)
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("register drop user")
	err = tool.RegisterDropUser(conn, t.DropUserHandle, logger, isDebug)
	logger.Debugf("register drop user cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.WithError(err).Errorln("register drop user error")
		syncinterface.TaosClose(conn, logger, isDebug)
		wstool.WSError(ctx, session, logger, err, WSConnect, req.ReqID)
		return
	}
	t.conn = conn
	logger.Trace("start wait signal goroutine")
	notifyRegistered = true
	go wstool.WaitSignal(t, conn, t.ip, t.ipStr, t.WhitelistChangeHandle, t.DropUserHandle, t.WhitelistChangeChan, t.DropUserChan, t.exit, t.logger)
	wstool.WSWriteJson(session, logger, &WSConnectResp{
		Action: WSConnect,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
	})
}

type WSQueryReq struct {
	ReqID uint64 `json:"req_id"`
	SQL   string `json:"sql"`
}

type WSQueryResult struct {
	Code          int                `json:"code"`
	Message       string             `json:"message"`
	Action        string             `json:"action"`
	ReqID         uint64             `json:"req_id"`
	Timing        int64              `json:"timing"`
	ID            uint64             `json:"id"`
	IsUpdate      bool               `json:"is_update"`
	AffectedRows  int                `json:"affected_rows"`
	FieldsCount   int                `json:"fields_count"`
	FieldsNames   []string           `json:"fields_names"`
	FieldsTypes   jsontype.JsonUint8 `json:"fields_types"`
	FieldsLengths []int64            `json:"fields_lengths"`
	Precision     int                `json:"precision"`
}

func (t *Taos) query(ctx context.Context, session *melody.Session, req *WSQueryReq) {
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSQuery, config.ReqIDKey: req.ReqID},
	)
	if t.conn == nil {
		logger.Trace("server not connected")
		wsErrorMsg(ctx, session, logger, 0xffff, "server not connected", WSQuery, req.ReqID)
		return
	}
	logger.Tracef("req_id: 0x%x,query sql: %s", req.ReqID, req.SQL)
	sqlType := monitor.WSRecordRequest(req.SQL)
	isDebug := log.IsDebug()
	logger.Trace("get handler from pool")
	s := log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	logger.Tracef("get handler cost:%s", log.GetLogDuration(isDebug, s))
	defer async.GlobalAsync.HandlerPool.Put(handler)
	logger.Trace("execute query")
	s = log.GetLogNow(isDebug)
	result := async.GlobalAsync.TaosQuery(t.conn, logger, isDebug, req.SQL, handler, int64(req.ReqID))
	logger.Tracef("query cost:%s", log.GetLogDuration(isDebug, s))
	code := syncinterface.TaosError(result.Res, logger, isDebug)
	if code != httperror.SUCCESS {
		monitor.WSRecordResult(sqlType, false)
		errStr := syncinterface.TaosErrorStr(result.Res, logger, isDebug)
		logger.Errorf("query error, code: %d, message: %s", code, errStr)
		logger.Trace("get thread lock for free result")
		async.FreeResultAsync(result.Res, logger, isDebug)
		wsErrorMsg(ctx, session, logger, code, errStr, WSQuery, req.ReqID)
		return
	}
	monitor.WSRecordResult(sqlType, true)
	logger.Trace("check is_update_query")
	s = log.GetLogNow(isDebug)
	isUpdate := syncinterface.TaosIsUpdateQuery(result.Res, logger, isDebug)
	logger.Debugf("is_update_query %t cost: %s", isUpdate, log.GetLogDuration(isDebug, s))
	queryResult := &WSQueryResult{Action: WSQuery, ReqID: req.ReqID}
	if isUpdate {
		var affectRows int
		s = log.GetLogNow(isDebug)
		affectRows = syncinterface.TaosAffectedRows(result.Res, logger, isDebug)
		logger.Debugf("affected_rows %d, cost: %s", affectRows, log.GetLogDuration(isDebug, s))
		queryResult.IsUpdate = true
		queryResult.AffectedRows = affectRows
		logger.Trace("get thread lock for free result")
		async.FreeResultAsync(result.Res, logger, isDebug)
		queryResult.Timing = wstool.GetDuration(ctx)
		wstool.WSWriteJson(session, logger, queryResult)
		return
	}
	// query
	s = log.GetLogNow(isDebug)
	fieldsCount := syncinterface.TaosNumFields(result.Res, logger, isDebug)
	logger.Debugf("num_fields %d cost: %s", fieldsCount, log.GetLogDuration(isDebug, s))
	queryResult.FieldsCount = fieldsCount
	s = log.GetLogNow(isDebug)
	rowsHeader, _ := syncinterface.ReadColumn(result.Res, fieldsCount, logger, isDebug)
	logger.Tracef("read column cost:%s", log.GetLogDuration(isDebug, s))
	queryResult.FieldsNames = rowsHeader.ColNames
	queryResult.FieldsLengths = rowsHeader.ColLength
	queryResult.FieldsTypes = rowsHeader.ColTypes
	s = log.GetLogNow(isDebug)
	precision := syncinterface.TaosResultPrecision(result.Res, logger, isDebug)
	logger.Tracef("result_precision %d, cost: %s ", precision, log.GetLogDuration(isDebug, s))
	queryResult.Precision = precision
	resultItem := &Result{
		TaosResult:  result.Res,
		FieldsCount: fieldsCount,
		Header:      rowsHeader,
		precision:   precision,
	}
	logger.Trace("add result to list")
	t.addResult(resultItem)
	queryResult.ID = resultItem.index
	queryResult.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, logger, queryResult)
}

type WSWriteMetaResp struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Action    string `json:"action"`
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
	Timing    int64  `json:"timing"`
}

func (t *Taos) writeRaw(ctx context.Context, session *melody.Session, reqID, messageID uint64, length uint32, metaType uint16, data unsafe.Pointer) {
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSWriteRaw, config.ReqIDKey: reqID},
	)
	isDebug := log.IsDebug()
	t.Lock(logger, isDebug)
	defer t.Unlock()
	if t.IsClosed() {
		logger.Trace("server closed")
		return
	}
	if t.conn == nil {
		logger.Error("server not connected")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "server not connected", WSWriteRaw, reqID, &messageID)
		return
	}
	errCode := syncinterface.TMQWriteRaw(t.conn, length, metaType, data, logger, isDebug)
	if errCode != 0 {
		errStr := syncinterface.TMQErr2Str(errCode, logger, isDebug)
		logger.Errorf("write raw meta error, code: %d, message: %s", errCode, errStr)
		wsErrorMsg(ctx, session, logger, int(errCode)&0xffff, errStr, WSWriteRaw, reqID)
		return
	}
	resp := &WSWriteMetaResp{Action: WSWriteRaw, ReqID: reqID, MessageID: messageID, Timing: wstool.GetDuration(ctx)}
	wstool.WSWriteJson(session, logger, resp)
}

type WSWriteRawBlockResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *Taos) writeRawBlock(ctx context.Context, session *melody.Session, reqID uint64, numOfRows int, tableName string, rawBlock unsafe.Pointer) {
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSWriteRawBlock, config.ReqIDKey: reqID},
	)
	isDebug := log.IsDebug()
	t.Lock(logger, isDebug)
	defer t.Unlock()
	if t.IsClosed() {
		logger.Trace("server closed")
		return
	}
	if t.conn == nil {
		wsErrorMsg(ctx, session, logger, 0xffff, "server not connected", WSWriteRawBlock, reqID)
		return
	}
	errCode := syncinterface.TaosWriteRawBlockWithReqID(t.conn, numOfRows, rawBlock, tableName, int64(reqID), logger, isDebug)
	if errCode != 0 {
		errStr := syncinterface.TMQErr2Str(int32(errCode), logger, isDebug)
		logger.Errorf("write raw block error, code: %d, message: %s", errCode, errStr)
		wsErrorMsg(ctx, session, logger, errCode&0xffff, errStr, WSWriteRawBlock, reqID)
		return
	}
	resp := &WSWriteRawBlockResp{Action: WSWriteRawBlock, ReqID: reqID, Timing: wstool.GetDuration(ctx)}
	wstool.WSWriteJson(session, logger, resp)
}

type WSWriteRawBlockWithFieldsResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *Taos) writeRawBlockWithFields(ctx context.Context, session *melody.Session, reqID uint64, numOfRows int, tableName string, rawBlock unsafe.Pointer, fields unsafe.Pointer, numFields int) {
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSWriteRawBlockWithFields, config.ReqIDKey: reqID},
	)
	isDebug := log.IsDebug()
	t.Lock(logger, isDebug)
	defer t.Unlock()
	if t.IsClosed() {
		logger.Trace("server closed")
		return
	}
	if t.conn == nil {
		logger.Errorf("server not connected")
		wsErrorMsg(ctx, session, logger, 0xffff, "server not connected", WSWriteRawBlockWithFields, reqID)
		return
	}
	errCode := syncinterface.TaosWriteRawBlockWithFieldsWithReqID(t.conn, numOfRows, rawBlock, tableName, fields, numFields, int64(reqID), logger, isDebug)
	if errCode != 0 {
		errStr := syncinterface.TMQErr2Str(int32(errCode), logger, isDebug)
		logger.Errorf("write raw block with fields error, code: %d, message: %s", errCode, errStr)
		wsErrorMsg(ctx, session, logger, errCode&0xffff, errStr, WSWriteRawBlockWithFields, reqID)
		return
	}
	resp := &WSWriteRawBlockWithFieldsResp{Action: WSWriteRawBlockWithFields, ReqID: reqID, Timing: wstool.GetDuration(ctx)}
	wstool.WSWriteJson(session, logger, resp)
}

type WSFetchReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

type WSFetchResp struct {
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

func (t *Taos) fetch(ctx context.Context, session *melody.Session, req *WSFetchReq) {
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSFetch, config.ReqIDKey: req.ReqID},
	)
	if t.conn == nil {
		logger.Errorf("server not connected")
		wsErrorMsg(ctx, session, logger, 0xffff, "server not connected", WSFetch, req.ReqID)
		return
	}
	isDebug := log.IsDebug()
	resultItem := t.getResult(req.ID)
	if resultItem == nil {
		logger.Debug("result is nil")
		wsErrorMsg(ctx, session, logger, 0xffff, "result is nil", WSFetch, req.ReqID)
		return
	}
	resultS := resultItem.Value.(*Result)
	resultS.Lock()
	if resultS.TaosResult == nil {
		resultS.Unlock()
		logger.Debug("result is nil")
		wsErrorMsg(ctx, session, logger, 0xffff, "result is nil", WSFetch, req.ReqID)
		return
	}
	s := log.GetLogNow(isDebug)
	handler := async.GlobalAsync.HandlerPool.Get()
	logger.Tracef("get handler cost:%s", log.GetLogDuration(isDebug, s))
	defer async.GlobalAsync.HandlerPool.Put(handler)
	s = log.GetLogNow(isDebug)
	logger.Trace("call fetch_raw_block_a")
	result := async.GlobalAsync.TaosFetchRawBlockA(resultS.TaosResult, logger, isDebug, handler)
	logger.Debugf("fetch_raw_block_a cost:%s", log.GetLogDuration(isDebug, s))
	if result.N == 0 {
		logger.Trace("fetch raw block completed")
		resultS.Unlock()
		t.FreeResult(resultItem, logger)
		wstool.WSWriteJson(session, logger, &WSFetchResp{
			Action:    WSFetch,
			ReqID:     req.ReqID,
			Timing:    wstool.GetDuration(ctx),
			ID:        req.ID,
			Completed: true,
		})
		return
	}
	if result.N < 0 {
		errStr := syncinterface.TaosErrorStr(result.Res, logger, isDebug)
		logger.Errorf("fetch raw block error, code: %d, message: %s", result.N, errStr)
		resultS.Unlock()
		t.FreeResult(resultItem, logger)
		wsErrorMsg(ctx, session, logger, result.N&0xffff, errStr, WSFetch, req.ReqID)
		return
	}
	s = log.GetLogNow(isDebug)
	resultS.Lengths = syncinterface.TaosFetchLengths(resultS.TaosResult, resultS.FieldsCount, logger, isDebug)
	logger.Debugf("fetch_lengths %d cost: %s", resultS.Lengths, log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	logger.Trace("get raw block")
	block := syncinterface.TaosGetRawBlock(resultS.TaosResult, logger, isDebug)
	logger.Debugf("get_raw_block cost:%s", log.GetLogDuration(isDebug, s))
	resultS.Block = block
	resultS.Size = result.N
	resultS.Unlock()
	wstool.WSWriteJson(session, logger, &WSFetchResp{
		Action:  WSFetch,
		ReqID:   req.ReqID,
		Timing:  wstool.GetDuration(ctx),
		ID:      req.ID,
		Lengths: resultS.Lengths,
		Rows:    result.N,
	})
}

type WSFetchBlockReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

func (t *Taos) fetchBlock(ctx context.Context, session *melody.Session, req *WSFetchBlockReq) {
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSFetchBlock, config.ReqIDKey: req.ReqID},
	)
	if t.conn == nil {
		logger.Error("server not connected")
		wsErrorMsg(ctx, session, logger, 0xffff, "server not connected", WSFetchBlock, req.ReqID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	resultItem := t.getResult(req.ID)
	if resultItem == nil {
		wsErrorMsg(ctx, session, logger, 0xffff, "result is nil", WSFetchBlock, req.ReqID)
		return
	}
	resultS := resultItem.Value.(*Result)
	resultS.Lock()
	if resultS.TaosResult == nil {
		resultS.Unlock()
		wsErrorMsg(ctx, session, logger, 0xffff, "result is nil", WSFetchBlock, req.ReqID)
		return
	}
	if resultS.Block == nil {
		resultS.Unlock()
		wsErrorMsg(ctx, session, logger, 0xffff, "block is nil", WSFetchBlock, req.ReqID)
		return
	}
	blockLength := int(parser.RawBlockGetLength(resultS.Block))
	if resultS.buffer == nil {
		resultS.buffer = new(bytes.Buffer)
	} else {
		resultS.buffer.Reset()
	}
	resultS.buffer.Grow(blockLength + 16)
	wstool.WriteUint64(resultS.buffer, uint64(wstool.GetDuration(ctx)))
	wstool.WriteUint64(resultS.buffer, req.ID)
	for offset := 0; offset < blockLength; offset++ {
		resultS.buffer.WriteByte(*((*byte)(unsafe.Pointer(uintptr(resultS.Block) + uintptr(offset)))))
	}
	b := resultS.buffer.Bytes()
	resultS.Unlock()
	logger.Debugf("handle binary content cost:%s", log.GetLogDuration(isDebug, s))
	wstool.WSWriteBinary(session, b, logger)
}

type WSFreeResultReq struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

func (t *Taos) freeResult(req *WSFreeResultReq) {
	logger := t.logger.WithFields(
		logrus.Fields{"action": WSFreeResult, config.ReqIDKey: req.ReqID},
	)
	if t.conn == nil {
		logger.Trace("server not connected")
		return
	}
	resultItem := t.getResult(req.ID)
	if resultItem == nil {
		logger.Trace("result not found")
		return
	}
	resultS, ok := resultItem.Value.(*Result)
	if ok && resultS != nil {
		t.removeResult(resultItem)
		resultS.FreeResult(logger)
	}
}

func (t *Taos) FreeResult(element *list.Element, logger *logrus.Entry) {
	if element == nil {
		return
	}
	r := element.Value.(*Result)
	if r != nil {
		r.FreeResult(logger)
	}
	t.removeResult(element)
}

func (t *Taos) freeAllResult() {
	t.resultLocker.Lock()
	defer t.resultLocker.Unlock()
	defer func() {
		// clean up the list
		t.Results = t.Results.Init()
	}()
	root := t.Results.Front()
	if root == nil {
		return
	}
	root.Value.(*Result).FreeResult(t.logger)
	monitor.WSQuerySqlResultCount.Dec()
	item := root.Next()
	for {
		if item == nil || item == root {
			return
		}
		item.Value.(*Result).FreeResult(t.logger)
		monitor.WSQuerySqlResultCount.Dec()
		item = item.Next()
	}
}

func (t *Taos) Close() {
	isDebug := log.IsDebug()
	t.Lock(t.logger, isDebug)
	defer t.Unlock()
	if t.IsClosed() {
		t.logger.Trace("server closed")
		return
	}
	t.setClosed()
	t.stop()
}

func (t *Taos) stop() {
	t.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		done := make(chan struct{})
		go func() {
			t.logger.Trace("wait task to finish")
			t.wg.Wait()
			close(done)
		}()
		select {
		case <-ctx.Done():
			t.logger.Warn("wait stop over 1 minute")
			<-done
			break
		case <-done:
			t.logger.Trace("all task finished")
		}
		t.logger.Debug("wait stop done")
		t.logger.Trace("free all result")
		isDebug := log.IsDebug()
		s := log.GetLogNow(isDebug)
		t.freeAllResult()
		t.logger.Debugf("free all result cost:%s", log.GetLogDuration(isDebug, s))
		if t.conn != nil {
			t.logger.Trace("get thread lock for close")
			syncinterface.TaosClose(t.conn, t.logger, isDebug)
			t.conn = nil
		}
		t.logger.Trace("close exit channel")
		close(t.exit)
	})
}

type WSAction struct {
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args"`
}

type WSVersionResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	Version string `json:"version"`
}

type WSErrorResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func wsErrorMsg(ctx context.Context, session *melody.Session, logger *logrus.Entry, code int, message string, action string, reqID uint64) {
	data := &WSErrorResp{
		Code:    code & 0xffff,
		Message: message,
		Action:  action,
		ReqID:   reqID,
		Timing:  wstool.GetDuration(ctx),
	}
	wstool.WSWriteJson(session, logger, data)
}

type WSTMQErrorResp struct {
	Code      int     `json:"code"`
	Message   string  `json:"message"`
	Action    string  `json:"action"`
	ReqID     uint64  `json:"req_id"`
	Timing    int64   `json:"timing"`
	MessageID *uint64 `json:"message_id,omitempty"`
}

func wsTMQErrorMsg(ctx context.Context, session *melody.Session, logger *logrus.Entry, code int, message string, action string, reqID uint64, messageID *uint64) {
	data := &WSTMQErrorResp{
		Code:      code & 0xffff,
		Message:   message,
		Action:    action,
		ReqID:     reqID,
		Timing:    wstool.GetDuration(ctx),
		MessageID: messageID,
	}
	wstool.WSWriteJson(session, logger, data)
}

func init() {
	c := NewQueryController()
	controller.AddController(c)
}
