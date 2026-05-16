package tmq

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
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
	"github.com/taosdata/taosadapter/v3/db/asynctmq"
	"github.com/taosdata/taosadapter/v3/db/asynctmq/tmqhandle"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	taoserrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/tools/bytesutil"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/jsontype"
	"github.com/taosdata/taosadapter/v3/tools/melody"
	"github.com/taosdata/taosadapter/v3/version"
)

type TMQController struct {
	tmqM *melody.Melody
}

func NewTMQController() *TMQController {
	tmqM := melody.New()
	tmqM.Upgrader.EnableCompression = true
	tmqM.Config.MaxMessageSize = 0

	tmqM.HandleConnect(func(session *melody.Session) {
		monitor.RecordWSTMQConn()
		logger := wstool.GetLogger(session)
		logger.Debug("ws connect")
		session.Set(TaosTMQKey, NewTaosTMQ(session))
	})

	tmqM.HandleMessage(func(session *melody.Session, data []byte) {
		t := session.MustGet(TaosTMQKey).(*TMQ)
		if t.IsClosed() {
			return
		}
		ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now())
		logger := wstool.GetLogger(session)
		var action wstool.WSAction
		err := json.Unmarshal(data, &action)
		if err != nil {
			logger.Errorf("unmarshal ws request error, err:%s", err)
			return
		}
		if action.Action != TMQSubscribe {
			logger.Debugf("get ws message data:%s", data)
		}
		if action.Action == TMQPoll {
			var req TMQPollReq
			err = json.Unmarshal(action.Args, &req)
			if err != nil {
				logger.Errorf("unmarshal poll args, err:%s, args:%s", err.Error(), action.Args)
				return
			}
			req.ctx = ctx
			t.setPollRequest(&req)
			return
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			if t.IsClosed() {
				return
			}
			switch action.Action {
			case wstool.ClientVersion:
				reqID := uint64(0)
				if len(action.Args) != 0 {
					var req versionRequest
					err = json.Unmarshal(action.Args, &req)
					if err != nil {
						logger.Errorf("unmarshal version args, err:%s, args:%s", err.Error(), action.Args)
						return
					}
					reqID = req.ReqID
				}
				t.version(ctx, session, action.Action, reqID)
			case TMQSubscribe:
				var req TMQSubscribeReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal subscribe args, err:%s, args:%s", err.Error(), action.Args)
					return
				}
				logger.Debugf("get ws message, subscribe action:%s", &req)
				t.subscribe(ctx, session, &req)
			case TMQFetch:
				var req TMQFetchReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal fetch args, err:%s, args:%s", err, action.Args)
					return
				}
				t.fetch(ctx, session, &req)
			case TMQFetchBlock:
				var req TMQFetchBlockReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal fetch_block args, err:%s, args:%s", err, action.Args)
					return
				}
				t.fetchBlock(ctx, session, &req)
			case TMQCommit:
				var req TMQCommitReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal commit args, err:%s, args:%s", err, action.Args)
					return
				}
				t.commit(ctx, session, &req)
			case TMQFetchJsonMeta:
				var req TMQFetchJsonMetaReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal fetch_json_meta args, err:%s, args:%s", err, action.Args)
					return
				}
				t.fetchJsonMeta(ctx, session, &req)
			case TMQFetchRaw:
				var req TMQFetchRawReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal fetch_raw args, err:%s, args:%s", err, action.Args)
					return
				}
				t.fetchRawBlock(ctx, session, &req)
			case TMQFetchRawData:
				var req TMQFetchRawReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal fetch_raw_data args, err:%s, args:%s", err, action.Args)
					return
				}
				t.fetchRawBlockNew(ctx, session, &req)
			case TMQUnsubscribe:
				var req TMQUnsubscribeReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal unsubscribe args, err:%s, args:%s", err, action.Args)
					return
				}
				t.unsubscribe(ctx, session, &req)
			case TMQGetTopicAssignment:
				var req TMQGetTopicAssignmentReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal assignment args, err:%s, args:%s", err, action.Args)
					return
				}
				t.assignment(ctx, session, &req)
			case TMQSeek:
				var req TMQOffsetSeekReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal seek args, err:%s, args:%s", err, action.Args)
					return
				}
				t.offsetSeek(ctx, session, &req)
			case TMQCommitted:
				var req TMQCommittedReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal committed args, err:%s, args:%s", err, action.Args)
					return
				}
				session.MustGet(TaosTMQKey).(*TMQ).committed(ctx, session, &req)
			case TMQPosition:
				var req TMQPositionReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal position args, err:%s, args:%s", err, action.Args)
					return
				}
				session.MustGet(TaosTMQKey).(*TMQ).position(ctx, session, &req)
			case TMQListTopics:
				var req TMQListTopicsReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal list_topics args, err:%s, args:%s", err, action.Args)
					return
				}
				session.MustGet(TaosTMQKey).(*TMQ).listTopics(ctx, session, &req)
			case TMQCommitOffset:
				var req TMQCommitOffsetReq
				err = json.Unmarshal(action.Args, &req)
				if err != nil {
					logger.Errorf("unmarshal commit_offset args, err:%s, args:%s", err, action.Args)
					return
				}
				session.MustGet(TaosTMQKey).(*TMQ).commitOffset(ctx, session, &req)
			default:
				logger.Errorf("unknown action:%s", action.Action)
				return
			}
		}()
	})
	tmqM.HandleClose(func(session *melody.Session, i int, s string) error {
		//message := melody.FormatCloseMessage(i, "")
		//session.WriteControl(websocket.CloseMessage, message, time.Now().Add(time.Second))
		logger := wstool.GetLogger(session)
		logger.Debugf("ws close, code:%d, msg %s", i, s)
		CloseWs(session)
		return nil
	})

	tmqM.HandleError(func(session *melody.Session, err error) {
		wstool.LogWSError(session, err)
		CloseWs(session)
	})

	tmqM.HandleDisconnect(func(session *melody.Session) {
		monitor.RecordWSTMQDisconnect()
		logger := wstool.GetLogger(session)
		logger.Debug("ws disconnect")
		CloseWs(session)
	})
	return &TMQController{tmqM: tmqM}
}

func CloseWs(session *melody.Session) {
	t, exist := session.Get(TaosTMQKey)
	if exist && t != nil {
		t.(*TMQ).Close()
	}
}

func (s *TMQController) Init(ctl gin.IRouter) {
	ctl.GET("rest/tmq", func(c *gin.Context) {
		sessionID := generator.GetSessionID()
		logger := log.GetLogger("TMQ").WithFields(logrus.Fields{
			config.SessionIDKey: sessionID})
		_ = s.tmqM.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": logger})
	})
}

type TMQ struct {
	consumer           unsafe.Pointer
	logger             *logrus.Entry
	pollReqChan        chan *TMQPollReq
	tmpMessage         *Message
	asyncLocker        sync.Mutex
	thread             unsafe.Pointer
	handler            *tmqhandle.TMQHandler
	isAutoCommit       bool
	unsubscribed       bool
	closed             uint32
	autocommitInterval time.Duration
	nextTime           time.Time
	exit               chan struct{}
	wstool.NotifyHandles
	session *melody.Session
	ip      net.IP
	ipStr   string
	wg      sync.WaitGroup
	conn    unsafe.Pointer
	mutex   sync.Mutex
	once    sync.Once
}

func (t *TMQ) setPollRequest(req *TMQPollReq) {
	select {
	// try to send poll request to pollReqChan
	case t.pollReqChan <- req:
	default:
		// if pollReqChan is full, try drop the oldest poll request, then send the new poll request
		select {
		case r := <-t.pollReqChan:
			// drop the oldest poll request
			t.logger.Warnf("drop poll request,req:%+v", r)
			// send the new poll request
			t.pollReqChan <- req
		default:
			// send the new poll request
			t.pollReqChan <- req
		}
	}
}

func (t *TMQ) IsClosed() bool {
	return atomic.LoadUint32(&t.closed) == 1
}

func (t *TMQ) setClosed() {
	atomic.StoreUint32(&t.closed, 1)
}

type Message struct {
	Index    uint64
	Topic    string
	VGroupID int32
	Offset   int64
	Type     int32
	Database string
	CPointer unsafe.Pointer
	buffer   []byte
	sync.Mutex
}

func NewTaosTMQ(session *melody.Session) *TMQ {
	logger := wstool.GetLogger(session)
	ipAddr := iptool.GetRealIP(session.Request)
	return &TMQ{
		tmpMessage:         &Message{},
		handler:            tmqhandle.GlobalTMQHandlerPoll.Get(),
		thread:             asynctmq.InitTMQThread(),
		isAutoCommit:       true,
		autocommitInterval: time.Second * 5,
		exit:               make(chan struct{}),
		session:            session,
		ip:                 ipAddr,
		ipStr:              ipAddr.String(),
		logger:             logger,
		pollReqChan:        make(chan *TMQPollReq, 1),
	}
}

func (t *TMQ) waitPoll(logger *logrus.Entry) {
	defer func() {
		logger.Trace("exit wait poll")
	}()
	for {
		select {
		case <-t.exit:
			return
		case req := <-t.pollReqChan:
			t.poll(req.ctx, t.session, req)
		}
	}
}

func (t *TMQ) UnlockAndExit(logger *logrus.Entry, isDebug bool) {
	logger.Trace("close session")
	s := log.GetLogNow(isDebug)
	_ = t.session.Close()
	logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
	t.Unlock()
	s = log.GetLogNow(isDebug)
	t.Close()
	logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
}

func (t *TMQ) Lock(logger *logrus.Entry, isDebug bool) {
	s := log.GetLogNow(isDebug)
	logger.Debug("get handler lock")
	t.mutex.Lock()
	logger.Debugf("get handler lock finish, cost:%s", log.GetLogDuration(isDebug, s))
}

func (t *TMQ) Unlock() {
	t.mutex.Unlock()
}

type TMQSubscribeReq struct {
	ReqID                uint64            `json:"req_id"`
	User                 string            `json:"user"`
	Password             string            `json:"password"`
	DB                   string            `json:"db"`
	GroupID              string            `json:"group_id"`
	ClientID             string            `json:"client_id"`
	OffsetRest           string            `json:"offset_rest"` // typo
	OffsetReset          string            `json:"offset_reset"`
	Topics               []string          `json:"topics"`
	AutoCommit           string            `json:"auto_commit"`
	AutoCommitIntervalMS string            `json:"auto_commit_interval_ms"`
	SnapshotEnable       string            `json:"snapshot_enable"`
	WithTableName        string            `json:"with_table_name"`
	EnableBatchMeta      string            `json:"enable_batch_meta"`
	MsgConsumeExcluded   string            `json:"msg_consume_excluded"`
	SessionTimeoutMS     string            `json:"session_timeout_ms"`
	MaxPollIntervalMS    string            `json:"max_poll_interval_ms"`
	TZ                   string            `json:"tz"`
	App                  string            `json:"app"`
	IP                   string            `json:"ip"`
	Connector            string            `json:"connector"`
	MsgConsumeRawdata    string            `json:"msg_consume_rawdata"`
	Config               map[string]string `json:"config"`
}

func (r *TMQSubscribeReq) String() string {
	builder := &strings.Builder{}

	builder.WriteString("{")
	_, _ = fmt.Fprintf(builder, "req_id: %d,", r.ReqID)
	_, _ = fmt.Fprintf(builder, "user: %q,", r.User)
	builder.WriteString("password: \"[HIDDEN]\",")
	_, _ = fmt.Fprintf(builder, "db: %q,", r.DB)
	_, _ = fmt.Fprintf(builder, "group_id: %q,", r.GroupID)
	_, _ = fmt.Fprintf(builder, "client_id: %q,", r.ClientID)
	_, _ = fmt.Fprintf(builder, "offset_rest: %q,", r.OffsetRest)
	_, _ = fmt.Fprintf(builder, "offset_reset: %q,", r.OffsetReset)
	_, _ = fmt.Fprintf(builder, "topics: %v,", r.Topics)
	_, _ = fmt.Fprintf(builder, "auto_commit: %q,", r.AutoCommit)
	_, _ = fmt.Fprintf(builder, "auto_commit_interval_ms: %q,", r.AutoCommitIntervalMS)
	_, _ = fmt.Fprintf(builder, "snapshot_enable: %q,", r.SnapshotEnable)
	_, _ = fmt.Fprintf(builder, "with_table_name: %q,", r.WithTableName)
	_, _ = fmt.Fprintf(builder, "enable_batch_meta: %q,", r.EnableBatchMeta)
	_, _ = fmt.Fprintf(builder, "msg_consume_excluded: %q,", r.MsgConsumeExcluded)
	_, _ = fmt.Fprintf(builder, "session_timeout_ms: %q,", r.SessionTimeoutMS)
	_, _ = fmt.Fprintf(builder, "max_poll_interval_ms: %q,", r.MaxPollIntervalMS)
	_, _ = fmt.Fprintf(builder, "tz: %q,", r.TZ)
	_, _ = fmt.Fprintf(builder, "app: %q,", r.App)
	_, _ = fmt.Fprintf(builder, "ip: %q,", r.IP)
	_, _ = fmt.Fprintf(builder, "connector: %q,", r.Connector)
	_, _ = fmt.Fprintf(builder, "msg_consume_rawdata: %q,", r.MsgConsumeRawdata)

	builder.WriteString(" config: {")
	if len(r.Config) > 0 {
		first := true
		for k, v := range r.Config {
			if k == "td.connect.pass" || k == "td.connect.token" {
				continue
			}
			if !first {
				builder.WriteString(", ")
			}
			_, _ = fmt.Fprintf(builder, "%q: %q", k, v)
			first = false
		}
	}
	builder.WriteString("},")
	builder.WriteString("}")

	return builder.String()
}

type TMQSubscribeResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	Version string `json:"version"`
}

var ignoreSubscribeConfig = map[string]struct{}{
	"td.connect.user": {},
	"td.connect.pass": {},
	"td.connect.ip":   {},
	"td.connect.port": {},
}

type versionRequest struct {
	ReqID uint64 `json:"req_id"`
}
type versionResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	Version string `json:"version"`
}

func (t *TMQ) version(ctx context.Context, session *melody.Session, action string, redID uint64) {
	resp := &versionResponse{
		Action:  action,
		ReqID:   redID,
		Timing:  wstool.GetDuration(ctx),
		Version: version.TaosClientVersion,
	}
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, redID)
	wstool.WSWriteJson(session, logger, resp)
}

func (t *TMQ) subscribe(ctx context.Context, session *melody.Session, req *TMQSubscribeReq) {
	action := TMQSubscribe
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	// lock for consumer and unsubscribed
	// used for subscribe and unsubscribe
	t.Lock(logger, isDebug)
	defer t.Unlock()
	if t.IsClosed() {
		logger.Trace("server closed")
		return
	}
	if t.consumer != nil {
		if t.unsubscribed {
			logger.Debug("tmq resubscribe")
			topicList := syncinterface.TMQListNew(logger, isDebug)
			if topicList == nil {
				logger.Errorf("tmq list new error")
				wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq list new return nil", action, req.ReqID, nil)
				return
			}
			defer func() {
				syncinterface.TMQListDestroy(topicList, logger, isDebug)
			}()
			for _, topic := range req.Topics {
				errCode := syncinterface.TMQListAppend(topicList, topic, logger, isDebug)
				if errCode != 0 {
					errStr := syncinterface.TMQErr2Str(errCode, logger, isDebug)
					logger.Errorf("tmq list append error, code:%d, msg:%s", errCode, errStr)
					wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, action, req.ReqID, nil)
					return
				}
			}
			errCode := t.wrapperSubscribe(logger, isDebug, t.consumer, topicList)
			if errCode != 0 {
				errStr := syncinterface.TMQErr2Str(errCode, logger, isDebug)
				wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, action, req.ReqID, nil)
				return
			}
			t.unsubscribed = false
			wstool.WSWriteJson(session, logger, &TMQSubscribeResp{
				Action:  action,
				ReqID:   req.ReqID,
				Timing:  wstool.GetDuration(ctx),
				Version: version.TaosClientVersion,
			})
			return
		}
		logger.Errorf("tmq should have unsubscribed first")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq should have unsubscribed first", action, req.ReqID, nil)
		return

	}
	tmqConfig := syncinterface.TMQConfNew(logger, isDebug)
	if tmqConfig == nil {
		logger.Errorf("tmq conf new error")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq conf new return nil", action, req.ReqID, nil)
		return
	}
	defer func() {
		syncinterface.TMQConfDestroy(tmqConfig, logger, isDebug)
	}()
	// reset autocommit and autocommit interval
	t.isAutoCommit = false
	t.autocommitInterval = time.Second * 5
	t.nextTime = time.Time{}

	var tmqOptions = make(map[string]string)
	if len(req.GroupID) != 0 {
		tmqOptions["group.id"] = req.GroupID
	}
	if len(req.ClientID) != 0 {
		tmqOptions["client.id"] = req.ClientID
	}
	if len(req.DB) != 0 {
		tmqOptions["td.connect.db"] = req.DB
	}
	if len(req.SnapshotEnable) != 0 {
		tmqOptions["experimental.snapshot.enable"] = req.SnapshotEnable
	}
	if len(req.EnableBatchMeta) != 0 {
		tmqOptions["msg.enable.batchmeta"] = req.EnableBatchMeta
	}
	if len(req.SessionTimeoutMS) != 0 {
		tmqOptions["session.timeout.ms"] = req.SessionTimeoutMS
	}
	if len(req.MaxPollIntervalMS) != 0 {
		tmqOptions["max.poll.interval.ms"] = req.MaxPollIntervalMS
	}
	if len(req.MsgConsumeRawdata) != 0 {
		tmqOptions["msg.consume.rawdata"] = req.MsgConsumeRawdata
	}
	if len(req.WithTableName) != 0 {
		tmqOptions["msg.with.table.name"] = req.WithTableName
	}
	if len(req.MsgConsumeExcluded) != 0 {
		tmqOptions["msg.consume.excluded"] = req.MsgConsumeExcluded
	}
	if len(req.AutoCommit) != 0 {
		tmqOptions["enable.auto.commit"] = req.AutoCommit
	}
	if len(req.AutoCommitIntervalMS) != 0 {
		tmqOptions["auto.commit.interval.ms"] = req.AutoCommitIntervalMS
	}
	tmqOptions["td.connect.user"] = req.User
	tmqOptions["td.connect.pass"] = req.Password
	offsetReset := req.OffsetRest
	if len(req.OffsetReset) != 0 {
		offsetReset = req.OffsetReset
	}
	if len(offsetReset) != 0 {
		tmqOptions["auto.offset.reset"] = offsetReset
	}

	// set config
	for k, v := range req.Config {
		if _, ok := ignoreSubscribeConfig[k]; ok {
			continue
		}
		tmqOptions[k] = v
	}

	// autocommit always false
	if v, exists := tmqOptions["enable.auto.commit"]; exists {
		var err error
		autoCommit, err := strconv.ParseBool(v)
		if err != nil {
			logger.Errorf("parse auto commit:%s as bool error:%s", v, err)
			wsTMQErrorMsg(ctx, session, logger, 0xffff, err.Error(), action, req.ReqID, nil)
			return
		}
		t.isAutoCommit = autoCommit
	}
	tmqOptions["enable.auto.commit"] = "false"

	// autocommit interval
	if v, exists := tmqOptions["auto.commit.interval.ms"]; exists {
		autocommitIntervalMS, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			logger.Errorf("parse auto commit interval:%s as int error:%s", v, err)
			wsTMQErrorMsg(ctx, session, logger, 0xffff, err.Error(), action, req.ReqID, nil)
			return
		}
		t.autocommitInterval = time.Duration(autocommitIntervalMS) * time.Millisecond
	}

	// set tmq config
	var errCode int32
	logger.Debug("call tmq_conf_set")
	for k, v := range tmqOptions {
		errCode = syncinterface.TMQConfSet(tmqConfig, k, v, logger, isDebug)
		if errCode != TmqConfOk {
			// change error code to TSDB_CODE_INVALID_PARA
			returnCode := TsdbCodeInvalidPara
			var errStr string
			switch errCode {
			case TmqConfUnknown:
				errStr = fmt.Sprintf("unknown configuration key: %s", k)
			case TmqConfInvalid:
				errStr = fmt.Sprintf("invalid value for key '%s': %s", k, v)
			default:
				errStr = fmt.Sprintf("unexpected error (code %d) when setting key '%s' to value '%s'", errCode, k, v)
			}
			logger.Errorf("tmq conf set error, k:%s, v:%s, code:%d, return code:%d, msg:%s", k, v, errCode, returnCode, errStr)
			wsTMQErrorMsg(ctx, session, logger, returnCode, errStr, action, req.ReqID, nil)
			return
		}
	}
	logger.Debug("tmq_config_set finish")
	cPointer, err := t.wrapperConsumerNew(logger, isDebug, tmqConfig)
	if err != nil {
		logger.Errorf("tmq consumer new error:%s", err)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, err.Error(), action, req.ReqID, nil)
		return
	}
	topicList := syncinterface.TMQListNew(logger, isDebug)
	defer func() {
		syncinterface.TMQListDestroy(topicList, logger, isDebug)
	}()
	for _, topic := range req.Topics {
		logger.Tracef("tmq append topic:%s", topic)
		errCode = syncinterface.TMQListAppend(topicList, topic, logger, isDebug)
		if errCode != 0 {
			t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, taoserrors.NewError(int(errCode), syncinterface.TMQErr2Str(errCode, logger, isDebug)), fmt.Sprintf("tmq list append error, tpic:%s", topic))
			return
		}
	}

	errCode = t.wrapperSubscribe(logger, isDebug, cPointer, topicList)
	if errCode != 0 {
		t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, taoserrors.NewError(int(errCode), syncinterface.TMQErr2Str(errCode, logger, isDebug)), "tmq subscribe error")
		return
	}
	conn := syncinterface.TMQGetConnect(cPointer, logger, isDebug)
	logger.Debugf("call taos_fetch_whitelist_a, conn:%p", conn)
	allowlist, blocklist, err := tool.GetWhitelist(conn, logger, isDebug)
	logger.Debug("taos_fetch_whitelist_a finish")
	if err != nil {
		t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, err, "get whitelist error")
		return
	}
	allowlistStr := tool.IpNetSliceToString(allowlist)
	blocklistStr := tool.IpNetSliceToString(blocklist)
	logger.Tracef("check whitelist, ip: %s, allowlist: %s, blocklist: %s", t.ipStr, allowlistStr, blocklistStr)
	valid := tool.CheckWhitelist(allowlist, blocklist, t.ip)
	if !valid {
		errorExt := fmt.Sprintf("ip not in whitelist, ip: %s, allowlist: %s, blocklist: %s", t.ipStr, allowlistStr, blocklistStr)
		err = errors.New("whitelist prohibits current IP access")
		t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, err, errorExt)
		return
	}
	t.InitNotifyHandles()
	notifyRegistered := false
	defer func() {
		if !notifyRegistered {
			t.PutNotifyHandles()
		}
	}()
	logger.Debug("register change whitelist")
	err = tool.RegisterChangeWhitelist(conn, t.WhitelistChangeHandle, logger, isDebug)
	if err != nil {
		t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, err, "register change whitelist error")
		return
	}
	logger.Debug("register drop user")
	err = tool.RegisterDropUser(conn, t.DropUserHandle, logger, isDebug)
	if err != nil {
		t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, req.ReqID, err, "register drop user error")
		return
	}

	// set connection ip
	clientIP := t.ipStr
	if req.IP != "" {
		clientIP = req.IP
	}
	if !t.setConnectOption(
		ctx,
		cPointer,
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
		if !t.setConnectOption(
			ctx,
			cPointer,
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
		if !t.setConnectOption(
			ctx,
			cPointer,
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
		if !t.setConnectOption(
			ctx,
			cPointer,
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

	t.conn = conn
	t.consumer = cPointer
	logger.Debug("start to wait signal")
	notifyRegistered = true
	go t.waitPoll(t.logger)
	go wstool.WaitSignal(t, conn, t.ip, t.ipStr, t.WhitelistChangeHandle, t.DropUserHandle, t.WhitelistChangeChan, t.DropUserChan, t.exit, t.logger)
	wstool.WSWriteJson(session, logger, &TMQSubscribeResp{
		Action:  action,
		ReqID:   req.ReqID,
		Timing:  wstool.GetDuration(ctx),
		Version: version.TaosClientVersion,
	})
}

func (t *TMQ) setConnectOption(ctx context.Context, cPointer, conn unsafe.Pointer, session *melody.Session, logger *logrus.Entry, isDebug bool, action string, reqID uint64, option int, value string, optionName string) bool {
	logger.Debugf("set %s, value: %s", optionName, value)
	code := syncinterface.TaosOptionsConnection(conn, option, &value, logger, isDebug)
	logger.Debugf("set %s done", optionName)
	if code != 0 {
		t.closeConsumerWithErrLog(ctx, cPointer, session, logger, isDebug, action, reqID, taoserrors.NewError(code, syncinterface.TaosErrorStr(nil, logger, isDebug)), fmt.Sprintf("set %s error", optionName))
		return false
	}
	return true
}

func (t *TMQ) closeConsumerWithErrLog(
	ctx context.Context,
	consumer unsafe.Pointer,
	session *melody.Session,
	logger *logrus.Entry,
	isDebug bool,
	action string,
	reqID uint64,
	err error,
	errorExt string,
) {
	logger.Errorf("%s, err: %s", errorExt, err)
	errCode := t.wrapperCloseConsumer(logger, isDebug, consumer)
	if errCode != 0 {
		errMsg := syncinterface.TMQErr2Str(errCode, logger, isDebug)
		logger.Errorf("tmq close consumer error, consumer:%p, code:%d, msg:%s", t.consumer, errCode, errMsg)
	}
	wstool.WSError(ctx, session, logger, err, action, reqID)
}

type TMQCommitReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"` // unused
}

type TMQCommitResp struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Action    string `json:"action"`
	ReqID     uint64 `json:"req_id"`
	Timing    int64  `json:"timing"`
	MessageID uint64 `json:"message_id"`
}

func (t *TMQ) commit(ctx context.Context, session *melody.Session, req *TMQCommitReq) {
	action := TMQCommit
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("commit request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	// commit all
	isDebug := log.IsDebug()
	errCode, closed := t.wrapperCommit(logger, isDebug)
	if closed {
		return
	}
	if errCode != 0 {
		errStr := syncinterface.TMQErr2Str(errCode, logger, isDebug)
		logger.Errorf("tmq commit error, code: %d, message: %s", errCode, errStr)
		wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, action, req.ReqID, nil)
		return
	}
	resp := &TMQCommitResp{
		Action:    action,
		ReqID:     req.ReqID,
		MessageID: req.MessageID,
		Timing:    wstool.GetDuration(ctx),
	}
	wstool.WSWriteJson(session, logger, resp)
}

type TMQPollReq struct {
	ReqID        uint64  `json:"req_id"`
	BlockingTime int64   `json:"blocking_time"`
	MessageID    *uint64 `json:"message_id"`
	ctx          context.Context
}

func (req *TMQPollReq) String() string {
	if req.MessageID == nil {
		return fmt.Sprintf("&{ReqID:%d BlockingTime:%d MessageID:nil}", req.ReqID, req.BlockingTime)
	}
	return fmt.Sprintf("&{ReqID:%d BlockingTime:%d MessageID:%d}", req.ReqID, req.BlockingTime, *req.MessageID)
}

type TMQPollResp struct {
	Code        int    `json:"code"`
	Message     string `json:"message"`
	Action      string `json:"action"`
	ReqID       uint64 `json:"req_id"`
	Timing      int64  `json:"timing"`
	HaveMessage bool   `json:"have_message"`
	Topic       string `json:"topic"`
	Database    string `json:"database"`
	VgroupID    int32  `json:"vgroup_id"`
	MessageType int32  `json:"message_type"`
	MessageID   uint64 `json:"message_id"`
	Offset      int64  `json:"offset"`
}

func (t *TMQ) poll(ctx context.Context, session *melody.Session, req *TMQPollReq) {
	t.wg.Add(1)
	defer t.wg.Done()
	action := TMQPoll
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("poll request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	// check whether sending cache message
	finished := t.checkPollMessageID(ctx, session, req, action, logger)
	if finished {
		return
	}
	now := time.Now()
	isDebug := log.IsDebug()
	if t.isAutoCommit && now.After(t.nextTime) {
		errCode, closed := t.wrapperCommit(logger, isDebug)
		if closed {
			return
		}
		if errCode != 0 {
			errStr := syncinterface.TMQErr2Str(errCode, logger, isDebug)
			logger.Errorf("tmq autocommit error:%s", taoserrors.NewError(int(errCode), errStr))
		}
		t.nextTime = now.Add(t.autocommitInterval)
	}
	pollResult, closed := t.wrapperPoll(logger, isDebug, req.BlockingTime)
	if closed {
		return
	}
	resp := &TMQPollResp{
		Action: action,
		ReqID:  req.ReqID,
	}
	message := pollResult.Res
	if message != nil {
		messageType := syncinterface.TMQGetResType(message, logger, isDebug)
		logger.Tracef("poll message type:%d", messageType)
		if messageTypeIsValid(messageType) {
			t.tmpMessage.Lock()
			defer t.tmpMessage.Unlock()
			if t.tmpMessage.CPointer != nil {
				closed = t.wrapperFreeResult(logger, isDebug)
				if closed {
					logger.Debugf("server closed, free result directly, message:%p", message)
					monitor.TMQFreeResultCounter.Inc()
					wrapper.TaosFreeResult(message)
					monitor.TMQFreeResultSuccessCounter.Inc()
					return
				}
			}
			index := atomic.AddUint64(&t.tmpMessage.Index, 1)

			t.tmpMessage.Index = index
			t.tmpMessage.Topic = syncinterface.TMQGetTopicName(message, logger, isDebug)
			t.tmpMessage.VGroupID = syncinterface.TMQGetVgroupID(message, logger, isDebug)
			t.tmpMessage.Offset = syncinterface.TMQGetVgroupOffset(message, logger, isDebug)
			t.tmpMessage.Database = syncinterface.TMQGetDBName(message, logger, isDebug)
			t.tmpMessage.Type = messageType
			t.tmpMessage.CPointer = message

			resp.HaveMessage = true
			resp.Topic = t.tmpMessage.Topic
			resp.Database = t.tmpMessage.Database
			resp.VgroupID = t.tmpMessage.VGroupID
			resp.MessageID = t.tmpMessage.Index
			resp.MessageType = messageType
			resp.Offset = t.tmpMessage.Offset
			logger.Tracef("get message %d, topic:%s, vgroup:%d, offset:%d, db:%s", uintptr(message), t.tmpMessage.Topic, t.tmpMessage.VGroupID, t.tmpMessage.Offset, resp.Database)
		} else {
			logger.Errorf("unavailable tmq type:%d", messageType)
			monitor.TMQFreeResultCounter.Inc()
			wrapper.TaosFreeResult(message)
			monitor.TMQFreeResultSuccessCounter.Inc()
			logger.Trace("free result directly finish")
			wsTMQErrorMsg(ctx, session, logger, 0xffff, "unavailable tmq type:"+strconv.Itoa(int(messageType)), action, req.ReqID, nil)
			return
		}
	}
	if pollResult.Code != 0 {
		logger.Errorf("tmq poll error, code:%d, msg:%s", pollResult.Code, pollResult.ErrStr)
		wsTMQErrorMsg(ctx, session, logger, int(pollResult.Code), pollResult.ErrStr, action, req.ReqID, nil)
		return
	}
	resp.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, logger, resp)
}

func (t *TMQ) checkPollMessageID(ctx context.Context, session *melody.Session, req *TMQPollReq, action string, logger *logrus.Entry) (finish bool) {
	if req.MessageID == nil {
		logger.Trace("no message id")
		return false
	}
	messageID := *req.MessageID
	logger.Trace("lock message")
	t.tmpMessage.Lock()
	logger.Trace("message locked")
	defer func() {
		t.tmpMessage.Unlock()
		logger.Trace("message unlock")
	}()
	if t.tmpMessage.CPointer == nil {
		logger.Trace("no cache message")
		return false
	}
	if messageID == t.tmpMessage.Index {
		logger.Trace("message id equal")
		return false
	}
	// return cache message
	logger.Warnf("message id not equal, req:%d, cache:%d, will send cache message", messageID, t.tmpMessage.Index)
	resp := &TMQPollResp{
		Code:        0,
		Message:     "",
		Action:      action,
		ReqID:       req.ReqID,
		Timing:      wstool.GetDuration(ctx),
		HaveMessage: true,
		Topic:       t.tmpMessage.Topic,
		Database:    t.tmpMessage.Database,
		VgroupID:    t.tmpMessage.VGroupID,
		MessageType: t.tmpMessage.Type,
		MessageID:   t.tmpMessage.Index,
		Offset:      t.tmpMessage.Offset,
	}
	wstool.WSWriteJson(session, logger, resp)
	return true
}

type TMQFetchReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}
type TMQFetchResp struct {
	Code          int                `json:"code"`
	Message       string             `json:"message"`
	Action        string             `json:"action"`
	ReqID         uint64             `json:"req_id"`
	Timing        int64              `json:"timing"`
	MessageID     uint64             `json:"message_id"`
	Completed     bool               `json:"completed"`
	TableName     string             `json:"table_name"`
	Rows          int                `json:"rows"`
	FieldsCount   int                `json:"fields_count"`
	FieldsNames   []string           `json:"fields_names"`
	FieldsTypes   jsontype.JsonUint8 `json:"fields_types"`
	FieldsLengths []int64            `json:"fields_lengths"`
	Precision     int                `json:"precision"` // timestamp precision
}

func (t *TMQ) fetch(ctx context.Context, session *melody.Session, req *TMQFetchReq) {
	action := TMQFetch
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, &req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	logger.Tracef("message lock, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.tmpMessage.Unlock()
		logger.Trace("message unlock")
	}()
	if t.tmpMessage.CPointer == nil {
		logger.Error("message has been freed")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message has been freed", action, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req:%d, message:%d", req.MessageID, message.Index)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message ID are not equal", action, req.ReqID, &req.MessageID)
		return
	}

	if !canGetData(message.Type) {
		logger.Errorf("message type is not data, type:%d", message.Type)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message type is not data", action, req.ReqID, &req.MessageID)
		return
	}
	rawBlock, closed := t.wrapperFetchRawBlock(logger, isDebug, message.CPointer)
	if closed {
		return
	}
	errCode := rawBlock.Code
	blockSize := rawBlock.BlockSize
	block := rawBlock.Block
	if errCode != 0 {
		errStr := syncinterface.TMQErr2Str(int32(errCode), logger, isDebug)
		logger.Errorf("tmq fetch raw block error, code:%d, msg:%s", errCode, errStr)
		wsTMQErrorMsg(ctx, session, logger, errCode, errStr, action, req.ReqID, &req.MessageID)
		return
	}
	resp := &TMQFetchResp{
		Action:    action,
		ReqID:     req.ReqID,
		MessageID: req.MessageID,
	}
	if blockSize == 0 {
		resp.Completed = true
		wstool.WSWriteJson(session, logger, resp)
		return
	}
	s = log.GetLogNow(isDebug)
	logger.Trace("call tmq_get_table_name")
	resp.TableName = syncinterface.TMQGetTableName(message.CPointer, logger, isDebug)
	logger.Tracef("tmq_get_table_name finish, cost:%s", log.GetLogDuration(isDebug, s))
	resp.Rows = blockSize
	logger.Trace("call taos_num_fields")
	resp.FieldsCount = syncinterface.TaosNumFields(message.CPointer, logger, isDebug)
	logger.Trace("taos_num_fields finish")
	s = log.GetLogNow(isDebug)
	rowsHeader, _ := syncinterface.ReadColumn(message.CPointer, resp.FieldsCount, logger, isDebug)
	logger.Tracef("read column finish, cost:%s", log.GetLogDuration(isDebug, s))
	resp.FieldsNames = rowsHeader.ColNames
	resp.FieldsTypes = rowsHeader.ColTypes
	resp.FieldsLengths = rowsHeader.ColLength
	s = log.GetLogNow(isDebug)
	resp.Precision = syncinterface.TaosResultPrecision(message.CPointer, logger, isDebug)
	logger.Tracef("result_precision finish, cost:%s", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	blockLength := int(parser.RawBlockGetLength(block))
	if cap(message.buffer) < blockLength+24 {
		message.buffer = make([]byte, 0, blockLength+24)
	}
	message.buffer = message.buffer[:blockLength+24]
	binary.LittleEndian.PutUint64(message.buffer, 0)
	binary.LittleEndian.PutUint64(message.buffer[8:], req.ReqID)
	binary.LittleEndian.PutUint64(message.buffer[16:], req.MessageID)
	bytesutil.Copy(block, message.buffer, 24, blockLength)
	resp.Timing = wstool.GetDuration(ctx)
	logger.Tracef("handle data cost:%s", log.GetLogDuration(isDebug, s))
	wstool.WSWriteJson(session, logger, resp)
}

type TMQFetchBlockReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

func (t *TMQ) fetchBlock(ctx context.Context, session *melody.Session, req *TMQFetchBlockReq) {
	action := TMQFetchBlock
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch block request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, &req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	logger.Tracef("message lock, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.tmpMessage.Unlock()
		logger.Trace("message unlock")
	}()
	if t.tmpMessage.CPointer == nil {
		logger.Error("message has been freed")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message has been freed", action, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req:%d, message:%d", req.MessageID, message.Index)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message is nil", action, req.ReqID, &req.MessageID)
		return
	}
	if !canGetData(message.Type) {
		logger.Errorf("message type is not data, type:%d", message.Type)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message type is not data", action, req.ReqID, &req.MessageID)
		return
	}
	if len(message.buffer) == 0 {
		logger.Errorf("no fetch data")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "no fetch data", action, req.ReqID, &req.MessageID)
		return
	}
	binary.LittleEndian.PutUint64(message.buffer, uint64(wstool.GetDuration(ctx)))
	wstool.WSWriteBinary(session, message.buffer, logger)
}

type TMQFetchRawReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}

func (t *TMQ) fetchRawBlock(ctx context.Context, session *melody.Session, req *TMQFetchRawReq) {
	action := TMQFetchRaw
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch raw request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, &req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	logger.Tracef("message lock, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.tmpMessage.Unlock()
		logger.Trace("message unlock")
	}()
	if t.tmpMessage.CPointer == nil {
		logger.Error("message has been freed")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message has been freed", action, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req:%d, message:%d", req.MessageID, message.Index)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message ID is not equal", action, req.ReqID, &req.MessageID)
		return
	}
	rawData := asynctmq.TaosaInitTMQRaw()
	defer asynctmq.TaosaFreeTMQRaw(rawData)
	errCode, closed := t.wrapperGetRaw(logger, isDebug, message.CPointer, rawData)
	if closed {
		return
	}
	if errCode != 0 {
		errStr := syncinterface.TMQErr2Str(errCode, logger, isDebug)
		logger.Errorf("tmq get raw error, code:%d, msg:%s", errCode, errStr)
		wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, action, req.ReqID, &req.MessageID)
		return
	}
	length, metaType, data := wrapper.ParseRawMeta(rawData)
	blockLength := int(length)
	if cap(message.buffer) < blockLength+38 {
		message.buffer = make([]byte, 0, blockLength+38)
	}
	message.buffer = message.buffer[:blockLength+38]
	binary.LittleEndian.PutUint64(message.buffer, uint64(wstool.GetDuration(ctx)))
	binary.LittleEndian.PutUint64(message.buffer[8:], req.ReqID)
	binary.LittleEndian.PutUint64(message.buffer[16:], req.MessageID)
	binary.LittleEndian.PutUint64(message.buffer[24:], TMQRawMessage)
	binary.LittleEndian.PutUint32(message.buffer[32:], length)
	binary.LittleEndian.PutUint16(message.buffer[36:], metaType)
	bytesutil.Copy(data, message.buffer, 38, blockLength)
	logger.Debugf("call struct_tmq_raw_data, raw:%p", rawData)
	syncinterface.TMQFreeRaw(rawData, logger, isDebug)
	logger.Debug("struct_tmq_raw_data finish")
	wstool.WSWriteBinary(session, message.buffer, logger)
}

func (t *TMQ) fetchRawBlockNew(ctx context.Context, session *melody.Session, req *TMQFetchRawReq) {
	action := TMQFetchRawData
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch raw request:%+v", req)
	if t.consumer == nil {
		logger.Trace("tmq not init")
		tmqFetchRawBlockErrorMsg(ctx, session, logger, 0xffff, "tmq not init", req.ReqID, req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	logger.Tracef("message lock, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.tmpMessage.Unlock()
		logger.Trace("message unlock")
	}()
	if t.tmpMessage.CPointer == nil {
		logger.Error("message has been freed")
		tmqFetchRawBlockErrorMsg(ctx, session, logger, 0xffff, "message has been freed", req.ReqID, req.MessageID)
		return
	}
	message := t.tmpMessage
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req:%d, message:%d", req.MessageID, message.Index)
		tmqFetchRawBlockErrorMsg(ctx, session, logger, 0xffff, "message ID is not equal", req.ReqID, req.MessageID)
		return
	}
	rawData := asynctmq.TaosaInitTMQRaw()
	defer asynctmq.TaosaFreeTMQRaw(rawData)
	errCode, closed := t.wrapperGetRaw(logger, isDebug, message.CPointer, rawData)
	if closed {
		return
	}
	if errCode != 0 {
		errStr := syncinterface.TMQErr2Str(errCode, logger, isDebug)
		logger.Errorf("tmq get raw error, code:%d, msg:%s", errCode, errStr)
		tmqFetchRawBlockErrorMsg(ctx, session, logger, int(errCode), errStr, req.ReqID, req.MessageID)
		return
	}
	length, metaType, data := wrapper.ParseRawMeta(rawData)
	message.buffer = wsFetchRawBlockMessage(ctx, message.buffer, req.ReqID, req.MessageID, metaType, length, data)
	logger.Debugf("call tmq_free_raw, raw:%p", rawData)
	syncinterface.TMQFreeRaw(rawData, logger, isDebug)
	logger.Debug("tmq_free_raw finish")
	wstool.WSWriteBinary(session, message.buffer, logger)
}

type TMQFetchJsonMetaReq struct {
	ReqID     uint64 `json:"req_id"`
	MessageID uint64 `json:"message_id"`
}
type TMQFetchJsonMetaResp struct {
	Code      int             `json:"code"`
	Message   string          `json:"message"`
	Action    string          `json:"action"`
	ReqID     uint64          `json:"req_id"`
	Timing    int64           `json:"timing"`
	MessageID uint64          `json:"message_id"`
	Data      json.RawMessage `json:"data"`
}

func (t *TMQ) fetchJsonMeta(ctx context.Context, session *melody.Session, req *TMQFetchJsonMetaReq) {
	action := TMQFetchJsonMeta
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("fetch json meta request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, &req.MessageID)
		return
	}
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.tmpMessage.Lock()
	logger.Tracef("message lock, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.tmpMessage.Unlock()
		logger.Trace("message unlock")
	}()
	if t.tmpMessage.CPointer == nil {
		logger.Error("message has been freed")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message has been freed", action, req.ReqID, &req.MessageID)
		return
	}
	message := t.tmpMessage
	if !canGetMeta(message.Type) {
		logger.Errorf("message type can not get meta, type:%d", message.Type)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, fmt.Sprintf("message type can not get meta, type: %d", message.Type), action, req.ReqID, &req.MessageID)
		return
	}
	if message.Index != req.MessageID {
		logger.Errorf("message ID are not equal, req:%d, message:%d", req.MessageID, message.Index)
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "message ID is not equal", action, req.ReqID, &req.MessageID)
		return
	}
	jsonMeta, closed := t.wrapperGetJsonMeta(logger, isDebug, message.CPointer)
	if closed {
		return
	}
	resp := TMQFetchJsonMetaResp{
		Action:    action,
		ReqID:     req.ReqID,
		MessageID: req.MessageID,
	}
	if jsonMeta == nil {
		resp.Data = nil
	} else {
		var binaryVal []byte
		i := 0
		c := byte(0)
		for {
			c = *((*byte)(unsafe.Pointer(uintptr(jsonMeta) + uintptr(i))))
			if c != 0 {
				binaryVal = append(binaryVal, c)
				i += 1
			} else {
				break
			}
		}
		resp.Data = binaryVal
	}
	logger.Debugf("call tmq_free_json_meta, jsonMeta:%p", jsonMeta)
	syncinterface.TMQFreeJsonMeta(jsonMeta, logger, isDebug)
	logger.Debug("tmq_free_json_meta finish")
	resp.Timing = wstool.GetDuration(ctx)
	wstool.WSWriteJson(session, logger, resp)
}

type TMQUnsubscribeReq struct {
	ReqID uint64 `json:"req_id"`
}

type TMQUnsubscribeResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *TMQ) unsubscribe(ctx context.Context, session *melody.Session, req *TMQUnsubscribeReq) {
	action := TMQUnsubscribe
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("unsubscribe request:%+v", req)
	isDebug := log.IsDebug()
	t.Lock(logger, isDebug)
	defer t.Unlock()
	if t.IsClosed() {
		logger.Trace("server closed")
		return
	}
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	errCode, closed := t.wrapperUnsubscribe(logger, isDebug, true)
	if closed {
		return
	}
	if errCode != 0 {
		errStr := syncinterface.TMQErr2Str(errCode, logger, isDebug)
		logger.Errorf("tmq unsubscribe error,consumer:%p, code:%d, msg:%s", t.consumer, errCode, errStr)
		wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, action, req.ReqID, nil)
		return
	}
	logger.Trace("free all result")
	t.freeMessage(false)
	logger.Trace("free all result finish")
	t.unsubscribed = true
	wstool.WSWriteJson(session, logger, &TMQUnsubscribeResp{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
	})
}

type TMQGetTopicAssignmentReq struct {
	ReqID uint64 `json:"req_id"`
	Topic string `json:"topic"`
}

type TMQGetTopicAssignmentResp struct {
	Code       int                     `json:"code"`
	Message    string                  `json:"message"`
	Action     string                  `json:"action"`
	ReqID      uint64                  `json:"req_id"`
	Timing     int64                   `json:"timing"`
	Assignment []*tmqhandle.Assignment `json:"assignment"`
}

func (t *TMQ) assignment(ctx context.Context, session *melody.Session, req *TMQGetTopicAssignmentReq) {
	action := TMQGetTopicAssignment
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("assignment request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	result, closed := t.wrapperGetTopicAssignment(logger, log.IsDebug(), req.Topic)
	if closed {
		return
	}
	if result.Code != 0 {
		errStr := syncinterface.TMQErr2Str(result.Code, logger, log.IsDebug())
		logger.Errorf("tmq assignment error,consumer:%p, code:%d, msg:%s", t.consumer, result.Code, errStr)
		wsTMQErrorMsg(ctx, session, logger, int(result.Code), errStr, action, req.ReqID, nil)
		return
	}
	wstool.WSWriteJson(session, logger, TMQGetTopicAssignmentResp{
		Action:     action,
		ReqID:      req.ReqID,
		Timing:     wstool.GetDuration(ctx),
		Assignment: result.Assignment,
	})
}

type TMQOffsetSeekReq struct {
	ReqID    uint64 `json:"req_id"`
	Topic    string `json:"topic"`
	VgroupID int32  `json:"vgroup_id"`
	Offset   int64  `json:"offset"`
}

type TMQOffsetSeekResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *TMQ) offsetSeek(ctx context.Context, session *melody.Session, req *TMQOffsetSeekReq) {
	action := TMQSeek
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("offset seek request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	isDebug := log.IsDebug()
	errCode, closed := t.wrapperOffsetSeek(logger, isDebug, req.Topic, req.VgroupID, req.Offset)
	if closed {
		return
	}
	if errCode != 0 {
		errStr := syncinterface.TMQErr2Str(errCode, logger, isDebug)
		logger.Errorf("tmq offset seek error, consumer:%p, code:%d, msg:%s", t.consumer, errCode, errStr)
		wsTMQErrorMsg(ctx, session, logger, int(errCode), errStr, action, req.ReqID, nil)
		return
	}
	wstool.WSWriteJson(session, logger, TMQOffsetSeekResp{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
	})
}

func (t *TMQ) Close() {
	t.Lock(t.logger, log.IsDebug())
	defer t.Unlock()
	if t.IsClosed() {
		return
	}
	t.setClosed()
	t.stop()
}

func (t *TMQ) stop() {
	t.once.Do(func() {
		logger := t.logger
		start := time.Now()
		logger.Trace("tmq close")
		defer func() {
			logger.Debugf("tmq close end, cost:%s", time.Since(start).String())
		}()
		close(t.exit)
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		done := make(chan struct{})
		go func() {
			t.wg.Wait()
			close(done)
		}()
		select {
		case <-ctx.Done():
			logger.Warn("wait stop over 1 minute")
			<-done
		case <-done:
		}
		logger.Debug("wait stop done")
		isDebug := log.IsDebug()

		defer func() {
			s := log.GetLogNow(isDebug)
			t.asyncLocker.Lock()
			asynctmq.DestroyTMQThread(t.thread)
			t.asyncLocker.Unlock()
			logger.Tracef("destroy tmq thread cost:%s", log.GetLogDuration(isDebug, s))
			tmqhandle.GlobalTMQHandlerPoll.Put(t.handler)
		}()

		defer func() {
			if t.consumer != nil {
				if !t.unsubscribed {
					errCode, _ := t.wrapperUnsubscribe(logger, isDebug, false)
					if errCode != 0 {
						errMsg := syncinterface.TMQErr2Str(errCode, logger, isDebug)
						logger.Errorf("tmq unsubscribe consumer error, consumer:%p, code:%d, msg:%s", t.consumer, errCode, errMsg)
					}
				}
				errCode := t.wrapperCloseConsumer(logger, log.IsDebug(), t.consumer)
				if errCode != 0 {
					errMsg := syncinterface.TMQErr2Str(errCode, logger, isDebug)
					logger.Errorf("tmq close consumer error, consumer:%p, code:%d, msg:%s", t.consumer, errCode, errMsg)
				}
			}
		}()
		logger.Trace("start to free message")
		s := log.GetLogNow(isDebug)
		t.freeMessage(true)
		logger.Debugf("free message cost:%s", log.GetLogDuration(isDebug, s))
	})
}

func (t *TMQ) freeMessage(closing bool) {
	t.tmpMessage.Lock()
	defer t.tmpMessage.Unlock()
	if t.tmpMessage.CPointer != nil {
		t.asyncLocker.Lock()
		defer func() {
			t.asyncLocker.Unlock()
		}()
		if !closing && t.IsClosed() {
			return
		}
		monitor.TMQFreeResultCounter.Inc()
		asynctmq.TaosaTMQFreeResultA(t.thread, t.tmpMessage.CPointer, t.handler.Handler)
		<-t.handler.Caller.FreeResult
		monitor.TMQFreeResultSuccessCounter.Inc()
		t.tmpMessage.CPointer = nil
	}
	t.tmpMessage.buffer = nil
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

func canGetData(messageType int32) bool {
	return messageType == common.TMQ_RES_DATA || messageType == common.TMQ_RES_METADATA
}

func canGetMeta(messageType int32) bool {
	return messageType == common.TMQ_RES_TABLE_META || messageType == common.TMQ_RES_METADATA
}

func messageTypeIsValid(messageType int32) bool {
	switch messageType {
	case common.TMQ_RES_DATA, common.TMQ_RES_TABLE_META, common.TMQ_RES_METADATA, common.TMQ_RES_RAWDATA:
		return true
	}
	return false
}

type TMQCommittedReq struct {
	ReqID          uint64          `json:"req_id"`
	TopicVgroupIDs []TopicVgroupID `json:"topic_vgroup_ids"`
}

type TMQCommittedResp struct {
	Code      int     `json:"code"`
	Message   string  `json:"message"`
	Action    string  `json:"action"`
	ReqID     uint64  `json:"req_id"`
	Timing    int64   `json:"timing"`
	Committed []int64 `json:"committed"`
}

func (t *TMQ) committed(ctx context.Context, session *melody.Session, req *TMQCommittedReq) {
	action := TMQCommitted
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("committed request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	isDebug := log.IsDebug()
	offsets := make([]int64, 0, len(req.TopicVgroupIDs))
	for _, tv := range req.TopicVgroupIDs {
		res, closed := t.wrapperCommitted(logger, isDebug, tv.Topic, tv.VgroupID)
		if closed {
			return
		}
		if res < 0 && res != OffsetInvalid {
			errStr := syncinterface.TMQErr2Str(int32(res), logger, isDebug)
			logger.Errorf("tmq get committed error, code:%d, msg:%s", res, errStr)
			wsTMQErrorMsg(ctx, session, logger, int(res), errStr, action, req.ReqID, nil)
			return
		}
		offsets = append(offsets, res)
	}
	wstool.WSWriteJson(session, logger, TMQCommittedResp{
		Action:    action,
		ReqID:     req.ReqID,
		Timing:    wstool.GetDuration(ctx),
		Committed: offsets,
	})
}

type TopicVgroupID struct {
	Topic    string `json:"topic"`
	VgroupID int32  `json:"vgroup_id"`
}

type TMQPositionReq struct {
	ReqID          uint64          `json:"req_id"`
	TopicVgroupIDs []TopicVgroupID `json:"topic_vgroup_ids"`
}

type TMQPositionResp struct {
	Code     int     `json:"code"`
	Message  string  `json:"message"`
	Action   string  `json:"action"`
	ReqID    uint64  `json:"req_id"`
	Timing   int64   `json:"timing"`
	Position []int64 `json:"position"`
}

func (t *TMQ) position(ctx context.Context, session *melody.Session, req *TMQPositionReq) {
	action := TMQPosition
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("position request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	isDebug := log.IsDebug()
	positions := make([]int64, 0, len(req.TopicVgroupIDs))
	for _, tv := range req.TopicVgroupIDs {
		res, closed := t.wrapperPosition(logger, isDebug, tv.Topic, tv.VgroupID)
		if closed {
			return
		}
		if res < 0 && res != OffsetInvalid {
			errStr := syncinterface.TMQErr2Str(int32(res), logger, isDebug)
			logger.Errorf("get position error, code:%d, msg:%s", res, errStr)
			wsTMQErrorMsg(ctx, session, logger, int(res), errStr, action, req.ReqID, nil)
			return
		}
		positions = append(positions, res)
	}

	wstool.WSWriteJson(session, logger, TMQPositionResp{
		Action:   action,
		ReqID:    req.ReqID,
		Timing:   wstool.GetDuration(ctx),
		Position: positions,
	})
}

type TMQListTopicsReq struct {
	ReqID uint64 `json:"req_id"`
}

type TMQListTopicsResp struct {
	Code    int      `json:"code"`
	Message string   `json:"message"`
	Action  string   `json:"action"`
	ReqID   uint64   `json:"req_id"`
	Timing  int64    `json:"timing"`
	Topics  []string `json:"topics"`
}

func (t *TMQ) listTopics(ctx context.Context, session *melody.Session, req *TMQListTopicsReq) {
	action := TMQListTopics
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("list topics request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}
	if t.IsClosed() {
		logger.Trace("server closed")
		return
	}
	isDebug := log.IsDebug()
	code, topicsPointer := syncinterface.TMQSubscription(t.consumer, logger, isDebug)
	defer func() {
		if topicsPointer != nil {
			logger.Tracef("call tmq_list_destroy, list:%p", topicsPointer)
			syncinterface.TMQListDestroy(topicsPointer, logger, isDebug)
			logger.Trace("tmq_list_destroy finish")
		}
	}()
	if code != 0 {
		errStr := syncinterface.TMQErr2Str(code, logger, isDebug)
		logger.Errorf("tmq list topic error, code:%d, msg:%s", code, errStr)
		wsTMQErrorMsg(ctx, session, logger, int(code), errStr, action, req.ReqID, nil)
		return
	}
	logger.Debugf("call tmq_list_to_c_array, list:%p", topicsPointer)
	topics := wrapper.TMQListToCArray(topicsPointer, int(syncinterface.TMQListGetSize(topicsPointer, logger, isDebug)))
	logger.Debug("tmq_list_to_c_array finish")
	wstool.WSWriteJson(session, logger, TMQListTopicsResp{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		Topics: topics,
	})
}

type TMQCommitOffsetReq struct {
	ReqID    uint64 `json:"req_id"`
	Topic    string `json:"topic"`
	VgroupID int32  `json:"vgroup_id"`
	Offset   int64  `json:"offset"`
}

type TMQCommitOffsetResp struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	Action   string `json:"action"`
	ReqID    uint64 `json:"req_id"`
	Timing   int64  `json:"timing"`
	Topic    string `json:"topic"`
	VgroupID int32  `json:"vgroup_id"`
	Offset   int64  `json:"offset"`
}

func (t *TMQ) commitOffset(ctx context.Context, session *melody.Session, req *TMQCommitOffsetReq) {
	action := TMQCommitOffset
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("commit offset request:%+v", req)
	if t.consumer == nil {
		logger.Error("tmq not init")
		wsTMQErrorMsg(ctx, session, logger, 0xffff, "tmq not init", action, req.ReqID, nil)
		return
	}

	code, closed := t.wrapperCommitOffset(logger, log.IsDebug(), req.Topic, req.VgroupID, req.Offset)
	if closed {
		return
	}
	if code != 0 {
		errStr := syncinterface.TMQErr2Str(code, logger, log.IsDebug())
		logger.Errorf("tmq commit offset error, code:%d, msg:%s", code, errStr)
		wsTMQErrorMsg(ctx, session, logger, int(code), errStr, action, req.ReqID, nil)
		return
	}

	wstool.WSWriteJson(session, logger, TMQCommitOffsetResp{
		Action:   action,
		ReqID:    req.ReqID,
		Timing:   wstool.GetDuration(ctx),
		Topic:    req.Topic,
		VgroupID: req.VgroupID,
		Offset:   req.Offset,
	})
}

func (t *TMQ) wrapperCloseConsumer(logger *logrus.Entry, isDebug bool, consumer unsafe.Pointer) int32 {
	logger.Debugf("call tmq_consumer_close, consumer:%p", consumer)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("tmq_consumer_close async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("tmq_consumer_close async locker unlocked")
	}()
	monitor.TMQConsumerCloseCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQConsumerCloseA(t.thread, consumer, t.handler.Handler)
	code := <-t.handler.Caller.ConsumerCloseResult
	logger.Debugf("tmq_consumer_close finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TMQConsumerCloseFailCounter.Inc()
	} else {
		monitor.TMQConsumerCloseSuccessCounter.Inc()
	}
	return code
}

func (t *TMQ) wrapperSubscribe(logger *logrus.Entry, isDebug bool, consumer, topicList unsafe.Pointer) int32 {
	logger.Debugf("call tmq_subscribe, consumer:%p", consumer)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("tmq_subscribe async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("tmq_subscribe async locker unlocked")
	}()
	monitor.TMQSubscribeCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQSubscribeA(t.thread, consumer, topicList, t.handler.Handler)
	errCode := <-t.handler.Caller.SubscribeResult
	logger.Debugf("tmq_subscribe finish, code:%d, cost:%s", errCode, log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		monitor.TMQSubscribeFailCounter.Inc()
	} else {
		monitor.TMQSubscribeSuccessCounter.Inc()
	}
	return errCode
}

func (t *TMQ) wrapperConsumerNew(logger *logrus.Entry, isDebug bool, tmqConfig unsafe.Pointer) (consumer unsafe.Pointer, err error) {
	logger.Debugf("call tmq_consumer_new, tmqConfig:%p", tmqConfig)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("tmq_consumer_new async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("tmq_consumer_new async locker unlocked")
	}()
	monitor.TMQConsumerNewCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQNewConsumerA(t.thread, tmqConfig, t.handler.Handler)
	result := <-t.handler.Caller.NewConsumerResult
	logger.Tracef("new consumer result %x", uintptr(result.Consumer))
	if len(result.ErrStr) > 0 {
		err = taoserrors.NewError(-1, result.ErrStr)
	} else if result.Consumer == nil {
		err = taoserrors.NewError(-1, "new consumer return nil")
	}
	logger.Debugf("tmq_consumer_new finish, consumer:%p, err:%v, cost:%s", result.Consumer, err, log.GetLogDuration(isDebug, s))
	if err != nil {
		monitor.TMQConsumerNewFailCounter.Inc()
	} else {
		monitor.TMQConsumerNewSuccessCounter.Inc()
	}
	return result.Consumer, err
}

func (t *TMQ) wrapperCommit(logger *logrus.Entry, isDebug bool) (int32, bool) {
	logger.Debugf("call tmq_commit_sync, consumer:%p", t.consumer)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("tmq_commit_sync async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("tmq_commit_sync async locker unlocked")
	}()
	if t.IsClosed() {
		logger.Debug("tmq_commit_sync finish, server closed")
		return 0, true
	}
	monitor.TMQCommitSyncCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQCommitA(t.thread, t.consumer, nil, t.handler.Handler)
	errCode := <-t.handler.Caller.CommitResult
	logger.Debugf("tmq_commit_sync finish, code:%d, cost:%s", errCode, log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		monitor.TMQCommitSyncFailCounter.Inc()
	} else {
		monitor.TMQCommitSyncSuccessCounter.Inc()
	}
	return errCode, false
}

func (t *TMQ) wrapperPoll(logger *logrus.Entry, isDebug bool, blockingTime int64) (*tmqhandle.PollResult, bool) {
	logger.Debugf("call tmq_poll, consumer:%p", t.consumer)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("tmq_poll async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("tmq_poll async locker unlocked")
	}()
	if t.IsClosed() {
		logger.Debug("tmq_poll finish, server closed")
		return nil, true
	}
	monitor.TMQPollCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQPollA(t.thread, t.consumer, blockingTime, t.handler.Handler)
	result := <-t.handler.Caller.PollResult
	logger.Debugf("tmq_poll finish, res:%p, code:%d, errmsg:%s, cost:%s", result.Res, result.Code, result.ErrStr, log.GetLogDuration(isDebug, s))
	if result.Code != 0 {
		monitor.TMQPollFailCounter.Inc()
	} else {
		monitor.TMQPollSuccessCounter.Inc()
		if result.Res != nil {
			monitor.TMQPollResultCounter.Inc()
		}
	}
	return result, false
}

func (t *TMQ) wrapperFreeResult(logger *logrus.Entry, isDebug bool) bool {
	logger.Debugf("call free result, consumer:%p", t.consumer)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("free result async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("free result async locker unlocked")
	}()
	if t.IsClosed() {
		logger.Debug("free result finish, server closed")
		return true
	}
	if t.tmpMessage.CPointer == nil {
		logger.Debug("free result finish, message has been freed")
		return false
	}
	monitor.TMQFreeResultCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQFreeResultA(t.thread, t.tmpMessage.CPointer, t.handler.Handler)
	<-t.handler.Caller.FreeResult
	logger.Debugf("free result finish, cost:%s", log.GetLogDuration(isDebug, s))
	monitor.TMQFreeResultSuccessCounter.Inc()
	return false
}

func (t *TMQ) wrapperFetchRawBlock(logger *logrus.Entry, isDebug bool, res unsafe.Pointer) (*tmqhandle.FetchRawBlockResult, bool) {
	logger.Debugf("call fetch_raw_block, res:%p", res)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("fetch_raw_block async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("fetch_raw_block async locker unlocked")
	}()
	if t.IsClosed() {
		logger.Debug("fetch_raw_block finish, server closed")
		return nil, true
	}
	monitor.TMQFetchRawBlockCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQFetchRawBlockA(t.thread, res, t.handler.Handler)
	rawBlock := <-t.handler.Caller.FetchRawBlockResult
	logger.Debugf("fetch_raw_block finish, code:%d, block_size:%d, block:%p, cost:%s", rawBlock.Code, rawBlock.BlockSize, rawBlock.Block, log.GetLogDuration(isDebug, s))
	if rawBlock.Code != 0 {
		monitor.TMQFetchRawBlockFailCounter.Inc()
	} else {
		monitor.TMQFetchRawBlockSuccessCounter.Inc()
	}
	return rawBlock, false
}

func (t *TMQ) wrapperGetRaw(logger *logrus.Entry, isDebug bool, res unsafe.Pointer, rawData unsafe.Pointer) (int32, bool) {
	logger.Debugf("call tmq_get_raw, res:%p, rawData:%p", res, rawData)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("tmq_get_raw async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("tmq_get_raw async locker unlocked")
	}()
	if t.IsClosed() {
		logger.Debug("tmq_get_raw finish, server closed")
		return 0, true
	}
	monitor.TMQGetRawCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQGetRawA(t.thread, res, rawData, t.handler.Handler)
	errCode := <-t.handler.Caller.GetRawResult
	logger.Debugf("tmq_get_raw finish, code:%d, cost:%s", errCode, log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		monitor.TMQGetRawFailCounter.Inc()
	} else {
		monitor.TMQGetRawSuccessCounter.Inc()
	}
	return errCode, false
}

func (t *TMQ) wrapperGetJsonMeta(logger *logrus.Entry, isDebug bool, res unsafe.Pointer) (unsafe.Pointer, bool) {
	logger.Debugf("call tmq_get_json_meta, result:%p", res)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("tmq_get_json_meta async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("tmq_get_json_meta async locker unlocked")
	}()
	if t.IsClosed() {
		logger.Debug("tmq_get_json_meta finish, server closed")
		return nil, true
	}
	monitor.TMQGetJsonMetaCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQGetJsonMetaA(t.thread, res, t.handler.Handler)
	jsonMeta := <-t.handler.Caller.GetJsonMetaResult
	logger.Debugf("tmq_get_json_meta finish, result:%p, cost:%s", jsonMeta, log.GetLogDuration(isDebug, s))
	monitor.TMQGetJsonMetaSuccessCounter.Inc()
	return jsonMeta, false
}

func (t *TMQ) wrapperUnsubscribe(logger *logrus.Entry, isDebug bool, checkClose bool) (int32, bool) {
	logger.Debugf("call tmq_unsubscribe, consumer:%p, checkClose: %t", t.consumer, checkClose)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("unsubscribe async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("unsubscribe async locker unlocked")
	}()
	if checkClose && t.IsClosed() {
		logger.Debug("tmq_unsubscribe finish, server closed")
		return 0, true
	}
	monitor.TMQUnsubscribeCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQUnsubscribeA(t.thread, t.consumer, t.handler.Handler)
	errCode := <-t.handler.Caller.UnsubscribeResult
	logger.Debugf("tmq_unsubscribe finish, code:%d, cost:%s", errCode, log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		monitor.TMQUnsubscribeFailCounter.Inc()
	} else {
		monitor.TMQUnsubscribeSuccessCounter.Inc()
	}
	return errCode, false
}

func (t *TMQ) wrapperGetTopicAssignment(logger *logrus.Entry, isDebug bool, topic string) (*tmqhandle.GetTopicAssignmentResult, bool) {
	logger.Debugf("call tmq_get_topic_assignment, consumer:%p, topic:%s", t.consumer, topic)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("tmq_get_topic_assignment async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("tmq_get_topic_assignment async locker unlocked")
	}()
	if t.IsClosed() {
		logger.Debug("tmq_get_topic_assignment finish, server closed")
		return nil, true
	}
	monitor.TMQGetTopicAssignmentCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQGetTopicAssignmentA(t.thread, t.consumer, topic, t.handler.Handler)
	result := <-t.handler.Caller.GetTopicAssignmentResult
	logger.Debugf("tmq_get_topic_assignment finish, result:%+v, cost:%s", result, log.GetLogDuration(isDebug, s))
	if result.Code != 0 {
		monitor.TMQGetTopicAssignmentFailCounter.Inc()
	} else {
		monitor.TMQGetTopicAssignmentSuccessCounter.Inc()
	}
	return result, false
}

func (t *TMQ) wrapperOffsetSeek(logger *logrus.Entry, isDebug bool, topic string, vgroupID int32, offset int64) (int32, bool) {
	logger.Debugf("call offset_seek, comsumer:%p, topic:%s, vgroup_id:%d, offset:%d", t.consumer, topic, vgroupID, offset)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("offset_seek async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("offset_seek async locker unlocked")
	}()
	if t.IsClosed() {
		logger.Debug("tmq_offset_seek finish, server closed")
		return 0, true
	}
	monitor.TMQOffsetSeekCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQOffsetSeekA(t.thread, t.consumer, topic, vgroupID, offset, t.handler.Handler)
	errCode := <-t.handler.Caller.OffsetSeekResult
	logger.Debugf("offset_seek finish, code:%d, cost:%s", errCode, log.GetLogDuration(isDebug, s))
	if errCode != 0 {
		monitor.TMQOffsetSeekFailCounter.Inc()
	} else {
		monitor.TMQOffsetSeekSuccessCounter.Inc()
	}
	return errCode, false
}

func (t *TMQ) wrapperCommitted(logger *logrus.Entry, isDebug bool, topic string, vgroupID int32) (int64, bool) {
	logger.Debugf("call tmq_committed, consumer:%p, topic:%s, vgroup_id:%d", t.consumer, topic, vgroupID)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("tmq_committed async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("tmq_committed async locker unlocked")
	}()
	if t.IsClosed() {
		logger.Debug("tmq_committed finish, server closed")
		return 0, true
	}
	monitor.TMQCommittedCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQCommitted(t.thread, t.consumer, topic, vgroupID, t.handler.Handler)
	offset := <-t.handler.Caller.CommittedResult
	logger.Debugf("tmq_committed finish, offset:%d, cost:%s", offset, log.GetLogDuration(isDebug, s))
	monitor.TMQCommittedSuccessCounter.Inc()
	return offset, false
}

func (t *TMQ) wrapperPosition(logger *logrus.Entry, isDebug bool, topic string, vgroupID int32) (int64, bool) {
	logger.Debugf("call tmq_position, consumer:%p, topic:%s, vgroup_id:%d", t.consumer, topic, vgroupID)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("tmq_position async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("tmq_position async locker unlocked")
	}()
	if t.IsClosed() {
		logger.Debug("tmq_position finish, server closed")
		return 0, true
	}
	monitor.TMQPositionCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQPosition(t.thread, t.consumer, topic, vgroupID, t.handler.Handler)
	res := <-t.handler.Caller.PositionResult
	logger.Debugf("tmq_position finish, position:%d, cost:%s", res, log.GetLogDuration(isDebug, s))
	monitor.TMQPositionSuccessCounter.Inc()
	return res, false
}

func (t *TMQ) wrapperCommitOffset(logger *logrus.Entry, isDebug bool, topic string, vgroupID int32, offset int64) (int32, bool) {
	logger.Debugf("call commit_offset, consumer:%p, topic:%s, vgroup_id:%d, offset:%d", t.consumer, topic, vgroupID, offset)
	s := log.GetLogNow(isDebug)
	t.asyncLocker.Lock()
	logger.Tracef("commit_offset async locker locked, cost:%s", log.GetLogDuration(isDebug, s))
	defer func() {
		t.asyncLocker.Unlock()
		logger.Trace("commit_offset async locker unlocked")
	}()
	if t.IsClosed() {
		logger.Debug("tmq_commit_offset finish, server closed")
		return 0, true
	}
	monitor.TMQCommitOffsetCounter.Inc()
	s = log.GetLogNow(isDebug)
	asynctmq.TaosaTMQCommitOffset(t.thread, t.consumer, topic, vgroupID, offset, t.handler.Handler)
	code := <-t.handler.Caller.CommitResult
	logger.Debugf("commit_offset finish, code:%d, cost:%s", code, log.GetLogDuration(isDebug, s))
	if code != 0 {
		monitor.TMQCommitOffsetFailCounter.Inc()
	} else {
		monitor.TMQCommitOffsetSuccessCounter.Inc()
	}
	return code, false
}

/*
	Flag           uint64 //8               0
	Action         uint64 //8               8
	Version        uint16 //2               16
	Time           uint64 //8               18
	ReqID          uint64 //8               26
	Code           uint32 //4               34
	MessageLen     uint32 //4               38
	Message        string //MessageLen      42
	MessageID      uint64 //8               42 + MessageLen
	MetaType       uint16 //2               50 + MessageLen
	RawBlockLength uint32 //4               52 + MessageLen
	TMQRawBlock    []byte //RawBlockLength  56 + MessageLen + RawBlockLength
*/

func tmqFetchRawBlockErrorMsg(ctx context.Context, session *melody.Session, logger *logrus.Entry, code int, message string, reqID uint64, messageID uint64) {
	bufLength := 8 + 8 + 2 + 8 + 8 + 4 + 4 + len(message) + 8
	buf := make([]byte, bufLength)
	binary.LittleEndian.PutUint64(buf, 0xffffffffffffffff)
	binary.LittleEndian.PutUint64(buf[8:], uint64(TMQFetchRawNewMessage))
	binary.LittleEndian.PutUint16(buf[16:], 1)
	binary.LittleEndian.PutUint64(buf[18:], uint64(wstool.GetDuration(ctx)))
	binary.LittleEndian.PutUint64(buf[26:], reqID)
	binary.LittleEndian.PutUint32(buf[34:], uint32(code&0xffff))
	binary.LittleEndian.PutUint32(buf[38:], uint32(len(message)))
	copy(buf[42:], message)
	binary.LittleEndian.PutUint64(buf[42+len(message):], messageID)
	wstool.WSWriteBinary(session, buf, logger)
}

func wsFetchRawBlockMessage(ctx context.Context, buf []byte, reqID uint64, resultID uint64, MetaType uint16, blockLength uint32, rawBlock unsafe.Pointer) []byte {
	bufLength := 8 + 8 + 2 + 8 + 8 + 4 + 4 + 8 + 2 + 4 + int(blockLength)
	if cap(buf) < bufLength {
		buf = make([]byte, 0, bufLength)
	}
	buf = buf[:bufLength]
	binary.LittleEndian.PutUint64(buf, 0xffffffffffffffff)
	binary.LittleEndian.PutUint64(buf[8:], uint64(TMQFetchRawNewMessage))
	binary.LittleEndian.PutUint16(buf[16:], 1)
	binary.LittleEndian.PutUint64(buf[18:], uint64(wstool.GetDuration(ctx)))
	binary.LittleEndian.PutUint64(buf[26:], reqID)
	binary.LittleEndian.PutUint32(buf[34:], 0)
	binary.LittleEndian.PutUint32(buf[38:], 0)
	binary.LittleEndian.PutUint64(buf[42:], resultID)
	binary.LittleEndian.PutUint16(buf[50:], MetaType)
	binary.LittleEndian.PutUint32(buf[52:], blockLength)
	bytesutil.Copy(rawBlock, buf, 56, int(blockLength))
	return buf
}

func init() {
	c := NewTMQController()
	controller.AddController(c)
}
