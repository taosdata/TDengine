package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor/recordsql"
)

type ConfigController struct {
}

func (ctl *ConfigController) Init(r gin.IRouter) {
	r.PUT("config", prepareCtx, checkConcurrent(changeConfigLocker), CheckAuth, checkTDengineConnection, ctl.changeConfig)

	r.POST("record_sql", prepareCtx, checkConcurrent(recordSqlLocker), CheckAuth, checkTDengineConnection, ctl.startRecordSql)
	r.DELETE("record_sql", prepareCtx, checkConcurrent(recordSqlLocker), CheckAuth, checkTDengineConnection, ctl.stopRecordSql)
	r.GET("record_sql", prepareCtx, CheckAuth, checkTDengineConnection, ctl.getRecordSqlState)

	r.POST("record_stmt", prepareCtx, checkConcurrent(recordStmtLocker), CheckAuth, checkTDengineConnection, ctl.startRecordStmt)
	r.DELETE("record_stmt", prepareCtx, checkConcurrent(recordStmtLocker), CheckAuth, checkTDengineConnection, ctl.stopRecordStmt)
	r.GET("record_stmt", prepareCtx, CheckAuth, checkTDengineConnection, ctl.getRecordStmtState)
}

const defaultStmtRecordDuration = time.Hour
const (
	unlocked = 0
	locked   = 1
)

var changeConfigLocker = &locker{
	locking: unlocked,
}

var recordSqlLocker = &locker{
	locking: unlocked,
}

var recordStmtLocker = &locker{
	locking: unlocked,
}

type locker struct {
	locking int32
}

func (l *locker) tryLock() bool {
	return atomic.CompareAndSwapInt32(&l.locking, unlocked, locked)
}

func (l *locker) unlock() {
	atomic.StoreInt32(&l.locking, unlocked)
}

type ModifyConfig struct {
	LogLevel *string `json:"log.level"`
}

type RecordRequest struct {
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
	Location  string `json:"location"`
}

func checkConcurrent(locker *locker) gin.HandlerFunc {
	return func(c *gin.Context) {
		logger := c.MustGet(LoggerKey).(*logrus.Entry)
		if !locker.tryLock() {
			TooManyRequestResponse(c, logger, fmt.Sprintf("API '%s' does not allow concurrent execution", c.FullPath()))
			return
		}
		c.Next()
		defer locker.unlock()
	}
}

func (ctl *ConfigController) changeConfig(c *gin.Context) {
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	body, err := c.GetRawData()
	if err != nil {
		logger.Errorf("get request body error, err:%s", err)
		BadRequestResponseWithMsg(c, logger, 0xffff, "get request body error")
		return
	}
	logger.Tracef("get modify config request, req:%s", body)
	var modifyConfig ModifyConfig
	err = json.Unmarshal(body, &modifyConfig)
	if err != nil {
		logger.Errorf("unmarshal json error, err:%s, req:%s", err, body)
		BadRequestResponseWithMsg(c, logger, 0xffff, "unmarshal json error")
		return
	}
	if modifyConfig.LogLevel != nil {
		logLevel := *modifyConfig.LogLevel
		logger.Debugf("change config, log.level:%s", logLevel)
		err = log.SetLevel(logLevel)
		if err != nil {
			logger.Errorf("change log.level error, err:%s", err)
			BadRequestResponseWithMsg(c, logger, 0xffff, "change log.level error")
			return
		}
	}
	c.JSON(http.StatusOK, &Message{
		Code: 0,
		Desc: "",
	})
	logger.Debugf("change config success")
}

func (ctl *ConfigController) startRecordSql(c *gin.Context) {
	ctl.startRecord(c, recordsql.RecordTypeSQL)
}

func (ctl *ConfigController) startRecordStmt(c *gin.Context) {
	ctl.startRecord(c, recordsql.RecordTypeStmt)
}

func (ctl *ConfigController) startRecord(c *gin.Context, recordType recordsql.RecordType) {
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	logger.Debugf("start record, type:%s", recordType.String())
	body, err := c.GetRawData()
	if err != nil {
		logger.Errorf("get request body error, err:%s", err)
		BadRequestResponseWithMsg(c, logger, 0xffff, "get request body error")
		return
	}
	logger.Tracef("get start record request, req:%s", body)
	var recordRequest RecordRequest
	if len(body) != 0 {
		err = json.Unmarshal(body, &recordRequest)
		if err != nil {
			logger.Errorf("unmarshal json error, err:%s, req:%s", err, body)
			BadRequestResponseWithMsg(c, logger, 0xffff, "unmarshal json error")
			return
		}
	} else {
		logger.Debugf("no request body, use default record config")
		now := time.Now()
		endTime := recordsql.DefaultRecordSqlEndTime
		if recordType == recordsql.RecordTypeStmt {
			endTime = now.Add(defaultStmtRecordDuration).Format(recordsql.InputTimeFormat)
		}
		recordRequest = RecordRequest{
			StartTime: now.Format(recordsql.InputTimeFormat),
			EndTime:   endTime,
			Location:  "",
		}
	}

	switch recordType {
	case recordsql.RecordTypeSQL:
		err = recordsql.StartRecordSql(
			recordRequest.StartTime,
			recordRequest.EndTime,
			recordRequest.Location,
		)
	case recordsql.RecordTypeStmt:
		err = recordsql.StartRecordStmt(
			recordRequest.StartTime,
			recordRequest.EndTime,
			recordRequest.Location,
		)
	default:
		err = fmt.Errorf("unknown record type: %d", recordType)
	}
	if err != nil {
		logger.Errorf("start record error, err:%s", err)
		BadRequestResponseWithMsg(c, logger, 0xffff, fmt.Sprintf("start record error: %s", err))
		return
	}
	c.JSON(http.StatusOK, &Message{
		Code: 0,
		Desc: "",
	})
	logger.Debugf("start record success")
}

type StopRecordResp struct {
	Message
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
}

func (ctl *ConfigController) stopRecordSql(c *gin.Context) {
	ctl.stopRecord(c, recordsql.RecordTypeSQL)
}

func (ctl *ConfigController) stopRecordStmt(c *gin.Context) {
	ctl.stopRecord(c, recordsql.RecordTypeStmt)
}

func (ctl *ConfigController) stopRecord(c *gin.Context, recordType recordsql.RecordType) {
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	logger.Debugf("get stop record, type:%s", recordType.String())
	var info *recordsql.MissionInfo
	switch recordType {
	case recordsql.RecordTypeSQL:
		info = recordsql.StopRecordSqlMission()
	case recordsql.RecordTypeStmt:
		info = recordsql.StopRecordStmtMission()
	}
	if info != nil {
		logger.Tracef("stop record info: %+v", info)
		c.JSON(http.StatusOK, &StopRecordResp{
			StartTime: info.StartTime,
			EndTime:   info.EndTime,
		})
	} else {
		logger.Tracef("no record running")
		c.JSON(http.StatusOK, &Message{
			Code: 0,
			Desc: "",
		})
	}
	logger.Debugf("stop record success")
}

type GetRecordSqlStateResp struct {
	Message
	Exists            bool   `json:"exists"`
	Running           bool   `json:"running"`
	StartTime         string `json:"start_time"`
	EndTime           string `json:"end_time"`
	CurrentConcurrent int32  `json:"current_concurrent"`
}

func (ctl *ConfigController) getRecordSqlState(c *gin.Context) {
	ctl.getRecordState(c, recordsql.RecordTypeSQL)
}

func (ctl *ConfigController) getRecordStmtState(c *gin.Context) {
	ctl.getRecordState(c, recordsql.RecordTypeStmt)
}

func (ctl *ConfigController) getRecordState(c *gin.Context, recordType recordsql.RecordType) {
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	logger.Debugf("get record state, type:%s", recordType.String())
	var state recordsql.RecordState
	switch recordType {
	case recordsql.RecordTypeSQL:
		state = recordsql.GetSqlMissionState()
	case recordsql.RecordTypeStmt:
		state = recordsql.GetStmtMissionState()
	}
	resp := &GetRecordSqlStateResp{
		Exists:            state.Exists,
		Running:           state.Running,
		StartTime:         state.StartTime,
		EndTime:           state.EndTime,
		CurrentConcurrent: state.CurrentConcurrent,
	}
	logger.Tracef("get record state: %+v", state)
	c.JSON(http.StatusOK, resp)
	logger.Debugf("get record success")
}

func init() {
	r := &ConfigController{}
	controller.AddController(r)
}
