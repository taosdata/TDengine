package admin

import "C"
import (
	"bytes"
	"database/sql/driver"
	"encoding/csv"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v3/common"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/controller/rest"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/thread"
	"github.com/taosdata/taosadapter/v3/tools/jsonbuilder"
	"github.com/taosdata/taosadapter/v3/tools/pool"
	"github.com/taosdata/taosadapter/v3/tools/web"
)

var logger = log.GetLogger("admin")

type Controller struct {
}

func (ctl *Controller) Init(router gin.IRouter) {
	api := router.Group("admin")
	api.GET("info", rest.CheckAuth, ctl.info)
	api.POST("info", rest.CheckAuth, ctl.info)
	api.POST("meta", rest.CheckAuth, ctl.meta)
	api.POST("login", rest.CheckAuth, ctl.login)
	api.POST("logout", rest.CheckAuth, ctl.logout)
	api.POST("sql", rest.CheckAuth, ctl.sql)
	api.POST("sql/:db", rest.CheckAuth, ctl.sql)
	api.POST("result", rest.CheckAuth, ctl.resultFile)
	api.POST("result/:db", rest.CheckAuth, ctl.resultFile)
}

type Info struct {
	Dbs    int `json:"dbs"`
	Tables int `json:"tables"`
	Users  int `json:"users"`
	Mnodes int `json:"mnodes"`
	Dnodes int `json:"dnodes"`
}
type InfoResponse struct {
	Status string  `json:"status"`
	Data   []*Info `json:"data"`
}

func (ctl *Controller) info(c *gin.Context) {
	var s time.Time
	isDebug := logger.Logger.IsLevelEnabled(logrus.DebugLevel)
	id := web.GetRequestID(c)
	logger := logger.WithField("sessionID", id)
	user := c.MustGet(rest.UserKey).(string)
	password := c.MustGet(rest.PasswordKey).(string)
	if isDebug {
		s = time.Now()
	}
	taosConnect, err := commonpool.GetConnection(user, password)
	logger.Debugln("taos connect cost:", time.Now().Sub(s))
	if err != nil {
		logger.WithError(err).Error("connect taosd error")
		var tError *tErrors.TaosError
		if errors.As(err, &tError) {
			rest.ErrorResponseWithMsg(c, int(tError.Code), tError.ErrStr)
			return
		} else {
			rest.ErrorResponseWithMsg(c, 0xffff, err.Error())
			return
		}
	}
	defer func() {
		if isDebug {
			s = time.Now()
		}
		err := taosConnect.Put()
		if err != nil {
			panic(err)
		}
		logger.Debugln("taos put connect cost:", time.Now().Sub(s))
	}()
	var (
		dbCounts    = 0
		tableCounts = int64(0)
		userCounts  = 0
		mnodeCounts = 0
		dnodeCounts = 0
	)

	{
		//show databases
		startExec := time.Now()
		sql := "show databases"
		logger.Debugln(startExec, "start execute sql:", sql)
		result, err := async.GlobalAsync.TaosExec(taosConnect.TaosConnection, sql, func(ts int64, precision int) driver.Value {
			return ts
		})
		logger.Debugln("execute sql cost:", time.Now().Sub(startExec))
		if err != nil {
			tError, ok := err.(*tErrors.TaosError)
			if ok {
				rest.ErrorResponseWithMsg(c, int(tError.Code), tError.ErrStr)
			} else {
				rest.ErrorResponseWithMsg(c, 0xffff, err.Error())
			}
			return
		}
		dbCounts = len(result.Data)

		tableFieldID := -1
		for i, name := range result.Header.ColNames {
			if name == "ntables" {
				tableFieldID = i
			}
		}
		if tableFieldID < 0 {
			rest.ErrorResponseWithMsg(c, 0xffff, "ntables not found")
			return
		}
		for _, datum := range result.Data {
			tableCounts += datum[tableFieldID].(int64)
		}
	}

	{
		//show users
		startExec := time.Now()
		sql := "show users"
		logger.Debugln(startExec, "start execute sql:", sql)
		result, err := async.GlobalAsync.TaosExec(taosConnect.TaosConnection, sql, func(ts int64, precision int) driver.Value {
			return ts
		})
		logger.Debugln("execute sql cost:", time.Now().Sub(startExec))
		if err != nil {
			tError, ok := err.(*tErrors.TaosError)
			if ok {
				rest.ErrorResponseWithMsg(c, int(tError.Code), tError.ErrStr)
			} else {
				rest.ErrorResponseWithMsg(c, 0xffff, err.Error())
			}
			return
		}
		userCounts = len(result.Data)
	}

	{
		//show mnodes
		startExec := time.Now()
		sql := "show mnodes"
		logger.Debugln(startExec, "start execute sql:", sql)
		result, err := async.GlobalAsync.TaosExec(taosConnect.TaosConnection, sql, func(ts int64, precision int) driver.Value {
			return ts
		})
		logger.Debugln("execute sql cost:", time.Now().Sub(startExec))
		if err != nil {
			tError, ok := err.(*tErrors.TaosError)
			if ok {
				rest.ErrorResponseWithMsg(c, int(tError.Code), tError.ErrStr)
			} else {
				rest.ErrorResponseWithMsg(c, 0xffff, err.Error())
			}
			return
		}
		mnodeCounts = len(result.Data)
	}

	{
		//show dnodes
		startExec := time.Now()
		sql := "show dnodes"
		logger.Debugln(startExec, "start execute sql:", sql)
		result, err := async.GlobalAsync.TaosExec(taosConnect.TaosConnection, sql, func(ts int64, precision int) driver.Value {
			return ts
		})
		logger.Debugln("execute sql cost:", time.Now().Sub(startExec))
		if err != nil {
			tError, ok := err.(*tErrors.TaosError)
			if ok {
				rest.ErrorResponseWithMsg(c, int(tError.Code), tError.ErrStr)
			} else {
				rest.ErrorResponseWithMsg(c, 0xffff, err.Error())
			}
			return
		}
		dnodeCounts = len(result.Data)
	}
	info := &Info{
		Dbs:    dbCounts,
		Tables: int(tableCounts),
		Users:  userCounts,
		Mnodes: mnodeCounts,
		Dnodes: dnodeCounts,
	}

	infoResp := &InfoResponse{
		Status: "succ",
		Data:   []*Info{info},
	}
	c.JSON(http.StatusOK, infoResp)
}

type Meta struct {
	Status string          `json:"status"`
	Head   []string        `json:"head"`
	Data   [][]interface{} `json:"data"`
	Rows   int             `json:"rows"`
}

func (ctl *Controller) meta(c *gin.Context) {
	var s time.Time
	isDebug := logger.Logger.IsLevelEnabled(logrus.DebugLevel)
	id := web.GetRequestID(c)
	logger := logger.WithField("sessionID", id)
	b, err := c.GetRawData()
	if err != nil {
		logger.WithError(err).Error("get request body error")
		rest.ErrorResponse(c, httperror.HTTP_INVALID_CONTENT_LENGTH)
		return
	}
	if len(b) == 0 {
		logger.Errorln("no msg got")
		rest.ErrorResponse(c, httperror.HTTP_NO_MSG_INPUT)
		return
	}
	sql := strings.TrimSpace(string(b))
	user := c.MustGet(rest.UserKey).(string)
	password := c.MustGet(rest.PasswordKey).(string)
	if isDebug {
		s = time.Now()
	}
	taosConnect, err := commonpool.GetConnection(user, password)
	logger.Debugln("taos connect cost:", time.Now().Sub(s))
	if err != nil {
		logger.WithError(err).Error("connect taosd error")
		var tError *tErrors.TaosError
		if errors.As(err, &tError) {
			rest.ErrorResponseWithMsg(c, int(tError.Code), tError.ErrStr)
			return
		} else {
			rest.ErrorResponseWithMsg(c, 0xffff, err.Error())
			return
		}
	}
	defer func() {
		if isDebug {
			s = time.Now()
		}
		err := taosConnect.Put()
		if err != nil {
			panic(err)
		}
		logger.Debugln("taos put connect cost:", time.Now().Sub(s))
	}()

	startExec := time.Now()
	logger.Debugln(startExec, "start execute sql:", sql)
	header, err := meta(taosConnect.TaosConnection, sql)
	logger.Debugln("execute sql cost:", time.Now().Sub(startExec))
	if err != nil {
		tError, ok := err.(*tErrors.TaosError)
		if ok {
			rest.ErrorResponseWithMsg(c, int(tError.Code), tError.ErrStr)
		} else {
			rest.ErrorResponseWithMsg(c, 0xffff, err.Error())
		}
		return
	}
	data := make([][]interface{}, len(header.ColNames))
	for i, name := range header.ColNames {
		columnType := header.TypeDatabaseName(i)
		columnType = strings.ToLower(columnType)
		if columnType == "" {
			columnType = "unknown"
		}
		data[i] = []interface{}{
			columnType,
			name,
			header.ColLength[i],
		}
	}
	resp := &Meta{
		Status: "succ",
		Head:   []string{"column type", "column name", "column bytes"},
		Data:   data,
		Rows:   len(header.ColNames),
	}
	c.JSON(http.StatusOK, resp)
}

func meta(taosConnect unsafe.Pointer, sql string) (*wrapper.RowsHeader, error) {
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	result, err := async.GlobalAsync.TaosQuery(taosConnect, sql, handler)
	defer func() {
		if result != nil && result.Res != nil {
			thread.Lock()
			wrapper.TaosFreeResult(result.Res)
			thread.Unlock()
		}
	}()
	if err != nil {
		return nil, err
	}
	res := result.Res
	code := wrapper.TaosError(res)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosErrorStr(res)
		return nil, tErrors.NewError(code, errStr)
	}
	var fieldsCount int
	isUpdate := wrapper.TaosIsUpdateQuery(res)
	if isUpdate {
		return nil, errors.New("update syntax not supported")
	}
	fieldsCount = wrapper.TaosNumFields(res)
	var rowsHeader *wrapper.RowsHeader
	rowsHeader, err = wrapper.ReadColumn(res, fieldsCount)
	if err != nil {
		return nil, err
	}
	return rowsHeader, nil
}

func (ctl *Controller) login(c *gin.Context) {
	user := c.MustGet(rest.UserKey).(string)
	password := c.MustGet(rest.PasswordKey).(string)
	if len(user) < 0 || len(user) > 24 || len(password) < 0 || len(password) > 24 {
		rest.ErrorResponse(c, httperror.HTTP_GEN_TAOSD_TOKEN_ERR)
		return
	}
	conn, err := commonpool.GetConnection(user, password)
	if err != nil {
		rest.ErrorResponse(c, httperror.TSDB_CODE_RPC_AUTH_FAILURE)
		return
	}
	conn.Put()
	token, err := rest.EncodeDes(user, password)
	if err != nil {
		rest.ErrorResponse(c, httperror.HTTP_GEN_TAOSD_TOKEN_ERR)
		return
	}
	c.JSON(http.StatusOK, &rest.Message{
		Code: 0,
		Desc: token,
	})
}

type Logout struct {
	Code int    `json:"code"`
	Desc string `json:"desc"`
}

var LogoutSuccess = &Logout{
	Code: 0,
	Desc: "logout success",
}

func (ctl *Controller) logout(c *gin.Context) {
	c.JSON(http.StatusOK, LogoutSuccess)
}

const LayoutMillSecond = "2006-01-02 15:04:05.000"
const LayoutMicroSecond = "2006-01-02 15:04:05.000000"
const LayoutNanoSecond = "2006-01-02 15:04:05.000000000"

func (ctl *Controller) sql(c *gin.Context) {
	db := c.Param("db")
	rest.DoQuery(c, db, func(builder *jsonbuilder.Stream, ts int64, precision int) {
		switch precision {
		case common.PrecisionMilliSecond:
			builder.WriteString(common.TimestampConvertToTime(ts, precision).Local().Format(LayoutMillSecond))
		case common.PrecisionMicroSecond:
			builder.WriteString(common.TimestampConvertToTime(ts, precision).Local().Format(LayoutMicroSecond))
		case common.PrecisionNanoSecond:
			builder.WriteString(common.TimestampConvertToTime(ts, precision).Local().Format(LayoutNanoSecond))
		default:
			builder.WriteNil()
		}
	})
}

func (ctl *Controller) resultFile(c *gin.Context) {
	db := c.Param("db")
	DoQuery(c, db, func(buffer *bytes.Buffer, ts int64, precision int) {
		switch precision {
		case common.PrecisionMilliSecond:
			buffer.WriteString(common.TimestampConvertToTime(ts, precision).Local().Format(LayoutMillSecond))
		case common.PrecisionMicroSecond:
			buffer.WriteString(common.TimestampConvertToTime(ts, precision).Local().Format(LayoutMicroSecond))
		case common.PrecisionNanoSecond:
			buffer.WriteString(common.TimestampConvertToTime(ts, precision).Local().Format(LayoutNanoSecond))
		default:
			buffer.WriteString("null")
		}
	})
}

func DoQuery(c *gin.Context, db string, timeFunc FormatTimeFunc) {
	var s time.Time
	isDebug := logger.Logger.IsLevelEnabled(logrus.DebugLevel)
	id := web.GetRequestID(c)
	logger := logger.WithField("sessionID", id)
	b, err := c.GetRawData()
	if err != nil {
		logger.WithError(err).Error("get request body error")
		c.AbortWithStatusJSON(http.StatusBadRequest, tErrors.NewError(httperror.HTTP_INVALID_CONTENT_LENGTH, httperror.ErrorMsgMap[httperror.HTTP_INVALID_CONTENT_LENGTH]))
		return
	}
	if len(b) == 0 {
		logger.Errorln("no msg got")
		c.AbortWithStatusJSON(http.StatusBadRequest, tErrors.NewError(httperror.HTTP_NO_MSG_INPUT, httperror.ErrorMsgMap[httperror.HTTP_NO_MSG_INPUT]))
		return
	}
	sql := strings.TrimSpace(string(b))
	if len(sql) == 0 {
		logger.Errorln("no sql got")
		c.AbortWithStatusJSON(http.StatusBadRequest, tErrors.NewError(httperror.HTTP_NO_SQL_INPUT, httperror.ErrorMsgMap[httperror.HTTP_NO_SQL_INPUT]))
		return
	}
	user := c.MustGet(rest.UserKey).(string)
	password := c.MustGet(rest.PasswordKey).(string)
	if isDebug {
		s = time.Now()
	}
	taosConnect, err := commonpool.GetConnection(user, password)
	if isDebug {
		logger.Debugln("taos connect cost:", time.Now().Sub(s))
	}
	if err != nil {
		logger.WithError(err).Error("connect taosd error")
		c.AbortWithStatusJSON(http.StatusBadRequest, err)
	}
	defer func() {
		if isDebug {
			s = time.Now()
		}
		err := taosConnect.Put()
		if err != nil {
			panic(err)
		}
		if isDebug {
			logger.Debugln("taos put connect cost:", time.Now().Sub(s))
		}
	}()

	if len(db) > 0 {
		if isDebug {
			s = time.Now()
		}
		// Attempt to select the database does not return even if there is an error
		// To avoid error reporting in the `create database` statement
		thread.Lock()
		_ = wrapper.TaosSelectDB(taosConnect.TaosConnection, db)
		thread.Unlock()
		logger.Debugln("taos select db cost:", time.Now().Sub(s))
	}
	execute(c, logger, taosConnect.TaosConnection, sql, timeFunc)
}

func execute(c *gin.Context, logger *logrus.Entry, taosConnect unsafe.Pointer, sql string, timeFormat FormatTimeFunc) {
	isDebug := logger.Logger.IsLevelEnabled(logrus.DebugLevel)
	handler := async.GlobalAsync.HandlerPool.Get()
	defer async.GlobalAsync.HandlerPool.Put(handler)
	var s time.Time
	if isDebug {
		s = time.Now()
	}
	result, _ := async.GlobalAsync.TaosQuery(taosConnect, sql, handler)
	if isDebug {
		logger.Debugln("taos query cost:", time.Now().Sub(s))
	}
	defer func() {
		if result != nil && result.Res != nil {
			if isDebug {
				s = time.Now()
			}
			thread.Lock()
			wrapper.TaosFreeResult(result.Res)
			thread.Unlock()
			if isDebug {
				logger.Debugln("taos free result cost:", time.Now().Sub(s))
			}
		}
	}()
	res := result.Res
	code := wrapper.TaosError(res)
	if code != httperror.SUCCESS {
		errStr := wrapper.TaosErrorStr(res)
		c.AbortWithStatusJSON(http.StatusInternalServerError, tErrors.NewError(code, errStr))
		return
	}
	isUpdate := wrapper.TaosIsUpdateQuery(res)
	if isUpdate {
		c.AbortWithStatusJSON(http.StatusBadRequest, errors.New("only query statements are supported"))
		return
	}
	c.Status(http.StatusOK)
	c.Header("Content-type", "text/csv")
	c.Header("Content-Disposition", "attachment; filename=query.csv")
	c.Header("Transfer-Encoding", "chunked")
	c.Header("Access-Control-Expose-Headers", "Content-Disposition")
	//UTF-8 BOM
	//_, err := c.Writer.WriteString("\xEF\xBB\xBF")
	//if err != nil {
	//	c.AbortWithStatusJSON(http.StatusInternalServerError, err)
	//	return
	//}
	w := csv.NewWriter(c.Writer)
	if monitor.QueryPaused() {
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, errors.New("query memory exceeds threshold"))
		return
	}
	fieldsCount := wrapper.TaosNumFields(res)
	rowsHeader, err := wrapper.ReadColumn(res, fieldsCount)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err)
		return
	}
	err = w.Write(rowsHeader.ColNames)
	if err != nil {
		return
	}
	w.Flush()
	err = w.Error()
	if err != nil {
		return
	}
	precision := wrapper.TaosResultPrecision(res)
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	RowBuffer := make([]string, fieldsCount)
	tmpSize := 0
	for {
		if isDebug {
			s = time.Now()
		}
		result, _ = async.GlobalAsync.TaosFetchRowsA(res, handler)
		if isDebug {
			logger.Debugln("taos fetch_rows_a cost:", time.Now().Sub(s))
		}
		if result.N == 0 {
			break
		} else {
			if result.N < 0 {
				break
			}
			res = result.Res
			for i := 0; i < result.N; i++ {
				var row unsafe.Pointer
				thread.Lock()
				row = wrapper.TaosFetchRow(res)
				thread.Unlock()
				lengths := wrapper.FetchLengths(res, fieldsCount)
				if err != nil {
					return
				}
				for j := 0; j < fieldsCount; j++ {
					WriteRow(b, row, j, rowsHeader.ColTypes[j], lengths[j], precision, timeFormat)
					tmpSize += b.Len()
					RowBuffer[j] = b.String()
					b.Reset()
				}
				err = w.Write(RowBuffer)
				if err != nil {
					return
				}
				if tmpSize > 16000 {
					w.Flush()
					err = w.Error()
					if err != nil {
						return
					}
					tmpSize = 0
				}
			}
		}
	}
	w.Flush()
	err = w.Error()
	if err != nil {
		return
	}
}

type FormatTimeFunc func(buffer *bytes.Buffer, ts int64, precision int)

func WriteRow(buffer *bytes.Buffer, row unsafe.Pointer, offset int, colType uint8, length int, precision int, timeFormat FormatTimeFunc) {
	p := unsafe.Pointer(*(*uintptr)(unsafe.Pointer(uintptr(row) + uintptr(offset)*wrapper.PointerSize)))
	if p == nil {
		buffer.WriteString("null")
		return
	}
	switch colType {
	case common.TSDB_DATA_TYPE_BOOL:
		if v := *((*byte)(p)); v != 0 {
			buffer.WriteString("true")
		} else {
			buffer.WriteString("false")
		}
	case common.TSDB_DATA_TYPE_TINYINT:
		fmt.Fprintf(buffer, "%d", *((*int8)(p)))
	case common.TSDB_DATA_TYPE_SMALLINT:
		fmt.Fprintf(buffer, "%d", *((*int16)(p)))
	case common.TSDB_DATA_TYPE_INT:
		fmt.Fprintf(buffer, "%d", *((*int32)(p)))
	case common.TSDB_DATA_TYPE_BIGINT:
		fmt.Fprintf(buffer, "%d", *((*int64)(p)))
	case common.TSDB_DATA_TYPE_UTINYINT:
		fmt.Fprintf(buffer, "%d", *((*uint8)(p)))
	case common.TSDB_DATA_TYPE_USMALLINT:
		fmt.Fprintf(buffer, "%d", *((*uint16)(p)))
	case common.TSDB_DATA_TYPE_UINT:
		fmt.Fprintf(buffer, "%d", *((*uint32)(p)))
	case common.TSDB_DATA_TYPE_UBIGINT:
		fmt.Fprintf(buffer, "%d", *((*uint64)(p)))
	case common.TSDB_DATA_TYPE_FLOAT:
		fmt.Fprintf(buffer, "%f", *((*float32)(p)))
	case common.TSDB_DATA_TYPE_DOUBLE:
		fmt.Fprintf(buffer, "%f", *((*float64)(p)))
	case common.TSDB_DATA_TYPE_BINARY, common.TSDB_DATA_TYPE_NCHAR:
		for i := 0; i < length; i++ {
			buffer.WriteByte(*((*byte)(unsafe.Pointer(uintptr(p) + uintptr(i)))))
		}
	case common.TSDB_DATA_TYPE_TIMESTAMP:
		timeFormat(buffer, *((*int64)(p)), precision)
	case common.TSDB_DATA_TYPE_JSON:
		for i := 0; i < length; i++ {
			buffer.WriteByte(*((*byte)(unsafe.Pointer(uintptr(p) + uintptr(i)))))
		}
	default:
		buffer.WriteString("null")
		return
	}
}

func init() {
	controller.AddController(&Controller{})
}
