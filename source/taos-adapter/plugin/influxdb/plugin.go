package influxdb

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/common"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/plugin"
	"github.com/taosdata/taosadapter/v3/schemaless/inserter"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/connectpool"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/web"
)

var logger = log.GetLogger("PLG").WithField("mod", "influxdb")

type Influxdb struct {
	conf Config
}

func (p *Influxdb) String() string {
	return "influxdb"
}

func (p *Influxdb) Version() string {
	return "v1"
}

func (p *Influxdb) Init(r gin.IRouter) error {
	p.conf.setValue()
	if !p.conf.Enable {
		logger.Debug("influxdb disabled")
		return nil
	}
	r.Use(func(c *gin.Context) {
		if monitor.AllPaused() {
			c.Header("Retry-After", "120")
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, "memory exceeds threshold")
			return
		}
	})
	r.POST("write", getAuth, p.write)
	return nil
}

func (p *Influxdb) Start() error {
	if !p.conf.Enable {
		return nil
	}
	return nil
}

func (p *Influxdb) Stop() error {
	return nil
}

// @Tags influxdb
// @Summary influxdb write
// @Description influxdb write v1 https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/
// @Accept plain
// @Produce json
// @Param Authorization header string false "basic authorization"
// @Param u query string false "username to authenticate the request"
// @Param p query string false "username to authenticate the request"
// @Param db query string true "the database to write data to"
// @Param precision query string false "the precision of Unix timestamps in the line protocol"
// @Success 204 {string} string "no content"
// @Failure 401 {object} message
// @Failure 400 {object} badRequest
// @Failure 500 {object} message
// @Router /influxdb/v1/write [post]
func (p *Influxdb) write(c *gin.Context) {
	var reqID uint64
	var err error
	if reqIDStr := c.Query("req_id"); len(reqIDStr) > 0 {
		if reqID, err = strconv.ParseUint(reqIDStr, 10, 64); err != nil {
			logger.Errorf("illegal param, req_id must be numeric %s, req_id:%s", err.Error(), reqIDStr)
			p.badRequestResponse(c, &badRequest{
				Code:    "illegal param",
				Message: "req_id must be numeric",
				Op:      "parse req_id",
				Err:     "req_id must be numeric",
			})
			return
		}
	}
	if reqID == 0 {
		reqID = uint64(generator.GetReqID())
		logger.Tracef("req_id is 0, generate new req_id, QID:0x%x", reqID)
	}
	c.Set(config.ReqIDKey, reqID)
	logger := logger.WithField(config.ReqIDKey, reqID)

	isDebug := log.IsDebug()
	precision := c.Query("precision")
	if len(precision) == 0 {
		precision = "ns"
	}
	logger.Debugf("request precision:%s", precision)
	user, password, token, err := plugin.GetAuth(c)
	if err != nil {
		logger.Errorf("get user and password error:%s", err.Error())
		p.commonResponse(c, http.StatusUnauthorized, &message{
			Code:    "forbidden",
			Message: err.Error(),
		})
		return
	}
	db := c.Query("db")
	logger.Tracef("request db:%s", db)
	if len(db) == 0 {
		logger.Error("db required")
		p.badRequestResponse(c, &badRequest{
			Code:    "not found",
			Message: "db required",
			Op:      "get db query",
			Err:     "db required",
			Line:    0,
		})
		return
	}
	var ttl int
	ttlStr := c.Query("ttl")
	if len(ttlStr) > 0 {
		logger.Tracef("request ttl %s", ttlStr)
		ttl, err = strconv.Atoi(ttlStr)
		if err != nil {
			logger.Errorf("illegal param, ttl must be numeric %s, ttl:%s", err, ttlStr)
			p.badRequestResponse(c, &badRequest{
				Code:    "illegal param",
				Message: "ttl must be numeric",
				Op:      "parse ttl",
				Err:     "ttl must be numeric",
			})
			return
		}
	}
	tableNameKey := c.Query("table_name_key")
	data, err := c.GetRawData()
	if err != nil {
		logger.Errorf("read line error, err:%s", err)
		p.badRequestResponse(c, &badRequest{
			Code:    "internal error",
			Message: "read line error",
			Op:      "read line",
			Err:     err.Error(),
			Line:    0,
		})
		return
	}
	logger.Debugf("request data:%s", data)
	s := log.GetLogNow(isDebug)
	taosConn, err := commonpool.GetConnection(user, password, token, iptool.GetRealIP(c.Request))
	logger.Debugf("get connection finish, cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("connect server error, err:%s", err)
		if errors.Is(err, commonpool.ErrWhitelistForbidden) {
			p.commonResponse(c, http.StatusUnauthorized, &message{
				Code:    "forbidden",
				Message: err.Error(),
			})
			return
		}
		if errors.Is(err, connectpool.ErrTimeout) || errors.Is(err, connectpool.ErrMaxWait) {
			p.commonResponse(c, http.StatusServiceUnavailable, &message{
				Code:    "internal error",
				Message: err.Error(),
			})
			return
		}
		p.commonResponse(c, http.StatusInternalServerError, &message{Code: "internal error", Message: err.Error()})
		return
	}
	defer func() {
		logger.Tracef("put connection")
		putErr := taosConn.Put()
		if putErr != nil {
			logger.Errorf("connect pool put error, err:%s", putErr)
		}
	}()
	conn := taosConn.TaosConnection
	app := c.Query("app")
	if app != "" {
		errCode := syncinterface.TaosOptionsConnection(conn, common.TSDB_OPTION_CONNECTION_USER_APP, &app, logger, isDebug)
		if errCode != 0 {
			logger.Errorf("set app error, app:%s, code:%d, msg:%s", app, errCode, syncinterface.TaosErrorStr(nil, logger, isDebug))
		}
	}
	s = log.GetLogNow(isDebug)
	logger.Tracef("start insert influxdb, data:%s", data)
	err = inserter.InsertInfluxdb(conn, data, db, precision, ttl, reqID, tableNameKey, logger)
	logger.Debugf("finish insert influxdb, cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("insert line error, data:%s, err:%s", data, err)
		taosError, is := err.(*tErrors.TaosError)
		if is {
			web.SetTaosErrorCode(c, int(taosError.Code))
		}
		p.commonResponse(c, http.StatusInternalServerError, &message{Code: "internal error", Message: err.Error()})
		return
	}
	logger.Debugf("insert line success")
	c.Status(http.StatusNoContent)
}

type badRequest struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Op      string `json:"op"`
	Err     string `json:"err"`
	Line    int    `json:"line"`
}

type message struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (p *Influxdb) badRequestResponse(c *gin.Context, resp *badRequest) {
	c.JSON(http.StatusBadRequest, resp)
}
func (p *Influxdb) commonResponse(c *gin.Context, code int, resp *message) {
	c.JSON(code, resp)
}

func getAuth(c *gin.Context) {
	_, cloudTokenExists := c.GetQuery("token")
	auth := c.GetHeader("Authorization")
	if len(auth) != 0 {
		auth = strings.TrimSpace(auth)
		if strings.HasPrefix(auth, "Basic ") && len(auth) > 6 {
			user, password, err := tools.DecodeBasic(auth[6:])
			if err == nil {
				c.Set(plugin.UserKey, user)
				c.Set(plugin.PasswordKey, password)
			}
		} else if !cloudTokenExists && strings.HasPrefix(auth, "Bearer ") && len(auth) > 7 {
			token := strings.TrimSpace(auth[7:])
			c.Set(plugin.TokenKey, token)
		}
	}

	user := c.Query("u")
	password := c.Query("p")
	if len(user) != 0 {
		c.Set(plugin.UserKey, user)
	}
	if len(password) != 0 {
		c.Set(plugin.PasswordKey, password)
	}
}

func init() {
	plugin.Register(&Influxdb{})
}
