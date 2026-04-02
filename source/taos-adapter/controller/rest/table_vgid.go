package rest

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/connectpool"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
)

func (ctl *Restful) tableVgID(c *gin.Context) {
	logger := c.MustGet(LoggerKey).(*logrus.Entry)
	db := c.Param("db")
	var tables []string
	err := c.ShouldBindJSON(&tables)
	if err != nil {
		logger.Errorf("get json requeset error, %s", err.Error())
		BadRequestResponseWithMsg(c, logger, 0xffff, err.Error())
		return
	}
	logger.Tracef("get tableVgID, db:%s, tables:%v", db, tables)
	if len(db) == 0 || len(tables) == 0 {
		logger.Errorf("illegal params, db:%s, tables:%v", db, tables)
		BadRequestResponseWithMsg(c, logger, 0xffff, "illegal params")
		return
	}

	user, password, token := getAuthInfo(c)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	taosConn, err := commonpool.GetConnection(user, password, token, iptool.GetRealIP(c.Request))
	logger.Debugf("get connect cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("connect server error, err:%s", err)
		if errors.Is(err, commonpool.ErrWhitelistForbidden) {
			ForbiddenResponse(c, logger, commonpool.ErrWhitelistForbidden.Error())
			return
		}
		if errors.Is(err, connectpool.ErrTimeout) || errors.Is(err, connectpool.ErrMaxWait) {
			ServiceUnavailable(c, logger, err.Error())
			return
		}
		var tError *tErrors.TaosError
		if errors.As(err, &tError) {
			TaosErrorResponse(c, logger, int(tError.Code), tError.ErrStr)
			return
		}
		CommonErrorResponse(c, logger, err.Error())
		return
	}
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.Errorf("put connection error, err:%s", err)
		}
	}()
	vgIDs, code := syncinterface.TaosGetTablesVgID(taosConn.TaosConnection, db, tables, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TaosErrorStr(nil, logger, isDebug)
		logger.Errorf("get table vgID error, code:%d, msg:%s", code, errStr)
		TaosErrorResponse(c, logger, code, errStr)
		return
	}
	logger.Debugf("get table vgID cost:%s", log.GetLogDuration(isDebug, s))
	c.JSON(http.StatusOK, tableVgIDResp{Code: 0, VgIDs: vgIDs})
}

type tableVgIDResp struct {
	Code  int   `json:"code"`
	VgIDs []int `json:"vgIDs"`
}
