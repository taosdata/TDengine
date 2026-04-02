package log

import (
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
)

func GinLog() gin.HandlerFunc {
	logger := GetLogger("WEB")

	return func(c *gin.Context) {
		startTime := time.Now()
		c.Next()
		latencyTime := time.Since(startTime)
		reqMethod := c.Request.Method
		reqUri := url.QueryEscape(c.Request.RequestURI)
		statusCode := c.Writer.Status()
		clientIP := iptool.GetRealIP(c.Request)
		reqID, exist := c.Get(config.ReqIDKey)
		if exist {
			logger.Debugf("QID:0x%x finish request, status_code:%3d, latency:%v, client_ip:%s, method:%s, uri:%s", reqID, statusCode, latencyTime, clientIP, reqMethod, reqUri)
		} else {
			logger.Debugf("finish request, status_code:%3d, latency:%v, client_ip:%s, method:%s, uri:%s", statusCode, latencyTime, clientIP, reqMethod, reqUri)
		}
		if config.Conf.Log.EnableRecordHttpSql {
			sql, exist := c.Get("sql")
			if exist {
				sqlLogger.Debugf("%d '%s' '%s' '%s' %s", reqID, reqUri, reqMethod, clientIP, sql)
			}
		}
	}
}

type recoverLog struct {
	logger logrus.FieldLogger
}

func (r *recoverLog) Write(p []byte) (n int, err error) {
	r.logger.Error(string(p))
	return len(p), nil
}

func GinRecoverLog() gin.HandlerFunc {
	logger := GetLogger("WEB")
	return func(c *gin.Context) {
		writer := &recoverLog{logger: logger}
		gin.RecoveryWithWriter(writer)(c)
	}
}
