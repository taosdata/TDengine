package log

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/util"
)

func GinLog() gin.HandlerFunc {
	logger := GetLogger("WEB")

	return func(c *gin.Context) {
		qid := util.GetQid(c.GetHeader("X-QID"))

		logger := logger.WithFields(
			logrus.Fields{config.ReqIDKey: qid},
		)
		statusCode := c.Writer.Status()

		startTime := time.Now()
		c.Next()
		endTime := time.Now()
		latencyTime := endTime.Sub(startTime)
		reqMethod := c.Request.Method
		reqUri := c.Request.RequestURI

		clientIP := c.ClientIP()

		if statusCode != 200 {
			logger.Errorf("finish request, status_code:%3d, latency:%v, client_ip:%s, method:%s, uri:%s", statusCode, latencyTime, clientIP, reqMethod, reqUri)
			return
		}
		logger.Infof("finish request, status_code:%3d, latency:%v, client_ip:%s, method:%s, uri:%s", statusCode, latencyTime, clientIP, reqMethod, reqUri)
	}
}

type recoverLog struct {
	logger logrus.FieldLogger
}

func (r *recoverLog) Write(p []byte) (n int, err error) {
	r.logger.Errorln(string(p))
	return len(p), nil
}

func GinRecoverLog() gin.HandlerFunc {
	logger := GetLogger("WEB")
	return func(c *gin.Context) {
		writer := &recoverLog{logger: logger}
		gin.RecoveryWithWriter(writer)(c)
	}
}
