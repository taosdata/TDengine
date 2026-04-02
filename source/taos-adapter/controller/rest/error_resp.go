package rest

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/tools/web"
)

func UnAuthResponse(c *gin.Context, logger *logrus.Entry, code int) {
	badResponse(c, logger, http.StatusUnauthorized, code)
}

func BadRequestResponse(c *gin.Context, logger *logrus.Entry, code int) {
	badResponse(c, logger, http.StatusBadRequest, code)
}

func TooManyRequestResponse(c *gin.Context, logger *logrus.Entry, msg string) {
	errorResp(c, logger, http.StatusTooManyRequests, 0xffff, msg)
}

func InternalErrorResponse(c *gin.Context, logger *logrus.Entry, code int, msg string) {
	errorResp(c, logger, http.StatusInternalServerError, code, msg)
}

func ErrorResponse(c *gin.Context, logger *logrus.Entry, httpCode, code int, msg string) {
	errorResp(c, logger, httpCode, code, msg)
}

func badResponse(c *gin.Context, logger *logrus.Entry, httpCode int, code int) {
	errStr, exist := httperror.ErrorMsgMap[code]
	if !exist {
		errStr = "unknown error"
	}
	errorResp(c, logger, httpCode, code, errStr)
}

func BadRequestResponseWithMsg(c *gin.Context, logger *logrus.Entry, code int, msg string) {
	errorResp(c, logger, http.StatusBadRequest, code, msg)
}

func getErrorHttpStatus(errCode int32) int {
	if config.Conf.HttpCodeServerError {
		httpCode, exist := config.ErrorStatusMap[errCode]
		if exist {
			return httpCode
		}
		return http.StatusInternalServerError
	}
	return http.StatusOK
}

func TaosErrorResponse(c *gin.Context, logger *logrus.Entry, code int, msg string) {
	code = code & 0xffff
	httpCode := getErrorHttpStatus(int32(code))
	errorResp(c, logger, httpCode, code, msg)
}

func CommonErrorResponse(c *gin.Context, logger *logrus.Entry, msg string) {
	httpCode := getErrorHttpStatus(0xffff)
	errorResp(c, logger, httpCode, 0xffff, msg)
}

func ForbiddenResponse(c *gin.Context, logger *logrus.Entry, msg string) {
	errorResp(c, logger, http.StatusForbidden, 0xffff, msg)
}

func ServiceUnavailable(c *gin.Context, logger *logrus.Entry, msg string) {
	errorResp(c, logger, http.StatusServiceUnavailable, 0xffff, msg)
}

type MessageWithTiming struct {
	Code   int    `json:"code"`
	Desc   string `json:"desc"`
	Timing int64  `json:"timing"`
}

func errorResp(c *gin.Context, logger *logrus.Entry, httpCode int, code int, msg string) {
	st, ok := c.Get(StartTimeKey)
	if ok {
		timing := time.Since(st.(time.Time)).Nanoseconds()
		c.AbortWithStatusJSON(httpCode, &MessageWithTiming{
			Code:   code,
			Desc:   msg,
			Timing: timing,
		})
		logger.Tracef("error response, code: %d, desc: %s, timing: %d", code, msg, timing)
	} else {
		c.AbortWithStatusJSON(httpCode, &Message{
			Code: code,
			Desc: msg,
		})
		logger.Tracef("error response, code: %d, desc: %s", code, msg)
	}
	web.SetTaosErrorCode(c, code)
}
