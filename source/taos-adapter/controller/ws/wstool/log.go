package wstool

import (
	"context"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

func GetDuration(ctx context.Context) int64 {
	startTime := ctx.Value(StartTimeKey).(time.Time)
	duration := time.Since(startTime).Nanoseconds()
	if duration < 0 {
		return 0
	}
	return duration
}

func GetLogger(session *melody.Session) *logrus.Entry {
	return session.MustGet("logger").(*logrus.Entry)
}

func LogWSError(session *melody.Session, err error) {
	logger := session.MustGet("logger").(*logrus.Entry)
	var wsCloseErr *websocket.CloseError
	wsCloseErr, is := err.(*websocket.CloseError)
	if is {
		if wsCloseErr.Code == websocket.CloseNormalClosure {
			logger.Debug("ws close normal")
		} else {
			logger.Debugf("ws close in error, err:%s", wsCloseErr)
		}
	} else {
		if os.IsTimeout(err) {
			logger.Debugf("ws close due to timeout, err:%s", err)
		} else {
			logger.Debugf("ws error, err:%s", err)
		}
	}
}
