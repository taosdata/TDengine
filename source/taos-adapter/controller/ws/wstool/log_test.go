package wstool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

func TestGetDuration(t *testing.T) {
	startTime := time.Now()
	ctx := context.WithValue(context.Background(), StartTimeKey, startTime)
	time.Sleep(100 * time.Millisecond)
	duration := GetDuration(ctx)
	assert.Greater(t, duration, int64(0))

	startTime = startTime.Add(5 * time.Second)
	ctx = context.WithValue(context.Background(), StartTimeKey, startTime)
	duration = GetDuration(ctx)
	assert.Equal(t, int64(0), duration)
}

func TestGetLogger(t *testing.T) {
	logger := log.GetLogger("test")
	session := &melody.Session{}
	session.Set("logger", logger.WithField("test_field", "test_value"))
	entry := GetLogger(session)
	assert.Equal(t, "test_value", entry.Data["test_field"])
}

func TestLogWSError(t *testing.T) {
	logger := log.GetLogger("test")
	session := &melody.Session{}
	session.Set("logger", logger.WithField("test_field", "test_value"))
	LogWSError(session, nil)
	LogWSError(session, &websocket.CloseError{Code: websocket.CloseNormalClosure})
	LogWSError(session, &websocket.CloseError{Code: websocket.CloseAbnormalClosure})
	LogWSError(session, errors.New("common error"))
}
