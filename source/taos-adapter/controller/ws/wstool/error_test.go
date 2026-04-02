package wstool

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

func TestWSError(t *testing.T) {
	m := melody.New()
	m.Config.MaxMessageSize = 4 * 1024 * 1024
	ctx := context.WithValue(context.Background(), StartTimeKey, time.Now())
	reqID := uint64(12345)
	taosErr := &tErrors.TaosError{
		Code:   1001,
		ErrStr: "test error",
	}
	commonErr := errors.New("test common error")
	logger := log.GetLogger("test").WithField("test", "TestWSError")
	m.HandleMessage(func(session *melody.Session, data []byte) {
		switch data[0] {
		case '1':
			WSError(ctx, session, logger, taosErr, "test action", reqID)
		case '2':
			WSError(ctx, session, logger, commonErr, "test common error action", reqID)
		}

	})
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.GET("/test", func(c *gin.Context) {
		_ = m.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{})
	})
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/test", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	err = ws.WriteMessage(websocket.TextMessage, []byte{'1'})
	assert.NoError(t, err)
	wt, resp, err := ws.ReadMessage()
	assert.NoError(t, err)
	assert.NoError(t, err)
	assert.Equal(t, websocket.TextMessage, wt)
	var errorResp WSErrorResp
	err = json.Unmarshal(resp, &errorResp)
	assert.NoError(t, err)
	assert.Equal(t, 1001, errorResp.Code)
	assert.Equal(t, reqID, errorResp.ReqID)
	assert.Equal(t, "test error", errorResp.Message)
	assert.Equal(t, "test action", errorResp.Action)
	assert.Greater(t, errorResp.Timing, int64(0))

	err = ws.WriteMessage(websocket.TextMessage, []byte{'2'})
	assert.NoError(t, err)
	wt, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	assert.NoError(t, err)
	assert.Equal(t, websocket.TextMessage, wt)
	err = json.Unmarshal(resp, &errorResp)
	assert.NoError(t, err)
	assert.Equal(t, 65535, errorResp.Code)
	assert.Equal(t, reqID, errorResp.ReqID)
	assert.Equal(t, "test common error", errorResp.Message)
	assert.Equal(t, "test common error action", errorResp.Action)
	assert.Greater(t, errorResp.Timing, int64(0))
}
