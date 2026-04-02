package ws

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	_ "github.com/taosdata/taosadapter/v3/controller/rest"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
	"github.com/taosdata/taosadapter/v3/version"
)

var router *gin.Engine

func TestMain(m *testing.M) {
	viper.Set("pool.maxConnect", 10000)
	viper.Set("pool.maxIdle", 10000)
	viper.Set("logLevel", "trace")
	viper.Set("uploadKeeper.enable", false)
	config.Init()
	config.Conf.Request = &config.Request{
		QueryLimitEnable:                 false,
		ExcludeQueryLimitSql:             []string{"selectserver_version()", "select1"},
		ExcludeQueryLimitSqlMaxByteCount: 22,
		ExcludeQueryLimitSqlMinByteCount: 7,
		ExcludeQueryLimitSqlRegex:        []*regexp.Regexp{regexp.MustCompile(`(?i)^select\s+.*from\s+information_schema.*`)},
		Default: &config.LimitConfig{
			QueryLimit:       1,
			QueryWaitTimeout: 1,
			QueryMaxWait:     0,
		},
	}
	log.ConfigLog()
	db.PrepareConnection()
	gin.SetMode(gin.ReleaseMode)
	router = gin.New()
	controllers := controller.GetControllers()
	for _, webController := range controllers {
		webController.Init(router)
	}
	os.Exit(m.Run())
}

type restResp struct {
	Code int    `json:"code"`
	Desc string `json:"desc"`
}

func doRestful(sql string, db string) (code int, message string) {
	w := httptest.NewRecorder()
	body := strings.NewReader(sql)
	url := "/rest/sql"
	if db != "" {
		url = fmt.Sprintf("/rest/sql/%s", db)
	}
	req, _ := http.NewRequest(http.MethodPost, url, body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		return w.Code, w.Body.String()
	}
	b, _ := io.ReadAll(w.Body)
	var res restResp
	_ = json.Unmarshal(b, &res)
	return res.Code, res.Desc
}

type httpQueryResp struct {
	Code       int              `json:"code,omitempty"`
	Desc       string           `json:"desc,omitempty"`
	ColumnMeta [][]driver.Value `json:"column_meta,omitempty"`
	Data       [][]driver.Value `json:"data,omitempty"`
	Rows       int              `json:"rows,omitempty"`
}

func restQuery(sql string, db string) *httpQueryResp {
	w := httptest.NewRecorder()
	body := strings.NewReader(sql)
	url := "/rest/sql"
	if db != "" {
		url = fmt.Sprintf("/rest/sql/%s", db)
	}
	req, _ := http.NewRequest(http.MethodPost, url, body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		return &httpQueryResp{
			Code: w.Code,
			Desc: w.Body.String(),
		}
	}
	b, _ := io.ReadAll(w.Body)
	var res httpQueryResp
	_ = json.Unmarshal(b, &res)
	return &res
}

func doWebSocket(ws *websocket.Conn, action string, arg interface{}) (resp []byte, err error) {
	var b []byte
	if arg != nil {
		b, err = json.Marshal(arg)
		if err != nil {
			return nil, err
		}
	}
	a, err := json.Marshal(Request{Action: action, Args: b})
	if err != nil {
		return nil, err
	}
	err = ws.WriteMessage(websocket.TextMessage, a)
	if err != nil {
		return nil, err
	}
	_, message, err := ws.ReadMessage()
	return message, err
}

func doWebSocketWithoutResp(ws *websocket.Conn, action string, arg interface{}) error {
	var b []byte
	if arg != nil {
		b, _ = json.Marshal(arg)
	}
	a, _ := json.Marshal(Request{Action: action, Args: b})
	err := ws.WriteMessage(websocket.TextMessage, a)
	if err != nil {
		return err
	}
	return nil
}

func TestVersion(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	resp, err := doWebSocket(ws, wstool.ClientVersion, nil)
	assert.NoError(t, err)
	var versionResp versionResp
	err = json.Unmarshal(resp, &versionResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, versionResp.Code, versionResp.Message)
	assert.Equal(t, version.TaosClientVersion, versionResp.Version)
	assert.Equal(t, wstool.ClientVersion, versionResp.Action)

	req := "{\"action\":\"version\"}"
	err = ws.WriteMessage(websocket.TextMessage, []byte(req))
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)

	assert.NoError(t, err)
	err = json.Unmarshal(resp, &versionResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, versionResp.Code, versionResp.Message)
	assert.Equal(t, version.TaosClientVersion, versionResp.Version)
	assert.Equal(t, wstool.ClientVersion, versionResp.Action)
}
