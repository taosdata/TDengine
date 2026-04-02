package schemaless

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	_ "github.com/taosdata/taosadapter/v3/controller/rest"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

var router *gin.Engine

func TestMain(m *testing.M) {
	viper.Set("pool.maxConnect", 10000)
	viper.Set("pool.maxIdle", 10000)
	viper.Set("logLevel", "trace")
	viper.Set("uploadKeeper.enable", false)
	config.Init()
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

func TestRestful_InitSchemaless(t *testing.T) {
	code, message := doRestful("drop database if exists test_schemaless_ws", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_schemaless_ws", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_schemaless_ws"))

	defer func() {
		code, message = doRestful("drop database if exists test_schemaless_ws", "")
		assert.Equal(t, 0, code, message)
	}()

	s := httptest.NewServer(router)
	defer s.Close()
	url := strings.Replace(s.URL, "http", "ws", 1) + "/rest/schemaless"

	cases := []struct {
		name         string
		protocol     int
		precision    string
		data         string
		ttl          int
		code         int
		totalRows    int32
		affectedRows int
	}{
		{
			name:      "influxdb",
			protocol:  wrapper.InfluxDBLineProtocol,
			precision: "ms",
			data: "measurement,host=host1 field1=2i,field2=2.0 1577837300000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837400000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837500000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837600000",
			ttl:          1000,
			code:         0,
			totalRows:    4,
			affectedRows: 4,
		},
		{
			name:      "opentsdb_telnet",
			protocol:  wrapper.OpenTSDBTelnetLineProtocol,
			precision: "ms",
			data: "meters.current 1648432611249 10.3 location=California.SanFrancisco group=2\n" +
				"meters.current 1648432611250 12.6 location=California.SanFrancisco group=2\n" +
				"meters.current 1648432611249 10.8 location=California.LosAngeles group=3\n" +
				"meters.current 1648432611250 11.3 location=California.LosAngeles group=3\n" +
				"meters.voltage 1648432611249 219 location=California.SanFrancisco group=2\n" +
				"meters.voltage 1648432611250 218 location=California.SanFrancisco group=2\n" +
				"meters.voltage 1648432611249 221 location=California.LosAngeles group=3\n" +
				"meters.voltage 1648432611250 217 location=California.LosAngeles group=3",
			ttl:          1000,
			code:         0,
			totalRows:    8,
			affectedRows: 8,
		},
		{
			name:      "opentsdb_json",
			protocol:  wrapper.OpenTSDBJsonFormatProtocol,
			precision: "ms",
			data: `[
    {
        "metric": "meters2.current",
        "timestamp": 1648432611249,
        "value": 10.3,
        "tags": {
            "location": "California.SanFrancisco",
            "groupid": 2
        }
    },
    {
        "metric": "meters2.voltage",
        "timestamp": 1648432611249,
        "value": 219,
        "tags": {
            "location": "California.LosAngeles",
            "groupid": 1
        }
    },
    {
        "metric": "meters2.current",
        "timestamp": 1648432611250,
        "value": 12.6,
        "tags": {
            "location": "California.SanFrancisco",
            "groupid": 2
        }
    },
    {
        "metric": "meters2.voltage",
        "timestamp": 1648432611250,
        "value": 221,
        "tags": {
            "location": "California.LosAngeles",
            "groupid": 1
        }
    }
]`,
			ttl:          100,
			code:         0,
			affectedRows: 4,
		},
	}

	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal("connect error", err)
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	j, _ := json.Marshal(map[string]interface{}{
		"action": "conn",
		"args": map[string]string{
			"user":     "root",
			"password": "taosdata",
			"db":       "test_schemaless_ws",
		},
	})

	if err := ws.WriteMessage(websocket.TextMessage, j); err != nil {
		t.Fatal("send connect message error", err)
	}
	_, msg, err := ws.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var resp wstool.WSErrorResp
	_ = json.Unmarshal(msg, &resp)
	if resp.Code != 0 {
		t.Fatal(resp)
	}
	for _, c := range cases {
		reqID := uint64(1)
		t.Run(c.name, func(t *testing.T) {
			j, _ := json.Marshal(map[string]interface{}{
				"action": "insert",
				"args": map[string]interface{}{
					"req_id":    reqID,
					"protocol":  c.protocol,
					"precision": c.precision,
					"data":      c.data,
					"ttl":       c.ttl,
				},
			})
			if err := ws.WriteMessage(websocket.TextMessage, j); err != nil {
				t.Fatal(c.name, err)
			}
			_, msg, err := ws.ReadMessage()
			if err != nil {
				t.Fatal(c.name, err)
			}
			var schemalessResp schemalessResp
			err = json.Unmarshal(msg, &schemalessResp)
			if resp.Code != 0 {
				t.Fatal(c.name, string(msg))
			}
			assert.NoError(t, err, string(msg))
			assert.Equal(t, reqID, schemalessResp.ReqID)
			assert.Equal(t, 0, schemalessResp.Code, schemalessResp.Message)
			if c.protocol != wrapper.OpenTSDBJsonFormatProtocol {
				assert.Equal(t, c.totalRows, schemalessResp.TotalRows)
			}
			assert.Equal(t, c.affectedRows, schemalessResp.AffectedRows)
		})
	}
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

func doWebSocket(ws *websocket.Conn, action string, arg interface{}) (resp []byte, err error) {
	var b []byte
	if arg != nil {
		b, _ = json.Marshal(arg)
	}
	a, _ := json.Marshal(wstool.WSAction{Action: action, Args: b})
	err = ws.WriteMessage(websocket.TextMessage, a)
	if err != nil {
		return nil, err
	}
	_, message, err := ws.ReadMessage()
	return message, err
}

func TestDropUser(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/schemaless", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	defer doRestful("drop user test_ws_sml_drop_user", "")
	code, message := doRestful("create user test_ws_sml_drop_user pass 'pass_123'", "")
	assert.Equal(t, 0, code, message)
	// connect
	connReq := &schemalessConnReq{ReqID: 1, User: "test_ws_sml_drop_user", Password: "pass_123"}
	resp, err := doWebSocket(ws, SchemalessConn, &connReq)
	assert.NoError(t, err)
	var connResp schemalessConnResp
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	// drop user
	code, message = doRestful("drop user test_ws_sml_drop_user", "")
	assert.Equal(t, 0, code, message)
	time.Sleep(time.Second * 3)
	resp, err = doWebSocket(ws, wstool.ClientVersion, nil)
	assert.Error(t, err, resp)
}
