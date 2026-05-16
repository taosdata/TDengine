package tmq

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	_ "github.com/taosdata/taosadapter/v3/controller/rest"
	_ "github.com/taosdata/taosadapter/v3/controller/ws/ws"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/driver/common/tmq"
	taoserrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/layout"
	"github.com/taosdata/taosadapter/v3/tools/parseblock"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
	"github.com/taosdata/taosadapter/v3/tools/testtools/testenv"
	"github.com/taosdata/taosadapter/v3/version"
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

func TestTMQ(t *testing.T) {
	doTMQTest(t, "test_ws_tmq", "test_tmq_ws_topic", "")
}

func doTMQTest(t *testing.T, dbName string, topicName string, token string) {
	if token != "" {
		t.Log("token test")
		if !testenv.IsEnterpriseTest() {
			t.Skip("token test only for enterprise edition")
			return
		}
	}
	ts1 := time.Now()
	ts2 := ts1.Add(time.Second)
	ts3 := ts2.Add(time.Second)
	code, message := doHttpSql(fmt.Sprintf("create database if not exists %s WAL_RETENTION_PERIOD 86400", dbName))
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated(dbName))
	defer func() {
		code, message := doHttpSql(fmt.Sprintf("drop database if exists %s", dbName))
		assert.Equal(t, 0, code, message)
	}()

	initSqls := []string{
		"create table if not exists ct0 (ts timestamp, c1 int)",
		"create table if not exists ct1 (ts timestamp, c1 int, c2 float)",
		"create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))",
		fmt.Sprintf("create topic if not exists %s as DATABASE %s", topicName, dbName),
		fmt.Sprintf(`insert into ct0 values('%s',1)`, ts1.Format(time.RFC3339Nano)),
		fmt.Sprintf(`insert into ct1 values('%s',1,2)`, ts2.Format(time.RFC3339Nano)),
		fmt.Sprintf(`insert into ct2 values('%s',1,2,'3')`, ts3.Format(time.RFC3339Nano)),
	}
	for _, initSql := range initSqls {
		code, message = doHttpSqlWithDB(initSql, dbName)
		assert.Equal(t, 0, code, message)
	}
	defer func() {
		cleanSqls := []string{
			fmt.Sprintf("drop topic if exists %s", topicName),
			fmt.Sprintf("drop database if exists %s", dbName),
		}
		for _, cleanSql := range cleanSqls {
			assert.Eventually(t, func() bool {
				code, message = doHttpSql(cleanSql)
				return code == 0
			}, 5*time.Second, 500*time.Millisecond, message)
		}
	}()

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	// subscribe
	initReq := &TMQSubscribeReq{
		ReqID:                0,
		GroupID:              "test",
		Topics:               []string{topicName},
		AutoCommit:           "true",
		AutoCommitIntervalMS: "5000",
		SnapshotEnable:       "true",
		WithTableName:        "true",
		OffsetReset:          "earliest",
		Config:               map[string]string{},
	}
	if token != "" {
		initReq.Config["td.connect.token"] = token
	} else {
		initReq.User = "root"
		initReq.Password = "taosdata"
	}
	_, err = subscribe(t, ws, initReq)
	assert.NoError(t, err)

	// poll
	gotCt0 := false
	gotCt1 := false
	gotCt2 := false
	for i := 0; i < 5; i++ {
		if gotCt0 && gotCt1 && gotCt2 {
			break
		}
		pollReq := &TMQPollReq{
			ReqID:        3,
			BlockingTime: 500,
		}
		pollResp, err := poll(t, ws, pollReq)
		assert.NoError(t, err)
		if pollResp.HaveMessage {
			messageID := pollResp.MessageID
			for {
				fetchReq := &TMQFetchReq{
					ReqID:     4,
					MessageID: messageID,
				}
				fetchResp, err := fetch(t, ws, fetchReq)
				assert.NoError(t, err)
				if fetchResp.Completed {
					commitReq := &TMQCommitReq{
						ReqID:     3,
						MessageID: messageID,
					}
					_, err = commit(t, ws, commitReq)
					assert.NoError(t, err)
					break
				} else {
					message := fetchBlock(t, ws, &TMQFetchBlockReq{
						ReqID:     0,
						MessageID: messageID,
					})
					_, _, value, err := parseblock.ParseTmqBlock(message[8:], fetchResp.FieldsTypes, fetchResp.Rows, fetchResp.Precision)
					assert.NoError(t, err)
					switch fetchResp.TableName {
					case "ct0":
						gotCt0 = true
						assert.Equal(t, 1, len(value))
						assert.Equal(t, ts1.UnixNano()/1e6, value[0][0].(time.Time).UnixNano()/1e6)
						assert.Equal(t, int32(1), value[0][1])
					case "ct1":
						gotCt1 = true
						assert.Equal(t, 1, len(value))
						assert.Equal(t, ts2.UnixNano()/1e6, value[0][0].(time.Time).UnixNano()/1e6)
						assert.Equal(t, int32(1), value[0][1])
						assert.Equal(t, float32(2), value[0][2])
					case "ct2":
						gotCt2 = true
						assert.Equal(t, 1, len(value))
						assert.Equal(t, ts3.UnixNano()/1e6, value[0][0].(time.Time).UnixNano()/1e6)
						assert.Equal(t, int32(1), value[0][1])
						assert.Equal(t, float32(2), value[0][2])
						assert.Equal(t, "3", value[0][3])
					}
				}
			}
		}
	}
	assert.True(t, gotCt0)
	assert.True(t, gotCt1)
	assert.True(t, gotCt2)
	resp := getVersion(t, ws)
	assert.NotEmpty(t, resp.Version)
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)
}

func subscribe(t *testing.T, ws *websocket.Conn, req *TMQSubscribeReq) (*TMQSubscribeResp, error) {
	action := TMQSubscribe
	var resp TMQSubscribeResp
	err := sendJsonBackJson(t, ws, action, req, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, err
}

func poll(t *testing.T, ws *websocket.Conn, req *TMQPollReq) (*TMQPollResp, error) {
	action := TMQPoll
	var resp TMQPollResp
	err := sendJsonBackJson(t, ws, action, req, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, err
}

func fetch(t *testing.T, ws *websocket.Conn, req *TMQFetchReq) (*TMQFetchResp, error) {
	action := TMQFetch
	var resp TMQFetchResp
	err := sendJsonBackJson(t, ws, action, req, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, err
}

func commit(t *testing.T, ws *websocket.Conn, req *TMQCommitReq) (*TMQCommitResp, error) {
	action := TMQCommit
	var resp TMQCommitResp
	err := sendJsonBackJson(t, ws, action, req, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, err
}

func fetchBlock(t *testing.T, ws *websocket.Conn, req *TMQFetchBlockReq) []byte {
	action := TMQFetchBlock
	return sendJsonBackBinary(t, ws, action, req)
}

func getVersion(t *testing.T, ws *websocket.Conn) *wstool.WSVersionResp {
	action := wstool.ClientVersion
	var resp wstool.WSVersionResp
	err := sendJsonBackJson(t, ws, action, nil, &resp)
	assert.NoError(t, err)
	return &resp
}

func fetchJsonMeta(t *testing.T, ws *websocket.Conn, req *TMQFetchJsonMetaReq) (*TMQFetchJsonMetaResp, error) {
	action := TMQFetchJsonMeta
	var resp TMQFetchJsonMetaResp
	err := sendJsonBackJson(t, ws, action, req, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, err
}

func fetchRaw(t *testing.T, ws *websocket.Conn, req *TMQFetchRawReq) []byte {
	action := TMQFetchRaw
	return sendJsonBackBinary(t, ws, action, req)
}

func unsubscribe(t *testing.T, ws *websocket.Conn, req *TMQUnsubscribeReq) {
	action := TMQUnsubscribe
	var resp TMQUnsubscribeResp
	err := sendJsonBackJson(t, ws, action, req, &resp)
	assert.NoError(t, err)
}

type connRequest struct {
	ReqID       uint64 `json:"req_id"`
	User        string `json:"user"`
	Password    string `json:"password"`
	DB          string `json:"db"`
	Mode        *int   `json:"mode"`
	TZ          string `json:"tz"`
	App         string `json:"app"`
	IP          string `json:"ip"`
	Connector   string `json:"connector"`
	TOTPCode    string `json:"totp_code"`
	BearerToken string `json:"bearer_token"`
}
type connResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	Version string `json:"version"`
}

func connect(t *testing.T, ws *websocket.Conn, req *connRequest) {
	action := "conn"
	var resp connResponse
	err := sendJsonBackJson(t, ws, action, req, &resp)
	assert.NoError(t, err)
}

type BaseResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func sendJsonBackJson(t *testing.T, ws *websocket.Conn, action string, req interface{}, resp interface{}) error {
	bs, err := json.Marshal(req)
	assert.NoError(t, err)
	message, err := doWebSocket(ws, action, bs)
	assert.NoError(t, err)
	var baseResp BaseResp
	err = json.Unmarshal(message, &baseResp)
	assert.NoError(t, err)
	if baseResp.Code != 0 {
		return taoserrors.NewError(baseResp.Code, baseResp.Message)
	}
	err = json.Unmarshal(message, &resp)
	assert.NoError(t, err)
	return nil
}

func sendJsonBackBinary(t *testing.T, ws *websocket.Conn, action string, req interface{}) []byte {
	bs, err := json.Marshal(req)
	assert.NoError(t, err)
	message, err := doWebSocket(ws, action, bs)
	assert.NoError(t, err)
	return message
}

type MultiMeta struct {
	TmqMetaVersion string     `json:"tmq_meta_version"`
	Metas          []tmq.Meta `json:"metas"`
}

func TestMeta(t *testing.T) {
	code, message := doHttpSql("create database if not exists test_ws_tmq_meta WAL_RETENTION_PERIOD 86400")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_tmq_meta"))

	code, message = doHttpSql("create topic if not exists test_tmq_meta_ws_topic with meta as DATABASE test_ws_tmq_meta")
	assert.Equal(t, 0, code, message)
	defer func() {
		cleanSqls := []string{
			"drop topic if exists test_tmq_meta_ws_topic",
			"drop database if exists test_ws_tmq_meta_target",
			"drop database if exists test_ws_tmq_meta",
		}
		for _, cleanSql := range cleanSqls {
			assert.Eventually(t, func() bool {
				code, message = doHttpSql(cleanSql)
				return code == 0
			}, 5*time.Second, 500*time.Millisecond, message)
		}
	}()
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	expect := [][]driver.Value{
		{"ts", "TIMESTAMP", float64(8), ""},
		{"c1", "BOOL", float64(1), ""},
		{"c2", "TINYINT", float64(1), ""},
		{"c3", "SMALLINT", float64(2), ""},
		{"c4", "INT", float64(4), ""},
		{"c5", "BIGINT", float64(8), ""},
		{"c6", "TINYINT UNSIGNED", float64(1), ""},
		{"c7", "SMALLINT UNSIGNED", float64(2), ""},
		{"c8", "INT UNSIGNED", float64(4), ""},
		{"c9", "BIGINT UNSIGNED", float64(8), ""},
		{"c10", "FLOAT", float64(4), ""},
		{"c11", "DOUBLE", float64(8), ""},
		{"c12", "VARCHAR", float64(20), ""},
		{"c13", "NCHAR", float64(20), ""},
		{"c14", "VARBINARY", float64(20), ""},
		{"c15", "GEOMETRY", float64(100), ""},
		{"c16", "DECIMAL(20, 4)", float64(16), ""},
		{"tts", "TIMESTAMP", float64(8), "TAG"},
		{"tc1", "BOOL", float64(1), "TAG"},
		{"tc2", "TINYINT", float64(1), "TAG"},
		{"tc3", "SMALLINT", float64(2), "TAG"},
		{"tc4", "INT", float64(4), "TAG"},
		{"tc5", "BIGINT", float64(8), "TAG"},
		{"tc6", "TINYINT UNSIGNED", float64(1), "TAG"},
		{"tc7", "SMALLINT UNSIGNED", float64(2), "TAG"},
		{"tc8", "INT UNSIGNED", float64(4), "TAG"},
		{"tc9", "BIGINT UNSIGNED", float64(8), "TAG"},
		{"tc10", "FLOAT", float64(4), "TAG"},
		{"tc11", "DOUBLE", float64(8), "TAG"},
		{"tc12", "VARCHAR", float64(20), "TAG"},
		{"tc13", "NCHAR", float64(20), "TAG"},
		{"tc14", "VARBINARY", float64(20), "TAG"},
		{"tc15", "GEOMETRY", float64(100), "TAG"},
	}
	init := &TMQSubscribeReq{
		ReqID:                0,
		User:                 "root",
		Password:             "taosdata",
		GroupID:              "test",
		Topics:               []string{"test_tmq_meta_ws_topic"},
		AutoCommit:           "true",
		AutoCommitIntervalMS: "5000",
		SnapshotEnable:       "true",
		WithTableName:        "true",
		OffsetReset:          "earliest",
		EnableBatchMeta:      "1",
		SessionTimeoutMS:     "12000",
		MaxPollIntervalMS:    "300000",
	}
	subResp, err := subscribe(t, ws, init)
	assert.NoError(t, err)
	assert.Equal(t, version.TaosClientVersion, subResp.Version)
	doHttpSqlWithDB("create table stb (ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20),"+
		"c14 varbinary(20),"+
		"c15 geometry(100),"+
		"c16 decimal(20,4)"+
		")"+
		"tags(tts timestamp,"+
		"tc1 bool,"+
		"tc2 tinyint,"+
		"tc3 smallint,"+
		"tc4 int,"+
		"tc5 bigint,"+
		"tc6 tinyint unsigned,"+
		"tc7 smallint unsigned,"+
		"tc8 int unsigned,"+
		"tc9 bigint unsigned,"+
		"tc10 float,"+
		"tc11 double,"+
		"tc12 binary(20),"+
		"tc13 nchar(20),"+
		"tc14 varbinary(20),"+
		"tc15 geometry(100)"+
		")", "test_ws_tmq_meta")
	pollReq := &TMQPollReq{
		ReqID:        3,
		BlockingTime: 500,
	}
	success := false
	for i := 0; i < 5; i++ {
		pollResp, err := poll(t, ws, pollReq)
		assert.NoError(t, err)
		if pollResp.HaveMessage {
			messageID := pollResp.MessageID
			fetchJsonMetaResp, err := fetchJsonMeta(t, ws, &TMQFetchJsonMetaReq{
				ReqID:     4,
				MessageID: pollResp.MessageID,
			})
			assert.NoError(t, err)
			t.Log(string(fetchJsonMetaResp.Data))
			valid := jsoniter.Valid(fetchJsonMetaResp.Data)
			assert.True(t, valid)
			var meta tmq.Meta
			err = jsoniter.Unmarshal(fetchJsonMetaResp.Data, &meta)
			if err != nil {
				var multiMeta MultiMeta
				err = jsoniter.Unmarshal(fetchJsonMetaResp.Data, &multiMeta)
				assert.NoError(t, err)
			}
			fetchRawReq := &TMQFetchRawReq{
				ReqID:     3,
				MessageID: messageID,
			}
			data := fetchRaw(t, ws, fetchRawReq)
			writeRaw(t, data[8:], "test_ws_tmq_meta_target")
			resp := restQuery("describe stb", "test_ws_tmq_meta_target")

			for index, values := range expect {
				for i := 0; i < 4; i++ {
					assert.Equal(t, values[i], resp.Data[index][i])
				}
			}
			success = true
			commitReq := &TMQCommitReq{
				ReqID:     3,
				MessageID: messageID,
			}
			_, err = commit(t, ws, commitReq)
			assert.NoError(t, err)
			unsubscribe(t, ws, &TMQUnsubscribeReq{ReqID: 6})
			getVersion(t, ws)
			break
		}
	}
	assert.True(t, success)
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)
	resp := restQuery("describe stb", "test_ws_tmq_meta_target")
	for index, values := range expect {
		for i := 0; i < 4; i++ {
			assert.Equal(t, values[i], resp.Data[index][i])
		}
	}
}

func writeRaw(t *testing.T, rawData []byte, db string) {
	code, message := doHttpSql(fmt.Sprintf("create database if not exists %s WAL_RETENTION_PERIOD 86400", db))
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated(db))
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
	connectReq := &connRequest{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		DB:       db,
	}
	connect(t, ws, connectReq)

	resp, err := sendWSMessage(ws, websocket.BinaryMessage, rawData)
	assert.NoError(t, err)
	var writeMetaResp BaseResp
	err = json.Unmarshal(resp, &writeMetaResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, writeMetaResp.Code, writeMetaResp.Message)
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)
}

func TestTMQAutoCommit(t *testing.T) {
	ts1 := time.Now()
	ts2 := ts1.Add(time.Second)
	ts3 := ts2.Add(time.Second)
	code, message := doHttpSql("create database if not exists test_ws_tmq_auto_commit WAL_RETENTION_PERIOD 86400")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_tmq_auto_commit"))
	initSqls := []string{
		"create table if not exists ct0 (ts timestamp, c1 int)",
		"create table if not exists ct1 (ts timestamp, c1 int, c2 float)",
		"create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))",
		"create topic if not exists test_tmq_ws_auto_commit_topic as DATABASE test_ws_tmq_auto_commit",
		fmt.Sprintf(`insert into ct0 values('%s',1)`, ts1.Format(time.RFC3339Nano)),
		fmt.Sprintf(`insert into ct1 values('%s',1,2)`, ts2.Format(time.RFC3339Nano)),
		fmt.Sprintf(`insert into ct2 values('%s',1,2,'3')`, ts3.Format(time.RFC3339Nano)),
	}
	for _, initSql := range initSqls {
		code, message = doHttpSqlWithDB(initSql, "test_ws_tmq_auto_commit")
		assert.Equal(t, 0, code, message)
	}

	defer func() {
		cleanSqls := []string{
			"drop topic if exists test_tmq_ws_auto_commit_topic",
			"drop database if exists test_ws_tmq_auto_commit",
		}
		for _, cleanSql := range cleanSqls {
			assert.Eventually(t, func() bool {
				code, message = doHttpSql(cleanSql)
				return code == 0
			}, 5*time.Second, 500*time.Millisecond, message)
		}
	}()

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	init := &TMQSubscribeReq{
		ReqID:                0,
		User:                 "root",
		Password:             "taosdata",
		GroupID:              "test",
		Topics:               []string{"test_tmq_ws_auto_commit_topic"},
		AutoCommit:           "true",
		OffsetReset:          "earliest",
		AutoCommitIntervalMS: "500",
		SnapshotEnable:       "true",
		WithTableName:        "true",
	}
	_, err = subscribe(t, ws, init)
	assert.NoError(t, err)
	pollReq := &TMQPollReq{
		ReqID:        3,
		BlockingTime: 500,
	}
	gotCt0 := false
	gotCt1 := false
	gotCt2 := false
	for i := 0; i < 5; i++ {
		pollResp, err := poll(t, ws, pollReq)
		assert.NoError(t, err)
		if pollResp.HaveMessage {
			messageID := pollResp.MessageID
			fetchReq := &TMQFetchReq{
				ReqID:     4,
				MessageID: messageID,
			}
			for {
				fetchResp, err := fetch(t, ws, fetchReq)
				assert.NoError(t, err)
				if fetchResp.Completed {
					break
				}
				message := fetchBlock(t, ws, &TMQFetchBlockReq{
					ReqID:     0,
					MessageID: messageID,
				})
				_, _, value, err := parseblock.ParseTmqBlock(message[8:], fetchResp.FieldsTypes, fetchResp.Rows, fetchResp.Precision)
				assert.NoError(t, err)
				switch fetchResp.TableName {
				case "ct0":
					gotCt0 = true
					assert.Equal(t, 1, len(value))
					assert.Equal(t, ts1.UnixNano()/1e6, value[0][0].(time.Time).UnixNano()/1e6)
					assert.Equal(t, int32(1), value[0][1])
				case "ct1":
					gotCt1 = true
					assert.Equal(t, 1, len(value))
					assert.Equal(t, ts2.UnixNano()/1e6, value[0][0].(time.Time).UnixNano()/1e6)
					assert.Equal(t, int32(1), value[0][1])
					assert.Equal(t, float32(2), value[0][2])
				case "ct2":
					gotCt2 = true
					assert.Equal(t, 1, len(value))
					assert.Equal(t, ts3.UnixNano()/1e6, value[0][0].(time.Time).UnixNano()/1e6)
					assert.Equal(t, int32(1), value[0][1])
					assert.Equal(t, float32(2), value[0][2])
					assert.Equal(t, "3", value[0][3])
				}
				time.Sleep(3 * 500 * time.Millisecond)
			}
		}
	}
	assert.True(t, gotCt0)
	assert.True(t, gotCt1)
	assert.True(t, gotCt2)
	resp := getVersion(t, ws)
	assert.NotEmpty(t, resp.Version)
}

func TestTMQUnsubscribeAndSubscribe(t *testing.T) {
	ts1 := time.Now()
	ts2 := ts1.Add(time.Second)
	ts3 := ts2.Add(time.Second)
	code, message := doHttpSql("create database if not exists test_ws_tmq_unsubscribe WAL_RETENTION_PERIOD 86400")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_tmq_unsubscribe"))
	initSqls := []string{
		"create table if not exists ct0 (ts timestamp, c1 int)",
		"create table if not exists ct1 (ts timestamp, c1 int, c2 float)",
		"create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))",
		"create topic if not exists test_tmq_ws_unsubscribe_topic as DATABASE test_ws_tmq_unsubscribe",
		fmt.Sprintf(`insert into ct0 values('%s',1)`, ts1.Format(time.RFC3339Nano)),
		"create topic if not exists test_tmq_ws_unsubscribe2_topic as select * from ct0",
		fmt.Sprintf(`insert into ct1 values('%s',1,2)`, ts2.Format(time.RFC3339Nano)),
		fmt.Sprintf(`insert into ct2 values('%s',1,2,'3')`, ts3.Format(time.RFC3339Nano)),
	}

	for _, initSql := range initSqls {
		code, message = doHttpSqlWithDB(initSql, "test_ws_tmq_unsubscribe")
		assert.Equal(t, 0, code, message)
	}

	defer func() {
		cleanSqls := []string{
			"drop topic if exists test_tmq_ws_unsubscribe_topic",
			"drop topic if exists test_tmq_ws_unsubscribe2_topic",
			"drop database if exists test_ws_tmq_unsubscribe",
		}
		for _, cleanSql := range cleanSqls {
			assert.Eventually(t, func() bool {
				code, message = doHttpSql(cleanSql)
				return code == 0
			}, 5*time.Second, 500*time.Millisecond, message)
		}
	}()

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	init := &TMQSubscribeReq{
		ReqID:                0,
		User:                 "root",
		Password:             "taosdata",
		GroupID:              "test",
		OffsetReset:          "earliest",
		Topics:               []string{"test_tmq_ws_unsubscribe_topic"},
		AutoCommit:           "true",
		AutoCommitIntervalMS: "500",
		SnapshotEnable:       "true",
		WithTableName:        "true",
	}
	_, err = subscribe(t, ws, init)
	assert.NoError(t, err)

	pollTest := func(topic string) {
		gotCt0 := false
		gotCt1 := false
		gotCt2 := false
		for i := 0; i < 5; i++ {
			pollReq := &TMQPollReq{
				ReqID:        3,
				BlockingTime: 500,
			}
			pollResp, err := poll(t, ws, pollReq)
			assert.NoError(t, err)
			if pollResp.HaveMessage {
				messageID := pollResp.MessageID
				fetchReq := &TMQFetchReq{
					ReqID:     4,
					MessageID: messageID,
				}
				for {
					fetchResp, err := fetch(t, ws, fetchReq)
					assert.NoError(t, err)
					if fetchResp.Completed {
						break
					}
					message := fetchBlock(t, ws, &TMQFetchBlockReq{
						ReqID:     0,
						MessageID: messageID,
					})
					_, _, value, err := parseblock.ParseTmqBlock(message[8:], fetchResp.FieldsTypes, fetchResp.Rows, fetchResp.Precision)
					assert.NoError(t, err)
					switch fetchResp.TableName {
					case "ct0":
						gotCt0 = true
						assert.Equal(t, 1, len(value))
						assert.Equal(t, ts1.UnixNano()/1e6, value[0][0].(time.Time).UnixNano()/1e6)
						assert.Equal(t, int32(1), value[0][1])
					case "ct1":
						gotCt1 = true
						assert.Equal(t, 1, len(value))
						assert.Equal(t, ts2.UnixNano()/1e6, value[0][0].(time.Time).UnixNano()/1e6)
						assert.Equal(t, int32(1), value[0][1])
						assert.Equal(t, float32(2), value[0][2])
					case "ct2":
						gotCt2 = true
						assert.Equal(t, 1, len(value))
						assert.Equal(t, ts3.UnixNano()/1e6, value[0][0].(time.Time).UnixNano()/1e6)
						assert.Equal(t, int32(1), value[0][1])
						assert.Equal(t, float32(2), value[0][2])
						assert.Equal(t, "3", value[0][3])
					case "":
						gotCt0 = true
						assert.Equal(t, 1, len(value))
						assert.Equal(t, ts1.UnixNano()/1e6, value[0][0].(time.Time).UnixNano()/1e6)
						assert.Equal(t, int32(1), value[0][1])
					}
				}
			}
		}
		if topic == "test_tmq_ws_unsubscribe_topic" {
			assert.True(t, gotCt0)
			assert.True(t, gotCt1)
			assert.True(t, gotCt2)
		}
		if topic == "test_tmq_ws_unsubscribe2_topic" {
			assert.True(t, gotCt0)
			assert.False(t, gotCt1)
			assert.False(t, gotCt2)
		}
		unsubscribe(t, ws, &TMQUnsubscribeReq{ReqID: 6})
		getVersion(t, ws)
	}
	pollTest("test_tmq_ws_unsubscribe_topic")

	// resubscribe to another topic
	resubReq := &TMQSubscribeReq{
		ReqID:       0,
		OffsetReset: "earliest",
		Topics:      []string{"test_tmq_ws_unsubscribe2_topic"},
	}
	_, err = subscribe(t, ws, resubReq)
	assert.NoError(t, err)
	pollTest("test_tmq_ws_unsubscribe2_topic")
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)
}

func TestTMQSeek(t *testing.T) {
	vgroups := 2
	ts1 := time.Now()
	ts2 := ts1.Add(time.Second)
	ts3 := ts2.Add(time.Second)
	insertSql := []string{
		fmt.Sprintf(`insert into ct0 values('%s',1)`, ts1.Format(time.RFC3339Nano)),
		fmt.Sprintf(`insert into ct1 values('%s',1,2)`, ts2.Format(time.RFC3339Nano)),
		fmt.Sprintf(`insert into ct2 values('%s',1,2,'3')`, ts3.Format(time.RFC3339Nano)),
	}
	insertCount := len(insertSql)
	tryPollCount := 3 * insertCount
	topic := "test_tmq_ws_seek_topic"
	dbName := "test_ws_tmq_seek"
	code, message := doHttpSql("create database if not exists " + dbName + " vgroups " + strconv.Itoa(vgroups) + " WAL_RETENTION_PERIOD 86400")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated(dbName))
	code, message = doHttpSqlWithDB("create table if not exists ct0 (ts timestamp, c1 int)", dbName)
	assert.Equal(t, 0, code, message)
	code, message = doHttpSqlWithDB("create table if not exists ct1 (ts timestamp, c1 int, c2 float)", dbName)
	assert.Equal(t, 0, code, message)
	code, message = doHttpSqlWithDB("create table if not exists ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))", dbName)
	assert.Equal(t, 0, code, message)

	for i := 0; i < insertCount; i++ {
		code, message = doHttpSqlWithDB(insertSql[i], dbName)
		assert.Equal(t, 0, code, message)
	}

	code, message = doHttpSqlWithDB("create topic if not exists "+topic+" as database "+dbName, dbName)
	assert.Equal(t, 0, code, message)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	//sub
	{
		req := &TMQSubscribeReq{
			ReqID:         0,
			User:          "root",
			Password:      "taosdata",
			GroupID:       "test",
			Topics:        []string{topic},
			OffsetReset:   "earliest",
			AutoCommit:    "false",
			WithTableName: "true",
		}
		b, _ := json.Marshal(req)
		action, _ := json.Marshal(&wstool.WSAction{
			Action: TMQSubscribe,
			Args:   b,
		})
		err = ws.WriteMessage(
			websocket.TextMessage,
			action,
		)
		assert.NoError(t, err)
		mt, message, err := ws.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, mt)
		var resp TMQSubscribeResp
		err = json.Unmarshal(message, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.Code)
	}
	//assignment 1
	vgID := make([]int32, vgroups)
	{
		req := TMQGetTopicAssignmentReq{
			ReqID: 1,
			Topic: topic,
		}
		b, _ := json.Marshal(req)
		action, _ := json.Marshal(&wstool.WSAction{
			Action: TMQGetTopicAssignment,
			Args:   b,
		})
		err = ws.WriteMessage(
			websocket.TextMessage,
			action,
		)
		assert.NoError(t, err)
		mt, message, err := ws.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, mt)
		var resp TMQGetTopicAssignmentResp
		err = json.Unmarshal(message, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.Code)
		assert.Equal(t, vgroups, len(resp.Assignment))
		for i := 0; i < vgroups; i++ {
			assert.Equal(t, int64(0), resp.Assignment[i].Offset)
			assert.Equal(t, int64(0), resp.Assignment[i].Begin)
			vgID[i] = resp.Assignment[i].VGroupID
		}
	}
	//poll 1
	{
		rowCount := 0
		for i := 0; i < tryPollCount; i++ {
			if rowCount >= insertCount {
				break
			}
			req := TMQPollReq{
				ReqID:        1,
				BlockingTime: 500,
			}
			b, _ := json.Marshal(req)
			action, _ := json.Marshal(&wstool.WSAction{
				Action: TMQPoll,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			assert.NoError(t, err)
			mt, message, err := ws.ReadMessage()
			assert.NoError(t, err)
			assert.Equal(t, websocket.TextMessage, mt)
			var resp TMQPollResp
			err = json.Unmarshal(message, &resp)
			assert.NoError(t, err)
			assert.Equal(t, 0, resp.Code)
			if resp.HaveMessage {
				for {
					req := TMQFetchReq{
						ReqID:     1,
						MessageID: resp.MessageID,
					}
					b, _ := json.Marshal(req)
					action, _ := json.Marshal(&wstool.WSAction{
						Action: TMQFetch,
						Args:   b,
					})
					err = ws.WriteMessage(
						websocket.TextMessage,
						action,
					)
					assert.NoError(t, err)
					mt, message, err := ws.ReadMessage()
					assert.NoError(t, err)
					assert.Equal(t, websocket.TextMessage, mt)
					var tmqFetchResp TMQFetchResp
					err = json.Unmarshal(message, &tmqFetchResp)
					assert.NoError(t, err)
					assert.Equal(t, 0, tmqFetchResp.Code)
					if tmqFetchResp.Completed {
						break
					}
					fetchBlockReq := TMQFetchBlockReq{
						ReqID:     1,
						MessageID: tmqFetchResp.MessageID,
					}
					b, _ = json.Marshal(fetchBlockReq)
					action, _ = json.Marshal(&wstool.WSAction{
						Action: TMQFetchBlock,
						Args:   b,
					})
					err = ws.WriteMessage(
						websocket.TextMessage,
						action,
					)
					assert.NoError(t, err)
					mt, message, err = ws.ReadMessage()
					assert.NoError(t, err)
					assert.Equal(t, websocket.BinaryMessage, mt)
					_, _, value, err := parseblock.ParseTmqBlock(message[8:], tmqFetchResp.FieldsTypes, tmqFetchResp.Rows, tmqFetchResp.Precision)
					assert.NoError(t, err)
					t.Log(value)
					rowCount += 1

				}
				{
					req := TMQCommitReq{
						ReqID:     1,
						MessageID: resp.MessageID,
					}
					b, _ := json.Marshal(req)
					action, _ := json.Marshal(&wstool.WSAction{
						Action: TMQCommit,
						Args:   b,
					})
					err = ws.WriteMessage(
						websocket.TextMessage,
						action,
					)
					assert.NoError(t, err)
					mt, message, err := ws.ReadMessage()
					assert.NoError(t, err)
					assert.Equal(t, websocket.TextMessage, mt)
					var resp TMQPollResp
					err = json.Unmarshal(message, &resp)
					assert.NoError(t, err)
					assert.Equal(t, 0, resp.Code)
				}
			}
		}
		assert.Equal(t, insertCount, rowCount)
	}
	//
	code, message = doHttpSql(fmt.Sprintf("insert into %s.ct0 values(now,2)", dbName))
	assert.Equal(t, 0, code, message)
	insertCount += 1

	// poll2
	for i := 0; i < tryPollCount; i++ {
		b, _ := json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
		msg, err := doWebSocket(ws, TMQPoll, b)
		assert.NoError(t, err)
		var resp TMQPollResp
		err = json.Unmarshal(msg, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.Code)
		if resp.HaveMessage {
			break
		}
	}

	//assignment after poll
	{
		req := TMQGetTopicAssignmentReq{
			ReqID: 1,
			Topic: topic,
		}
		b, _ := json.Marshal(req)
		action, _ := json.Marshal(&wstool.WSAction{
			Action: TMQGetTopicAssignment,
			Args:   b,
		})
		err = ws.WriteMessage(
			websocket.TextMessage,
			action,
		)
		assert.NoError(t, err)
		mt, message, err := ws.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, mt)
		var resp TMQGetTopicAssignmentResp
		err = json.Unmarshal(message, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.Code)
		assert.Equal(t, vgroups, len(resp.Assignment))
		for i := 0; i < vgroups; i++ {
			assert.Equal(t, int64(0), resp.Assignment[0].Begin)
		}
	}
	//seek
	for i := 0; i < vgroups; i++ {
		req := TMQOffsetSeekReq{
			ReqID:    uint64(i),
			Topic:    topic,
			VgroupID: vgID[i],
			Offset:   0,
		}
		b, _ := json.Marshal(req)
		action, _ := json.Marshal(&wstool.WSAction{
			Action: TMQSeek,
			Args:   b,
		})
		err = ws.WriteMessage(
			websocket.TextMessage,
			action,
		)
		assert.NoError(t, err)
		mt, message, err := ws.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, mt)
		var resp TMQOffsetSeekResp
		err = json.Unmarshal(message, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.Code)
	}
	//assignment after seek
	{
		req := TMQGetTopicAssignmentReq{
			ReqID: 1,
			Topic: topic,
		}
		b, _ := json.Marshal(req)
		action, _ := json.Marshal(&wstool.WSAction{
			Action: TMQGetTopicAssignment,
			Args:   b,
		})
		err = ws.WriteMessage(
			websocket.TextMessage,
			action,
		)
		assert.NoError(t, err)
		mt, message, err := ws.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, mt)
		var resp TMQGetTopicAssignmentResp
		err = json.Unmarshal(message, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.Code)
		assert.Equal(t, vgroups, len(resp.Assignment))
		for i := 0; i < vgroups; i++ {
			assert.Equal(t, int64(0), resp.Assignment[i].Offset)
			assert.Equal(t, int64(0), resp.Assignment[i].Begin)
		}

	}
	//poll after seek
	{
		rowCount := 0
		for i := 0; i < tryPollCount; i++ {
			if rowCount >= insertCount {
				break
			}
			req := TMQPollReq{
				ReqID:        1,
				BlockingTime: 500,
			}
			b, _ := json.Marshal(req)
			action, _ := json.Marshal(&wstool.WSAction{
				Action: TMQPoll,
				Args:   b,
			})
			err = ws.WriteMessage(
				websocket.TextMessage,
				action,
			)
			assert.NoError(t, err)
			mt, message, err := ws.ReadMessage()
			assert.NoError(t, err)
			assert.Equal(t, websocket.TextMessage, mt)
			var resp TMQPollResp
			err = json.Unmarshal(message, &resp)
			assert.NoError(t, err)
			assert.Equal(t, 0, resp.Code)
			if resp.HaveMessage {
				for {
					req := TMQFetchReq{
						ReqID:     1,
						MessageID: resp.MessageID,
					}
					b, _ := json.Marshal(req)
					action, _ := json.Marshal(&wstool.WSAction{
						Action: TMQFetch,
						Args:   b,
					})
					err = ws.WriteMessage(
						websocket.TextMessage,
						action,
					)
					assert.NoError(t, err)
					mt, message, err := ws.ReadMessage()
					assert.NoError(t, err)
					assert.Equal(t, websocket.TextMessage, mt)
					var tmqFetchResp TMQFetchResp
					err = json.Unmarshal(message, &tmqFetchResp)
					assert.NoError(t, err)
					assert.Equal(t, 0, tmqFetchResp.Code)
					if tmqFetchResp.Completed {
						break
					}
					fetchBlockReq := TMQFetchBlockReq{
						ReqID:     1,
						MessageID: tmqFetchResp.MessageID,
					}
					b, _ = json.Marshal(fetchBlockReq)
					action, _ = json.Marshal(&wstool.WSAction{
						Action: TMQFetchBlock,
						Args:   b,
					})
					err = ws.WriteMessage(
						websocket.TextMessage,
						action,
					)
					assert.NoError(t, err)
					mt, message, err = ws.ReadMessage()
					assert.NoError(t, err)
					assert.Equal(t, websocket.BinaryMessage, mt)
					_, _, value, err := parseblock.ParseTmqBlock(message[8:], tmqFetchResp.FieldsTypes, tmqFetchResp.Rows, tmqFetchResp.Precision)
					assert.NoError(t, err)
					t.Log(value)
					rowCount += 1
				}
				{
					req := TMQCommitReq{
						ReqID:     1,
						MessageID: resp.MessageID,
					}
					b, _ := json.Marshal(req)
					action, _ := json.Marshal(&wstool.WSAction{
						Action: TMQCommit,
						Args:   b,
					})
					err = ws.WriteMessage(
						websocket.TextMessage,
						action,
					)
					assert.NoError(t, err)
					mt, message, err := ws.ReadMessage()
					assert.NoError(t, err)
					assert.Equal(t, websocket.TextMessage, mt)
					var resp TMQPollResp
					err = json.Unmarshal(message, &resp)
					assert.NoError(t, err)
					assert.Equal(t, 0, resp.Code)
				}
			}
		}
		assert.Equal(t, insertCount, rowCount)
	}
	//assignment after poll2
	{
		req := TMQGetTopicAssignmentReq{
			ReqID: 1,
			Topic: topic,
		}
		b, _ := json.Marshal(req)
		action, _ := json.Marshal(&wstool.WSAction{
			Action: TMQGetTopicAssignment,
			Args:   b,
		})
		err = ws.WriteMessage(
			websocket.TextMessage,
			action,
		)
		assert.NoError(t, err)
		mt, message, err := ws.ReadMessage()
		assert.NoError(t, err)
		assert.Equal(t, websocket.TextMessage, mt)
		var resp TMQGetTopicAssignmentResp
		err = json.Unmarshal(message, &resp)
		assert.NoError(t, err)
		assert.Equal(t, 0, resp.Code)
		assert.Equal(t, vgroups, len(resp.Assignment))
		for i := 0; i < vgroups; i++ {
			assert.Equal(t, int64(0), resp.Assignment[i].Begin)
		}
	}

	b, _ := json.Marshal(TMQUnsubscribeReq{})
	action, _ := json.Marshal(&wstool.WSAction{
		Action: TMQUnsubscribe,
		Args:   b,
	})
	err = ws.WriteMessage(websocket.TextMessage, action)
	assert.NoError(t, err)
	_, _, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)
	assert.Eventually(t, func() bool {
		code, message = doHttpSql("drop topic if exists " + topic)
		return code == 0
	}, 5*time.Second, 500*time.Millisecond, message)
	assert.Eventually(t, func() bool {
		code, message = doHttpSql("drop database if exists " + dbName)
		return code == 0
	}, 5*time.Second, 500*time.Millisecond, message)
}

func doHttpSql(sql string) (code int, message string) {
	return doHttpSqlWithDB(sql, "")
}

type restResp struct {
	Code int    `json:"code"`
	Desc string `json:"desc"`
}

func doHttpSqlWithDB(sql string, dbName string) (code int, message string) {
	w := httptest.NewRecorder()
	body := strings.NewReader(sql)
	url := "/rest/sql"
	if dbName != "" {
		url = fmt.Sprintf("/rest/sql/%s", dbName)
	}
	req, _ := http.NewRequest(http.MethodPost, url, body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	b, _ := io.ReadAll(w.Body)
	var res restResp
	_ = json.Unmarshal(b, &res)
	return res.Code, res.Desc
}

func doWebSocket(ws *websocket.Conn, action string, arg []byte) (resp []byte, err error) {
	a, _ := json.Marshal(&wstool.WSAction{Action: action, Args: arg})
	message, err := sendWSMessage(ws, websocket.TextMessage, a)
	return message, err
}

func sendWSMessage(ws *websocket.Conn, messageType int, data []byte) (resp []byte, err error) {
	err = ws.WriteMessage(messageType, data)
	if err != nil {
		return nil, err
	}
	_, message, err := ws.ReadMessage()
	return message, err
}

func before(t *testing.T, dbName string, topic string) {
	doHttpSql(fmt.Sprintf("drop topic if exists %s", topic))
	doHttpSql(fmt.Sprintf("drop database if exists %s", dbName))
	code, message := doHttpSql(fmt.Sprintf("create database if not exists %s WAL_RETENTION_PERIOD 86400", dbName))
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated(dbName))

	code, message = doHttpSql(fmt.Sprintf("create table if not exists %s.ct0 (ts timestamp, c1 int)", dbName))
	assert.Equal(t, 0, code, message)

	code, message = doHttpSql(fmt.Sprintf("create table if not exists %s.ct1 (ts timestamp, c1 int, c2 float)", dbName))
	assert.Equal(t, 0, code, message)

	code, message = doHttpSql(fmt.Sprintf("create table if not exists %s.ct2 (ts timestamp, c1 int, c2 float, c3 binary(10))", dbName))
	assert.Equal(t, 0, code, message)

	code, message = doHttpSql(fmt.Sprintf("insert into %s.ct0 values (now, 1)", dbName))
	assert.Equal(t, 0, code, message)
	code, message = doHttpSql(fmt.Sprintf("insert into %s.ct1 values (now, 1, 2)", dbName))
	assert.Equal(t, 0, code, message)
	code, message = doHttpSql(fmt.Sprintf("insert into %s.ct2 values (now, 1, 2, '3')", dbName))
	assert.Equal(t, 0, code, message)

	code, message = doHttpSql(fmt.Sprintf("create topic if not exists %s as database %s", topic, dbName))
	assert.Equal(t, 0, code, message)
}

func after(t *testing.T, ws *websocket.Conn, dbName string, topic string) error {
	b, _ := json.Marshal(TMQUnsubscribeReq{ReqID: 0})
	_, _ = doWebSocket(ws, TMQUnsubscribe, b)
	err := ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil {
		return err
	}
	var code int
	var message string
	assert.Eventually(t, func() bool {
		code, message = doHttpSql(fmt.Sprintf("drop topic if exists %s", topic))
		return code == 0
	}, 5*time.Second, 500*time.Millisecond, message)
	assert.Eventually(t, func() bool {
		code, message = doHttpSql(fmt.Sprintf("drop database if exists %s", dbName))
		return code == 0
	}, 5*time.Second, 500*time.Millisecond, message)
	return nil
}

func TestTMQ_Position_And_Committed(t *testing.T) {
	dbName := "test_ws_tmq_position_and_committed"
	topic := "test_ws_tmq_position_and_committed_topic"

	before(t, dbName, topic)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	defer func() {
		err = after(t, ws, dbName, topic)
		assert.NoError(t, err)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:        "root",
		Password:    "taosdata",
		DB:          dbName,
		GroupID:     "test",
		Topics:      []string{topic},
		AutoCommit:  "false",
		OffsetReset: "earliest",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)

	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	var pollResp TMQPollResp
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, pollResp.Message)
	assert.True(t, pollResp.HaveMessage)

	//commit
	b, _ = json.Marshal(TMQCommitReq{ReqID: 0, MessageID: pollResp.MessageID})
	msg, err = doWebSocket(ws, TMQCommit, b)
	assert.NoError(t, err)
	var commitResp TMQCommitResp
	err = json.Unmarshal(msg, &commitResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, commitResp.Code, commitResp.Message)

	// committed
	b, _ = json.Marshal(TMQCommittedReq{ReqID: 0, TopicVgroupIDs: []TopicVgroupID{{Topic: topic, VgroupID: pollResp.VgroupID}}})
	msg, err = doWebSocket(ws, TMQCommitted, b)
	assert.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	var committedResp TMQCommittedResp
	err = json.Unmarshal(msg, &committedResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, committedResp.Code, committedResp.Message)
	assert.Equal(t, 1, len(committedResp.Committed))
	assert.Equal(t, true, committedResp.Committed[0] > 0)

	// position
	b, _ = json.Marshal(TMQPositionReq{ReqID: 0, TopicVgroupIDs: []TopicVgroupID{{Topic: topic, VgroupID: pollResp.VgroupID}}})
	msg, err = doWebSocket(ws, TMQPosition, b)
	assert.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	var positionResp TMQPositionResp
	err = json.Unmarshal(msg, &positionResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, positionResp.Code, positionResp.Message)
	assert.Equal(t, 1, len(positionResp.Position))
	assert.Equal(t, true, positionResp.Position[0] > 0)
}

func TestTMQ_ListTopics(t *testing.T) {
	dbName := "test_ws_tmq_list_topics"
	topic := "test_ws_tmq_list_topics"

	before(t, dbName, topic)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	defer func() {
		err = after(t, ws, dbName, topic)
		assert.NoError(t, err)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:        "root",
		Password:    "taosdata",
		DB:          dbName,
		GroupID:     "test",
		Topics:      []string{topic},
		AutoCommit:  "false",
		OffsetReset: "earliest",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)

	b, _ = json.Marshal(TMQListTopicsReq{ReqID: 0})
	msg, err = doWebSocket(ws, TMQListTopics, b)
	assert.NoError(t, err)
	var listTopicResp TMQListTopicsResp
	err = json.Unmarshal(msg, &listTopicResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, listTopicResp.Code, listTopicResp.Message)
	assert.Equal(t, []string{topic}, listTopicResp.Topics)
}

func TestTMQ_CommitOffset(t *testing.T) {
	dbName := "test_ws_tmq_commit_offset"
	topic := "test_ws_tmq_commit_offset_topic"

	before(t, dbName, topic)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	defer func() {
		err = after(t, ws, dbName, topic)
		assert.NoError(t, err)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:        "root",
		Password:    "taosdata",
		DB:          dbName,
		GroupID:     "test",
		Topics:      []string{topic},
		AutoCommit:  "false",
		OffsetReset: "earliest",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)

	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	var pollResp TMQPollResp
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))

	// insert
	code, message := doHttpSql(fmt.Sprintf("insert into %s.ct0 values (now, 2)", dbName))
	assert.Equal(t, 0, code, message)

	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.True(t, pollResp.HaveMessage, string(msg))
	assert.True(t, pollResp.Offset >= 0, string(msg))

	//commit offset
	b, _ = json.Marshal(TMQCommitOffsetReq{ReqID: 0, Topic: topic, VgroupID: pollResp.VgroupID, Offset: pollResp.Offset})
	msg, err = doWebSocket(ws, TMQCommitOffset, b)
	assert.NoError(t, err)
	var commitOffsetResp TMQCommitOffsetResp
	err = json.Unmarshal(msg, &commitOffsetResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, commitOffsetResp.Code, commitOffsetResp.Message)

	// committed
	b, _ = json.Marshal(TMQCommittedReq{ReqID: 0, TopicVgroupIDs: []TopicVgroupID{{Topic: topic, VgroupID: pollResp.VgroupID}}})
	msg, err = doWebSocket(ws, TMQCommitted, b)
	assert.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	var committedResp TMQCommittedResp
	err = json.Unmarshal(msg, &committedResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, committedResp.Code, string(msg))
	assert.Equal(t, 1, len(committedResp.Committed), string(msg))
	assert.Equal(t, pollResp.Offset, committedResp.Committed[0], string(msg))
}

func TestTMQ_PollWithMessageID(t *testing.T) {
	dbName := "test_ws_tmq_poll_with_message_id"
	topic := "test_ws_tmq_poll_with_message_id_topic"

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	before(t, dbName, topic)

	defer func() {
		err = after(t, ws, dbName, topic)
		assert.NoError(t, err)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:        "root",
		Password:    "taosdata",
		DB:          dbName,
		GroupID:     "test",
		Topics:      []string{topic},
		AutoCommit:  "false",
		OffsetReset: "earliest",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)
	// poll
	messageID := uint64(0)
	b, _ = json.Marshal(TMQPollReq{ReqID: 100, BlockingTime: 500, MessageID: &messageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	var pollResp TMQPollResp
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.True(t, pollResp.HaveMessage, string(msg))
	assert.Equal(t, uint64(100), pollResp.ReqID, string(msg))

	// poll with old messageID
	b, _ = json.Marshal(TMQPollReq{ReqID: 101, BlockingTime: 500, MessageID: &messageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp2 TMQPollResp
	err = json.Unmarshal(msg, &pollResp2)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.True(t, pollResp2.HaveMessage, string(msg))
	assert.Equal(t, uint64(101), pollResp2.ReqID, string(msg))
	pollResp2.ReqID = 100
	pollResp2.Timing = pollResp.Timing
	assert.Equal(t, pollResp, pollResp2)

	// poll with new messageID
	messageID = pollResp2.MessageID
	b, _ = json.Marshal(TMQPollReq{ReqID: 102, BlockingTime: 500, MessageID: &messageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp3 TMQPollResp
	err = json.Unmarshal(msg, &pollResp3)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.True(t, pollResp3.HaveMessage, string(msg))
	assert.Equal(t, uint64(102), pollResp3.ReqID, string(msg))
	assert.NotEqual(t, pollResp2.MessageID, pollResp3.MessageID)

	// poll with old messageID
	b, _ = json.Marshal(TMQPollReq{ReqID: 103, BlockingTime: 500, MessageID: &messageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp4 TMQPollResp
	err = json.Unmarshal(msg, &pollResp4)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.True(t, pollResp4.HaveMessage, string(msg))
	assert.Equal(t, uint64(103), pollResp4.ReqID, string(msg))
	pollResp4.ReqID = pollResp3.ReqID
	pollResp4.Timing = pollResp3.Timing
	assert.Equal(t, pollResp3, pollResp4)
	latestMessageID := pollResp4.MessageID
	// poll until no message
	for {
		b, _ = json.Marshal(TMQPollReq{ReqID: 104, BlockingTime: 500, MessageID: &messageID})
		msg, err = doWebSocket(ws, TMQPoll, b)
		assert.NoError(t, err)
		t.Log(string(msg))
		var pollResp5 TMQPollResp
		err = json.Unmarshal(msg, &pollResp4)
		assert.NoError(t, err)
		assert.Equal(t, 0, pollResp.Code, string(msg))
		if !pollResp5.HaveMessage {
			break
		}
		latestMessageID = pollResp5.MessageID
	}
	// poll with new messageID
	b, _ = json.Marshal(TMQPollReq{ReqID: 105, BlockingTime: 500, MessageID: &latestMessageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp6 TMQPollResp
	err = json.Unmarshal(msg, &pollResp6)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.False(t, pollResp6.HaveMessage, string(msg))
	// insert data
	code, message := doHttpSql(fmt.Sprintf("insert into %s.ct2 values (now, 1, 2, '3')", dbName))
	assert.Equal(t, 0, code, message)
	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 106, BlockingTime: 1000, MessageID: &latestMessageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp7 TMQPollResp
	err = json.Unmarshal(msg, &pollResp7)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.True(t, pollResp7.HaveMessage, string(msg))
	assert.NotEqual(t, latestMessageID, pollResp7.MessageID)
	// poll with new messageID
	latestMessageID = pollResp7.MessageID
	b, _ = json.Marshal(TMQPollReq{ReqID: 107, BlockingTime: 500, MessageID: &latestMessageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp8 TMQPollResp
	err = json.Unmarshal(msg, &pollResp8)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.False(t, pollResp8.HaveMessage, string(msg))

	// commit
	b, _ = json.Marshal(TMQCommitReq{ReqID: 107, MessageID: latestMessageID})
	msg, err = doWebSocket(ws, TMQCommit, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var commitResp TMQCommitResp
	err = json.Unmarshal(msg, &commitResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, commitResp.Code, string(msg))

	// commit all
	b, _ = json.Marshal(TMQCommitReq{ReqID: 108})
	msg, err = doWebSocket(ws, TMQCommit, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	err = json.Unmarshal(msg, &commitResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, commitResp.Code, string(msg))

	// unsubscribe
	b, _ = json.Marshal(TMQUnsubscribeReq{ReqID: 109})
	msg, err = doWebSocket(ws, TMQUnsubscribe, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var unsubscribeResp TMQUnsubscribeResp
	err = json.Unmarshal(msg, &unsubscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, unsubscribeResp.Code, string(msg))

	// subscribe
	b, _ = json.Marshal(TMQSubscribeReq{
		Topics: []string{topic},
	})
	msg, err = doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, string(msg))

	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 107, BlockingTime: 500, MessageID: &latestMessageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp9 TMQPollResp
	err = json.Unmarshal(msg, &pollResp9)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	assert.False(t, pollResp9.HaveMessage, string(msg))
	// insert data
	code, message = doHttpSql(fmt.Sprintf("insert into %s.ct2 values (now, 1, 2, '3')", dbName))
	assert.Equal(t, 0, code, message)
	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 107, BlockingTime: 10000, MessageID: &latestMessageID})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	t.Log(string(msg))
	var pollResp10 TMQPollResp
	err = json.Unmarshal(msg, &pollResp10)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp10.Code, string(msg))
	assert.True(t, pollResp10.HaveMessage, string(msg))
	assert.Greater(t, pollResp10.MessageID, latestMessageID)
}

type fetchRawNewResponse struct {
	Flag           uint64 //8               0
	Action         uint64 //8               8
	Version        uint16 //2               16
	Time           uint64 //8               18
	ReqID          uint64 //8               26
	Code           uint32 //4               34
	MessageLen     uint32 //4               38
	Message        string //MessageLen      42
	MessageID      uint64 //8               42 + MessageLen
	MetaType       uint16 //2               50 + MessageLen
	RawBlockLength uint32 //4               52 + MessageLen
	TMQRawBlock    []byte //RawBlockLength  56 + MessageLen + RawBlockLength
}

func parseFetchRawNewResponse(bs []byte) *fetchRawNewResponse {
	resp := &fetchRawNewResponse{}
	resp.Flag = binary.LittleEndian.Uint64(bs)
	resp.Action = binary.LittleEndian.Uint64(bs[8:])
	resp.Version = binary.LittleEndian.Uint16(bs[16:])
	resp.Time = binary.LittleEndian.Uint64(bs[18:])
	resp.ReqID = binary.LittleEndian.Uint64(bs[26:])
	resp.Code = binary.LittleEndian.Uint32(bs[34:])
	resp.MessageLen = binary.LittleEndian.Uint32(bs[38:])
	resp.Message = string(bs[42 : 42+resp.MessageLen])
	resp.MessageID = binary.LittleEndian.Uint64(bs[42+resp.MessageLen:])
	if resp.Code != 0 {
		return resp
	}
	resp.MetaType = binary.LittleEndian.Uint16(bs[50+resp.MessageLen:])
	resp.RawBlockLength = binary.LittleEndian.Uint32(bs[52+resp.MessageLen:])
	resp.TMQRawBlock = bs[56+resp.MessageLen : 56+resp.MessageLen+resp.RawBlockLength]
	return resp
}

func prepareAllType(t *testing.T, dbName string, topic string) {
	doHttpSql(fmt.Sprintf("drop topic if exists %s", topic))
	doHttpSql(fmt.Sprintf("drop database if exists %s", dbName))
	code, message := doHttpSql(fmt.Sprintf("create database if not exists %s WAL_RETENTION_PERIOD 86400", dbName))
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated(dbName))
	code, message = doHttpSql(fmt.Sprintf("create table %s.stb (ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20),"+
		"c14 varbinary(20),"+
		"c15 geometry(100),"+
		"c16 decimal(20,4)"+
		")"+
		"tags(tts timestamp,"+
		"tc1 bool,"+
		"tc2 tinyint,"+
		"tc3 smallint,"+
		"tc4 int,"+
		"tc5 bigint,"+
		"tc6 tinyint unsigned,"+
		"tc7 smallint unsigned,"+
		"tc8 int unsigned,"+
		"tc9 bigint unsigned,"+
		"tc10 float,"+
		"tc11 double,"+
		"tc12 binary(20),"+
		"tc13 nchar(20),"+
		"tc14 varbinary(20),"+
		"tc15 geometry(100)"+
		")", dbName))
	assert.Equal(t, 0, code, message)

	now := time.Now().Round(time.Millisecond).UTC()
	nowStr := now.Format(time.RFC3339Nano)
	code, message = doHttpSql(fmt.Sprintf("create table %s.ctb using %s.stb tags('%s', true,1,1,1,1,1,1,1,1,1,1,'tg','ntg','\\xaabbcc','point(100 100)')", dbName, dbName, nowStr))
	if code != 0 {
		t.Fatalf("insert failed: %s", message)
	}
	code, message = doHttpSql(fmt.Sprintf("insert into %s.ctb values('%s',true,1,1,1,1,1,1,1,1,1,1,'vl','nvl','\\xaabbcc','point(100 100)',123456789.123)", dbName, nowStr))
	if code != 0 {
		t.Fatalf("insert failed: %s", message)
	}
	code, message = doHttpSql(fmt.Sprintf("create topic if not exists %s as database %s", topic, dbName))
	assert.Equal(t, 0, code, message)
}

func afterAllType(t *testing.T, ws *websocket.Conn, dbName string, topic string) error {
	b, _ := json.Marshal(TMQUnsubscribeReq{ReqID: 0})
	_, _ = doWebSocket(ws, TMQUnsubscribe, b)
	err := ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil {
		return err
	}
	var code int
	var message string
	assert.Eventually(t, func() bool {
		code, message = doHttpSql(fmt.Sprintf("drop topic if exists %s", topic))
		return code == 0
	}, 5*time.Second, 500*time.Millisecond, message)
	assert.Eventually(t, func() bool {
		code, message = doHttpSql(fmt.Sprintf("drop database if exists %s", dbName))
		return code == 0
	}, 5*time.Second, 500*time.Millisecond, message)
	return nil
}

func TestTMQ_FetchRawNew(t *testing.T) {
	dbName := "test_ws_tmq_fetch_raw_new"
	topic := "test_ws_tmq_fetch_raw_new_topic"
	prepareAllType(t, dbName, topic)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	defer func() {
		err = afterAllType(t, ws, dbName, topic)
		assert.NoError(t, err)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:        "root",
		Password:    "taosdata",
		DB:          dbName,
		GroupID:     "test",
		Topics:      []string{topic},
		AutoCommit:  "false",
		OffsetReset: "latest",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)

	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	var pollResp TMQPollResp
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))

	// insert
	now := time.Now().Round(time.Millisecond)
	nowStr := now.Format(time.RFC3339Nano)
	code, message := doHttpSql(fmt.Sprintf("insert into %s.ctb values('%s',true,1,1,1,1,1,1,1,1,1,1,'vl','nvl','\\xaabbcc','point(100 100)',123456789.123)", dbName, nowStr))
	assert.Equal(t, 0, code, message)

	// poll
	gotMessage := false
	for i := 0; i < 5; i++ {
		b, _ = json.Marshal(TMQPollReq{ReqID: 0, BlockingTime: 500})
		msg, err = doWebSocket(ws, TMQPoll, b)
		assert.NoError(t, err)
		err = json.Unmarshal(msg, &pollResp)
		assert.NoError(t, err)
		assert.Equal(t, 0, pollResp.Code, string(msg))
		if pollResp.HaveMessage {
			assert.True(t, pollResp.Offset >= 0, string(msg))
			gotMessage = true
			assert.True(t, pollResp.Offset >= 0, string(msg))
			break
		}
	}
	if !assert.True(t, gotMessage) {
		return
	}

	// fetch raw new
	b, _ = json.Marshal(TMQFetchRawReq{ReqID: 100, MessageID: pollResp.MessageID})
	msg, err = doWebSocket(ws, TMQFetchRawData, b)
	assert.NoError(t, err)
	resp := parseFetchRawNewResponse(msg)
	assert.Equal(t, uint64(0xffffffffffffffff), resp.Flag, resp.Flag)
	assert.Equal(t, uint32(0), resp.Code, resp.Message)
	assert.Equal(t, uint16(1), resp.Version)
	assert.Equal(t, uint64(TMQFetchRawNewMessage), resp.Action)
	assert.Greater(t, resp.Time, uint64(0))
	assert.Equal(t, uint64(100), resp.ReqID)
	assert.Equal(t, pollResp.MessageID, resp.MessageID)
	assert.Equal(t, int(resp.RawBlockLength), len(resp.TMQRawBlock))
	ps := parser.NewTMQRawDataParser()
	blockInfo, err := ps.Parse(unsafe.Pointer(&resp.TMQRawBlock[0]))
	assert.NoError(t, err)
	for _, info := range blockInfo {
		t.Log(info.TableName)
		data, err := parser.ReadBlockSimple(info.RawBlock, info.Precision)
		assert.NoError(t, err)
		for i, schema := range info.Schema {
			t.Log(schema.Name, schema.ColType, schema.Flag, schema.Bytes, schema.ColID)
			assert.Equal(t, i+1, schema.ColID)
		}
		expect := [][]driver.Value{
			{
				now,
				true,
				int8(1),
				int16(1),
				int32(1),
				int64(1),
				uint8(1),
				uint16(1),
				uint32(1),
				uint64(1),
				float32(1),
				float64(1),
				"vl",
				"nvl",
				[]byte{0xaa, 0xbb, 0xcc},
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
				"123456789.1230",
			},
		}
		assert.Equal(t, expect, data)
		v, err := json.Marshal(data)
		assert.NoError(t, err)
		t.Log(string(v))
	}

	// fetch wrong
	b, _ = json.Marshal(TMQFetchRawReq{ReqID: 100, MessageID: 8000})
	msg, err = doWebSocket(ws, TMQFetchRawData, b)
	assert.NoError(t, err)
	resp = parseFetchRawNewResponse(msg)
	assert.Equal(t, uint64(0xffffffffffffffff), resp.Flag, resp.Flag)
	assert.Equal(t, uint32(65535), resp.Code, resp.Message)
	assert.Equal(t, uint16(1), resp.Version)
	assert.Equal(t, uint64(TMQFetchRawNewMessage), resp.Action)
	assert.Greater(t, resp.Time, uint64(0))
	assert.Equal(t, uint64(100), resp.ReqID)
	assert.Equal(t, uint64(8000), resp.MessageID)
	t.Log(resp.Message)

	//commit offset
	b, _ = json.Marshal(TMQCommitOffsetReq{ReqID: 0, Topic: topic, VgroupID: pollResp.VgroupID, Offset: pollResp.Offset})
	msg, err = doWebSocket(ws, TMQCommitOffset, b)
	assert.NoError(t, err)
	var commitOffsetResp TMQCommitOffsetResp
	err = json.Unmarshal(msg, &commitOffsetResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, commitOffsetResp.Code, commitOffsetResp.Message)

	// committed
	b, _ = json.Marshal(TMQCommittedReq{ReqID: 0, TopicVgroupIDs: []TopicVgroupID{{Topic: topic, VgroupID: pollResp.VgroupID}}})
	msg, err = doWebSocket(ws, TMQCommitted, b)
	assert.NoError(t, err)
	if err != nil {
		t.Fatal(err)
	}
	var committedResp TMQCommittedResp
	err = json.Unmarshal(msg, &committedResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, committedResp.Code, string(msg))
	assert.Equal(t, 1, len(committedResp.Committed), string(msg))
	assert.Equal(t, pollResp.Offset, committedResp.Committed[0], string(msg))
}

func TestTMQ_SetMsgConsumeExcluded(t *testing.T) {
	dbName := "test_ws_tmq_set_msg_consume_excluded"
	topic := "test_ws_tmq_set_msg_consume_excluded_topic"

	before(t, dbName, topic)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	defer func() {
		err = after(t, ws, dbName, topic)
		assert.NoError(t, err)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:               "root",
		Password:           "taosdata",
		DB:                 dbName,
		GroupID:            "test",
		Topics:             []string{topic},
		AutoCommit:         "false",
		OffsetReset:        "earliest",
		MsgConsumeExcluded: "1",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)
}

func TestDropUser(t *testing.T) {
	dbName := "test_ws_tmq_drop_user"
	topic := "test_ws_tmq_drop_user_topic"

	before(t, dbName, topic)
	defer doHttpSql("drop user test_tmq_drop_user")
	code, message := doHttpSql("create user test_tmq_drop_user pass 'pass_123'")
	assert.Equal(t, 0, code, message)
	code, message = doHttpSql(fmt.Sprintf("grant subscribe on topic %s.%s to test_tmq_drop_user", dbName, topic))
	if testenv.IsEnterpriseTest() {
		require.Equal(t, 0, code, message)
	} else {
		require.NotEqual(t, 0, code, message)
	}
	code, message = doHttpSql(fmt.Sprintf("grant all on database %s to test_tmq_drop_user", dbName))
	if testenv.IsEnterpriseTest() {
		require.Equal(t, 0, code, message)
	} else {
		require.NotEqual(t, 0, code, message)
	}

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	defer func() {
		assert.Eventually(t, func() bool {
			code, message = doHttpSql(fmt.Sprintf("drop topic if exists %s", topic))
			return code == 0
		}, 5*time.Second, 500*time.Millisecond, message)
		assert.Eventually(t, func() bool {
			code, message = doHttpSql(fmt.Sprintf("drop database if exists %s", dbName))
			return code == 0
		}, 5*time.Second, 500*time.Millisecond, message)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:        "test_tmq_drop_user",
		Password:    "pass_123",
		GroupID:     "test",
		Topics:      []string{topic},
		AutoCommit:  "false",
		OffsetReset: "earliest",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	require.NoError(t, err)
	require.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)
	// drop user
	code, message = doHttpSql("drop user test_tmq_drop_user")
	assert.Equal(t, 0, code, message)
	assert.Eventually(t, func() bool {
		_, err := doWebSocket(ws, wstool.ClientVersion, nil)
		return err != nil
	}, 10*time.Second, 500*time.Millisecond)
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

func TestConnectionOptions(t *testing.T) {
	dbName := "test_ws_tmq_conn_options"
	topic := "test_ws_tmq_conn_options_topic"

	before(t, dbName, topic)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	defer func() {
		var code int
		var message string
		assert.Eventually(t, func() bool {
			code, message = doHttpSql(fmt.Sprintf("drop topic if exists %s", topic))
			return code == 0
		}, 5*time.Second, 500*time.Millisecond, message)
		assert.Eventually(t, func() bool {
			code, message = doHttpSql(fmt.Sprintf("drop database if exists %s", dbName))
			return code == 0
		}, 5*time.Second, 500*time.Millisecond, message)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:             "root",
		Password:         "taosdata",
		DB:               dbName,
		GroupID:          "test",
		Topics:           []string{topic},
		AutoCommit:       "false",
		OffsetReset:      "earliest",
		SessionTimeoutMS: "100000",
		App:              "tmq_test_conn_protocol",
		IP:               "192.168.55.55",
		TZ:               "Asia/Shanghai",
		Connector:        "tmq_test_connector_info",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)

	var connId float64
	assert.Eventually(t, func() bool {
		queryResp := restQuery("select conn_id from performance_schema.perf_connections where user_app = 'tmq_test_conn_protocol' and user_ip = '192.168.55.55'", "")
		got := queryResp.Code == 0 && len(queryResp.Data) > 0
		if got {
			connId = queryResp.Data[0][0].(float64)
		}
		return got
	}, 10*time.Second, 500*time.Millisecond)

	b, _ = json.Marshal(TMQUnsubscribeReq{ReqID: 0})
	_, _ = doWebSocket(ws, TMQUnsubscribe, b)
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		queryResp := restQuery("select * from performance_schema.perf_connections where conn_id = "+fmt.Sprintf("%d", int64(connId)), "")
		return queryResp.Code == 0 && len(queryResp.Data) == 0
	}, 10*time.Second, 500*time.Millisecond)

	queryResp := restQuery("select conn_id from performance_schema.perf_connections where user_app = 'tmq_test_conn_protocol' and user_ip = '192.168.55.55'", "")
	assert.Equal(t, 0, queryResp.Code)
	assert.Equal(t, 0, len(queryResp.Data))
}

func TestWrongPass(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:             "root",
		Password:         "wrong_pass",
		GroupID:          "test",
		Topics:           []string{"test"},
		AutoCommit:       "false",
		OffsetReset:      "earliest",
		SessionTimeoutMS: "100000",
		App:              "tmq_test_conn_protocol",
		IP:               "192.168.55.55",
		TZ:               "Asia/Shanghai",
		Connector:        "tmq_test_connector_info",
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, subscribeResp.Code, subscribeResp.Message)
}

func TestPollError(t *testing.T) {
	dbName := "test_ws_tmq_poll_error"
	topic := "test_ws_tmq_poll_error_topic"

	before(t, dbName, topic)

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	defer func() {
		err = after(t, ws, dbName, topic)
		assert.NoError(t, err)
	}()

	// subscribe
	b, _ := json.Marshal(TMQSubscribeReq{
		User:              "root",
		Password:          "taosdata",
		DB:                dbName,
		GroupID:           "test",
		Topics:            []string{topic},
		AutoCommit:        "false",
		OffsetReset:       "earliest",
		SessionTimeoutMS:  "10000",
		MaxPollIntervalMS: "3000", // from 3.4.0.0 rebalance time will be added to poll interval to calculate expiration time, so must be greater than 2 seconds
	})
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)

	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 100, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	var pollResp TMQPollResp
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, pollResp.Code, string(msg))
	for {
		// poll until no message and no error
		b, _ = json.Marshal(TMQPollReq{ReqID: 101, BlockingTime: 500})
		msg, err = doWebSocket(ws, TMQPoll, b)
		assert.NoError(t, err)
		err = json.Unmarshal(msg, &pollResp)
		assert.NoError(t, err)
		if pollResp.Code != 0 {
			t.Errorf("poll error: %s", pollResp.Message)
			return
		}
		if !pollResp.HaveMessage {
			break
		}
	}
	t.Log("sleep 6s to wait for timeout")
	// sleep
	time.Sleep(time.Second * 6)
	// poll
	b, _ = json.Marshal(TMQPollReq{ReqID: 102, BlockingTime: 500})
	msg, err = doWebSocket(ws, TMQPoll, b)
	assert.NoError(t, err)
	err = json.Unmarshal(msg, &pollResp)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, pollResp.Code, string(msg))
}

func TestConsumeRawdata(t *testing.T) {
	code, message := doHttpSql("create database if not exists test_ws_rawdata WAL_RETENTION_PERIOD 86400")
	if code != 0 {
		t.Fatalf("create database failed: %s", message)
	}
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_rawdata"))
	code, message = doHttpSql("create topic if not exists test_tmq_rawdata_ws_topic with meta as DATABASE test_ws_rawdata")
	if code != 0 {
		t.Fatalf("create topic failed: %s", message)
	}

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	init := &TMQSubscribeReq{
		ReqID:                0,
		User:                 "root",
		Password:             "taosdata",
		GroupID:              "test",
		Topics:               []string{"test_tmq_rawdata_ws_topic"},
		AutoCommit:           "true",
		AutoCommitIntervalMS: "5000",
		SnapshotEnable:       "true",
		WithTableName:        "true",
		OffsetReset:          "earliest",
		EnableBatchMeta:      "1",
		SessionTimeoutMS:     "12000",
		MaxPollIntervalMS:    "300000",
		MsgConsumeRawdata:    "1",
	}
	b, _ := json.Marshal(init)
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)
	code, message = doHttpSql("create table test_ws_rawdata.stb (ts timestamp," +
		"c1 bool," +
		"c2 tinyint," +
		"c3 smallint," +
		"c4 int," +
		"c5 bigint," +
		"c6 tinyint unsigned," +
		"c7 smallint unsigned," +
		"c8 int unsigned," +
		"c9 bigint unsigned," +
		"c10 float," +
		"c11 double," +
		"c12 binary(20)," +
		"c13 nchar(20)," +
		"c14 varbinary(20)," +
		"c15 geometry(100)," +
		"c16 decimal(20,4)" +
		")" +
		"tags(tts timestamp," +
		"tc1 bool," +
		"tc2 tinyint," +
		"tc3 smallint," +
		"tc4 int," +
		"tc5 bigint," +
		"tc6 tinyint unsigned," +
		"tc7 smallint unsigned," +
		"tc8 int unsigned," +
		"tc9 bigint unsigned," +
		"tc10 float," +
		"tc11 double," +
		"tc12 binary(20)," +
		"tc13 nchar(20)," +
		"tc14 varbinary(20)," +
		"tc15 geometry(100)" +
		")")
	if code != 0 {
		t.Fatalf("create table failed: %s", message)
	}
	now := time.Now().Round(time.Millisecond).UTC()
	nowStr := now.Format(time.RFC3339Nano)
	code, message = doHttpSql(fmt.Sprintf("create table test_ws_rawdata.ctb using test_ws_rawdata.stb tags('%s', true,1,1,1,1,1,1,1,1,1,1,'tg','ntg','\\xaabbcc','point(100 100)')", nowStr))
	if code != 0 {
		t.Fatalf("insert failed: %s", message)
	}
	code, message = doHttpSql(fmt.Sprintf("insert into test_ws_rawdata.ctb values('%s',true,1,1,1,1,1,1,1,1,1,1,'vl','nvl','\\xaabbcc','point(100 100)',123456789.123)", nowStr))
	if code != 0 {
		t.Fatalf("insert failed: %s", message)
	}
	gotRawMessage := false
	for i := 0; i < 5; i++ {
		b, _ = json.Marshal(&TMQPollReq{
			ReqID:        3,
			BlockingTime: 500,
		})
		msg, err = doWebSocket(ws, TMQPoll, b)
		assert.NoError(t, err)
		var pollResp TMQPollResp
		err = json.Unmarshal(msg, &pollResp)
		assert.NoError(t, err)
		assert.Equal(t, 0, pollResp.Code, string(msg))
		if pollResp.HaveMessage {
			if pollResp.MessageType == common.TMQ_RES_RAWDATA {
				gotRawMessage = true
				// can not call fetch
				b, _ = json.Marshal(TMQFetchReq{ReqID: 101, MessageID: pollResp.MessageID})
				msg, err = doWebSocket(ws, TMQFetch, b)
				assert.NoError(t, err)
				var fetchResp TMQFetchResp
				err = json.Unmarshal(msg, &fetchResp)
				assert.NoError(t, err)
				assert.Equal(t, uint64(101), fetchResp.ReqID, fetchResp)
				assert.NotEqual(t, 0, fetchResp.Code, fetchResp)
				// can not call fetch_block
				b, _ = json.Marshal(TMQFetchBlockReq{ReqID: 102, MessageID: pollResp.MessageID})
				msg, err = doWebSocket(ws, TMQFetchBlock, b)
				assert.NoError(t, err)
				var fetchBlockResp WSTMQErrorResp
				err = json.Unmarshal(msg, &fetchBlockResp)
				assert.NoError(t, err)
				assert.Equal(t, uint64(102), fetchBlockResp.ReqID, fetchResp)
				assert.NotEqual(t, 0, fetchBlockResp.Code, fetchBlockResp)
				// can not call fetch_json_meta
				b, _ = json.Marshal(TMQFetchJsonMetaReq{ReqID: 103, MessageID: pollResp.MessageID})
				msg, err = doWebSocket(ws, TMQFetchJsonMeta, b)
				assert.NoError(t, err)
				var fetchJsonMetaResp TMQFetchJsonMetaResp
				err = json.Unmarshal(msg, &fetchJsonMetaResp)
				assert.NoError(t, err)
				assert.Equal(t, uint64(103), fetchJsonMetaResp.ReqID, fetchJsonMetaResp)
				assert.NotEqual(t, 0, fetchJsonMetaResp.Code, fetchJsonMetaResp)
			}
			b, _ = json.Marshal(TMQFetchRawReq{ReqID: 100, MessageID: pollResp.MessageID})
			msg, err = doWebSocket(ws, TMQFetchRawData, b)
			assert.NoError(t, err)
			resp := parseFetchRawNewResponse(msg)
			assert.Equal(t, uint64(0xffffffffffffffff), resp.Flag, resp.Flag)
			assert.Equal(t, uint32(0), resp.Code, resp.Message)
			assert.Equal(t, uint16(1), resp.Version)
			assert.Equal(t, uint64(TMQFetchRawNewMessage), resp.Action)
			assert.Greater(t, resp.Time, uint64(0))
			assert.Equal(t, uint64(100), resp.ReqID)
			assert.Equal(t, pollResp.MessageID, resp.MessageID)
			assert.Equal(t, int(resp.RawBlockLength), len(resp.TMQRawBlock))

			writeMsg := make([]byte, 30+resp.RawBlockLength)
			binary.LittleEndian.PutUint64(writeMsg, resp.ReqID)
			binary.LittleEndian.PutUint64(writeMsg[8:], resp.MessageID)
			binary.LittleEndian.PutUint64(writeMsg[16:], TMQRawMessage)
			binary.LittleEndian.PutUint32(writeMsg[24:], resp.RawBlockLength)
			binary.LittleEndian.PutUint16(writeMsg[28:], resp.MetaType)
			copy(writeMsg[30:], resp.TMQRawBlock)
			writeRaw(t, writeMsg, "test_ws_rawdata_target")
		}
	}
	if !assert.True(t, gotRawMessage) {
		return
	}
	b, _ = json.Marshal(&TMQUnsubscribeReq{
		ReqID: 6,
	})
	msg, err = doWebSocket(ws, TMQUnsubscribe, b)
	assert.NoError(t, err)
	var unsubscribeResp TMQUnsubscribeResp
	err = json.Unmarshal(msg, &unsubscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, unsubscribeResp.Code, unsubscribeResp.Message)

	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)
	var resp *httpQueryResp
	assert.Eventually(t, func() bool {
		resp = restQuery("describe stb", "test_ws_rawdata_target")
		return resp.Code == 0
	}, 5*time.Second, 500*time.Millisecond, message)
	expect := [][]driver.Value{
		{"ts", "TIMESTAMP", float64(8), ""},
		{"c1", "BOOL", float64(1), ""},
		{"c2", "TINYINT", float64(1), ""},
		{"c3", "SMALLINT", float64(2), ""},
		{"c4", "INT", float64(4), ""},
		{"c5", "BIGINT", float64(8), ""},
		{"c6", "TINYINT UNSIGNED", float64(1), ""},
		{"c7", "SMALLINT UNSIGNED", float64(2), ""},
		{"c8", "INT UNSIGNED", float64(4), ""},
		{"c9", "BIGINT UNSIGNED", float64(8), ""},
		{"c10", "FLOAT", float64(4), ""},
		{"c11", "DOUBLE", float64(8), ""},
		{"c12", "VARCHAR", float64(20), ""},
		{"c13", "NCHAR", float64(20), ""},
		{"c14", "VARBINARY", float64(20), ""},
		{"c15", "GEOMETRY", float64(100), ""},
		{"c16", "DECIMAL(20, 4)", float64(16), ""},
		{"tts", "TIMESTAMP", float64(8), "TAG"},
		{"tc1", "BOOL", float64(1), "TAG"},
		{"tc2", "TINYINT", float64(1), "TAG"},
		{"tc3", "SMALLINT", float64(2), "TAG"},
		{"tc4", "INT", float64(4), "TAG"},
		{"tc5", "BIGINT", float64(8), "TAG"},
		{"tc6", "TINYINT UNSIGNED", float64(1), "TAG"},
		{"tc7", "SMALLINT UNSIGNED", float64(2), "TAG"},
		{"tc8", "INT UNSIGNED", float64(4), "TAG"},
		{"tc9", "BIGINT UNSIGNED", float64(8), "TAG"},
		{"tc10", "FLOAT", float64(4), "TAG"},
		{"tc11", "DOUBLE", float64(8), "TAG"},
		{"tc12", "VARCHAR", float64(20), "TAG"},
		{"tc13", "NCHAR", float64(20), "TAG"},
		{"tc14", "VARBINARY", float64(20), "TAG"},
		{"tc15", "GEOMETRY", float64(100), "TAG"},
	}
	for index, values := range expect {
		for i := 0; i < 4; i++ {
			assert.Equal(t, values[i], resp.Data[index][i])
		}
	}

	resp = restQuery("select * from stb limit 1", "test_ws_rawdata_target")
	expect = [][]driver.Value{
		{
			now.Format(layout.LayoutMillSecond),
			true,
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			"vl",
			"nvl",
			"aabbcc",
			"010100000000000000000059400000000000005940",
			"123456789.1230",
			now.Format(layout.LayoutMillSecond),
			true,
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			float64(1),
			"tg",
			"ntg",
			"aabbcc",
			"010100000000000000000059400000000000005940",
		},
	}
	assert.Equal(t, expect, resp.Data)

	assert.Eventually(t, func() bool {
		code, message = doHttpSql("drop topic if exists test_tmq_rawdata_ws_topic")
		return code == 0
	}, 5*time.Second, 500*time.Millisecond, message)
	assert.Eventually(t, func() bool {
		code, message = doHttpSql("drop database if exists test_ws_rawdata_target")
		return code == 0
	}, 5*time.Second, 500*time.Millisecond, message)
	assert.Eventually(t, func() bool {
		code, message = doHttpSql("drop database if exists test_ws_rawdata")
		return code == 0
	}, 5*time.Second, 500*time.Millisecond, message)
}

func TestSetConfig(t *testing.T) {
	code, message := doHttpSql("create database if not exists test_ws_tmq_set_conf WAL_RETENTION_PERIOD 86400")
	if code != 0 {
		t.Fatalf("create database failed: %s", message)
	}
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_tmq_set_conf"))
	code, message = doHttpSql("create topic if not exists test_ws_tmq_set_conf_topic with meta as DATABASE test_ws_tmq_set_conf")
	if code != 0 {
		t.Fatalf("create topic failed: %s", message)
	}
	defer func() {
		assert.Eventually(t, func() bool {
			code, message = doHttpSql("drop topic if exists test_ws_tmq_set_conf_topic")
			return code == 0
		}, 5*time.Second, 500*time.Millisecond, message)
		assert.Eventually(t, func() bool {
			code, message = doHttpSql("drop database if exists test_ws_tmq_set_conf")
			return code == 0
		}, 5*time.Second, 500*time.Millisecond, message)
	}()
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	initConfig := &TMQSubscribeReq{
		ReqID:                0,
		User:                 "root",
		Password:             "taosdata",
		GroupID:              "test",
		Topics:               []string{"test_ws_tmq_set_conf_topic"},
		AutoCommit:           "true",
		AutoCommitIntervalMS: "5000",
		SnapshotEnable:       "true",
		WithTableName:        "true",
		OffsetReset:          "earliest",
		EnableBatchMeta:      "1",
		SessionTimeoutMS:     "12000",
		MaxPollIntervalMS:    "300000",
		MsgConsumeRawdata:    "1",
		Config: map[string]string{
			"td.connect.user":         "wrong_user",
			"td.connect.pass":         "wrong_pass",
			"td.connect.ip":           "localhost",
			"td.connect.port":         "6030",
			"group.id":                "test_conf",
			"client.id":               "test_conf_client",
			"auto.offset.reset":       "latest",
			"enable.auto.commit":      "true",
			"auto.commit.interval.ms": "5000",
			"msg.with.table.name":     "true",
			"session.timeout.ms":      "10000",
			"max.poll.interval.ms":    "300000",
		},
	}
	b, _ := json.Marshal(initConfig)
	msg, err := doWebSocket(ws, TMQSubscribe, b)
	assert.NoError(t, err)
	var subscribeResp TMQSubscribeResp
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, subscribeResp.Code, subscribeResp.Message)

	// unsubscribe
	b, _ = json.Marshal(&TMQUnsubscribeReq{
		ReqID: 6,
	})
	msg, err = doWebSocket(ws, TMQUnsubscribe, b)
	assert.NoError(t, err)
	var unsubscribeResp TMQUnsubscribeResp
	err = json.Unmarshal(msg, &unsubscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, unsubscribeResp.Code, unsubscribeResp.Message)
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)

	// unknown config key
	ws2, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws2.Close()
		assert.NoError(t, err)
	}()
	initConfig.Config["wrong_config"] = "wrong"
	b, _ = json.Marshal(initConfig)
	msg, err = doWebSocket(ws2, TMQSubscribe, b)
	assert.NoError(t, err)
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, TsdbCodeInvalidPara, subscribeResp.Code, subscribeResp.Message)

	// unknown value
	delete(initConfig.Config, "wrong_config")
	initConfig.Config["session.timeout.ms"] = "abcd"
	b, _ = json.Marshal(initConfig)
	msg, err = doWebSocket(ws2, TMQSubscribe, b)
	assert.NoError(t, err)
	err = json.Unmarshal(msg, &subscribeResp)
	assert.NoError(t, err)
	assert.Equal(t, TsdbCodeInvalidPara, subscribeResp.Code, subscribeResp.Message)

	err = ws2.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)

}

func TestTMQPollReq_String(t *testing.T) {
	type fields struct {
		ReqID        uint64
		BlockingTime int64
		MessageID    *uint64
		ctx          context.Context
	}
	messageID := uint64(1)
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "no messageid",
			fields: fields{
				ReqID:        1,
				BlockingTime: 500,
				MessageID:    nil,
				ctx:          nil,
			},
			want: "&{ReqID:1 BlockingTime:500 MessageID:nil}",
		},
		{
			name: "normal",
			fields: fields{
				ReqID:        1,
				BlockingTime: 500,
				MessageID:    &messageID,
				ctx:          nil,
			},
			want: "&{ReqID:1 BlockingTime:500 MessageID:1}",
		},
		{
			name: "with context",
			fields: fields{
				ReqID:        1,
				BlockingTime: 500,
				MessageID:    &messageID,
				ctx:          context.Background(),
			},
			want: "&{ReqID:1 BlockingTime:500 MessageID:1}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &TMQPollReq{
				ReqID:        tt.fields.ReqID,
				BlockingTime: tt.fields.BlockingTime,
				MessageID:    tt.fields.MessageID,
				ctx:          tt.fields.ctx,
			}
			assert.Equalf(t, tt.want, req.String(), "String()")
		})
	}
}

func TestVersion(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/tmq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	req := &versionRequest{
		ReqID: 0x123654,
	}
	bs, err := json.Marshal(req)
	assert.NoError(t, err)
	msg, err := doWebSocket(ws, wstool.ClientVersion, bs)
	assert.NoError(t, err)
	var versionResp versionResponse
	err = json.Unmarshal(msg, &versionResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, versionResp.Code, string(msg))
	assert.Equal(t, version.TaosClientVersion, versionResp.Version)
	assert.Equal(t, req.ReqID, versionResp.ReqID, string(msg))
	req2 := []byte(`{"action":"version"}`)
	msg, err = sendWSMessage(ws, websocket.TextMessage, req2)
	assert.NoError(t, err)
	err = json.Unmarshal(msg, &versionResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, versionResp.Code, string(msg))
	assert.Equal(t, version.TaosClientVersion, versionResp.Version)
	assert.Equal(t, uint64(0), versionResp.ReqID, string(msg))
}

func TestConnectToken(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("token test only for enterprise edition")
		return
	}
	user := "tmq_test_token_user"
	pass := "M^$RiK*vOLXQU5rD"
	tokenName := "tmq_test_token"
	dbName := "test_ws_tmq_token"
	topic := "test_ws_tmq_token_topic"
	code, message := doHttpSql(fmt.Sprintf("drop topic if exists %s", topic))
	assert.Equal(t, 0, code, message)
	code, message = doHttpSql(fmt.Sprintf("drop database if exists %s", dbName))
	assert.Equal(t, 0, code, message)
	code, message = doHttpSql(fmt.Sprintf("create database if not exists %s WAL_RETENTION_PERIOD 86400", dbName))
	assert.Equal(t, 0, code, message)
	code, message = doHttpSql(fmt.Sprintf("create topic if not exists %s as database %s", topic, dbName))
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated(dbName))

	code, message = doHttpSql(fmt.Sprintf("create user %s pass '%s'", user, pass))
	assert.Equal(t, 0, code, message)
	defer func() {
		code, message = doHttpSql(fmt.Sprintf("drop user %s", user))
		assert.Equal(t, 0, code, message)
	}()
	createTokenResp := restQuery(fmt.Sprintf("create token %s from user %s", tokenName, user), "")
	if createTokenResp.Code != 0 {
		t.Errorf("create token failed: %d,%s", createTokenResp.Code, createTokenResp.Desc)
		return
	}
	if len(createTokenResp.Data) < 1 || len(createTokenResp.Data[0]) < 1 {
		t.Error("create token response is empty")
		return
	}
	token := createTokenResp.Data[0][0].(string)
	assert.NoError(t, testtools.EnsureTokenCreated(tokenName))
	defer func() {
		code, message = doHttpSql(fmt.Sprintf("drop token %s", tokenName))
		assert.Equal(t, 0, code, message)
	}()
	code, message = doHttpSql(fmt.Sprintf("grant subscribe on topic %s.%s to %s", dbName, topic, user))
	require.Equal(t, 0, code, message)

	code, message = doHttpSql(fmt.Sprintf("grant all on database %s to %s", dbName, user))
	require.Equal(t, 0, code, message)

	doTMQTest(t, dbName, topic, token)
}
