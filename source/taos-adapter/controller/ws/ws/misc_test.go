package ws

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func TestGetCurrentDB(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	db := "test_current_db"
	code, message := doRestful(fmt.Sprintf("drop database if exists %s", db), "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create database if not exists %s", db), "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated(db))
	defer doRestful(fmt.Sprintf("drop database if exists %s", db), "")

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	// connect
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: db}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// current db
	currentDBReq := map[string]uint64{"req_id": 1}
	resp, err = doWebSocket(ws, WSGetCurrentDB, &currentDBReq)
	assert.NoError(t, err)
	var currentDBResp getCurrentDBResponse
	err = json.Unmarshal(resp, &currentDBResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), currentDBResp.ReqID)
	assert.Equal(t, 0, currentDBResp.Code, currentDBResp.Message)
	assert.Equal(t, db, currentDBResp.DB)
}

func TestGetServerInfo(t *testing.T) {
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

	// connect
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// server info
	serverInfoReq := map[string]uint64{"req_id": 1}
	resp, err = doWebSocket(ws, WSGetServerInfo, &serverInfoReq)
	assert.NoError(t, err)
	var serverInfoResp getServerInfoResponse
	err = json.Unmarshal(resp, &serverInfoResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), serverInfoResp.ReqID)
	assert.Equal(t, 0, serverInfoResp.Code, serverInfoResp.Message)
	t.Log(serverInfoResp.Info)
}

func TestOptionsConnection(t *testing.T) {
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

	// connect
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// set app name
	app := "ws_test_options"
	optionsConnectionReq := optionsConnectionRequest{
		ReqID: 2,
		Options: []*option{
			{Option: common.TSDB_OPTION_CONNECTION_USER_APP, Value: &app},
		},
	}
	resp, err = doWebSocket(ws, OptionsConnection, &optionsConnectionReq)
	assert.NoError(t, err)
	var optionsConnectionResp commonResp
	err = json.Unmarshal(resp, &optionsConnectionResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), optionsConnectionResp.ReqID)
	assert.Equal(t, 0, optionsConnectionResp.Code, optionsConnectionResp.Message)

	// get app name
	got := false
	for i := 0; i < 10; i++ {
		queryResp := restQuery("select conn_id from performance_schema.perf_connections where user_app = 'ws_test_options'", "")
		if queryResp.Code == 0 && len(queryResp.Data) > 0 {
			got = true
			break
		}
		time.Sleep(time.Second)
	}
	assert.True(t, got)
	// clear app name
	optionsConnectionReq = optionsConnectionRequest{
		ReqID: 3,
		Options: []*option{
			{Option: common.TSDB_OPTION_CONNECTION_USER_APP, Value: nil},
		},
	}
	resp, err = doWebSocket(ws, OptionsConnection, &optionsConnectionReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &optionsConnectionResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), optionsConnectionResp.ReqID)
	assert.Equal(t, 0, optionsConnectionResp.Code, optionsConnectionResp.Message)

	// wrong option with nil value
	optionsConnectionReq = optionsConnectionRequest{
		ReqID: 4,
		Options: []*option{
			{Option: -10000, Value: nil},
		},
	}
	resp, err = doWebSocket(ws, OptionsConnection, &optionsConnectionReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &optionsConnectionResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), optionsConnectionResp.ReqID)
	assert.NotEqual(t, 0, optionsConnectionResp.Code)
	// wrong option with non-nil value
	optionsConnectionReq = optionsConnectionRequest{
		ReqID: 5,
		Options: []*option{
			{Option: -10000, Value: &app},
		},
	}
	resp, err = doWebSocket(ws, OptionsConnection, &optionsConnectionReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &optionsConnectionResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), optionsConnectionResp.ReqID)
	assert.NotEqual(t, 0, optionsConnectionResp.Code)
}

func TestValidateSql(t *testing.T) {
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
	// connect
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	assert.Equal(t, Connect, connResp.Action)

	var buffer bytes.Buffer
	var validateSqlResp validateSqlResponse
	// wrong message length
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(ValidateSQL))
	wstool.WriteUint16(&buffer, 1) // version
	wstool.WriteUint32(&buffer, 0) // sql length
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &validateSqlResp)
	assert.NoError(t, err)
	assert.Equal(t, 65535, validateSqlResp.Code, validateSqlResp.Message)

	// wrong sql length
	buffer.Reset()
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(ValidateSQL))
	wstool.WriteUint16(&buffer, 1)   // version
	wstool.WriteUint32(&buffer, 100) // sql length
	buffer.WriteString("wrong sql length")
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &validateSqlResp)
	assert.NoError(t, err)
	assert.Equal(t, 65535, validateSqlResp.Code, validateSqlResp.Message)

	// wrong version
	buffer.Reset()
	sql := "select 1"
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(ValidateSQL))
	wstool.WriteUint16(&buffer, 100)              // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &validateSqlResp)
	assert.NoError(t, err)
	assert.Equal(t, 65535, validateSqlResp.Code, validateSqlResp.Message)

	// wrong sql
	buffer.Reset()
	sql = "wrong sql"
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(ValidateSQL))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &validateSqlResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, validateSqlResp.Code, validateSqlResp.Message)
	assert.NotEqual(t, int64(0), validateSqlResp.ResultCode)

	// valid sql
	buffer.Reset()
	sql = "select 1"
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(ValidateSQL))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &validateSqlResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, validateSqlResp.Code, validateSqlResp.Message)
	assert.Equal(t, int64(0), validateSqlResp.ResultCode)
	assert.Equal(t, "validate_sql", validateSqlResp.Action)
}

func TestCheckServerStatus(t *testing.T) {
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
	reqID := uint64(generator.GetReqID())
	localhost := "localhost"
	req := checkServerStatusRequest{
		ReqID: reqID,
		FQDN:  &localhost,
		Port:  6030,
	}
	resp, err := doWebSocket(ws, CheckServerStatus, &req)
	t.Log(string(resp))
	assert.NoError(t, err)
	var checkServerStatusResp checkServerStatusResponse
	err = json.Unmarshal(resp, &checkServerStatusResp)
	assert.NoError(t, err)
	assert.Equal(t, reqID, checkServerStatusResp.ReqID)
	assert.Equal(t, 0, checkServerStatusResp.Code, checkServerStatusResp.Message)
	assert.Equal(t, int32(2), checkServerStatusResp.Status)
	assert.Equal(t, "", checkServerStatusResp.Details)
	reqID = uint64(generator.GetReqID())
	req = checkServerStatusRequest{
		ReqID: reqID,
		FQDN:  nil,
		Port:  0,
	}
	resp, err = doWebSocket(ws, CheckServerStatus, &req)
	t.Log(string(resp))
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &checkServerStatusResp)
	assert.NoError(t, err)
	assert.Equal(t, reqID, checkServerStatusResp.ReqID)
	assert.Equal(t, 0, checkServerStatusResp.Code, checkServerStatusResp.Message)
	assert.Equal(t, int32(2), checkServerStatusResp.Status)
	assert.Equal(t, "", checkServerStatusResp.Details)
}

func TestGetConnectionInfo(t *testing.T) {
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
	// connect
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	assert.Equal(t, Connect, connResp.Action)
	// get connection info
	getConnectionInfoReq := getConnectionInfoRequest{
		ReqID: 2,
		Info:  common.TSDB_CONNECTION_INFO_USER,
	}
	resp, err = doWebSocket(ws, GetConnectionInfo, &getConnectionInfoReq)
	assert.NoError(t, err)
	var getConnectionInfoResp getConnectionInfoResponse
	err = json.Unmarshal(resp, &getConnectionInfoResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), getConnectionInfoResp.ReqID)
	assert.Equal(t, 0, getConnectionInfoResp.Code, getConnectionInfoResp.Message)
	assert.Equal(t, "root", getConnectionInfoResp.Info)

	// get invalid connection info
	getConnectionInfoReq = getConnectionInfoRequest{
		ReqID: 3,
		Info:  -10000,
	}
	resp, err = doWebSocket(ws, GetConnectionInfo, &getConnectionInfoReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &getConnectionInfoResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), getConnectionInfoResp.ReqID)
	assert.NotEqual(t, 0, getConnectionInfoResp.Code)
}
