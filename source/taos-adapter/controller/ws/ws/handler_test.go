package ws

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
)

func TestDropUser(t *testing.T) {
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
	defer doRestful("drop user test_ws_drop_user", "")
	code, message := doRestful("create user test_ws_drop_user pass 'pass_123'", "")
	assert.Equal(t, 0, code, message)
	// connect
	connReq := connRequest{ReqID: 1, User: "test_ws_drop_user", Password: "pass_123"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	// drop user
	code, message = doRestful("drop user test_ws_drop_user", "")
	assert.Equal(t, 0, code, message)
	time.Sleep(time.Second * 3)
	resp, err = doWebSocket(ws, wstool.ClientVersion, nil)
	assert.Error(t, err, resp)
}

func Test_WrongJsonRequest(t *testing.T) {
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
	err = ws.WriteMessage(websocket.TextMessage, []byte("{wrong json}"))
	assert.NoError(t, err)
	_, message, err := ws.ReadMessage()
	assert.NoError(t, err)
	var resp commonResp
	err = json.Unmarshal(message, &resp)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, resp.Code)
}

func Test_WrongJsonProtocol(t *testing.T) {
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
	connReq := connRequest{
		ReqID:    1,
		User:     "root",
		Password: "taosdata",
	}
	message, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	resp := commonResp{}
	err = json.Unmarshal(message, &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Code, resp.Message)
	tests := []struct {
		name        string
		action      string
		args        interface{}
		errorPrefix string
	}{
		{
			name:        "empty action",
			action:      "",
			args:        nil,
			errorPrefix: "request no action",
		},
		{
			name:        "version with wrong args",
			action:      wstool.ClientVersion,
			args:        "wrong",
			errorPrefix: "unmarshal version request error",
		},
		{
			name:        "connect with wrong args",
			action:      Connect,
			args:        "wrong",
			errorPrefix: "unmarshal connect request error",
		},
		{
			name:        "query with wrong args",
			action:      WSQuery,
			args:        "wrong",
			errorPrefix: "unmarshal query request error",
		},
		{
			name:        "fetch with wrong args",
			action:      WSFetch,
			args:        "wrong",
			errorPrefix: "unmarshal fetch request error",
		},
		{
			name:        "fetch_block with wrong args",
			action:      WSFetchBlock,
			args:        "wrong",
			errorPrefix: "unmarshal fetch block request error",
		},
		{
			name:        "free_result with wrong args",
			action:      WSFreeResult,
			args:        "wrong",
			errorPrefix: "unmarshal free result request error",
		},
		{
			name:        "num_fields with wrong args",
			action:      WSNumFields,
			args:        "wrong",
			errorPrefix: "unmarshal num fields request error",
		},
		{
			name:        "insert schemaless with wrong args",
			action:      SchemalessWrite,
			args:        "wrong",
			errorPrefix: "unmarshal schemaless insert request error",
		},
		{
			name:        "stmt init with wrong args",
			action:      STMTInit,
			args:        "wrong",
			errorPrefix: "unmarshal stmt init request error",
		},
		{
			name:        "stmt prepare with wrong args",
			action:      STMTPrepare,
			args:        "wrong",
			errorPrefix: "unmarshal stmt prepare request error",
		},
		{
			name:        "stmt set table name with wrong args",
			action:      STMTSetTableName,
			args:        "wrong",
			errorPrefix: "unmarshal stmt set table name request error",
		},
		{
			name:        "stmt set tags with wrong args",
			action:      STMTSetTags,
			args:        "wrong",
			errorPrefix: "unmarshal stmt set tags request error",
		},
		{
			name:        "stmt bind with wrong args",
			action:      STMTBind,
			args:        "wrong",
			errorPrefix: "unmarshal stmt bind request error",
		},
		{
			name:        "stmt add batch with wrong args",
			action:      STMTAddBatch,
			args:        "wrong",
			errorPrefix: "unmarshal stmt add batch request error",
		},
		{
			name:        "stmt exec with wrong args",
			action:      STMTExec,
			args:        "wrong",
			errorPrefix: "unmarshal stmt exec request error",
		},
		{
			name:        "stmt close with wrong args",
			action:      STMTClose,
			args:        "wrong",
			errorPrefix: "unmarshal stmt close request error",
		},
		{
			name:        "stmt get tag fields with wrong args",
			action:      STMTGetTagFields,
			args:        "wrong",
			errorPrefix: "unmarshal stmt get tag fields request error",
		},
		{
			name:        "stmt get col fields with wrong args",
			action:      STMTGetColFields,
			args:        "wrong",
			errorPrefix: "unmarshal stmt get col fields request error",
		},
		{
			name:        "stmt use result with wrong args",
			action:      STMTUseResult,
			args:        "wrong",
			errorPrefix: "unmarshal stmt use result request error",
		},
		{
			name:        "stmt num params with wrong args",
			action:      STMTNumParams,
			args:        "wrong",
			errorPrefix: "unmarshal stmt num params request error",
		},
		{
			name:        "stmt get param with wrong args",
			action:      STMTGetParam,
			args:        "wrong",
			errorPrefix: "unmarshal stmt get param request error",
		},
		{
			name:        "stmt2 init with wrong args",
			action:      STMT2Init,
			args:        "wrong",
			errorPrefix: "unmarshal stmt2 init request error",
		},
		{
			name:        "stmt2 prepare with wrong args",
			action:      STMT2Prepare,
			args:        "wrong",
			errorPrefix: "unmarshal stmt2 prepare request error",
		},
		{
			name:        "stmt2 exec with wrong args",
			action:      STMT2Exec,
			args:        "wrong",
			errorPrefix: "unmarshal stmt2 exec request error",
		},
		{
			name:        "stmt2 result with wrong args",
			action:      STMT2Result,
			args:        "wrong",
			errorPrefix: "unmarshal stmt2 result request error",
		},
		{
			name:        "stmt2 close with wrong args",
			action:      STMT2Close,
			args:        "wrong",
			errorPrefix: "unmarshal stmt2 close request error",
		},
		{
			name:        "get current db with wrong args",
			action:      WSGetCurrentDB,
			args:        "wrong",
			errorPrefix: "unmarshal get current db request error",
		},
		{
			name:        "get server info with wrong args",
			action:      WSGetServerInfo,
			args:        "wrong",
			errorPrefix: "unmarshal get server info request error",
		},
		{
			name:        "options connection with wrong args",
			action:      OptionsConnection,
			args:        "wrong",
			errorPrefix: "unmarshal options connection request error",
		},
		{
			name:        "check_server_status with wrong args",
			action:      CheckServerStatus,
			args:        "wrong",
			errorPrefix: "unmarshal check server status request error",
		},
		{
			name:        "get_connection_info with wrong args",
			action:      GetConnectionInfo,
			args:        "wrong",
			errorPrefix: "unmarshal get connection info request error",
		},
		{
			name:        "unknown action",
			action:      "unknown",
			args:        nil,
			errorPrefix: "unknown action",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message, err = doWebSocket(ws, tt.action, tt.args)
			assert.NoError(t, err)
			resp = commonResp{}
			err = json.Unmarshal(message, &resp)
			assert.NoError(t, err)
			assert.NotEqual(t, 0, resp.Code)
			if !strings.HasPrefix(resp.Message, tt.errorPrefix) {
				t.Errorf("expected error message to start with %s, got %s", tt.errorPrefix, resp.Message)
			}
		})
	}
}

func TestNotConnection(t *testing.T) {
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
	// json
	query := queryRequest{ReqID: 1, Sql: "select * from test"}
	message, err := doWebSocket(ws, WSQuery, &query)
	assert.NoError(t, err)
	resp := commonResp{}
	err = json.Unmarshal(message, &resp)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, resp.Code)
	assert.Equal(t, "server not connected", resp.Message)
	// binary

	sql := "select * from test"
	var buffer bytes.Buffer
	wstool.WriteUint64(&buffer, 2) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, message, err = ws.ReadMessage()
	assert.NoError(t, err)
	var queryResp queryResponse
	err = json.Unmarshal(message, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), queryResp.ReqID)
	assert.NotEqual(t, 0, queryResp.Code)
	assert.Equal(t, "server not connected", queryResp.Message)
}

func TestUnknownBinaryProtocol(t *testing.T) {
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

	connReq := connRequest{
		ReqID:    1,
		User:     "root",
		Password: "taosdata",
	}
	message, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	resp := commonResp{}
	err = json.Unmarshal(message, &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Code, resp.Message)

	sql := "select * from test"
	var buffer bytes.Buffer
	wstool.WriteUint64(&buffer, 2) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(9999))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, message, err = ws.ReadMessage()
	assert.NoError(t, err)
	var queryResp queryResponse
	err = json.Unmarshal(message, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), queryResp.ReqID)
	assert.NotEqual(t, 0, queryResp.Code)
	assert.Equal(t, "unknown", queryResp.Action)
	assert.Equal(t, "unknown binary action 9999", queryResp.Message)
}
