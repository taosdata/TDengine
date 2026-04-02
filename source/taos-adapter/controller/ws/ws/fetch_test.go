package ws

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func TestNumFields(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	db := "test_ws_num_fields"
	code, message := doRestful(fmt.Sprintf("drop database if exists %s", db), db)
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create database if not exists %s", db), db)
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated(db))
	code, message = doRestful(fmt.Sprintf("create stable if not exists %s.meters (ts timestamp,current float,voltage int,phase float) tags (groupid int,location varchar(24))", db), db)
	assert.Equal(t, 0, code, message)
	code, message = doRestful("INSERT INTO d1 USING meters TAGS (1, 'location1') VALUES (now, 10.2, 219, 0.31) "+
		"d2 USING meters TAGS (2, 'location2') VALUES (now, 10.3, 220, 0.32)", db)
	assert.Equal(t, 0, code, message)

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

	// query
	queryReq := queryRequest{ReqID: 2, Sql: "select * from meters"}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	var queryResp queryResponse
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), queryResp.ReqID)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// num fields
	numFieldsReq := numFieldsRequest{ReqID: 3, ResultID: queryResp.ID}
	resp, err = doWebSocket(ws, WSNumFields, &numFieldsReq)
	assert.NoError(t, err)
	var numFieldsResp numFieldsResponse
	err = json.Unmarshal(resp, &numFieldsResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), numFieldsResp.ReqID)
	assert.Equal(t, 0, numFieldsResp.Code, numFieldsResp.Message)
	assert.Equal(t, 6, numFieldsResp.NumFields)
}
