package ws

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/gorilla/websocket"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/monitor/recordsql"
	"github.com/taosdata/taosadapter/v3/tools/bytesutil"
	"github.com/taosdata/taosadapter/v3/tools/limiter"
	"github.com/taosdata/taosadapter/v3/tools/parseblock"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func TestWsQuery(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_query", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_query", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_query"))
	code, message = doRestful(
		"create table if not exists stb1 (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100),v16 decimal(20,4)) tags (info json)",
		"test_ws_query")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(
		`insert into t1 using stb1 tags ('{\"table\":\"t1\"}') values (now-2s,true,2,3,4,5,6,7,8,9,10,11,'中文\"binary','中文nchar','\xaabbcc','point(100 100)',4467440737095516.1230)(now-1s,false,12,13,14,15,16,17,18,19,110,111,'中文\"binary','中文nchar','\xaabbcc','point(100 100)',-5467440737095516.1230)(now,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)`,
		"test_ws_query")
	assert.Equal(t, 0, code, message)

	code, message = doRestful("create table t2 using stb1 tags('{\"table\":\"t2\"}')", "test_ws_query")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create table t3 using stb1 tags('{\"table\":\"t3\"}')", "test_ws_query")
	assert.Equal(t, 0, code, message)

	defer doRestful("drop database if exists test_ws_query", "")
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
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_query"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	assert.Equal(t, Connect, connResp.Action)

	// wrong sql
	queryReq := queryRequest{ReqID: 2, Sql: "wrong sql"}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	var queryResp queryResponse
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), queryResp.ReqID)
	assert.NotEqual(t, 0, queryResp.Code)
	assert.Equal(t, WSQuery, queryResp.Action)

	// query
	queryReq = queryRequest{ReqID: 2, Sql: "select * from stb1"}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), queryResp.ReqID)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch
	fetchReq := fetchRequest{ReqID: 3, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	var fetchResp fetchResponse
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), fetchResp.ReqID)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)
	assert.Equal(t, 3, fetchResp.Rows)

	// fetch block
	fetchBlockReq := fetchBlockRequest{ReqID: 4, ID: queryResp.ID}
	fetchBlockResp, err := doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	resultID, blockResult, err := parseblock.ParseBlock(fetchBlockResp[8:], queryResp.FieldsTypes, fetchResp.Rows, queryResp.Precision)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), resultID)
	checkBlockResult(t, blockResult)

	fetchReq = fetchRequest{ReqID: 5, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), fetchResp.ReqID)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	assert.Equal(t, true, fetchResp.Completed)

	// write block
	var buffer bytes.Buffer
	wstool.WriteUint64(&buffer, 300)                     // req id
	wstool.WriteUint64(&buffer, 400)                     // message id
	wstool.WriteUint64(&buffer, uint64(RawBlockMessage)) // action
	wstool.WriteUint32(&buffer, uint32(fetchResp.Rows))  // rows
	wstool.WriteUint16(&buffer, uint16(2))               // table name length
	buffer.WriteString("t2")                             // table name
	buffer.Write(fetchBlockResp[16:])                    // raw block
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var writeResp commonResp
	err = json.Unmarshal(resp, &writeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, writeResp.Code, writeResp.Message)

	// query
	queryReq = queryRequest{ReqID: 6, Sql: "select * from t2"}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch
	fetchReq = fetchRequest{ReqID: 7, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	// fetch block
	fetchBlockReq = fetchBlockRequest{ReqID: 8, ID: queryResp.ID}
	fetchBlockResp, err = doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	resultID, blockResult, err = parseblock.ParseBlock(fetchBlockResp[8:], queryResp.FieldsTypes, fetchResp.Rows, queryResp.Precision)
	assert.NoError(t, err)
	checkBlockResult(t, blockResult)
	assert.Equal(t, queryResp.ID, resultID)
	// fetch
	fetchReq = fetchRequest{ReqID: 9, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	assert.Equal(t, true, fetchResp.Completed)

	// write block with filed
	buffer.Reset()
	wstool.WriteUint64(&buffer, 300)                               // req id
	wstool.WriteUint64(&buffer, 400)                               // message id
	wstool.WriteUint64(&buffer, uint64(RawBlockMessageWithFields)) // action
	wstool.WriteUint32(&buffer, uint32(fetchResp.Rows))            // rows
	wstool.WriteUint16(&buffer, uint16(2))                         // table name length
	buffer.WriteString("t3")                                       // table name
	buffer.Write(fetchBlockResp[16:])                              // raw block
	fields := []byte{
		// ts
		0x74, 0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x09,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v1
		0x76, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x01,
		// padding
		0x00, 0x00,
		// bytes
		0x01, 0x00, 0x00, 0x00,

		// v2
		0x76, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x02,
		// padding
		0x00, 0x00,
		// bytes
		0x01, 0x00, 0x00, 0x00,

		// v3
		0x76, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x03,
		// padding
		0x00, 0x00,
		// bytes
		0x02, 0x00, 0x00, 0x00,

		// v4
		0x76, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x04,
		// padding
		0x00, 0x00,
		// bytes
		0x04, 0x00, 0x00, 0x00,

		// v5
		0x76, 0x35, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x05,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v6
		0x76, 0x36, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0b,
		// padding
		0x00, 0x00,
		// bytes
		0x01, 0x00, 0x00, 0x00,

		// v7
		0x76, 0x37, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0c,
		// padding
		0x00, 0x00,
		// bytes
		0x02, 0x00, 0x00, 0x00,

		// v8
		0x76, 0x38, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0d,
		// padding
		0x00, 0x00,
		// bytes
		0x04, 0x00, 0x00, 0x00,

		// v9
		0x76, 0x39, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0e,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v10
		0x76, 0x31, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x06,
		// padding
		0x00, 0x00,
		// bytes
		0x04, 0x00, 0x00, 0x00,

		// v11
		0x76, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x07,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v12
		0x76, 0x31, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x08,
		// padding
		0x00, 0x00,
		// bytes
		0x14, 0x00, 0x00, 0x00,

		// v13
		0x76, 0x31, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0a,
		// padding
		0x00, 0x00,
		// bytes
		0x14, 0x00, 0x00, 0x00,

		// v14
		0x76, 0x31, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x10,
		// padding
		0x00, 0x00,
		// bytes
		0x14, 0x00, 0x00, 0x00,

		// v15
		0x76, 0x31, 0x35, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x14,
		// padding
		0x00, 0x00,
		// bytes
		0x64, 0x00, 0x00, 0x00,

		// v16
		0x76, 0x31, 0x36, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x11,
		// padding
		0x00, 0x00,
		// bytes
		0x10, 0x00, 0x00, 0x00,

		// info
		0x69, 0x6e, 0x66, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0f,
		// padding
		0x00, 0x00,
		// bytes
		0x00, 0x10, 0x00, 0x00,
	}
	buffer.Write(fields)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &writeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, writeResp.Code, writeResp.Message)

	// query
	queryReq = queryRequest{ReqID: 10, Sql: "select * from t3"}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch
	fetchReq = fetchRequest{ReqID: 11, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	// fetch block
	fetchBlockReq = fetchBlockRequest{ReqID: 12, ID: queryResp.ID}
	fetchBlockResp, err = doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	resultID, blockResult, err = parseblock.ParseBlock(fetchBlockResp[8:], queryResp.FieldsTypes, fetchResp.Rows, queryResp.Precision)
	assert.NoError(t, err)
	assert.Equal(t, queryResp.ID, resultID)
	checkBlockResult(t, blockResult)
	// fetch
	fetchReq = fetchRequest{ReqID: 13, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	assert.Equal(t, true, fetchResp.Completed)

	// insert
	queryReq = queryRequest{ReqID: 14, Sql: `insert into t4 using stb1 tags ('{\"table\":\"t4\"}') values (now-2s,true,2,3,4,5,6,7,8,9,10,11,'中文\"binary','中文nchar','\xaabbcc','point(100 100)',4467440737095516.1230)(now-1s,false,12,13,14,15,16,17,18,19,110,111,'中文\"binary','中文nchar','\xaabbcc','point(100 100)',-5467440737095516.1230)(now,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)`}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(14), queryResp.ReqID)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)
	assert.Equal(t, WSQuery, queryResp.Action)
	assert.Equal(t, 3, queryResp.AffectedRows)
}

type FetchRawBlockResponse struct {
	Flag     uint64
	Version  uint16
	Time     uint64
	ReqID    uint64
	Code     uint32
	Message  string
	ResultID uint64
	Finished bool
	RawBlock []byte
}

func parseFetchRawBlock(message []byte) *FetchRawBlockResponse {
	var resp = &FetchRawBlockResponse{}
	resp.Flag = binary.LittleEndian.Uint64(message)
	resp.Version = binary.LittleEndian.Uint16(message[16:])
	resp.Time = binary.LittleEndian.Uint64(message[18:])
	resp.ReqID = binary.LittleEndian.Uint64(message[26:])
	resp.Code = binary.LittleEndian.Uint32(message[34:])
	msgLen := int(binary.LittleEndian.Uint32(message[38:]))
	resp.Message = string(message[42 : 42+msgLen])
	if resp.Code != 0 {
		return resp
	}
	resp.ResultID = binary.LittleEndian.Uint64(message[42+msgLen:])
	resp.Finished = message[50+msgLen] == 1
	if resp.Finished {
		return resp
	}
	blockLength := binary.LittleEndian.Uint32(message[51+msgLen:])
	resp.RawBlock = message[55+msgLen : 55+msgLen+int(blockLength)]
	return resp
}

func ReadBlockSimple(block unsafe.Pointer, precision int) ([][]driver.Value, error) {
	blockSize := parser.RawBlockGetNumOfRows(block)
	colCount := parser.RawBlockGetNumOfCols(block)
	colInfo := make([]parser.RawBlockColInfo, colCount)
	parser.RawBlockGetColInfo(block, colInfo)
	colTypes := make([]uint8, colCount)
	for i := int32(0); i < colCount; i++ {
		colTypes[i] = uint8(colInfo[i].ColType)
	}
	return parser.ReadBlock(block, int(blockSize), colTypes, precision)
}

func TestWsBinaryQuery(t *testing.T) {
	dbName := "test_ws_binary_query"
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful(fmt.Sprintf("drop database if exists %s", dbName), "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create database if not exists %s", dbName), "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated(dbName))
	code, message = doRestful(
		"create table if not exists stb1 (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100),v16 decimal(20,4)) tags (info json)",
		dbName)
	assert.Equal(t, 0, code, message)
	code, message = doRestful(
		`insert into t1 using stb1 tags ('{\"table\":\"t1\"}') values (now-2s,true,2,3,4,5,6,7,8,9,10,11,'中文\"binary','中文nchar','\xaabbcc','point(100 100)',4467440737095516.123)(now-1s,false,12,13,14,15,16,17,18,19,110,111,'中文\"binary','中文nchar','\xaabbcc','point(100 100)',-5467440737095516.123)(now,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)`,
		dbName)
	assert.Equal(t, 0, code, message)

	code, message = doRestful("create table t2 using stb1 tags('{\"table\":\"t2\"}')", dbName)
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create table t3 using stb1 tags('{\"table\":\"t3\"}')", dbName)
	assert.Equal(t, 0, code, message)

	defer doRestful(fmt.Sprintf("drop database if exists %s", dbName), "")
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
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: dbName}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// query
	sql := "select * from stb1"
	var buffer bytes.Buffer
	wstool.WriteUint64(&buffer, 2) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var queryResp queryResponse
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), queryResp.ReqID)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch raw block
	buffer.Reset()
	wstool.WriteUint64(&buffer, 3)            // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1) // version
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp := parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(3), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, uint64(1), fetchRawBlockResp.ResultID)
	assert.Equal(t, false, fetchRawBlockResp.Finished)
	rows := parser.RawBlockGetNumOfRows(unsafe.Pointer(&fetchRawBlockResp.RawBlock[0]))
	assert.Equal(t, int32(3), rows)
	blockResult, err := ReadBlockSimple(unsafe.Pointer(&fetchRawBlockResp.RawBlock[0]), queryResp.Precision)
	assert.NoError(t, err)
	checkBlockResult(t, blockResult)
	rawBlock := fetchRawBlockResp.RawBlock

	buffer.Reset()
	wstool.WriteUint64(&buffer, 5)            // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1) // version
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(5), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, uint64(1), fetchRawBlockResp.ResultID)
	assert.Equal(t, true, fetchRawBlockResp.Finished)

	// write block

	buffer.Reset()
	wstool.WriteUint64(&buffer, 300)                     // req id
	wstool.WriteUint64(&buffer, 400)                     // message id
	wstool.WriteUint64(&buffer, uint64(RawBlockMessage)) // action
	wstool.WriteUint32(&buffer, uint32(3))               // rows
	wstool.WriteUint16(&buffer, uint16(2))               // table name length
	buffer.WriteString("t2")                             // table name
	buffer.Write(rawBlock)                               // raw block
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var writeResp commonResp
	err = json.Unmarshal(resp, &writeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, writeResp.Code, writeResp.Message)

	// query
	sql = "select * from t2"
	buffer.Reset()
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch raw block
	buffer.Reset()
	wstool.WriteUint64(&buffer, 7)            // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1) // version
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(7), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, false, fetchRawBlockResp.Finished)
	blockResult, err = ReadBlockSimple(unsafe.Pointer(&fetchRawBlockResp.RawBlock[0]), queryResp.Precision)
	assert.NoError(t, err)
	checkBlockResult(t, blockResult)
	rawBlock = fetchRawBlockResp.RawBlock

	buffer.Reset()
	wstool.WriteUint64(&buffer, 9)            // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1) // version
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(9), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, true, fetchRawBlockResp.Finished)

	// write block with filed
	buffer.Reset()
	wstool.WriteUint64(&buffer, 300)                               // req id
	wstool.WriteUint64(&buffer, 400)                               // message id
	wstool.WriteUint64(&buffer, uint64(RawBlockMessageWithFields)) // action
	wstool.WriteUint32(&buffer, uint32(3))                         // rows
	wstool.WriteUint16(&buffer, uint16(2))                         // table name length
	buffer.WriteString("t3")                                       // table name
	buffer.Write(rawBlock)                                         // raw block
	fields := []byte{
		// ts
		0x74, 0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x09,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v1
		0x76, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x01,
		// padding
		0x00, 0x00,
		// bytes
		0x01, 0x00, 0x00, 0x00,

		// v2
		0x76, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x02,
		// padding
		0x00, 0x00,
		// bytes
		0x01, 0x00, 0x00, 0x00,

		// v3
		0x76, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x03,
		// padding
		0x00, 0x00,
		// bytes
		0x02, 0x00, 0x00, 0x00,

		// v4
		0x76, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x04,
		// padding
		0x00, 0x00,
		// bytes
		0x04, 0x00, 0x00, 0x00,

		// v5
		0x76, 0x35, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x05,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v6
		0x76, 0x36, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0b,
		// padding
		0x00, 0x00,
		// bytes
		0x01, 0x00, 0x00, 0x00,

		// v7
		0x76, 0x37, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0c,
		// padding
		0x00, 0x00,
		// bytes
		0x02, 0x00, 0x00, 0x00,

		// v8
		0x76, 0x38, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0d,
		// padding
		0x00, 0x00,
		// bytes
		0x04, 0x00, 0x00, 0x00,

		// v9
		0x76, 0x39, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0e,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v10
		0x76, 0x31, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x06,
		// padding
		0x00, 0x00,
		// bytes
		0x04, 0x00, 0x00, 0x00,

		// v11
		0x76, 0x31, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x07,
		// padding
		0x00, 0x00,
		// bytes
		0x08, 0x00, 0x00, 0x00,

		// v12
		0x76, 0x31, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x08,
		// padding
		0x00, 0x00,
		// bytes
		0x14, 0x00, 0x00, 0x00,

		// v13
		0x76, 0x31, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0a,
		// padding
		0x00, 0x00,
		// bytes
		0x14, 0x00, 0x00, 0x00,

		// v14
		0x76, 0x31, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x10,
		// padding
		0x00, 0x00,
		// bytes
		0x14, 0x00, 0x00, 0x00,

		// v15
		0x76, 0x31, 0x35, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x14,
		// padding
		0x00, 0x00,
		// bytes
		0x64, 0x00, 0x00, 0x00,

		// v16
		0x76, 0x31, 0x36, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x11,
		// padding
		0x00, 0x00,
		// bytes
		0x10, 0x00, 0x00, 0x00,

		// info
		0x69, 0x6e, 0x66, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		// type
		0x0f,
		// padding
		0x00, 0x00,
		// bytes
		0x00, 0x10, 0x00, 0x00,
	}
	buffer.Write(fields)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &writeResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, writeResp.Code, writeResp.Message)

	// query
	sql = "select * from t3"
	buffer.Reset()
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch raw block
	buffer.Reset()
	wstool.WriteUint64(&buffer, 11)           // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1) // version
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(11), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, false, fetchRawBlockResp.Finished)
	blockResult, err = ReadBlockSimple(unsafe.Pointer(&fetchRawBlockResp.RawBlock[0]), queryResp.Precision)
	assert.NoError(t, err)
	checkBlockResult(t, blockResult)

	buffer.Reset()
	wstool.WriteUint64(&buffer, 13)           // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1) // version
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(13), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, true, fetchRawBlockResp.Finished)

	// wrong message length
	buffer.Reset()
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1) // version
	wstool.WriteUint32(&buffer, 0) // sql length
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 65535, queryResp.Code, queryResp.Message)

	// wrong sql length
	buffer.Reset()
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)   // version
	wstool.WriteUint32(&buffer, 100) // sql length
	buffer.WriteString("wrong sql length")
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 65535, queryResp.Code, queryResp.Message)

	// wrong version
	buffer.Reset()
	sql = "select 1"
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 100)              // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 65535, queryResp.Code, queryResp.Message)

	// wrong sql
	buffer.Reset()
	sql = "wrong sql"
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, queryResp.Code, queryResp.Message)

	// insert sql
	buffer.Reset()
	sql = "create table t4 using stb1 tags('{\"table\":\"t4\"}')"
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)
	assert.Equal(t, true, queryResp.IsUpdate)

	// wrong fetch
	buffer.Reset()
	sql = "select 1"
	wstool.WriteUint64(&buffer, 6) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// wrong fetch raw block length
	buffer.Reset()
	wstool.WriteUint64(&buffer, 700)          // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(700), fetchRawBlockResp.ReqID)
	assert.NotEqual(t, uint32(0), fetchRawBlockResp.Code)

	// wrong fetch raw block version
	buffer.Reset()
	wstool.WriteUint64(&buffer, 800)          // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 100)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(800), fetchRawBlockResp.ReqID)
	assert.NotEqual(t, uint32(0), fetchRawBlockResp.Code)
	time.Sleep(time.Second)

	// wrong fetch raw block result
	buffer.Reset()
	wstool.WriteUint64(&buffer, 900)              // req id
	wstool.WriteUint64(&buffer, queryResp.ID+100) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(900), fetchRawBlockResp.ReqID)
	assert.NotEqual(t, uint32(0), fetchRawBlockResp.Code)

	// fetch freed raw block
	buffer.Reset()
	wstool.WriteUint64(&buffer, 600)          // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(600), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, int32(1), parser.RawBlockGetNumOfRows(unsafe.Pointer(&fetchRawBlockResp.RawBlock[0])))

	buffer.Reset()
	wstool.WriteUint64(&buffer, 700)          // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(700), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, true, fetchRawBlockResp.Finished)

	buffer.Reset()
	wstool.WriteUint64(&buffer, 400)          // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(400), fetchRawBlockResp.ReqID)
	assert.NotEqual(t, uint32(0), fetchRawBlockResp.Code)
	time.Sleep(time.Second)
}

func checkBlockResult(t *testing.T, blockResult [][]driver.Value) {
	assert.Equal(t, 3, len(blockResult))
	assert.Equal(t, true, blockResult[0][1])
	assert.Equal(t, int8(2), blockResult[0][2])
	assert.Equal(t, int16(3), blockResult[0][3])
	assert.Equal(t, int32(4), blockResult[0][4])
	assert.Equal(t, int64(5), blockResult[0][5])
	assert.Equal(t, uint8(6), blockResult[0][6])
	assert.Equal(t, uint16(7), blockResult[0][7])
	assert.Equal(t, uint32(8), blockResult[0][8])
	assert.Equal(t, uint64(9), blockResult[0][9])
	assert.Equal(t, float32(10), blockResult[0][10])
	assert.Equal(t, float64(11), blockResult[0][11])
	assert.Equal(t, "中文\"binary", blockResult[0][12])
	assert.Equal(t, "中文nchar", blockResult[0][13])
	assert.Equal(t, []byte{0xaa, 0xbb, 0xcc}, blockResult[0][14])
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, blockResult[0][15])
	assert.Equal(t, "4467440737095516.1230", blockResult[0][16])
	assert.Equal(t, false, blockResult[1][1])
	assert.Equal(t, int8(12), blockResult[1][2])
	assert.Equal(t, int16(13), blockResult[1][3])
	assert.Equal(t, int32(14), blockResult[1][4])
	assert.Equal(t, int64(15), blockResult[1][5])
	assert.Equal(t, uint8(16), blockResult[1][6])
	assert.Equal(t, uint16(17), blockResult[1][7])
	assert.Equal(t, uint32(18), blockResult[1][8])
	assert.Equal(t, uint64(19), blockResult[1][9])
	assert.Equal(t, float32(110), blockResult[1][10])
	assert.Equal(t, float64(111), blockResult[1][11])
	assert.Equal(t, "中文\"binary", blockResult[1][12])
	assert.Equal(t, "中文nchar", blockResult[1][13])
	assert.Equal(t, []byte{0xaa, 0xbb, 0xcc}, blockResult[1][14])
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, blockResult[1][15])
	assert.Equal(t, "-5467440737095516.1230", blockResult[1][16])
	assert.Equal(t, nil, blockResult[2][1])
	assert.Equal(t, nil, blockResult[2][2])
	assert.Equal(t, nil, blockResult[2][3])
	assert.Equal(t, nil, blockResult[2][4])
	assert.Equal(t, nil, blockResult[2][5])
	assert.Equal(t, nil, blockResult[2][6])
	assert.Equal(t, nil, blockResult[2][7])
	assert.Equal(t, nil, blockResult[2][8])
	assert.Equal(t, nil, blockResult[2][9])
	assert.Equal(t, nil, blockResult[2][10])
	assert.Equal(t, nil, blockResult[2][11])
	assert.Equal(t, nil, blockResult[2][12])
	assert.Equal(t, nil, blockResult[2][13])
	assert.Equal(t, nil, blockResult[2][14])
	assert.Equal(t, nil, blockResult[2][15])
	assert.Equal(t, nil, blockResult[2][16])
}

func TestQueryRecordSql(t *testing.T) {
	tmpDir := t.TempDir()
	file := "test.csv"
	output := filepath.Join(tmpDir, file)
	sql := "select 1"
	sql2 := "select 2"
	f, err := os.Create(output)
	require.NoError(t, err)
	now := time.Now()
	var port string
	var host string
	const appName = "test_record_app"
	defer func() {
		err = f.Close()
		require.NoError(t, err)
		var bs []byte
		require.Eventually(t, func() bool {
			bs, err = os.ReadFile(output)
			return err == nil && len(bs) > 0
		}, 5*time.Second, 500*time.Millisecond)
		t.Log(string(bs))
		csvReader := csv.NewReader(bytes.NewReader(bs))
		records, err := csvReader.ReadAll()
		assert.NoError(t, err)
		require.Equal(t, 2, len(records), records)
		var checkRecord = func(record []string, qid string, expectSql string) {
			t.Log(record)
			assert.Equal(t, expectSql, record[recordsql.SQLIndex])
			assert.Equal(t, host, record[recordsql.IPIndex])
			assert.Equal(t, "root", record[recordsql.UserIndex])
			assert.Equal(t, recordsql.WSType.String(), record[recordsql.ConnTypeIndex])
			assert.Equal(t, qid, record[recordsql.QIDIndex])
			receiveTime, err := time.Parse(recordsql.ResultTimeFormat, record[recordsql.ReceiveTimeIndex])
			assert.NoError(t, err)
			freeTime, err := time.Parse(recordsql.ResultTimeFormat, record[recordsql.FreeTimeIndex])
			assert.NoError(t, err)
			assert.True(t, freeTime.After(receiveTime))
			assert.True(t, receiveTime.After(now))
			assert.Equal(t, "0", record[recordsql.GetConnDurationIndex])
			queryDuration, err := strconv.Atoi(record[recordsql.QueryDurationIndex])
			assert.NoError(t, err)
			assert.Greater(t, queryDuration, 0)
			fetchDuration, err := strconv.Atoi(record[recordsql.FetchDurationIndex])
			assert.NoError(t, err)
			assert.Greater(t, fetchDuration, 0)
			assert.Equal(t, port, record[recordsql.SourcePortIndex])
			assert.Equal(t, appName, record[recordsql.AppNameIndex])
		}
		checkRecord(records[0], "0x2", sql)
		checkRecord(records[1], "0x223", sql2)
	}()
	start := time.Now().Format(recordsql.InputTimeFormat)
	end := time.Now().Add(time.Second * 5).Format(recordsql.InputTimeFormat)
	err = recordsql.StartRecordWithTestWriter(recordsql.RecordTypeSQL, start, end, "", f)
	require.NoError(t, err)
	defer func() {
		info := recordsql.StopRecordSqlMission()
		require.NotNil(t, info)
	}()
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
	host, port, err = net.SplitHostPort(strings.TrimSpace(ws.LocalAddr().String()))
	assert.NoError(t, err)
	t.Log(host, port)
	// connect
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", App: appName}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	assert.Equal(t, Connect, connResp.Action)

	// query
	queryReq := queryRequest{ReqID: 2, Sql: sql}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	var queryResp queryResponse
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), queryResp.ReqID)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch
	fetchReq := fetchRequest{ReqID: 3, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	var fetchResp fetchResponse
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), fetchResp.ReqID)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)
	assert.Equal(t, 1, fetchResp.Rows)

	// fetch block
	fetchBlockReq := fetchBlockRequest{ReqID: 4, ID: queryResp.ID}
	fetchBlockResp, err := doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	resultID, _, err := parseblock.ParseBlock(fetchBlockResp[8:], queryResp.FieldsTypes, fetchResp.Rows, queryResp.Precision)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), resultID)

	// fetch

	fetchReq = fetchRequest{ReqID: 5, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), fetchResp.ReqID)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)
	assert.Equal(t, true, fetchResp.Completed)

	// binary

	// query

	var buffer bytes.Buffer
	wstool.WriteUint64(&buffer, 0x223) // req id
	wstool.WriteUint64(&buffer, 0)     // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                 // version
	wstool.WriteUint32(&buffer, uint32(len(sql2))) // sql length
	buffer.WriteString(sql2)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0x223), queryResp.ReqID)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch raw block
	buffer.Reset()
	wstool.WriteUint64(&buffer, 3)            // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1) // version
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp := parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(3), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, uint64(2), fetchRawBlockResp.ResultID)
	assert.Equal(t, false, fetchRawBlockResp.Finished)
	rows := parser.RawBlockGetNumOfRows(unsafe.Pointer(&fetchRawBlockResp.RawBlock[0]))
	assert.Equal(t, int32(1), rows)
	_, err = ReadBlockSimple(unsafe.Pointer(&fetchRawBlockResp.RawBlock[0]), queryResp.Precision)
	assert.NoError(t, err)

	// fetch raw block
	buffer.Reset()
	wstool.WriteUint64(&buffer, 5)            // req id
	wstool.WriteUint64(&buffer, queryResp.ID) // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1) // version
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	fetchRawBlockResp = parseFetchRawBlock(resp)
	assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
	assert.Equal(t, uint64(5), fetchRawBlockResp.ReqID)
	assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	assert.Equal(t, uint64(2), fetchRawBlockResp.ResultID)
	assert.Equal(t, true, fetchRawBlockResp.Finished)

}

func TestLimitQuery(t *testing.T) {
	config.Conf.Request.QueryLimitEnable = true
	config.Conf.Request.Default.QueryLimit = 1
	config.Conf.Request.Default.QueryMaxWait = 1
	defer func() {
		config.Conf.Request.QueryLimitEnable = false
		limiter.GlobalLimiterMap.Clear()
	}()
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

	var queryFunctions = []func(t *testing.T, ws *websocket.Conn, reqID uint64, sql string) queryResponse{
		jsonQuery,
		binaryQuery,
	}
	var finishQueryFunctions = []func(t *testing.T, ws *websocket.Conn, reqID uint64, id uint64) error{
		freeResult,
		fetchAllResult,
		fetchAllResultWithBinary,
	}
	reqID := 0
	for _, queryFunc := range queryFunctions {
		for _, finishQueryFunc := range finishQueryFunctions {
			// first query should be ok
			reqID++
			queryResp := jsonQuery(t, ws, 2, "select 2")
			assert.Equal(t, 0, queryResp.Code, queryResp.Message)
			needFreeID := queryResp.ID
			// second query will be limited
			reqID++
			queryResp = queryFunc(t, ws, uint64(reqID), "select 2")
			assert.Equal(t, httperror.RequestOverLimit, queryResp.Code, queryResp.Message)
			// query with unlimited sql
			reqID++
			queryResp = queryFunc(t, ws, uint64(reqID), "select 1")
			assert.Equal(t, 0, queryResp.Code, queryResp.Message)
			reqID++
			err = finishQueryFunc(t, ws, uint64(reqID), queryResp.ID)
			require.NoError(t, err)
			// query with unlimited sql regex
			reqID++
			queryResp = queryFunc(t, ws, uint64(reqID), "select * from information_schema.ins_databases")
			assert.Equal(t, 0, queryResp.Code, queryResp.Message)
			reqID++
			err = finishQueryFunc(t, ws, uint64(reqID), queryResp.ID)
			require.NoError(t, err)
			// free the first query
			reqID++
			err = finishQueryFunc(t, ws, uint64(reqID), needFreeID)
			require.NoError(t, err)
			// query again should be ok now
			reqID++
			queryResp = queryFunc(t, ws, uint64(reqID), "select 2")
			assert.Equal(t, 0, queryResp.Code, queryResp.Message)
			reqID++
			err = finishQueryFunc(t, ws, uint64(reqID), queryResp.ID)
			require.NoError(t, err)
		}
	}
}

func TestRejectSql(t *testing.T) {
	v := viper.New()
	v.Set("rejectQuerySqlRegex", []string{"(?i)^drop\\s+database\\s+.*", "(?i)^drop\\s+table\\s+.*", "(?i)^alter\\s+table\\s+.*", `(?i)^select\s+.*from\s+testdb.*`})
	err := config.Conf.Reject.SetValue(v)
	require.NoError(t, err)
	defer func() {
		err = config.Conf.Reject.SetValue(viper.New())
		require.NoError(t, err)
	}()
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

	queryResp := binaryQuery(t, ws, 0x111, "DROP DATABASE testdb")
	assert.Equal(t, httperror.SQLForbidden, queryResp.Code, queryResp.Message)
	queryResp = binaryQuery(t, ws, 0x112, "DROP taBle testdb.stb")
	assert.Equal(t, httperror.SQLForbidden, queryResp.Code, queryResp.Message)
	queryResp = binaryQuery(t, ws, 0x113, "ALTER TABLE testdb.stb ADD COLUMN c1 INT")
	assert.Equal(t, httperror.SQLForbidden, queryResp.Code, queryResp.Message)

	queryResp = binaryQuery(t, ws, 0x114, "select * from information_schema.ins_databases")
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)
	err = freeResult(t, ws, 0x115, queryResp.ID)
	require.NoError(t, err)

	queryResp = binaryQuery(t, ws, 0x117, " select * from testdb.testtb ")
	assert.Equal(t, httperror.SQLForbidden, queryResp.Code, queryResp.Message)
}

func binaryQuery(t *testing.T, ws *websocket.Conn, reqID uint64, sql string) queryResponse {
	var buffer bytes.Buffer
	wstool.WriteUint64(&buffer, reqID) // req id
	wstool.WriteUint64(&buffer, 0)     // message id
	wstool.WriteUint64(&buffer, uint64(BinaryQueryMessage))
	wstool.WriteUint16(&buffer, 1)                // version
	wstool.WriteUint32(&buffer, uint32(len(sql))) // sql length
	buffer.WriteString(sql)
	err := ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err := ws.ReadMessage()
	assert.NoError(t, err)
	var queryResp queryResponse
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, reqID, queryResp.ReqID)
	return queryResp
}

func jsonQuery(t *testing.T, ws *websocket.Conn, reqID uint64, sql string) queryResponse {
	queryReq := queryRequest{ReqID: reqID, Sql: sql}
	resp, err := doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	var queryResp queryResponse
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, reqID, queryResp.ReqID)
	return queryResp
}

func freeResult(t *testing.T, ws *websocket.Conn, reqID uint64, id uint64) error {
	freeReq := freeResultRequest{
		ReqID: reqID,
		ID:    id,
	}
	err := doWebSocketWithoutResp(ws, WSFreeResult, &freeReq)
	return err
}

func fetchAllResult(t *testing.T, ws *websocket.Conn, reqID uint64, id uint64) error {
	var fetchResp fetchResponse
	for !fetchResp.Completed {
		fetchReq := fetchRequest{ReqID: reqID, ID: id}
		resp, err := doWebSocket(ws, WSFetch, &fetchReq)
		if err != nil {
			return err
		}
		err = json.Unmarshal(resp, &fetchResp)
		if err != nil {
			return err
		}
		assert.Equal(t, reqID, fetchResp.ReqID)
		assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)
	}
	return nil
}

func fetchAllResultWithBinary(t *testing.T, ws *websocket.Conn, reqID uint64, id uint64) error {
	buffer := bytes.Buffer{}
	wstool.WriteUint64(&buffer, reqID) // req id
	wstool.WriteUint64(&buffer, id)    // message id
	wstool.WriteUint64(&buffer, uint64(FetchRawBlockMessage))
	wstool.WriteUint16(&buffer, 1)
	msg := buffer.Bytes()
	fetchRawBlockResp := &FetchRawBlockResponse{}
	for !fetchRawBlockResp.Finished {
		err := ws.WriteMessage(websocket.BinaryMessage, msg)
		if err != nil {
			return err
		}
		_, resp, err := ws.ReadMessage()
		if err != nil {
			return err
		}
		assert.NoError(t, err)
		fetchRawBlockResp = parseFetchRawBlock(resp)
		assert.Equal(t, uint64(0xffffffffffffffff), fetchRawBlockResp.Flag)
		assert.Equal(t, reqID, fetchRawBlockResp.ReqID)
		assert.Equal(t, uint32(0), fetchRawBlockResp.Code, fetchRawBlockResp.Message)
	}
	return nil
}

type stringHeader struct {
	data unsafe.Pointer
	len  int
}

func TestUnsafeString(t *testing.T) {
	bs := []byte(" hello world ")
	bsDataP := unsafe.Pointer(&bs[0])
	str := bytesutil.ToUnsafeString(bs)
	strDataP := (*stringHeader)(unsafe.Pointer(&str)).data
	trimStr := strings.TrimSpace(str)
	trimStrDataP := (*stringHeader)(unsafe.Pointer(&trimStr)).data
	assert.Equal(t, bsDataP, strDataP)
	assert.Equal(t, unsafe.Add(strDataP, 1), trimStrDataP)
	// force gc to check trimStr is valid
	for i := 0; i < 1000; i++ {
		runtime.GC()
	}
	assert.Equal(t, "hello world", trimStr)
}
