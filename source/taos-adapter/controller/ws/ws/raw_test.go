package ws

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func TestWSTMQWriteRaw(t *testing.T) {
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

	data := []byte{
		0x64, 0x01, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x58, 0x01, 0x00, 0x00, 0x04, 0x73, 0x74, 0x62,
		0x00, 0xd5, 0xf0, 0xed, 0x8a, 0xe0, 0x23, 0xf3, 0x45, 0x00, 0x1c, 0x02, 0x09, 0x01, 0x10, 0x02,
		0x03, 0x74, 0x73, 0x00, 0x01, 0x01, 0x02, 0x04, 0x03, 0x63, 0x31, 0x00, 0x02, 0x01, 0x02, 0x06,
		0x03, 0x63, 0x32, 0x00, 0x03, 0x01, 0x04, 0x08, 0x03, 0x63, 0x33, 0x00, 0x04, 0x01, 0x08, 0x0a,
		0x03, 0x63, 0x34, 0x00, 0x05, 0x01, 0x10, 0x0c, 0x03, 0x63, 0x35, 0x00, 0x0b, 0x01, 0x02, 0x0e,
		0x03, 0x63, 0x36, 0x00, 0x0c, 0x01, 0x04, 0x10, 0x03, 0x63, 0x37, 0x00, 0x0d, 0x01, 0x08, 0x12,
		0x03, 0x63, 0x38, 0x00, 0x0e, 0x01, 0x10, 0x14, 0x03, 0x63, 0x39, 0x00, 0x06, 0x01, 0x08, 0x16,
		0x04, 0x63, 0x31, 0x30, 0x00, 0x07, 0x01, 0x10, 0x18, 0x04, 0x63, 0x31, 0x31, 0x00, 0x08, 0x01,
		0x2c, 0x1a, 0x04, 0x63, 0x31, 0x32, 0x00, 0x0a, 0x01, 0xa4, 0x01, 0x1c, 0x04, 0x63, 0x31, 0x33,
		0x00, 0x1c, 0x02, 0x09, 0x02, 0x10, 0x1e, 0x04, 0x74, 0x74, 0x73, 0x00, 0x01, 0x00, 0x02, 0x20,
		0x04, 0x74, 0x63, 0x31, 0x00, 0x02, 0x00, 0x02, 0x22, 0x04, 0x74, 0x63, 0x32, 0x00, 0x03, 0x00,
		0x04, 0x24, 0x04, 0x74, 0x63, 0x33, 0x00, 0x04, 0x00, 0x08, 0x26, 0x04, 0x74, 0x63, 0x34, 0x00,
		0x05, 0x00, 0x10, 0x28, 0x04, 0x74, 0x63, 0x35, 0x00, 0x0b, 0x00, 0x02, 0x2a, 0x04, 0x74, 0x63,
		0x36, 0x00, 0x0c, 0x00, 0x04, 0x2c, 0x04, 0x74, 0x63, 0x37, 0x00, 0x0d, 0x00, 0x08, 0x2e, 0x04,
		0x74, 0x63, 0x38, 0x00, 0x0e, 0x00, 0x10, 0x30, 0x04, 0x74, 0x63, 0x39, 0x00, 0x06, 0x00, 0x08,
		0x32, 0x05, 0x74, 0x63, 0x31, 0x30, 0x00, 0x07, 0x00, 0x10, 0x34, 0x05, 0x74, 0x63, 0x31, 0x31,
		0x00, 0x08, 0x00, 0x2c, 0x36, 0x05, 0x74, 0x63, 0x31, 0x32, 0x00, 0x0a, 0x00, 0xa4, 0x01, 0x38,
		0x05, 0x74, 0x63, 0x31, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x1c, 0x02, 0x02, 0x02,
		0x01, 0x00, 0x02, 0x04, 0x02, 0x01, 0x00, 0x03, 0x06, 0x02, 0x01, 0x00, 0x01, 0x08, 0x02, 0x01,
		0x00, 0x01, 0x0a, 0x02, 0x01, 0x00, 0x01, 0x0c, 0x02, 0x01, 0x00, 0x01, 0x0e, 0x02, 0x01, 0x00,
		0x01, 0x10, 0x02, 0x01, 0x00, 0x01, 0x12, 0x02, 0x01, 0x00, 0x01, 0x14, 0x02, 0x01, 0x00, 0x01,
		0x16, 0x02, 0x01, 0x00, 0x04, 0x18, 0x02, 0x01, 0x00, 0x04, 0x1a, 0x02, 0x01, 0x00, 0xff, 0x1c,
		0x02, 0x01, 0x00, 0xff,
	}
	length := uint32(356)
	metaType := uint16(531)
	code, message := doRestful("create database if not exists test_ws_tmq_write_raw", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_tmq_write_raw"))
	defer func() {
		code, message := doRestful("drop database if exists test_ws_tmq_write_raw", "")
		assert.Equal(t, 0, code, message)
	}()
	// connect
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_tmq_write_raw"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	buffer := bytes.Buffer{}
	wstool.WriteUint64(&buffer, 2) // req id
	wstool.WriteUint64(&buffer, 0) // message id
	wstool.WriteUint64(&buffer, uint64(TMQRawMessage))
	wstool.WriteUint32(&buffer, length)
	wstool.WriteUint16(&buffer, metaType)
	buffer.Write(data)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var tmqResp commonResp
	err = json.Unmarshal(resp, &tmqResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), tmqResp.ReqID)
	assert.Equal(t, 0, tmqResp.Code, tmqResp.Message)

	d := restQuery("describe stb", "test_ws_tmq_write_raw")
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
	}
	for rowIndex, values := range d.Data {
		for i := 0; i < 4; i++ {
			assert.Equal(t, expect[rowIndex][i], values[i])
		}
	}
	// wrong meta type
	buffer.Reset()
	metaType = 0
	reqID := uint64(generator.GetReqID())
	wstool.WriteUint64(&buffer, reqID) // req id
	wstool.WriteUint64(&buffer, 0)     // message id
	wstool.WriteUint64(&buffer, uint64(TMQRawMessage))
	wstool.WriteUint32(&buffer, length)
	wstool.WriteUint16(&buffer, metaType)
	buffer.Write(data)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &tmqResp)
	assert.NoError(t, err)
	assert.Equal(t, reqID, tmqResp.ReqID)
	assert.NotEqual(t, 0, tmqResp.Code)
	assert.Equal(t, getActionString(TMQRawMessage), tmqResp.Action)
}

func TestWSWriteRawBlockError(t *testing.T) {
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
	code, message := doRestful("create database if not exists test_ws_write_raw_block_error", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_write_raw_block_error"))
	defer func() {
		code, message := doRestful("drop database if exists test_ws_write_raw_block_error", "")
		assert.Equal(t, 0, code, message)
	}()
	code, message = doRestful("create table test_ws_write_raw_block_error.tb1 (ts timestamp,v int)", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("insert into test_ws_write_raw_block_error.tb1 values(now , 1)", "")
	assert.Equal(t, 0, code, message)
	// connect without db
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: ""}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// query
	queryReq := queryRequest{ReqID: 2, Sql: "select * from test_ws_write_raw_block_error.tb1"}
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

	fetchReq = fetchRequest{ReqID: 5, ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), fetchResp.ReqID)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	assert.Equal(t, true, fetchResp.Completed)

	// write raw block
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
	assert.Equal(t, getActionString(RawBlockMessage), writeResp.Action)
	assert.NotEqual(t, 0, writeResp.Code)

	// write raw block with fields
	buffer.Reset()
	wstool.WriteUint64(&buffer, 300)                               // req id
	wstool.WriteUint64(&buffer, 400)                               // message id
	wstool.WriteUint64(&buffer, uint64(RawBlockMessageWithFields)) // action
	wstool.WriteUint32(&buffer, uint32(fetchResp.Rows))            // rows
	wstool.WriteUint16(&buffer, uint16(2))                         // table name length
	buffer.WriteString("t2")                                       // table name
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
		0x04,
		// padding
		0x00, 0x00,
		// bytes
		0x04, 0x00, 0x00, 0x00,
	}
	buffer.Write(fields)
	err = ws.WriteMessage(websocket.BinaryMessage, buffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &writeResp)
	assert.NoError(t, err)
	assert.Equal(t, getActionString(RawBlockMessageWithFields), writeResp.Action)
	assert.NotEqual(t, 0, writeResp.Code)

}
