package ws

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/common/param"
	"github.com/taosdata/taosadapter/v3/driver/common/serializer"
	stmtCommon "github.com/taosdata/taosadapter/v3/driver/common/stmt"
	"github.com/taosdata/taosadapter/v3/driver/types"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/parseblock"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func Test_parseRowBlockInfo(t *testing.T) {
	b, err := serializer.SerializeRawBlock(
		[]*param.Param{
			param.NewParam(1).AddBool(true),
			param.NewParam(1).AddTinyint(1),
			param.NewParam(1).AddSmallint(1),
			param.NewParam(1).AddInt(1),
			param.NewParam(1).AddBigint(1),
			param.NewParam(1).AddFloat(1.1),
			param.NewParam(1).AddDouble(1.1),
			param.NewParam(1).AddBinary([]byte("California.SanFrancisco")),
			param.NewParam(1).AddNchar("California.SanFrancisco"),
			param.NewParam(1).AddUTinyint(1),
			param.NewParam(1).AddUSmallint(1),
			param.NewParam(1).AddUInt(1),
			param.NewParam(1).AddUBigint(1),
			param.NewParam(1).AddJson([]byte(`{"name":"taos"}`)),
			param.NewParam(1).AddVarBinary([]byte("California.SanFrancisco")),
		},
		param.NewColumnType(15).
			AddBool().
			AddTinyint().
			AddSmallint().
			AddInt().
			AddBigint().
			AddFloat().
			AddDouble().
			AddBinary(100).
			AddNchar(100).
			AddUTinyint().
			AddUSmallint().
			AddUInt().
			AddUBigint().
			AddJson(100).
			AddVarBinary(100),
	)
	assert.NoError(t, err)
	fields, fieldsType, err := parseRowBlockInfo(unsafe.Pointer(&b[0]), 15)
	assert.NoError(t, err)
	expectFields := []*stmtCommon.StmtField{
		{FieldType: common.TSDB_DATA_TYPE_BOOL},
		{FieldType: common.TSDB_DATA_TYPE_TINYINT},
		{FieldType: common.TSDB_DATA_TYPE_SMALLINT},
		{FieldType: common.TSDB_DATA_TYPE_INT},
		{FieldType: common.TSDB_DATA_TYPE_BIGINT},
		{FieldType: common.TSDB_DATA_TYPE_FLOAT},
		{FieldType: common.TSDB_DATA_TYPE_DOUBLE},
		{FieldType: common.TSDB_DATA_TYPE_BINARY},
		{FieldType: common.TSDB_DATA_TYPE_NCHAR},
		{FieldType: common.TSDB_DATA_TYPE_UTINYINT},
		{FieldType: common.TSDB_DATA_TYPE_USMALLINT},
		{FieldType: common.TSDB_DATA_TYPE_UINT},
		{FieldType: common.TSDB_DATA_TYPE_UBIGINT},
		{FieldType: common.TSDB_DATA_TYPE_JSON},
		{FieldType: common.TSDB_DATA_TYPE_VARBINARY},
	}
	assert.Equal(t, expectFields, fields)
	expectFieldsType := []*types.ColumnType{
		{Type: types.TaosBoolType},
		{Type: types.TaosTinyintType},
		{Type: types.TaosSmallintType},
		{Type: types.TaosIntType},
		{Type: types.TaosBigintType},
		{Type: types.TaosFloatType},
		{Type: types.TaosDoubleType},
		{Type: types.TaosBinaryType},
		{Type: types.TaosNcharType},
		{Type: types.TaosUTinyintType},
		{Type: types.TaosUSmallintType},
		{Type: types.TaosUIntType},
		{Type: types.TaosUBigintType},
		{Type: types.TaosJsonType},
		{Type: types.TaosBinaryType},
	}
	assert.Equal(t, expectFieldsType, fieldsType)
}

func TestWsStmt(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_stmt_ws", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_stmt_ws precision 'ns'", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_stmt_ws"))

	defer doRestful("drop database if exists test_ws_stmt_ws", "")

	code, message = doRestful(
		"create table if not exists stb (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100)) tags (info json)",
		"test_ws_stmt_ws")
	assert.Equal(t, 0, code, message)

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
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_stmt_ws"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// init
	initReq := map[string]uint64{"req_id": 2}
	resp, err = doWebSocket(ws, STMTInit, &initReq)
	assert.NoError(t, err)
	var initResp stmtInitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := stmtPrepareRequest{ReqID: 3, StmtID: initResp.StmtID, SQL: "insert into ? using test_ws_stmt_ws.stb tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"}
	resp, err = doWebSocket(ws, STMTPrepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp stmtPrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.True(t, prepareResp.IsInsert)

	// set table name
	setTableNameReq := stmtSetTableNameRequest{ReqID: 4, StmtID: prepareResp.StmtID, Name: "test_ws_stmt_ws.ct1"}
	resp, err = doWebSocket(ws, STMTSetTableName, &setTableNameReq)
	assert.NoError(t, err)
	var setTableNameResp commonResp
	err = json.Unmarshal(resp, &setTableNameResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), setTableNameResp.ReqID)
	assert.Equal(t, 0, setTableNameResp.Code, setTableNameResp.Message)

	// get tag fields
	getTagFieldsReq := stmtGetTagFieldsRequest{ReqID: 5, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTGetTagFields, &getTagFieldsReq)
	assert.NoError(t, err)
	var getTagFieldsResp stmtGetTagFieldsResponse
	err = json.Unmarshal(resp, &getTagFieldsResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), getTagFieldsResp.ReqID)
	assert.Equal(t, 0, getTagFieldsResp.Code, getTagFieldsResp.Message)

	// get col fields
	getColFieldsReq := stmtGetColFieldsRequest{ReqID: 6, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTGetColFields, &getColFieldsReq)
	assert.NoError(t, err)
	var getColFieldsResp stmtGetColFieldsResponse
	err = json.Unmarshal(resp, &getColFieldsResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), getColFieldsResp.ReqID)
	assert.Equal(t, 0, getColFieldsResp.Code, getColFieldsResp.Message)

	// set tags
	setTagsReq := stmtSetTagsRequest{ReqID: 7, StmtID: prepareResp.StmtID, Tags: json.RawMessage(`["{\"a\":\"b\"}"]`)}
	resp, err = doWebSocket(ws, STMTSetTags, &setTagsReq)
	assert.NoError(t, err)
	var setTagsResp stmtSetTagsResponse
	err = json.Unmarshal(resp, &setTagsResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), setTagsResp.ReqID)
	assert.Equal(t, 0, setTagsResp.Code, setTagsResp.Message)

	// bind
	now := time.Now()
	columns, _ := json.Marshal([][]driver.Value{
		{now, now.Add(time.Second), now.Add(time.Second * 2)},
		{true, false, nil},
		{2, 22, nil},
		{3, 33, nil},
		{4, 44, nil},
		{5, 55, nil},
		{6, 66, nil},
		{7, 77, nil},
		{8, 88, nil},
		{9, 99, nil},
		{10, 1010, nil},
		{11, 1111, nil},
		{"binary", "binary2", nil},
		{"nchar", "nchar2", nil},
		{"aabbcc", "aabbcc", nil},
		{"010100000000000000000059400000000000005940", "010100000000000000000059400000000000005940", nil},
	})
	bindReq := stmtBindRequest{ReqID: 8, StmtID: prepareResp.StmtID, Columns: columns}
	resp, err = doWebSocket(ws, STMTBind, &bindReq)
	assert.NoError(t, err)
	var bindResp stmtBindResponse
	err = json.Unmarshal(resp, &bindResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(8), bindResp.ReqID)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	// add batch
	addBatchReq := stmtAddBatchRequest{ReqID: 9, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTAddBatch, &addBatchReq)
	assert.NoError(t, err)
	var addBatchResp stmtAddBatchResponse
	err = json.Unmarshal(resp, &addBatchResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(9), addBatchResp.ReqID)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	// exec
	execReq := stmtExecRequest{ReqID: 10, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTExec, &execReq)
	assert.NoError(t, err)
	var execResp stmtExecResponse
	err = json.Unmarshal(resp, &execResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), execResp.ReqID)
	assert.Equal(t, 0, execResp.Code, execResp.Message)

	// close
	closeReq := stmtCloseRequest{ReqID: 11, StmtID: prepareResp.StmtID}
	err = doWebSocketWithoutResp(ws, STMTClose, &closeReq)
	assert.NoError(t, err)

	// query
	queryReq := queryRequest{Sql: "select * from test_ws_stmt_ws.stb"}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	var queryResp queryResponse
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch
	fetchReq := fetchRequest{ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	var fetchResp fetchResponse
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	// fetch block
	fetchBlockReq := fetchBlockRequest{ID: queryResp.ID}
	fetchBlockResp, err := doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	_, blockResult, err := parseblock.ParseBlock(fetchBlockResp[8:], queryResp.FieldsTypes, fetchResp.Rows, queryResp.Precision)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(blockResult))
	assert.Equal(t, now.UnixNano(), blockResult[0][0].(time.Time).UnixNano())

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
	assert.Equal(t, "binary", blockResult[0][12])
	assert.Equal(t, "nchar", blockResult[0][13])
	assert.Equal(t, []byte{0xaa, 0xbb, 0xcc}, blockResult[1][14])
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, blockResult[0][15])

	assert.Equal(t, now.Add(time.Second).UnixNano(), blockResult[1][0].(time.Time).UnixNano())
	assert.Equal(t, false, blockResult[1][1])
	assert.Equal(t, int8(22), blockResult[1][2])
	assert.Equal(t, int16(33), blockResult[1][3])
	assert.Equal(t, int32(44), blockResult[1][4])
	assert.Equal(t, int64(55), blockResult[1][5])
	assert.Equal(t, uint8(66), blockResult[1][6])
	assert.Equal(t, uint16(77), blockResult[1][7])
	assert.Equal(t, uint32(88), blockResult[1][8])
	assert.Equal(t, uint64(99), blockResult[1][9])
	assert.Equal(t, float32(1010), blockResult[1][10])
	assert.Equal(t, float64(1111), blockResult[1][11])
	assert.Equal(t, "binary2", blockResult[1][12])
	assert.Equal(t, "nchar2", blockResult[1][13])
	assert.Equal(t, []byte{0xaa, 0xbb, 0xcc}, blockResult[1][14])
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, blockResult[1][15])

	assert.Equal(t, now.Add(time.Second*2).UnixNano(), blockResult[2][0].(time.Time).UnixNano())
	for i := 1; i < 16; i++ {
		assert.Nil(t, blockResult[2][i])
	}

	// block message
	// init
	initReq = map[string]uint64{"req_id": 0x11}
	resp, err = doWebSocket(ws, STMTInit, &initReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, initResp.Code, initResp.Message)
	assert.Equal(t, uint64(0x11), initResp.ReqID)

	// prepare
	prepareReq = stmtPrepareRequest{StmtID: initResp.StmtID, SQL: "insert into ? using test_ws_stmt_ws.stb tags(?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"}
	resp, err = doWebSocket(ws, STMTPrepare, &prepareReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)

	// set table name
	setTableNameReq = stmtSetTableNameRequest{StmtID: prepareResp.StmtID, Name: "test_ws_stmt_ws.ct2"}
	resp, err = doWebSocket(ws, STMTSetTableName, &setTableNameReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &setTableNameResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, setTableNameResp.Code, setTableNameResp.Message)

	// set tags
	var tagBuffer bytes.Buffer
	wstool.WriteUint64(&tagBuffer, 100)
	wstool.WriteUint64(&tagBuffer, prepareResp.StmtID)
	wstool.WriteUint64(&tagBuffer, uint64(SetTagsMessage))
	tags, err := json.Marshal(map[string]string{"a": "b"})
	assert.NoError(t, err)
	b, err := serializer.SerializeRawBlock(
		[]*param.Param{
			param.NewParam(1).AddJson(tags),
		},
		param.NewColumnType(1).AddJson(50))
	assert.NoError(t, err)
	assert.NoError(t, err)
	tagBuffer.Write(b)

	err = ws.WriteMessage(websocket.BinaryMessage, tagBuffer.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &setTagsResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, setTagsResp.Code, setTagsResp.Message)

	// bind binary
	var block bytes.Buffer
	wstool.WriteUint64(&block, 10)
	wstool.WriteUint64(&block, prepareResp.StmtID)
	wstool.WriteUint64(&block, uint64(BindMessage))
	rawBlock := []byte{
		0x01, 0x00, 0x00, 0x00,
		0x11, 0x02, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00,
		0x10, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x80,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		0x09, 0x08, 0x00, 0x00, 0x00,
		0x01, 0x01, 0x00, 0x00, 0x00,
		0x02, 0x01, 0x00, 0x00, 0x00,
		0x03, 0x02, 0x00, 0x00, 0x00,
		0x04, 0x04, 0x00, 0x00, 0x00,
		0x05, 0x08, 0x00, 0x00, 0x00,
		0x0b, 0x01, 0x00, 0x00, 0x00,
		0x0c, 0x02, 0x00, 0x00, 0x00,
		0x0d, 0x04, 0x00, 0x00, 0x00,
		0x0e, 0x08, 0x00, 0x00, 0x00,
		0x06, 0x04, 0x00, 0x00, 0x00,
		0x07, 0x08, 0x00, 0x00, 0x00,
		0x08, 0x16, 0x00, 0x00, 0x00,
		0x0a, 0x52, 0x00, 0x00, 0x00,
		0x10, 0x20, 0x00, 0x00, 0x00,
		0x14, 0x20, 0x00, 0x00, 0x00,

		0x18, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00,
		0x06, 0x00, 0x00, 0x00,
		0x0c, 0x00, 0x00, 0x00,
		0x18, 0x00, 0x00, 0x00,
		0x03, 0x00, 0x00, 0x00,
		0x06, 0x00, 0x00, 0x00,
		0x0c, 0x00, 0x00, 0x00,
		0x18, 0x00, 0x00, 0x00,
		0x0c, 0x00, 0x00, 0x00,
		0x18, 0x00, 0x00, 0x00,
		0x11, 0x00, 0x00, 0x00,
		0x30, 0x00, 0x00, 0x00,
		0x21, 0x00, 0x00, 0x00,
		0x2e, 0x00, 0x00, 0x00,

		0x00,
		0x2c, 0x5b, 0x70, 0x86, 0x82, 0x01, 0x00, 0x00,
		0x14, 0x5f, 0x70, 0x86, 0x82, 0x01, 0x00, 0x00,
		0xfc, 0x62, 0x70, 0x86, 0x82, 0x01, 0x00, 0x00,

		0x20,
		0x01,
		0x00,
		0x00,

		0x20,
		0x02,
		0x16,
		0x00,

		0x20,
		0x03, 0x00,
		0x21, 0x00,
		0x00, 0x00,

		0x20,
		0x04, 0x00, 0x00, 0x00,
		0x2c, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,

		0x20,
		0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x37, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		0x20,
		0x06,
		0x42,
		0x00,

		0x20,
		0x07, 0x00,
		0x4d, 0x00,
		0x00, 0x00,

		0x20,
		0x08, 0x00, 0x00, 0x00,
		0x58, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,

		0x20,
		0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		0x20,
		0x00, 0x00, 0x20, 0x41,
		0x00, 0x80, 0x7c, 0x44,
		0x00, 0x00, 0x00, 0x00,

		0x20,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x40,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x5c, 0x91, 0x40,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x06, 0x00,
		0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,
		0x07, 0x00,
		0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x32,

		0x00, 0x00, 0x00, 0x00,
		0x16, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x14, 0x00,
		0x6e, 0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x68, 0x00,
		0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x72, 0x00, 0x00, 0x00,
		0x18, 0x00,
		0x6e, 0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x68, 0x00, 0x00, 0x00,
		0x61, 0x00, 0x00, 0x00, 0x72, 0x00, 0x00, 0x00, 0x32, 0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x00,
		0x10, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x0e, 0x00,
		0x74, 0x65, 0x73, 0x74, 0x5f, 0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,
		0x0f, 0x00,
		0x74, 0x65, 0x73, 0x74, 0x5f, 0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x32,

		0x00, 0x00, 0x00, 0x00,
		0x17, 0x00, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
		0x15, 0x00,
		0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
		0x15, 0x00,
		0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
	}
	binary.LittleEndian.PutUint64(rawBlock[173:], uint64(now.UnixNano()))
	binary.LittleEndian.PutUint64(rawBlock[181:], uint64(now.Add(time.Second).UnixNano()))
	binary.LittleEndian.PutUint64(rawBlock[189:], uint64(now.Add(time.Second*2).UnixNano()))
	block.Write(rawBlock)
	err = ws.WriteMessage(
		websocket.BinaryMessage,
		block.Bytes(),
	)
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &bindResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	// add batch
	addBatchReq = stmtAddBatchRequest{StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTAddBatch, &addBatchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &addBatchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	// exec
	execReq = stmtExecRequest{StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTExec, &execReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &execResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, execResp.Code, execResp.Message)

	// query
	queryReq = queryRequest{Sql: "select * from test_ws_stmt_ws.ct2"}
	resp, err = doWebSocket(ws, WSQuery, &queryReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &queryResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch
	fetchReq = fetchRequest{ID: queryResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	// fetch block
	fetchBlockReq = fetchBlockRequest{ID: queryResp.ID}
	fetchBlockResp, err = doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	_, blockResult, err = parseblock.ParseBlock(fetchBlockResp[8:], queryResp.FieldsTypes, fetchResp.Rows, queryResp.Precision)
	assert.NoError(t, err)
	assert.Equal(t, now.UnixNano(), blockResult[0][0].(time.Time).UnixNano())
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
	assert.Equal(t, "binary", blockResult[0][12])
	assert.Equal(t, "nchar", blockResult[0][13])
	assert.Equal(t, []byte("test_varbinary2"), blockResult[1][14])
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, blockResult[0][15])

	assert.Equal(t, now.Add(time.Second).UnixNano(), blockResult[1][0].(time.Time).UnixNano())
	assert.Equal(t, false, blockResult[1][1])
	assert.Equal(t, int8(22), blockResult[1][2])
	assert.Equal(t, int16(33), blockResult[1][3])
	assert.Equal(t, int32(44), blockResult[1][4])
	assert.Equal(t, int64(55), blockResult[1][5])
	assert.Equal(t, uint8(66), blockResult[1][6])
	assert.Equal(t, uint16(77), blockResult[1][7])
	assert.Equal(t, uint32(88), blockResult[1][8])
	assert.Equal(t, uint64(99), blockResult[1][9])
	assert.Equal(t, float32(1010), blockResult[1][10])
	assert.Equal(t, float64(1111), blockResult[1][11])
	assert.Equal(t, "binary2", blockResult[1][12])
	assert.Equal(t, "nchar2", blockResult[1][13])
	assert.Equal(t, []byte("test_varbinary2"), blockResult[1][14])
	assert.Equal(t, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, blockResult[1][15])

	assert.Equal(t, now.Add(time.Second*2).UnixNano(), blockResult[2][0].(time.Time).UnixNano())
	for i := 1; i < 16; i++ {
		assert.Nil(t, blockResult[2][i])
	}
}

func TestStmtQuery(t *testing.T) {
	//for stable
	prepareDataSql := []string{
		"create stable meters (ts timestamp,current float,voltage int,phase float) tags (group_id int, location varchar(24))",
		"insert into d0 using meters tags (2, 'California.SanFrancisco') values ('2023-09-13 17:53:52.123', 10.2, 219, 0.32) ",
		"insert into d1 using meters tags (1, 'California.SanFrancisco') values ('2023-09-13 17:54:43.321', 10.3, 218, 0.31) ",
	}
	StmtQuery(t, "test_ws_stmt_query_for_stable", prepareDataSql)

	// for table
	prepareDataSql = []string{
		"create table meters (ts timestamp,current float,voltage int,phase float, group_id int, location varchar(24))",
		"insert into meters values ('2023-09-13 17:53:52.123', 10.2, 219, 0.32, 2, 'California.SanFrancisco') ",
		"insert into meters values ('2023-09-13 17:54:43.321', 10.3, 218, 0.31, 1, 'California.SanFrancisco') ",
	}
	StmtQuery(t, "test_ws_stmt_query_for_table", prepareDataSql)
}

func StmtQuery(t *testing.T, db string, prepareDataSql []string) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful(fmt.Sprintf("drop database if exists %s", db), "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create database if not exists %s", db), "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated(db))
	defer doRestful(fmt.Sprintf("drop database if exists %s", db), "")

	for _, sql := range prepareDataSql {
		code, message = doRestful(sql, db)
		assert.Equal(t, 0, code, message)
	}

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

	// init
	initReq := map[string]uint64{"req_id": 2}
	resp, err = doWebSocket(ws, STMTInit, &initReq)
	assert.NoError(t, err)
	var initResp stmtInitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := stmtPrepareRequest{
		ReqID:  3,
		StmtID: initResp.StmtID,
		SQL:    fmt.Sprintf("select * from %s.meters where group_id=? and location=?", db),
	}
	resp, err = doWebSocket(ws, STMTPrepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp stmtPrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.False(t, prepareResp.IsInsert)

	// bind
	var block bytes.Buffer
	wstool.WriteUint64(&block, 5)
	wstool.WriteUint64(&block, prepareResp.StmtID)
	wstool.WriteUint64(&block, uint64(BindMessage))
	b, err := serializer.SerializeRawBlock(
		[]*param.Param{
			param.NewParam(1).AddInt(1),
			param.NewParam(1).AddBinary([]byte("California.SanFrancisco")),
		},
		param.NewColumnType(2).AddInt().AddBinary(24))
	assert.NoError(t, err)
	block.Write(b)

	err = ws.WriteMessage(websocket.BinaryMessage, block.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var bindResp stmtBindResponse
	err = json.Unmarshal(resp, &bindResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), bindResp.ReqID)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	// add batch
	addBatchReq := stmtAddBatchRequest{StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTAddBatch, &addBatchReq)
	assert.NoError(t, err)
	var addBatchResp stmtAddBatchResponse
	err = json.Unmarshal(resp, &addBatchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	// exec
	execReq := stmtExecRequest{ReqID: 6, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTExec, &execReq)
	assert.NoError(t, err)
	var execResp stmtExecResponse
	err = json.Unmarshal(resp, &execResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), execResp.ReqID)
	assert.Equal(t, 0, execResp.Code, execResp.Message)

	// use result
	useResultReq := stmtUseResultRequest{ReqID: 7, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTUseResult, &useResultReq)
	assert.NoError(t, err)
	var useResultResp stmtUseResultResponse
	err = json.Unmarshal(resp, &useResultResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), useResultResp.ReqID)
	assert.Equal(t, 0, useResultResp.Code, useResultResp.Message)

	// fetch
	fetchReq := fetchRequest{ReqID: 8, ID: useResultResp.ResultID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	var fetchResp fetchResponse
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(8), fetchResp.ReqID)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)
	assert.Equal(t, 1, fetchResp.Rows)

	// fetch block
	fetchBlockReq := fetchBlockRequest{ReqID: 9, ID: useResultResp.ResultID}
	fetchBlockResp, err := doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	_, blockResult, err := parseblock.ParseBlock(fetchBlockResp[8:], useResultResp.FieldsTypes, fetchResp.Rows, useResultResp.Precision)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(blockResult))
	assert.Equal(t, float32(10.3), blockResult[0][1])
	assert.Equal(t, int32(218), blockResult[0][2])
	assert.Equal(t, float32(0.31), blockResult[0][3])

	// free result
	freeResultReq, _ := json.Marshal(freeResultRequest{ReqID: 10, ID: useResultResp.ResultID})
	a, _ := json.Marshal(Request{Action: WSFreeResult, Args: freeResultReq})
	err = ws.WriteMessage(websocket.TextMessage, a)
	assert.NoError(t, err)

	// close
	closeReq := stmtCloseRequest{ReqID: 11, StmtID: prepareResp.StmtID}
	err = doWebSocketWithoutResp(ws, STMTClose, &closeReq)
	assert.NoError(t, err)
}

func TestStmtNumParams(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	db := "test_ws_stmt_num_params"
	code, message := doRestful(fmt.Sprintf("drop database if exists %s", db), "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create database if not exists %s", db), "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated(db))
	code, message = doRestful(fmt.Sprintf("create stable if not exists %s.meters (ts timestamp,current float,voltage int,phase float) tags (groupid int,location varchar(24))", db), "")
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

	// init
	initReq := map[string]uint64{"req_id": 2}
	resp, err = doWebSocket(ws, STMTInit, &initReq)
	assert.NoError(t, err)
	var initResp stmtInitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := stmtPrepareRequest{
		ReqID:  3,
		StmtID: initResp.StmtID,
		SQL:    fmt.Sprintf("insert into d1 using %s.meters tags(?, ?) values (?, ?, ?, ?)", db),
	}
	resp, err = doWebSocket(ws, STMTPrepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp stmtPrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)

	// num params
	numParamsReq := stmtNumParamsRequest{ReqID: 4, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTNumParams, &numParamsReq)
	assert.NoError(t, err)
	var numParamsResp stmtNumParamsResponse
	err = json.Unmarshal(resp, &numParamsResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, numParamsResp.Code, numParamsResp.Message)
	assert.Equal(t, uint64(4), numParamsResp.ReqID)
	assert.Equal(t, 4, numParamsResp.NumParams)
}

func TestStmtGetParams(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	db := "test_ws_stmt_get_params"
	code, message := doRestful(fmt.Sprintf("drop database if exists %s", db), "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create database if not exists %s", db), "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated(db))
	code, message = doRestful(fmt.Sprintf("create stable if not exists %s.meters (ts timestamp,current float,voltage int,phase float) tags (groupid int,location varchar(24))", db), "")
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

	// init
	initReq := map[string]uint64{"req_id": 2}
	resp, err = doWebSocket(ws, STMTInit, &initReq)
	assert.NoError(t, err)
	var initResp stmtInitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := stmtPrepareRequest{
		ReqID:  3,
		StmtID: initResp.StmtID,
		SQL:    fmt.Sprintf("insert into d1 using %s.meters tags(?, ?) values (?, ?, ?, ?)", db),
	}
	resp, err = doWebSocket(ws, STMTPrepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp stmtPrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)

	// get param
	getParamsReq := stmtGetParamRequest{ReqID: 4, StmtID: prepareResp.StmtID, Index: 0}
	resp, err = doWebSocket(ws, STMTGetParam, &getParamsReq)
	assert.NoError(t, err)
	var getParamsResp stmtGetParamResponse
	err = json.Unmarshal(resp, &getParamsResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, getParamsResp.Code, getParamsResp.Message)
	assert.Equal(t, uint64(4), getParamsResp.ReqID)
	assert.Equal(t, 0, getParamsResp.Index)
	assert.Equal(t, 9, getParamsResp.DataType)
	assert.Equal(t, 8, getParamsResp.Length)
}

func TestStmtInvalidStmtID(t *testing.T) {
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
	reqID := uint64(generator.GetReqID())
	connReq := connRequest{ReqID: reqID, User: "root", Password: "taosdata"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, Connect, connResp.Action)
	assert.Equal(t, reqID, connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// prepare
	reqID = uint64(generator.GetReqID())
	prepareReq := stmtPrepareRequest{ReqID: reqID, StmtID: 0, SQL: "insert into ? using test_ws_stmt_ws.stb tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"}
	resp, err = doWebSocket(ws, STMTPrepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp stmtPrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, STMTPrepare, prepareResp.Action)
	assert.Equal(t, reqID, prepareResp.ReqID)
	assert.NotEqual(t, 0, prepareResp.Code)

	// set table name
	reqID = uint64(generator.GetReqID())
	setTableNameReq := stmtSetTableNameRequest{ReqID: reqID, StmtID: prepareResp.StmtID, Name: "d1"}
	resp, err = doWebSocket(ws, STMTSetTableName, &setTableNameReq)
	assert.NoError(t, err)
	var setTableNameResp stmtSetTableNameResponse
	err = json.Unmarshal(resp, &setTableNameResp)
	assert.NoError(t, err)
	assert.Equal(t, STMTSetTableName, setTableNameResp.Action)
	assert.Equal(t, reqID, setTableNameResp.ReqID)
	assert.NotEqual(t, 0, setTableNameResp.Code)

	// set tags
	reqID = uint64(generator.GetReqID())
	setTagsReq := stmtSetTagsRequest{ReqID: reqID, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTSetTags, &setTagsReq)
	assert.NoError(t, err)
	var setTagsResp stmtSetTagsResponse
	err = json.Unmarshal(resp, &setTagsResp)
	assert.NoError(t, err)
	assert.Equal(t, STMTSetTags, setTagsResp.Action)
	assert.Equal(t, reqID, setTagsResp.ReqID)
	assert.NotEqual(t, 0, setTagsResp.Code)

	// bind
	reqID = uint64(generator.GetReqID())
	bindReq := stmtBindRequest{ReqID: reqID, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTBind, &bindReq)
	assert.NoError(t, err)
	var bindResp stmtBindResponse
	err = json.Unmarshal(resp, &bindResp)
	assert.NoError(t, err)
	assert.Equal(t, STMTBind, bindResp.Action)
	assert.Equal(t, reqID, bindResp.ReqID)
	assert.NotEqual(t, 0, bindResp.Code)

	// add batch
	reqID = uint64(generator.GetReqID())
	addBatchReq := stmtAddBatchRequest{ReqID: reqID, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTAddBatch, &addBatchReq)
	assert.NoError(t, err)
	var addBatchResp stmtAddBatchResponse
	err = json.Unmarshal(resp, &addBatchResp)
	assert.NoError(t, err)
	assert.Equal(t, STMTAddBatch, addBatchResp.Action)
	assert.Equal(t, reqID, addBatchResp.ReqID)
	assert.NotEqual(t, 0, addBatchResp.Code)

	// exec
	reqID = uint64(generator.GetReqID())
	execReq := stmtExecRequest{ReqID: reqID, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTExec, &execReq)
	assert.NoError(t, err)
	var execResp stmtExecResponse
	err = json.Unmarshal(resp, &execResp)
	assert.NoError(t, err)
	assert.Equal(t, STMTExec, execResp.Action)
	assert.Equal(t, reqID, execResp.ReqID)
	assert.NotEqual(t, 0, execResp.Code)

	// use result
	reqID = uint64(generator.GetReqID())
	useResultReq := stmtUseResultRequest{ReqID: reqID, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTUseResult, &useResultReq)
	assert.NoError(t, err)
	var useResultResp stmtUseResultResponse
	err = json.Unmarshal(resp, &useResultResp)
	assert.NoError(t, err)
	assert.Equal(t, STMTUseResult, useResultResp.Action)
	assert.Equal(t, reqID, useResultResp.ReqID)
	assert.NotEqual(t, 0, useResultResp.Code)

	// get tag fields
	reqID = uint64(generator.GetReqID())
	getTagFieldsReq := stmtGetTagFieldsRequest{ReqID: reqID, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTGetTagFields, &getTagFieldsReq)
	assert.NoError(t, err)
	var getTagFieldsResp stmtGetTagFieldsResponse
	err = json.Unmarshal(resp, &getTagFieldsResp)
	assert.NoError(t, err)
	assert.Equal(t, STMTGetTagFields, getTagFieldsResp.Action)
	assert.Equal(t, reqID, getTagFieldsResp.ReqID)
	assert.NotEqual(t, 0, getTagFieldsResp.Code)

	// get col fields
	reqID = uint64(generator.GetReqID())
	getColFieldsReq := stmtGetColFieldsRequest{ReqID: reqID, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTGetColFields, &getColFieldsReq)
	assert.NoError(t, err)
	var getColFieldsResp stmtGetColFieldsResponse
	err = json.Unmarshal(resp, &getColFieldsResp)
	assert.NoError(t, err)
	assert.Equal(t, STMTGetColFields, getColFieldsResp.Action)
	assert.Equal(t, reqID, getColFieldsResp.ReqID)
	assert.NotEqual(t, 0, getColFieldsResp.Code)

	// num params
	reqID = uint64(generator.GetReqID())
	numParamsReq := stmtNumParamsRequest{ReqID: reqID, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMTNumParams, &numParamsReq)
	assert.NoError(t, err)
	var numParamsResp stmtNumParamsResponse
	err = json.Unmarshal(resp, &numParamsResp)
	assert.NoError(t, err)
	assert.Equal(t, STMTNumParams, numParamsResp.Action)
	assert.Equal(t, reqID, numParamsResp.ReqID)
	assert.NotEqual(t, 0, numParamsResp.Code)

	// get param
	reqID = uint64(generator.GetReqID())
	getParamsReq := stmtGetParamRequest{ReqID: reqID, StmtID: prepareResp.StmtID, Index: 0}
	resp, err = doWebSocket(ws, STMTGetParam, &getParamsReq)
	assert.NoError(t, err)
	var getParamsResp stmtGetParamResponse
	err = json.Unmarshal(resp, &getParamsResp)
	assert.NoError(t, err)
	assert.Equal(t, STMTGetParam, getParamsResp.Action)
	assert.Equal(t, reqID, getParamsResp.ReqID)
	assert.NotEqual(t, 0, getParamsResp.Code)

}
