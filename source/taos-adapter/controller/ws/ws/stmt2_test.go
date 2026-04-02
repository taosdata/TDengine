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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/driver/common"
	stmtCommon "github.com/taosdata/taosadapter/v3/driver/common/stmt"
	"github.com/taosdata/taosadapter/v3/monitor/recordsql"
	"github.com/taosdata/taosadapter/v3/tools/parseblock"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

func TestWsStmt2(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_stmt2_ws", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_stmt2_ws precision 'ns'", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_stmt2_ws"))

	defer doRestful("drop database if exists test_ws_stmt2_ws", "")

	code, message = doRestful(
		"create table if not exists stb (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100),c16 decimal(20,4),c17 decimal(10,4),c18 blob) tags (info json)",
		"test_ws_stmt2_ws")
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
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_stmt2_ws"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// init
	initReq := stmt2InitRequest{
		ReqID:               0x123,
		SingleStbInsert:     false,
		SingleTableBindOnce: false,
	}
	resp, err = doWebSocket(ws, STMT2Init, &initReq)
	assert.NoError(t, err)
	var initResp stmt2InitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0x123), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := stmt2PrepareRequest{
		ReqID:     3,
		StmtID:    initResp.StmtID,
		SQL:       "insert into ? using test_ws_stmt2_ws.stb tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		GetFields: true,
	}
	resp, err = doWebSocket(ws, STMT2Prepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp stmt2PrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.True(t, prepareResp.IsInsert)
	assert.Equal(t, 21, len(prepareResp.Fields))
	// bind
	now := time.Now()
	cols := [][]driver.Value{
		// ts
		{now, now.Add(time.Second), now.Add(time.Second * 2)},
		// bool
		{true, false, nil},
		// tinyint
		{int8(2), int8(22), nil},
		// smallint
		{int16(3), int16(33), nil},
		// int
		{int32(4), int32(44), nil},
		// bigint
		{int64(5), int64(55), nil},
		// tinyint unsigned
		{uint8(6), uint8(66), nil},
		// smallint unsigned
		{uint16(7), uint16(77), nil},
		// int unsigned
		{uint32(8), uint32(88), nil},
		// bigint unsigned
		{uint64(9), uint64(99), nil},
		// float
		{float32(10), float32(1010), nil},
		// double
		{float64(11), float64(1111), nil},
		// binary
		{"binary", "binary2", nil},
		// nchar
		{"nchar", "nchar2", nil},
		// varbinary
		{[]byte{0xaa, 0xbb, 0xcc}, []byte{0xaa, 0xbb, 0xcc}, nil},
		// geometry
		{[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, nil},
		// decimal(20,4)
		{"12345.6789", "98765.4321", nil},
		// decimal(10,4)
		{"1234.5678", "8765.4321", nil},
		// blob
		{[]byte("this is blob data"), []byte("this is blob data2"), nil},
	}
	tbName := "test_ws_stmt2_ws.ct1"
	tag := []driver.Value{"{\"a\":\"b\"}"}
	binds := &stmtCommon.TaosStmt2BindData{
		TableName: tbName,
		Tags:      tag,
		Cols:      cols,
	}
	bs, err := stmtCommon.MarshalStmt2Binary([]*stmtCommon.TaosStmt2BindData{binds}, true, prepareResp.Fields)
	assert.NoError(t, err)
	bindReq := make([]byte, len(bs)+30)
	// req_id
	binary.LittleEndian.PutUint64(bindReq, 0x12345)
	// stmt_id
	binary.LittleEndian.PutUint64(bindReq[8:], prepareResp.StmtID)
	// action
	binary.LittleEndian.PutUint64(bindReq[16:], Stmt2BindMessage)
	// version
	binary.LittleEndian.PutUint16(bindReq[24:], Stmt2BindProtocolVersion1)
	// col_idx
	idx := int32(-1)
	binary.LittleEndian.PutUint32(bindReq[26:], uint32(idx))
	// data
	copy(bindReq[30:], bs)
	err = ws.WriteMessage(websocket.BinaryMessage, bindReq)
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var bindResp stmt2BindResponse
	err = json.Unmarshal(resp, &bindResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	//exec
	execReq := stmt2ExecRequest{ReqID: 10, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Exec, &execReq)
	assert.NoError(t, err)
	var execResp stmt2ExecResponse
	err = json.Unmarshal(resp, &execResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), execResp.ReqID)
	assert.Equal(t, 0, execResp.Code, execResp.Message)
	assert.Equal(t, 3, execResp.Affected)

	// use result will fail for insert stmt
	useResultReq := stmt2UseResultRequest{ReqID: 9, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Result, &useResultReq)
	assert.NoError(t, err)
	var useResultResp stmt2UseResultResponse
	err = json.Unmarshal(resp, &useResultResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(9), useResultResp.ReqID)
	assert.Equal(t, 0xffff, useResultResp.Code)
	assert.NotEmpty(t, useResultResp.Message)

	// close
	closeReq := stmt2CloseRequest{ReqID: 11, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Close, &closeReq)
	assert.NoError(t, err)
	var closeResp stmt2CloseResponse
	err = json.Unmarshal(resp, &closeResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), closeResp.ReqID)
	assert.Equal(t, 0, closeResp.Code, closeResp.Message)

	// query
	queryReq := queryRequest{Sql: "select * from test_ws_stmt2_ws.stb"}
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
	assert.Equal(t, "12345.6789", blockResult[0][16])
	assert.Equal(t, "1234.5678", blockResult[0][17])
	assert.Equal(t, []byte("this is blob data"), blockResult[0][18])

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
	assert.Equal(t, "98765.4321", blockResult[1][16])
	assert.Equal(t, "8765.4321", blockResult[1][17])
	assert.Equal(t, []byte("this is blob data2"), blockResult[1][18])

	assert.Equal(t, now.Add(time.Second*2).UnixNano(), blockResult[2][0].(time.Time).UnixNano())
	for i := 1; i < 19; i++ {
		assert.Nil(t, blockResult[2][i])
	}

}

func TestStmt2Prepare(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_stmt2_prepare_ws", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_stmt2_prepare_ws precision 'ns'", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_stmt2_prepare_ws"))
	defer doRestful("drop database if exists test_ws_stmt2_prepare_ws", "")

	code, message = doRestful(
		"create table if not exists stb (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100)) tags (info json)",
		"test_ws_stmt2_prepare_ws")
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
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_stmt2_prepare_ws"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// init
	initReq := stmt2InitRequest{
		ReqID:               0x123,
		SingleStbInsert:     false,
		SingleTableBindOnce: false,
	}
	resp, err = doWebSocket(ws, STMT2Init, &initReq)
	assert.NoError(t, err)
	var initResp stmt2InitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0x123), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := stmt2PrepareRequest{
		ReqID:     3,
		StmtID:    initResp.StmtID,
		SQL:       "insert into ctb using test_ws_stmt2_prepare_ws.stb tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		GetFields: true,
	}
	resp, err = doWebSocket(ws, STMT2Prepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp stmt2PrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.Equal(t, true, prepareResp.IsInsert)
	names := [17]string{
		"info",
		"ts",
		"v1",
		"v2",
		"v3",
		"v4",
		"v5",
		"v6",
		"v7",
		"v8",
		"v9",
		"v10",
		"v11",
		"v12",
		"v13",
		"v14",
		"v15",
	}
	fieldTypes := [17]int8{
		common.TSDB_DATA_TYPE_JSON,
		common.TSDB_DATA_TYPE_TIMESTAMP,
		common.TSDB_DATA_TYPE_BOOL,
		common.TSDB_DATA_TYPE_TINYINT,
		common.TSDB_DATA_TYPE_SMALLINT,
		common.TSDB_DATA_TYPE_INT,
		common.TSDB_DATA_TYPE_BIGINT,
		common.TSDB_DATA_TYPE_UTINYINT,
		common.TSDB_DATA_TYPE_USMALLINT,
		common.TSDB_DATA_TYPE_UINT,
		common.TSDB_DATA_TYPE_UBIGINT,
		common.TSDB_DATA_TYPE_FLOAT,
		common.TSDB_DATA_TYPE_DOUBLE,
		common.TSDB_DATA_TYPE_BINARY,
		common.TSDB_DATA_TYPE_NCHAR,
		common.TSDB_DATA_TYPE_VARBINARY,
		common.TSDB_DATA_TYPE_GEOMETRY,
	}
	assert.True(t, prepareResp.IsInsert)
	assert.Equal(t, 17, len(prepareResp.Fields))
	for i := 0; i < 17; i++ {
		assert.Equal(t, names[i], prepareResp.Fields[i].Name)
		assert.Equal(t, fieldTypes[i], prepareResp.Fields[i].FieldType)
	}
	// prepare query
	prepareReq = stmt2PrepareRequest{
		ReqID:     4,
		StmtID:    initResp.StmtID,
		SQL:       "select * from test_ws_stmt2_prepare_ws.stb where ts = ? and v1 = ?",
		GetFields: true,
	}
	resp, err = doWebSocket(ws, STMT2Prepare, &prepareReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.Equal(t, false, prepareResp.IsInsert)
	assert.Nil(t, prepareResp.Fields)
	assert.Equal(t, 2, prepareResp.FieldsCount)
}

func TestStmt2Query(t *testing.T) {
	//for stable
	prepareDataSql := []string{
		"create stable meters (ts timestamp,current float,voltage int,phase float) tags (group_id int, location varchar(24))",
		"insert into d0 using meters tags (2, 'California.SanFrancisco') values ('2023-09-13 17:53:52.123', 10.2, 219, 0.32) ",
		"insert into d1 using meters tags (1, 'California.SanFrancisco') values ('2023-09-13 17:54:43.321', 10.3, 218, 0.31) ",
	}
	Stmt2Query(t, "test_ws_stmt2_query_for_stable", prepareDataSql)

	// for table
	prepareDataSql = []string{
		"create table meters (ts timestamp,current float,voltage int,phase float, group_id int, location varchar(24))",
		"insert into meters values ('2023-09-13 17:53:52.123', 10.2, 219, 0.32, 2, 'California.SanFrancisco') ",
		"insert into meters values ('2023-09-13 17:54:43.321', 10.3, 218, 0.31, 1, 'California.SanFrancisco') ",
	}
	Stmt2Query(t, "test_ws_stmt2_query_for_table", prepareDataSql)
}

func Stmt2Query(t *testing.T, db string, prepareDataSql []string) {
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
	resp, err = doWebSocket(ws, STMT2Init, &initReq)
	assert.NoError(t, err)
	var initResp stmt2InitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := stmt2PrepareRequest{
		ReqID:     3,
		StmtID:    initResp.StmtID,
		SQL:       fmt.Sprintf("select * from %s.meters where group_id=? and location=?", db),
		GetFields: true,
	}
	resp, err = doWebSocket(ws, STMT2Prepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp stmt2PrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.False(t, prepareResp.IsInsert)
	assert.Equal(t, 2, prepareResp.FieldsCount)

	// bind
	var block bytes.Buffer
	wstool.WriteUint64(&block, 5)
	wstool.WriteUint64(&block, prepareResp.StmtID)
	wstool.WriteUint64(&block, uint64(Stmt2BindMessage))
	wstool.WriteUint16(&block, Stmt2BindProtocolVersion1)
	idx := int32(-1)
	wstool.WriteUint32(&block, uint32(idx))
	params := []*stmtCommon.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{int32(1)},
				{"California.SanFrancisco"},
			},
		},
	}
	b, err := stmtCommon.MarshalStmt2Binary(params, false, nil)
	assert.NoError(t, err)
	block.Write(b)

	err = ws.WriteMessage(websocket.BinaryMessage, block.Bytes())
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var bindResp stmt2BindResponse
	err = json.Unmarshal(resp, &bindResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), bindResp.ReqID)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	// exec
	execReq := stmtExecRequest{ReqID: 6, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Exec, &execReq)
	assert.NoError(t, err)
	var execResp stmtExecResponse
	err = json.Unmarshal(resp, &execResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), execResp.ReqID)
	assert.Equal(t, 0, execResp.Code, execResp.Message)

	// use result
	useResultReq := stmt2UseResultRequest{ReqID: 7, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Result, &useResultReq)
	assert.NoError(t, err)
	var useResultResp stmt2UseResultResponse
	err = json.Unmarshal(resp, &useResultResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), useResultResp.ReqID)
	assert.Equal(t, 0, useResultResp.Code, useResultResp.Message)

	// use result again will get failure
	resp, err = doWebSocket(ws, STMT2Result, &useResultReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &useResultResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), useResultResp.ReqID)
	assert.NotEqual(t, 0, useResultResp.Code)

	// fetch
	fetchReq := fetchRequest{ReqID: 8, ID: useResultResp.ID}
	resp, err = doWebSocket(ws, WSFetch, &fetchReq)
	assert.NoError(t, err)
	var fetchResp fetchResponse
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(8), fetchResp.ReqID)
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)
	assert.Equal(t, 1, fetchResp.Rows)

	// fetch block
	fetchBlockReq := fetchBlockRequest{ReqID: 9, ID: useResultResp.ID}
	fetchBlockResp, err := doWebSocket(ws, WSFetchBlock, &fetchBlockReq)
	assert.NoError(t, err)
	_, blockResult, err := parseblock.ParseBlock(fetchBlockResp[8:], useResultResp.FieldsTypes, fetchResp.Rows, useResultResp.Precision)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(blockResult))
	assert.Equal(t, float32(10.3), blockResult[0][1])
	assert.Equal(t, int32(218), blockResult[0][2])
	assert.Equal(t, float32(0.31), blockResult[0][3])

	// free result
	freeResultReq, _ := json.Marshal(freeResultRequest{ReqID: 10, ID: useResultResp.ID})
	a, _ := json.Marshal(Request{Action: WSFreeResult, Args: freeResultReq})
	err = ws.WriteMessage(websocket.TextMessage, a)
	assert.NoError(t, err)

	// close
	closeReq := stmtCloseRequest{ReqID: 11, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Close, &closeReq)
	assert.NoError(t, err)
	var closeResp stmt2CloseResponse
	err = json.Unmarshal(resp, &fetchResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, closeResp.Code, closeResp.Message)
}

func TestStmt2BindWithStbFields(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_stmt2_getstbfields_ws", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_stmt2_getstbfields_ws precision 'ns'", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_stmt2_getstbfields_ws"))

	defer func() {
		code, message = doRestful("drop database if exists test_ws_stmt2_getstbfields_ws", "")
		assert.Equal(t, 0, code, message)
	}()

	code, message = doRestful(
		"create table if not exists stb (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100)) tags (info json)",
		"test_ws_stmt2_getstbfields_ws")
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
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_stmt2_getstbfields_ws"}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// init
	initReq := stmt2InitRequest{
		ReqID:               0x123,
		SingleStbInsert:     false,
		SingleTableBindOnce: false,
	}
	resp, err = doWebSocket(ws, STMT2Init, &initReq)
	assert.NoError(t, err)
	var initResp stmt2InitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0x123), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := stmt2PrepareRequest{
		ReqID:     3,
		StmtID:    initResp.StmtID,
		SQL:       "insert into ? using test_ws_stmt2_getstbfields_ws.stb tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		GetFields: true,
	}
	resp, err = doWebSocket(ws, STMT2Prepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp stmt2PrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.Equal(t, true, prepareResp.IsInsert)
	expectFieldsName := [18]string{"tbname", "info", "ts", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15"}
	expectFieldsType := [18]int8{
		common.TSDB_DATA_TYPE_BINARY,
		common.TSDB_DATA_TYPE_JSON,
		common.TSDB_DATA_TYPE_TIMESTAMP,
		common.TSDB_DATA_TYPE_BOOL,
		common.TSDB_DATA_TYPE_TINYINT,
		common.TSDB_DATA_TYPE_SMALLINT,
		common.TSDB_DATA_TYPE_INT,
		common.TSDB_DATA_TYPE_BIGINT,
		common.TSDB_DATA_TYPE_UTINYINT,
		common.TSDB_DATA_TYPE_USMALLINT,
		common.TSDB_DATA_TYPE_UINT,
		common.TSDB_DATA_TYPE_UBIGINT,
		common.TSDB_DATA_TYPE_FLOAT,
		common.TSDB_DATA_TYPE_DOUBLE,
		common.TSDB_DATA_TYPE_BINARY,
		common.TSDB_DATA_TYPE_NCHAR,
		common.TSDB_DATA_TYPE_VARBINARY,
		common.TSDB_DATA_TYPE_GEOMETRY,
	}
	expectBindType := [18]int8{
		stmtCommon.TAOS_FIELD_TBNAME,
		stmtCommon.TAOS_FIELD_TAG,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
		stmtCommon.TAOS_FIELD_COL,
	}
	for i := 0; i < 18; i++ {
		assert.Equal(t, expectFieldsName[i], prepareResp.Fields[i].Name)
		assert.Equal(t, expectFieldsType[i], prepareResp.Fields[i].FieldType)
		assert.Equal(t, expectBindType[i], prepareResp.Fields[i].BindType)
		if prepareResp.Fields[i].FieldType == common.TSDB_DATA_TYPE_TIMESTAMP {
			assert.Equal(t, uint8(common.PrecisionNanoSecond), prepareResp.Fields[i].Precision)
		}
	}
	// bind
	now := time.Now()
	cols := [][]driver.Value{
		// ts
		{now, now.Add(time.Second), now.Add(time.Second * 2)},
		// bool
		{true, false, nil},
		// tinyint
		{int8(2), int8(22), nil},
		// smallint
		{int16(3), int16(33), nil},
		// int
		{int32(4), int32(44), nil},
		// bigint
		{int64(5), int64(55), nil},
		// tinyint unsigned
		{uint8(6), uint8(66), nil},
		// smallint unsigned
		{uint16(7), uint16(77), nil},
		// int unsigned
		{uint32(8), uint32(88), nil},
		// bigint unsigned
		{uint64(9), uint64(99), nil},
		// float
		{float32(10), float32(1010), nil},
		// double
		{float64(11), float64(1111), nil},
		// binary
		{"binary", "binary2", nil},
		// nchar
		{"nchar", "nchar2", nil},
		// varbinary
		{[]byte{0xaa, 0xbb, 0xcc}, []byte{0xaa, 0xbb, 0xcc}, nil},
		// geometry
		{[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, nil},
	}
	tbName := "test_ws_stmt2_getstbfields_ws.ct1"
	tag := []driver.Value{"{\"a\":\"b\"}"}
	binds := &stmtCommon.TaosStmt2BindData{
		TableName: tbName,
		Tags:      tag,
		Cols:      cols,
	}
	bs, err := stmtCommon.MarshalStmt2Binary([]*stmtCommon.TaosStmt2BindData{binds}, true, prepareResp.Fields)
	assert.NoError(t, err)
	bindReq := make([]byte, len(bs)+30)
	// req_id
	binary.LittleEndian.PutUint64(bindReq, 0xee12345)
	// stmt_id
	binary.LittleEndian.PutUint64(bindReq[8:], prepareResp.StmtID)
	// action
	binary.LittleEndian.PutUint64(bindReq[16:], Stmt2BindMessage)
	// version
	binary.LittleEndian.PutUint16(bindReq[24:], Stmt2BindProtocolVersion1)
	// col_idx
	idx := int32(-1)
	binary.LittleEndian.PutUint32(bindReq[26:], uint32(idx))
	// data
	copy(bindReq[30:], bs)
	err = ws.WriteMessage(websocket.BinaryMessage, bindReq)
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var bindResp stmt2BindResponse
	err = json.Unmarshal(resp, &bindResp)
	assert.NoError(t, err)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	//exec
	execReq := stmt2ExecRequest{ReqID: 10, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Exec, &execReq)
	assert.NoError(t, err)
	var execResp stmt2ExecResponse
	err = json.Unmarshal(resp, &execResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), execResp.ReqID)
	assert.Equal(t, 0, execResp.Code, execResp.Message)
	assert.Equal(t, 3, execResp.Affected)

	// close
	closeReq := stmt2CloseRequest{ReqID: 11, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Close, &closeReq)
	assert.NoError(t, err)
	var closeResp stmt2CloseResponse
	err = json.Unmarshal(resp, &closeResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), closeResp.ReqID)
	assert.Equal(t, 0, closeResp.Code, closeResp.Message)

	// query
	queryReq := queryRequest{Sql: "select * from test_ws_stmt2_getstbfields_ws.stb"}
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

}

func TestRecordStmt(t *testing.T) {
	tmpDir := t.TempDir()
	file := "test.csv"
	db := "test_ws_stmt2_record"
	output := filepath.Join(tmpDir, file)
	f, err := os.Create(output)
	require.NoError(t, err)
	now := time.Now()
	var port string
	var host string
	const appName = "test_record_stmt_app"
	prepareQid := uint64(0x123)
	bindQid := uint64(0x3456)
	execQid := uint64(0x5678)
	prepareSql := fmt.Sprintf("insert into ? using %s.stb tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", db)
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
		require.Equal(t, 5, len(records), records)
		var checkRecord = func(record []string, qid string, action string, success bool, expectData string) {
			t.Log(record)
			assert.Equal(t, host, record[recordsql.StmtIPIndex])
			assert.Equal(t, port, record[recordsql.StmtSourcePortIndex])
			assert.Equal(t, appName, record[recordsql.StmtAppNameIndex])
			assert.Equal(t, "root", record[recordsql.StmtUserIndex])
			assert.Equal(t, recordsql.WSType.String(), record[recordsql.StmtConnTypeIndex])
			assert.Equal(t, qid, record[recordsql.StmtQIDIndex])
			receiveTime, err := time.Parse(recordsql.ResultTimeFormat, record[recordsql.StmtStartTimeIndex])
			assert.NoError(t, err)
			assert.True(t, receiveTime.After(now))
			duration, err := strconv.Atoi(record[recordsql.StmtDurationIndex])
			assert.NoError(t, err)
			assert.Greater(t, duration, 0)
			assert.NotEmpty(t, record[recordsql.StmtStmtPointerIndex])
			assert.Equal(t, action, record[recordsql.StmtActionIndex])
			if success {
				assert.Equal(t, "0", record[recordsql.StmtResultCodeIndex])
			}
			assert.Equal(t, expectData, record[recordsql.StmtDataIndex])
		}
		checkRecord(records[0], "0x123", "prepare", true, prepareSql)
		checkRecord(records[1], "0x3456", "bind", true, `{"count":1,"table_names":["test_ws_stmt2_record.ct1"],"tags":[[{"type":15,"data":["{\"a\":\"b\"}"]}]],"cols":[[{"type":9,"data":[1767863488954108976,1767863489954108976,1767863490954108976]},{"type":1,"data":[true,false,null]},{"type":2,"data":[2,22,null]},{"type":3,"data":[3,33,null]},{"type":4,"data":[4,44,null]},{"type":5,"data":[5,55,null]},{"type":11,"data":[6,66,null]},{"type":12,"data":[7,77,null]},{"type":13,"data":[8,88,null]},{"type":14,"data":[9,99,null]},{"type":6,"data":[10,1010,null]},{"type":7,"data":[11,1111,null]},{"type":8,"data":["binary","binary2",null]},{"type":10,"data":["nchar","nchar2",null]},{"type":16,"data":["aabbcc","aabbcc",null]},{"type":20,"data":["010100000000000000000059400000000000005940","010100000000000000000059400000000000005940",null]},{"type":17,"data":["12345.6789","98765.4321",null]},{"type":21,"data":["1234.5678","8765.4321",null]},{"type":18,"data":["7468697320697320626c6f622064617461","7468697320697320626c6f62206461746132",null]}]]}`)
		checkRecord(records[2], "0x5678", "exec", true, "3")
		checkRecord(records[3], "0x5678", "exec", false, "0")
		checkRecord(records[4], "0x3456", "bind", false, "010203")
	}()

	start := time.Now().Format(recordsql.InputTimeFormat)
	end := time.Now().Add(time.Second * 5).Format(recordsql.InputTimeFormat)
	err = recordsql.StartRecordWithTestWriter(recordsql.RecordTypeStmt, start, end, "", f)
	require.NoError(t, err)
	defer func() {
		info := recordsql.StopRecordStmtMission()
		require.NotNil(t, info)
	}()

	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists "+db, "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful(fmt.Sprintf("create database if not exists %s precision 'ns'", db), "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated(db))

	defer doRestful(fmt.Sprintf("drop database if exists %s", db), "")

	code, message = doRestful(
		"create table if not exists stb (ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100),c16 decimal(20,4),c17 decimal(10,4),c18 blob) tags (info json)",
		db)
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
	host, port, err = net.SplitHostPort(strings.TrimSpace(ws.LocalAddr().String()))
	assert.NoError(t, err)
	// connect
	connReq := connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: db, App: appName}
	resp, err := doWebSocket(ws, Connect, &connReq)
	assert.NoError(t, err)
	var connResp connResponse
	err = json.Unmarshal(resp, &connResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	// init
	initReq := stmt2InitRequest{
		ReqID:               0x123,
		SingleStbInsert:     false,
		SingleTableBindOnce: false,
	}
	resp, err = doWebSocket(ws, STMT2Init, &initReq)
	assert.NoError(t, err)
	var initResp stmt2InitResponse
	err = json.Unmarshal(resp, &initResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0x123), initResp.ReqID)
	assert.Equal(t, 0, initResp.Code, initResp.Message)

	// prepare
	prepareReq := stmt2PrepareRequest{
		ReqID:     prepareQid,
		StmtID:    initResp.StmtID,
		SQL:       prepareSql,
		GetFields: true,
	}
	resp, err = doWebSocket(ws, STMT2Prepare, &prepareReq)
	assert.NoError(t, err)
	var prepareResp stmt2PrepareResponse
	err = json.Unmarshal(resp, &prepareResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(prepareQid), prepareResp.ReqID)
	assert.Equal(t, 0, prepareResp.Code, prepareResp.Message)
	assert.True(t, prepareResp.IsInsert)
	assert.Equal(t, 21, len(prepareResp.Fields))
	// bind
	cols := [][]driver.Value{
		// ts
		{int64(1767863488954108976), int64(1767863489954108976), int64(1767863490954108976)},
		// bool
		{true, false, nil},
		// tinyint
		{int8(2), int8(22), nil},
		// smallint
		{int16(3), int16(33), nil},
		// int
		{int32(4), int32(44), nil},
		// bigint
		{int64(5), int64(55), nil},
		// tinyint unsigned
		{uint8(6), uint8(66), nil},
		// smallint unsigned
		{uint16(7), uint16(77), nil},
		// int unsigned
		{uint32(8), uint32(88), nil},
		// bigint unsigned
		{uint64(9), uint64(99), nil},
		// float
		{float32(10), float32(1010), nil},
		// double
		{float64(11), float64(1111), nil},
		// binary
		{"binary", "binary2", nil},
		// nchar
		{"nchar", "nchar2", nil},
		// varbinary
		{[]byte{0xaa, 0xbb, 0xcc}, []byte{0xaa, 0xbb, 0xcc}, nil},
		// geometry
		{[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}, nil},
		// decimal(20,4)
		{"12345.6789", "98765.4321", nil},
		// decimal(10,4)
		{"1234.5678", "8765.4321", nil},
		// blob
		{[]byte("this is blob data"), []byte("this is blob data2"), nil},
	}
	tbName := fmt.Sprintf("%s.ct1", db)
	tag := []driver.Value{"{\"a\":\"b\"}"}
	binds := &stmtCommon.TaosStmt2BindData{
		TableName: tbName,
		Tags:      tag,
		Cols:      cols,
	}
	bs, err := stmtCommon.MarshalStmt2Binary([]*stmtCommon.TaosStmt2BindData{binds}, true, prepareResp.Fields)
	assert.NoError(t, err)
	bindReq := make([]byte, len(bs)+30)
	// req_id
	binary.LittleEndian.PutUint64(bindReq, bindQid)
	// stmt_id
	binary.LittleEndian.PutUint64(bindReq[8:], prepareResp.StmtID)
	// action
	binary.LittleEndian.PutUint64(bindReq[16:], Stmt2BindMessage)
	// version
	binary.LittleEndian.PutUint16(bindReq[24:], Stmt2BindProtocolVersion1)
	// col_idx
	idx := int32(-1)
	binary.LittleEndian.PutUint32(bindReq[26:], uint32(idx))
	// data
	copy(bindReq[30:], bs)
	err = ws.WriteMessage(websocket.BinaryMessage, bindReq)
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	var bindResp stmt2BindResponse
	err = json.Unmarshal(resp, &bindResp)
	assert.NoError(t, err)
	assert.Equal(t, bindQid, bindResp.ReqID)
	assert.Equal(t, 0, bindResp.Code, bindResp.Message)

	//exec
	execReq := stmt2ExecRequest{ReqID: execQid, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Exec, &execReq)
	assert.NoError(t, err)
	var execResp stmt2ExecResponse
	err = json.Unmarshal(resp, &execResp)
	assert.NoError(t, err)
	assert.Equal(t, execQid, execResp.ReqID)
	assert.Equal(t, 0, execResp.Code, execResp.Message)
	assert.Equal(t, 3, execResp.Affected)

	//exec
	resp, err = doWebSocket(ws, STMT2Exec, &execReq)
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &execResp)
	assert.NoError(t, err)
	assert.Equal(t, execQid, execResp.ReqID)
	assert.NotEqual(t, 0, execResp.Code, execResp.Message)

	// bind
	invalidData := []byte{0x01, 0x02, 0x03}
	bindReq = make([]byte, len(invalidData)+30)
	// req_id
	binary.LittleEndian.PutUint64(bindReq, bindQid)
	// stmt_id
	binary.LittleEndian.PutUint64(bindReq[8:], prepareResp.StmtID)
	// action
	binary.LittleEndian.PutUint64(bindReq[16:], Stmt2BindMessage)
	// version
	binary.LittleEndian.PutUint16(bindReq[24:], Stmt2BindProtocolVersion1)
	// col_idx
	binary.LittleEndian.PutUint32(bindReq[26:], uint32(idx))
	// data
	copy(bindReq[30:], invalidData)
	err = ws.WriteMessage(websocket.BinaryMessage, bindReq)
	assert.NoError(t, err)
	_, resp, err = ws.ReadMessage()
	assert.NoError(t, err)
	err = json.Unmarshal(resp, &bindResp)
	assert.NoError(t, err)
	assert.Equal(t, bindQid, bindResp.ReqID)
	assert.NotEqual(t, 0, bindResp.Code, bindResp.Message)

	// close
	closeReq := stmt2CloseRequest{ReqID: 11, StmtID: prepareResp.StmtID}
	resp, err = doWebSocket(ws, STMT2Close, &closeReq)
	assert.NoError(t, err)
	var closeResp stmt2CloseResponse
	err = json.Unmarshal(resp, &closeResp)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), closeResp.ReqID)
	assert.Equal(t, 0, closeResp.Code, closeResp.Message)
}
