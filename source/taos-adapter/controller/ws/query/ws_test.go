package query

import (
	"bytes"
	"database/sql/driver"
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
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	_ "github.com/taosdata/taosadapter/v3/controller/rest"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db"
	taoserrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/parseblock"
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
	assert.NoError(t, err, message)
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

func sendBinaryBackJson(t *testing.T, ws *websocket.Conn, req []byte, resp interface{}) error {
	message, err := sendWSMessage(ws, websocket.BinaryMessage, req)
	if err != nil {
		return err
	}
	var baseResp BaseResp
	err = json.Unmarshal(message, &baseResp)
	assert.NoError(t, err, message)
	if baseResp.Code != 0 {
		return taoserrors.NewError(baseResp.Code, baseResp.Message)
	}
	err = json.Unmarshal(message, &resp)
	assert.NoError(t, err)
	return nil
}

func conn(t *testing.T, ws *websocket.Conn, req *WSConnectReq) (*WSConnectResp, error) {
	var resp WSConnectResp
	err := sendJsonBackJson(t, ws, WSConnect, req, &resp)
	return &resp, err
}

func query(t *testing.T, ws *websocket.Conn, req *WSQueryReq) (*WSQueryResult, error) {
	var resp WSQueryResult
	err := sendJsonBackJson(t, ws, WSQuery, req, &resp)
	return &resp, err
}

func fetch(t *testing.T, ws *websocket.Conn, req *WSFetchReq) (*WSFetchResp, error) {
	var resp WSFetchResp
	err := sendJsonBackJson(t, ws, WSFetch, req, &resp)
	return &resp, err
}

func fetchBlock(t *testing.T, ws *websocket.Conn, req *WSFetchBlockReq) []byte {
	message := sendJsonBackBinary(t, ws, WSFetchBlock, req)
	return message
}

func getVersion(t *testing.T, ws *websocket.Conn) (*WSVersionResp, error) {
	var resp WSVersionResp
	err := sendJsonBackJson(t, ws, wstool.ClientVersion, nil, &resp)
	return &resp, err
}

func wsQuery(t *testing.T, ws *websocket.Conn, sql string, onFetchBlock func(*WSQueryResult, *WSFetchResp, []byte) error) (resultID uint64, blockResult [][]driver.Value, err error) {
	queryReq := &WSQueryReq{
		ReqID: 2,
		SQL:   sql,
	}
	queryResp, err := query(t, ws, queryReq)
	assert.NoError(t, err)
	for {
		fetchResp, err := fetch(t, ws, &WSFetchReq{
			ReqID: 3,
			ID:    queryResp.ID,
		})
		assert.NoError(t, err)
		if !fetchResp.Completed {
			blockMessage := fetchBlock(t, ws, &WSFetchBlockReq{
				ReqID: 4,
				ID:    queryResp.ID,
			})
			if onFetchBlock != nil {
				err = onFetchBlock(queryResp, fetchResp, blockMessage)
				assert.NoError(t, err)
				return 0, nil, err
			}
			resultID, blockResult, err = parseblock.ParseBlock(blockMessage[8:], queryResp.FieldsTypes, fetchResp.Rows, queryResp.Precision)
			assert.NoError(t, err)
			if err != nil {
				return 0, nil, err
			}
		} else {
			break
		}
	}
	return resultID, blockResult, nil
}

// @author: xftan
// @date: 2022/2/22 14:42
// @description: test websocket bulk pulling
func TestWebsocket(t *testing.T) {
	now := time.Now().Local().UnixNano() / 1e6
	code, message := doRestful("create database if not exists test_ws WAL_RETENTION_PERIOD 86400", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws"))
	initSqls := []string{
		"drop table if exists test_ws",
		"create table if not exists test_ws(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20)) tags (info json)",
		fmt.Sprintf(`insert into t1 using test_ws tags('{"table":"t1"}') values (%d,true,2,3,4,5,6,7,8,9,10,11,'中文"binary','中文nchar')(%d,false,12,13,14,15,16,17,18,19,110,111,'中文"binary','中文nchar')(%d,null,null,null,null,null,null,null,null,null,null,null,null,null)`, now, now+1, now+3),
	}
	for _, sql := range initSqls {
		code, message = doRestful(sql, "test_ws")
		assert.Equal(t, 0, code, message)
	}
	defer func() {
		code, message = doRestful("drop database if exists test_ws", "")
		assert.Equal(t, 0, code, message)
	}()
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	connect := &WSConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		DB:       "test_ws",
	}
	_, err = conn(t, ws, connect)
	assert.NoError(t, err)
	resultID, blockResult, err := wsQuery(t, ws, "select test_ws.*,info->'table' from test_ws", nil)
	require.NoError(t, err)
	versionResp, err := getVersion(t, ws)
	assert.NoError(t, err)
	assert.NotEmpty(t, versionResp.Version)

	assert.Equal(t, uint64(1), resultID)
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
	assert.Equal(t, []byte(`{"table":"t1"}`), blockResult[0][14])
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
	assert.Equal(t, []byte(`{"table":"t1"}`), blockResult[1][14])
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
	assert.Equal(t, []byte(`{"table":"t1"}`), blockResult[2][14])
}

func TestWriteBlock(t *testing.T) {
	now := time.Now().Local().UnixNano() / 1e6
	code, message := doRestful("create database if not exists test_ws_write_block WAL_RETENTION_PERIOD 86400", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_write_block"))

	initSqls := []string{
		"drop table if exists test_ws_write_block",
		"create table if not exists test_ws_write_block(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20)) tags (info json)",
		fmt.Sprintf(`insert into t1 using test_ws_write_block tags('{"table":"t1"}') values (%d,true,2,3,4,5,6,7,8,9,10,11,'中文"binary','中文nchar')(%d,false,12,13,14,15,16,17,18,19,110,111,'中文"binary','中文nchar')(%d,null,null,null,null,null,null,null,null,null,null,null,null,null)`, now, now+1, now+3),
		`create table t2 using test_ws_write_block tags('{"table":"t2"}')`,
	}
	for _, sql := range initSqls {
		code, message = doRestful(sql, "test_ws_write_block")
		assert.Equal(t, 0, code, message)
	}

	defer func() {
		code, message = doRestful("drop database if exists test_ws_write_block", "")
		assert.Equal(t, 0, code, message)
	}()
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}

	connect := &WSConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		DB:       "test_ws_write_block",
	}

	_, err = conn(t, ws, connect)
	assert.NoError(t, err)
	_, _, err = wsQuery(t, ws, "select * from t1", func(queryResp *WSQueryResult, fetchResp *WSFetchResp, blockMessage []byte) error {
		//block
		buffer := &bytes.Buffer{}
		// req id
		wstool.WriteUint64(buffer, 300)
		// message id
		wstool.WriteUint64(buffer, 400)
		// action
		wstool.WriteUint64(buffer, RawBlockMessage)
		// rows
		wstool.WriteUint32(buffer, uint32(fetchResp.Rows))
		// table name length
		wstool.WriteUint16(buffer, uint16(2))
		// table name
		buffer.WriteString("t2")
		// raw block
		buffer.Write(blockMessage[16:])
		var resp BaseResp
		err = sendBinaryBackJson(t, ws, buffer.Bytes(), &resp)
		assert.NoError(t, err)
		return err
	})
	require.NoError(t, err)
	versionResp, err := getVersion(t, ws)
	assert.NoError(t, err)
	assert.NotEmpty(t, versionResp.Version)

	err = ws.Close()
	assert.NoError(t, err)

	ws, _, err = websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}

	connect = &WSConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		DB:       "test_ws_write_block",
	}
	_, err = conn(t, ws, connect)
	assert.NoError(t, err)
	_, blockResult, err := wsQuery(t, ws, "select * from t2", nil)
	require.NoError(t, err)
	err = ws.Close()
	assert.NoError(t, err)
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

}

func TestWriteBlockWithFields(t *testing.T) {
	now := time.Now().Local().UnixNano() / 1e6
	code, message := doRestful("create database if not exists test_ws_write_block_with_fields WAL_RETENTION_PERIOD 86400", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_write_block_with_fields"))
	initSqls := []string{
		"drop table if exists test_ws_write_block_with_fields",
		"create table if not exists test_ws_write_block_with_fields(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20)) tags (info json)",
		fmt.Sprintf(`insert into t1 using test_ws_write_block_with_fields tags('{"table":"t1"}') values (%d,true,2,3,4,5,6,7,8,9,10,11,'中文"binary','中文nchar')(%d,false,12,13,14,15,16,17,18,19,110,111,'中文"binary','中文nchar')(%d,null,null,null,null,null,null,null,null,null,null,null,null,null)`, now, now+1, now+3),
		`create table t2 using test_ws_write_block_with_fields tags('{"table":"t2"}')`,
	}
	for _, sql := range initSqls {
		code, message = doRestful(sql, "test_ws_write_block_with_fields")
		assert.Equal(t, 0, code, message)
	}

	defer func() {
		code, message = doRestful("drop database if exists test_ws_write_block_with_fields", "")
		assert.Equal(t, 0, code, message)
	}()

	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	connect := &WSConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		DB:       "test_ws_write_block_with_fields",
	}
	_, err = conn(t, ws, connect)
	assert.NoError(t, err)
	_, _, err = wsQuery(t, ws, "select ts,v1 from t1", func(queryResp *WSQueryResult, fetchResponse *WSFetchResp, blockMessage []byte) error {
		//block
		buffer := &bytes.Buffer{}
		// req id
		wstool.WriteUint64(buffer, 300)
		// message id
		wstool.WriteUint64(buffer, 400)
		// action
		wstool.WriteUint64(buffer, RawBlockMessageWithFields)
		// rows
		wstool.WriteUint32(buffer, uint32(fetchResponse.Rows))
		// table name length
		wstool.WriteUint16(buffer, uint16(2))
		// table name
		buffer.WriteString("t2")
		// raw block
		buffer.Write(blockMessage[16:])
		// fields
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
		}
		buffer.Write(fields)
		var resp BaseResp
		err = sendBinaryBackJson(t, ws, buffer.Bytes(), &resp)
		assert.NoError(t, err)
		return err
	})
	require.NoError(t, err)
	versionResp, err := getVersion(t, ws)
	assert.NoError(t, err)
	assert.NotEmpty(t, versionResp.Version)

	err = ws.Close()
	assert.NoError(t, err)
	ws, _, err = websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}

	connect = &WSConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		DB:       "test_ws_write_block_with_fields",
	}
	_, err = conn(t, ws, connect)
	assert.NoError(t, err)
	_, blockResult, err := wsQuery(t, ws, "select * from t2", nil)
	require.NoError(t, err)

	err = ws.Close()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(blockResult))
	assert.Equal(t, now, blockResult[0][0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, true, blockResult[0][1])
	for i := 2; i < 14; i++ {
		assert.Equal(t, nil, blockResult[0][i])
	}
	assert.Equal(t, now+1, blockResult[1][0].(time.Time).UnixNano()/1e6)
	assert.Equal(t, false, blockResult[1][1])
	for i := 2; i < 14; i++ {
		assert.Equal(t, nil, blockResult[1][i])
	}
	assert.Equal(t, now+3, blockResult[2][0].(time.Time).UnixNano()/1e6)
	for i := 1; i < 14; i++ {
		assert.Equal(t, nil, blockResult[2][i])
	}

}

func TestQueryAllType(t *testing.T) {
	now := time.Now().Local().UnixNano() / 1e6
	code, message := doRestful("create database if not exists test_ws_all_query WAL_RETENTION_PERIOD 86400", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_all_query"))
	initSqls := []string{
		"drop table if exists test_ws_all_query",
		"create table if not exists test_ws_all_query(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100))",
		fmt.Sprintf(`insert into test_ws_all_query values (%d,true,2,3,4,5,6,7,8,9,10,11,'中文"binary','中文nchar','\xaabbcc','POINT(100 100)')(%d,false,12,13,14,15,16,17,18,19,110,111,'中文"binary','中文nchar','\xaabbcc','POINT(100 100)')(%d,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)`, now, now+1, now+3),
	}
	for _, sql := range initSqls {
		code, message = doRestful(sql, "test_ws_all_query")
		assert.Equal(t, 0, code, message)
	}
	defer func() {
		code, message = doRestful("drop database if exists test_ws_all_query", "")
		assert.Equal(t, 0, code, message)
	}()
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	connect := &WSConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
		DB:       "test_ws_all_query",
	}
	_, err = conn(t, ws, connect)
	assert.NoError(t, err)
	resultID, blockResult, err := wsQuery(t, ws, "select * from test_ws_all_query order by ts asc ", nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), resultID)
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
	assert.Equal(t, []byte{
		0x01,
		0x01,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
	}, blockResult[1][15])

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
	assert.Equal(t, []byte{0xaa, 0xbb, 0xcc}, blockResult[0][14])
	assert.Equal(t, []byte{
		0x01,
		0x01,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x00,
		0x59,
		0x40,
	}, blockResult[1][15])
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

func TestDropUser(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/ws", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	defer doRestful("drop user test_ws_query_drop_user", "")
	code, message := doRestful("create user test_ws_query_drop_user pass 'pass_123'", "")
	assert.Equal(t, 0, code, message)
	// connect
	connReq := &WSConnectReq{ReqID: 1, User: "test_ws_query_drop_user", Password: "pass_123"}
	connResp, err := conn(t, ws, connReq)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	// drop user
	code, message = doRestful("drop user test_ws_query_drop_user", "")
	assert.Equal(t, 0, code, message)
	time.Sleep(time.Second * 3)
	resp, err := doWebSocket(ws, wstool.ClientVersion, nil)
	assert.Error(t, err, resp)
}
