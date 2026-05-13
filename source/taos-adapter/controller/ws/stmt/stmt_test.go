package stmt

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
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
	taoserrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/layout"
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

func TestSTMT(t *testing.T) {
	now := time.Now()
	code, message := doRestful("drop database if exists test_ws_stmt", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_stmt precision 'ns'", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_stmt"))
	initSqls := []string{
		"create table if not exists st(ts timestamp," +
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
			"c14 varchar(20)," +
			"c15 geometry(100)" +
			") tags (info json)",
	}
	for _, sql := range initSqls {
		code, message = doRestful(sql, "test_ws_stmt")
		assert.Equal(t, 0, code, message)
	}
	defer func() {
		code, message = doRestful("drop database if exists test_ws_stmt", "")
		assert.Equal(t, 0, code, message)
	}()
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/stmt", nil)
	if err != nil {
		t.Error(err)
		return
	}
	connect := &StmtConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
	}
	_, err = conn(t, ws, connect)
	assert.NoError(t, err)
	initReq := &StmtInitReq{
		ReqID: 2,
	}
	initResp, err := stmtInit(t, ws, initReq)
	assert.NoError(t, err)
	prepareReq := &StmtPrepareReq{
		ReqID:  3,
		StmtID: initResp.StmtID,
		SQL:    "insert into ? using test_ws_stmt.st tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
	}
	_, err = stmtPrepare(t, ws, prepareReq)
	assert.NoError(t, err)
	stmtSetTableNameReq := &StmtSetTableNameReq{
		ReqID:  4,
		StmtID: initResp.StmtID,
		Name:   "test_ws_stmt.ct1",
	}
	_, err = stmtSetTableName(t, ws, stmtSetTableNameReq)
	assert.NoError(t, err)
	stmtGetTagFieldsReq := &StmtGetTagFieldsReq{
		ReqID:  5,
		StmtID: initResp.StmtID,
	}
	_, err = stmtGetTagFields(t, ws, stmtGetTagFieldsReq)
	assert.NoError(t, err)
	stmtGetColFieldsReq := &StmtGetColFieldsReq{
		ReqID:  5,
		StmtID: initResp.StmtID,
	}
	_, err = stmtGetColFields(t, ws, stmtGetColFieldsReq)
	assert.NoError(t, err)
	stmtSetTagsReq := &StmtSetTagsReq{
		ReqID:  5,
		StmtID: initResp.StmtID,
		// {"a":"b"}
		Tags: json.RawMessage(`["{\"a\":\"b\"}"]`),
	}
	_, err = stmtSetTags(t, ws, stmtSetTagsReq)
	assert.NoError(t, err)

	c, err := json.Marshal([][]driver.Value{
		{
			now,
			now.Add(time.Second),
			now.Add(time.Second * 2),
		},
		{
			true,
			false,
			nil,
		},
		{
			2,
			22,
			nil,
		},
		{
			3,
			33,
			nil,
		},
		{
			4,
			44,
			nil,
		},
		{
			5,
			55,
			nil,
		},
		{
			6,
			66,
			nil,
		},
		{
			7,
			77,
			nil,
		},
		{
			8,
			88,
			nil,
		},
		{
			9,
			99,
			nil,
		},
		{
			10,
			1010,
			nil,
		},
		{
			11,
			1111,
			nil,
		},
		{
			"binary",
			"binary2",
			nil,
		},
		{
			"nchar",
			"nchar2",
			nil,
		},
		{
			"varbinary",
			"varbinary2",
			nil,
		},
		{
			"010100000000000000000059400000000000005940",
			"010100000000000000000059400000000000005940",
			nil,
		},
	})
	assert.NoError(t, err)
	stmtBindReq := &StmtBindReq{
		ReqID:   5,
		StmtID:  initResp.StmtID,
		Columns: c,
	}
	_, err = stmtBind(t, ws, stmtBindReq)
	assert.NoError(t, err)
	stmtAddBatchReq := &StmtAddBatchReq{
		ReqID:  6,
		StmtID: initResp.StmtID,
	}
	_, err = stmtAddBatch(t, ws, stmtAddBatchReq)
	assert.NoError(t, err)
	stmtExecReq := &StmtExecReq{
		ReqID:  7,
		StmtID: initResp.StmtID,
	}
	_, err = stmtExec(t, ws, stmtExecReq)
	assert.NoError(t, err)
	b, _ := json.Marshal(&StmtClose{
		ReqID:  8,
		StmtID: initResp.StmtID,
	})
	action, _ := json.Marshal(&wstool.WSAction{
		Action: STMTClose,
		Args:   b,
	})
	err = ws.WriteMessage(
		websocket.TextMessage,
		action,
	)
	assert.NoError(t, err)
	err = ws.Close()
	assert.NoError(t, err)
	time.Sleep(time.Second)
	w := httptest.NewRecorder()
	body := strings.NewReader("select * from st")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql/test_ws_stmt", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	resultBody := fmt.Sprintf(`{"code":0,"column_meta":[["ts","TIMESTAMP",8],["c1","BOOL",1],["c2","TINYINT",1],["c3","SMALLINT",2],["c4","INT",4],["c5","BIGINT",8],["c6","TINYINT UNSIGNED",1],["c7","SMALLINT UNSIGNED",2],["c8","INT UNSIGNED",4],["c9","BIGINT UNSIGNED",8],["c10","FLOAT",4],["c11","DOUBLE",8],["c12","VARCHAR",20],["c13","NCHAR",20],["c14","VARCHAR",20],["c15","GEOMETRY",100],["info","JSON",4095]],"data":[["%s",true,2,3,4,5,6,7,8,9,10,11,"binary","nchar","varbinary","010100000000000000000059400000000000005940",{"a":"b"}],["%s",false,22,33,44,55,66,77,88,99,1010,1111,"binary2","nchar2","varbinary2","010100000000000000000059400000000000005940",{"a":"b"}],["%s",null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,{"a":"b"}]],"rows":3}`, now.UTC().Format(layout.LayoutNanoSecond), now.Add(time.Second).UTC().Format(layout.LayoutNanoSecond), now.Add(time.Second*2).UTC().Format(layout.LayoutNanoSecond))
	assert.Equal(t, resultBody, w.Body.String())
}

func TestBlock(t *testing.T) {
	code, message := doRestful("drop database if exists test_ws_stmt_block", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_stmt_block precision 'ns'", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_stmt_block"))

	initSqls := []string{
		"create table if not exists stb(ts timestamp," +
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
			"c14 varchar(20)," +
			"c15 geometry(100)" +
			") tags(info json)",
	}
	for _, sql := range initSqls {
		code, message = doRestful(sql, "test_ws_stmt_block")
		assert.Equal(t, 0, code, message)
	}
	defer func() {
		code, message = doRestful("drop database if exists test_ws_stmt_block", "")
		assert.Equal(t, 0, code, message)
	}()
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
	now := time.Now()
	binary.LittleEndian.PutUint64(rawBlock[173:], uint64(now.UnixNano()))
	binary.LittleEndian.PutUint64(rawBlock[181:], uint64(now.Add(time.Second).UnixNano()))
	binary.LittleEndian.PutUint64(rawBlock[189:], uint64(now.Add(time.Second*2).UnixNano()))
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/stmt", nil)
	if err != nil {
		t.Error(err)
		return
	}
	connect := &StmtConnectReq{
		ReqID:    0,
		User:     "root",
		Password: "taosdata",
	}
	_, err = conn(t, ws, connect)
	assert.NoError(t, err)
	initReq := &StmtInitReq{
		ReqID: 2,
	}
	initResp, err := stmtInit(t, ws, initReq)
	assert.NoError(t, err)
	prepareReq := &StmtPrepareReq{
		ReqID:  3,
		StmtID: initResp.StmtID,
		SQL:    "insert into ? using test_ws_stmt_block.stb tags (?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
	}
	_, err = stmtPrepare(t, ws, prepareReq)
	assert.NoError(t, err)
	stmtSetTableNameReq := &StmtSetTableNameReq{
		ReqID:  4,
		StmtID: initResp.StmtID,
		Name:   "test_ws_stmt_block.ctb",
	}
	_, err = stmtSetTableName(t, ws, stmtSetTableNameReq)
	assert.NoError(t, err)
	stmtSetTagsReq := &StmtSetTagsReq{
		ReqID:  5,
		StmtID: initResp.StmtID,
		// {"a":"b"}
		Tags: json.RawMessage(`["{\"a\":\"b\"}"]`),
	}
	_, err = stmtSetTags(t, ws, stmtSetTagsReq)
	assert.NoError(t, err)

	reqID := uint64(10)
	action := uint64(2)

	block := &bytes.Buffer{}
	wstool.WriteUint64(block, reqID)
	wstool.WriteUint64(block, initResp.StmtID)
	wstool.WriteUint64(block, action)
	block.Write(rawBlock)
	blockData := block.Bytes()
	t.Log(blockData)
	err = ws.WriteMessage(
		websocket.BinaryMessage,
		block.Bytes(),
	)
	assert.NoError(t, err)
	// read response
	_, wsMessage, err := ws.ReadMessage()
	assert.NoError(t, err)
	var baseResp BaseResp
	err = json.Unmarshal(wsMessage, &baseResp)
	assert.NoError(t, err, message)
	assert.Equal(t, uint64(10), baseResp.ReqID)
	assert.Equal(t, 0, baseResp.Code)

	stmtAddBatchReq := &StmtAddBatchReq{
		ReqID:  6,
		StmtID: initResp.StmtID,
	}
	_, err = stmtAddBatch(t, ws, stmtAddBatchReq)
	assert.NoError(t, err)
	stmtExecReq := &StmtExecReq{
		ReqID:  7,
		StmtID: initResp.StmtID,
	}
	_, err = stmtExec(t, ws, stmtExecReq)
	assert.NoError(t, err)

	versionResp, err := getVersion(t, ws)
	assert.NoError(t, err)
	assert.NotEmpty(t, versionResp.Version)
	err = ws.Close()
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	body := strings.NewReader("select * from stb")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql/test_ws_stmt_block", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	resultBody := fmt.Sprintf(`{"code":0,"column_meta":[["ts","TIMESTAMP",8],["c1","BOOL",1],["c2","TINYINT",1],["c3","SMALLINT",2],["c4","INT",4],["c5","BIGINT",8],["c6","TINYINT UNSIGNED",1],["c7","SMALLINT UNSIGNED",2],["c8","INT UNSIGNED",4],["c9","BIGINT UNSIGNED",8],["c10","FLOAT",4],["c11","DOUBLE",8],["c12","VARCHAR",20],["c13","NCHAR",20],["c14","VARCHAR",20],["c15","GEOMETRY",100],["info","JSON",4095]],"data":[["%s",true,2,3,4,5,6,7,8,9,10,11,"binary","nchar","test_varbinary","010100000000000000000059400000000000005940",{"a":"b"}],["%s",false,22,33,44,55,66,77,88,99,1010,1111,"binary2","nchar2","test_varbinary2","010100000000000000000059400000000000005940",{"a":"b"}],["%s",null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,{"a":"b"}]],"rows":3}`, now.UTC().Format(layout.LayoutNanoSecond), now.Add(time.Second).UTC().Format(layout.LayoutNanoSecond), now.Add(time.Second*2).UTC().Format(layout.LayoutNanoSecond))
	assert.Equal(t, resultBody, w.Body.String())
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

func conn(t *testing.T, ws *websocket.Conn, req *StmtConnectReq) (*StmtConnectResp, error) {
	var resp StmtConnectResp
	err := sendJsonBackJson(t, ws, STMTConnect, req, &resp)
	return &resp, err
}

func stmtInit(t *testing.T, ws *websocket.Conn, req *StmtInitReq) (*StmtInitResp, error) {
	var resp StmtInitResp
	err := sendJsonBackJson(t, ws, STMTInit, req, &resp)
	return &resp, err
}

func stmtPrepare(t *testing.T, ws *websocket.Conn, req *StmtPrepareReq) (*StmtPrepareResp, error) {
	var resp StmtPrepareResp
	err := sendJsonBackJson(t, ws, STMTPrepare, req, &resp)
	return &resp, err
}

func stmtSetTableName(t *testing.T, ws *websocket.Conn, req *StmtSetTableNameReq) (*StmtSetTableNameResp, error) {
	var resp StmtSetTableNameResp
	err := sendJsonBackJson(t, ws, STMTSetTableName, req, &resp)
	return &resp, err
}

func stmtSetTags(t *testing.T, ws *websocket.Conn, req *StmtSetTagsReq) (*StmtSetTagsResp, error) {
	var resp StmtSetTagsResp
	err := sendJsonBackJson(t, ws, STMTSetTags, req, &resp)
	return &resp, err
}

func stmtGetTagFields(t *testing.T, ws *websocket.Conn, req *StmtGetTagFieldsReq) (*StmtGetTagFieldsResp, error) {
	var resp StmtGetTagFieldsResp
	err := sendJsonBackJson(t, ws, STMTGetTagFields, req, &resp)
	return &resp, err
}

func stmtGetColFields(t *testing.T, ws *websocket.Conn, req *StmtGetColFieldsReq) (*StmtGetColFieldsResp, error) {
	var resp StmtGetColFieldsResp
	err := sendJsonBackJson(t, ws, STMTGetColFields, req, &resp)
	return &resp, err
}

func stmtAddBatch(t *testing.T, ws *websocket.Conn, req *StmtAddBatchReq) (*StmtAddBatchResp, error) {
	var resp StmtAddBatchResp
	err := sendJsonBackJson(t, ws, STMTAddBatch, req, &resp)
	return &resp, err
}

func stmtExec(t *testing.T, ws *websocket.Conn, req *StmtExecReq) (*StmtExecResp, error) {
	var resp StmtExecResp
	err := sendJsonBackJson(t, ws, STMTExec, req, &resp)
	return &resp, err
}

func stmtBind(t *testing.T, ws *websocket.Conn, req *StmtBindReq) (*StmtBindResp, error) {
	var resp StmtBindResp
	err := sendJsonBackJson(t, ws, STMTBind, req, &resp)
	return &resp, err
}

func getVersion(t *testing.T, ws *websocket.Conn) (*wstool.WSVersionResp, error) {
	var resp wstool.WSVersionResp
	err := sendJsonBackJson(t, ws, wstool.ClientVersion, nil, &resp)
	return &resp, err
}

func TestDropUser(t *testing.T) {
	s := httptest.NewServer(router)
	defer s.Close()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/rest/stmt", nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()
	defer doRestful("drop user test_ws_stmt_drop_user", "")
	code, message := doRestful("create user test_ws_stmt_drop_user pass 'pass_123'", "")
	assert.Equal(t, 0, code, message)
	// connect
	connReq := &StmtConnectReq{ReqID: 1, User: "test_ws_stmt_drop_user", Password: "pass_123"}
	connResp, err := conn(t, ws, connReq)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), connResp.ReqID)
	assert.Equal(t, 0, connResp.Code, connResp.Message)
	// drop user
	code, message = doRestful("drop user test_ws_stmt_drop_user", "")
	assert.Equal(t, 0, code, message)
	time.Sleep(time.Second * 3)
	resp, err := doWebSocket(ws, wstool.ClientVersion, nil)
	assert.Error(t, err, resp)
}
