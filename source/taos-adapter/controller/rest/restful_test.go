package rest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/layout"
	"github.com/taosdata/taosadapter/v3/tools/limiter"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
	"github.com/taosdata/taosadapter/v3/tools/testtools/testenv"
)

var router *gin.Engine

func TestMain(m *testing.M) {
	viper.Set("pool.maxConnect", 10000)
	viper.Set("pool.maxIdle", 10000)
	viper.Set("logLevel", "trace")
	viper.Set("uploadKeeper.enable", false)
	config.Init()
	config.Conf.Request = &config.Request{
		QueryLimitEnable:                 false,
		ExcludeQueryLimitSql:             []string{"selectserver_version()", "select1"},
		ExcludeQueryLimitSqlMaxByteCount: 22,
		ExcludeQueryLimitSqlMinByteCount: 7,
		ExcludeQueryLimitSqlRegex:        []*regexp.Regexp{regexp.MustCompile(`(?i)^select\s+.*from\s+information_schema.*`)},
		Default: &config.LimitConfig{
			QueryLimit:       1,
			QueryWaitTimeout: 2,
			QueryMaxWait:     0,
		},
	}
	db.PrepareConnection()
	log.ConfigLog()
	gin.SetMode(gin.ReleaseMode)
	router = gin.New()
	var ctl Restful
	ctl.Init(router)
	var configCtl = &ConfigController{}
	configCtl.Init(router)
	os.Exit(m.Run())
}

type TDEngineRestfulObjectResp struct {
	Code       int                      `json:"code,omitempty"`
	Desc       string                   `json:"desc,omitempty"`
	ColumnMeta [][]interface{}          `json:"column_meta,omitempty"`
	Data       []map[string]interface{} `json:"data,omitempty"`
	Rows       int                      `json:"rows,omitempty"`
	Timing     uint64                   `json:"timing,omitempty"`
}

func BenchmarkRestful(b *testing.B) {
	w := httptest.NewRecorder()
	for i := 0; i < b.N; i++ {
		body := strings.NewReader("show databases")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
		router.ServeHTTP(w, req)
		assert.Equal(b, 200, w.Code)
	}
}

// @author: xftan
// @date: 2021/12/14 15:10
// @description: test restful sql
func TestSql(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("show databases")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql/log", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

// @author: xftan
// @date: 2021/12/14 15:11
// @description: test restful login
func TestLogin(t *testing.T) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/rest/login/root/password", nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code)
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/rest/login/root111111111111111111111111111/password", nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code, w.Body)
	w = httptest.NewRecorder()
	password := "Aa!@#$%^&*()-_+=[]{}:;><?|~,."
	body := strings.NewReader(fmt.Sprintf("create user test_login pass '%s'", password))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code, w.Body.String())
	w = httptest.NewRecorder()
	escapedPass := url.PathEscape(password)
	req, _ = http.NewRequest(http.MethodGet, fmt.Sprintf("/rest/login/test_login/%s", escapedPass), nil)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	router.ServeHTTP(w, req)
	//hW4RbUYsnJP3d0A+Je4RNtryp201ma04/lzvLFFGxbLqp9fzHWNLSX9EkmY7TSyMLvEc3wbv93I=
	assert.Equal(t, 200, w.Code)
	resp := struct {
		Code int    `json:"code,omitempty"`
		Desc string `json:"desc,omitempty"`
	}{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Code)
	assert.Equal(t, "hW4RbUYsnJP3d0A+Je4RNtryp201ma04/lzvLFFGxbLqp9fzHWNLSX9EkmY7TSyMLvEc3wbv93I=", resp.Desc)
}

// @author: xftan
// @date: 2021/12/14 15:11
// @description: test restful wrong sql
func TestWrongSql(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("wrong sql")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql/log", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

// @author: xftan
// @date: 2021/12/14 15:11
// @description: test restful no sql
func TestNoSql(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql/log", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code)
}

// @author: xftan
// @date: 2022/1/10 15:15
// @description: test restful all type query
func TestAllType(t *testing.T) {
	now := time.Now().Local().UnixNano() / 1e6
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_alltype")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	assert.NoError(t, testtools.EnsureDBCreated("test_alltype"))
	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists alltype(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20),v14 varbinary(20),v15 geometry(100),v16 decimal(20,4),v17 blob) tags (info json)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_alltype", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	insertSql := fmt.Sprintf(`insert into t1 using alltype tags('{"table":"t1"}') values 
(%d,true,2,3,4,5,6,7,8,9,10.123,11.123,'中文"binary','中文nchar','\xaabbcc','point(100 100)',4467440737095516.123,'\x010203ffdd')
(%d,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)`,
		now, now+1)
	body = strings.NewReader(insertSql)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_alltype", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`select alltype.*,info->'table' from alltype where ts >= %d`, now))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_alltype", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	var result TDEngineRestfulRespDoc
	err := json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)
	expectMeta := [][]interface{}{
		{"ts", "TIMESTAMP", float64(8)},
		{"v1", "BOOL", float64(1)},
		{"v2", "TINYINT", float64(1)},
		{"v3", "SMALLINT", float64(2)},
		{"v4", "INT", float64(4)},
		{"v5", "BIGINT", float64(8)},
		{"v6", "TINYINT UNSIGNED", float64(1)},
		{"v7", "SMALLINT UNSIGNED", float64(2)},
		{"v8", "INT UNSIGNED", float64(4)},
		{"v9", "BIGINT UNSIGNED", float64(8)},
		{"v10", "FLOAT", float64(4)},
		{"v11", "DOUBLE", float64(8)},
		{"v12", "VARCHAR", float64(20)},
		{"v13", "NCHAR", float64(20)},
		{"v14", "VARBINARY", float64(20)},
		{"v15", "GEOMETRY", float64(100)},
		{"v16", "DECIMAL(20,4)", float64(16)},
		{"v17", "BLOB", float64(4194304)},
		{"info", "JSON", float64(4095)},
		{"info->'table'", "JSON", float64(4095)},
	}
	assert.Equal(t, expectMeta, result.ColumnMeta)
	expect := [19]interface{}{
		true,
		float64(2),
		float64(3),
		float64(4),
		float64(5),
		float64(6),
		float64(7),
		float64(8),
		float64(9),
		float64(10.123),
		float64(11.123),
		"中文\"binary",
		"中文nchar",
		"aabbcc",
		"010100000000000000000059400000000000005940",
		"4467440737095516.1230",
		"010203ffdd",
		map[string]interface{}{"table": "t1"},
		"t1",
	}
	assert.Equal(t, 0, result.Code)
	for i := 0; i < len(expect); i++ {
		assert.Equal(t, expect[i], result.Data[0][i+1])
	}
	for i := 0; i < len(expect)-2; i++ {
		assert.Nil(t, result.Data[1][i+1])
	}
	assert.Equal(t, expect[len(expect)-2], result.Data[1][len(expect)-1])
	assert.Equal(t, expect[len(expect)-1], result.Data[1][len(expect)])
	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`select alltype.*,info->'table' from alltype where ts = %d`, now))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_alltype?row_with_meta=true&timing=true", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	var objResult TDEngineRestfulObjectResp
	err = json.Unmarshal(w.Body.Bytes(), &objResult)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Code)
	assert.Greater(t, objResult.Timing, uint64(0))
	for i := 0; i < len(expect); i++ {
		colName := objResult.ColumnMeta[i+1][0].(string)
		assert.Equal(t, expect[i], objResult.Data[0][colName])
	}
	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into t2 using alltype tags('{"table":"t2"}') values (%d,true,2,3,4,5,6,7,8,9,10.123,11.123,'中文"binary','中文nchar','\xaabbcc','point(100 100)',6467440737095516.123,'\x010203ffdd')`, now))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_alltype?row_with_meta=true", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	err = json.Unmarshal(w.Body.Bytes(), &objResult)
	assert.NoError(t, err)
	assert.Equal(t, []byte(`{"code":0,"column_meta":[["affected_rows","INT",4]],"data":[{"affected_rows":1}],"rows":1}`), w.Body.Bytes())

	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into t3 using alltype tags('{"table":"t3"}') values (%d,true,2,3,4,5,6,7,8,9,10.123,11.123,'中文"binary','中文nchar','\xaabbcc','point(100 100)',8467440737095516.123,'\x010203ffdd')`, now))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_alltype?row_with_meta=true&timing=true", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	err = json.Unmarshal(w.Body.Bytes(), &objResult)
	assert.NoError(t, err)
	assert.Greater(t, objResult.Timing, uint64(0))
	expectRet := fmt.Sprintf(`{"code":0,"column_meta":[["affected_rows","INT",4]],"data":[{"affected_rows":1}],"rows":1,"timing":%d}`, objResult.Timing)
	assert.Equal(t, []byte(expectRet), w.Body.Bytes())

	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists test_alltype")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

// @author: xftan
// @date: 2022/1/18 18:12
// @description: test restful row limit
func TestRowLimit(t *testing.T) {
	config.Conf.RestfulRowLimit = 1
	now := time.Now().Local().UnixNano() / 1e6
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_rowlimit")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	assert.NoError(t, testtools.EnsureDBCreated("test_rowlimit"))
	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists test_rowlimit(ts timestamp,v1 bool,v2 tinyint,v3 smallint,v4 int,v5 bigint,v6 tinyint unsigned,v7 smallint unsigned,v8 int unsigned,v9 bigint unsigned,v10 float,v11 double,v12 binary(20),v13 nchar(20)) tags (info json)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_rowlimit", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into t1 using test_rowlimit tags('{"table":"t1"}') values (%d,true,2,3,4,5,6,7,8,9,10,11,'中文"binary','中文nchar')(%d,false,12,13,14,15,16,17,18,19,110,111,'中文"binary','中文nchar')`, now, now+1))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_rowlimit", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader(`select test_rowlimit.*,info->'table' from test_rowlimit`)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_rowlimit", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	var result TDEngineRestfulRespDoc
	err := json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Code)
	assert.Equal(t, 1, len(result.Data))
	assert.Equal(t, true, result.Data[0][1])
	assert.Equal(t, float64(2), result.Data[0][2])
	assert.Equal(t, float64(3), result.Data[0][3])
	assert.Equal(t, float64(4), result.Data[0][4])
	assert.Equal(t, float64(5), result.Data[0][5])
	assert.Equal(t, float64(6), result.Data[0][6])
	assert.Equal(t, float64(7), result.Data[0][7])
	assert.Equal(t, float64(8), result.Data[0][8])
	assert.Equal(t, float64(9), result.Data[0][9])
	assert.Equal(t, float64(10), result.Data[0][10])
	assert.Equal(t, float64(11), result.Data[0][11])
	assert.Equal(t, "中文\"binary", result.Data[0][12])
	assert.Equal(t, "中文nchar", result.Data[0][13])
	assert.Equal(t, map[string]interface{}{"table": "t1"}, result.Data[0][14])
	assert.Equal(t, "t1", result.Data[0][15])
	config.Conf.RestfulRowLimit = -1
	w = httptest.NewRecorder()
	body = strings.NewReader(`drop database if exists test_rowlimit`)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

func TestWrongReqID(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql/log?req_id=wrong", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code)
	var resp TDEngineRestfulRespDoc
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0xffff, resp.Code)
	if !strings.HasPrefix(resp.Desc, "illegal param, req_id must be numeric") {
		t.Errorf("wrong desc %s", resp.Desc)
	}
}

func TestWrongRowWithMeta(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader("")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql/log?row_with_meta=wrong", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code)
	var resp TDEngineRestfulRespDoc
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0xffff, resp.Code)
	if !strings.HasPrefix(resp.Desc, "illegal param, row_with_meta must be boolean") {
		t.Errorf("wrong desc %s", resp.Desc)
	}
}

func TestWrongEmptySql(t *testing.T) {
	w := httptest.NewRecorder()
	body := strings.NewReader(" ")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql/log", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code)
	var resp TDEngineRestfulRespDoc
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, httperror.HTTP_NO_SQL_INPUT, resp.Code)
	if !strings.HasPrefix(resp.Desc, "no sql input") {
		t.Errorf("wrong desc %s", resp.Desc)
	}
}

type ErrorReader struct{}

func (e *ErrorReader) Read(_ []byte) (n int, err error) {
	return 0, errors.New("forced read error")
}

func TestWrongRequest(t *testing.T) {
	w := httptest.NewRecorder()
	body := &ErrorReader{}
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql/log", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code)
	var resp TDEngineRestfulRespDoc
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, httperror.HTTP_INVALID_CONTENT_LENGTH, resp.Code)
	if !strings.HasPrefix(resp.Desc, "invalid content length") {
		t.Errorf("wrong desc %s", resp.Desc)
	}
}

func TestUpload(t *testing.T) {
	data := `2022-08-30 11:45:30.754,123,123.123000000,"abcd,abcd"
2022-08-30 11:45:40.871,123,123.123000000,"a""bcd,abcd"
2022-08-30 11:45:47.039,123,123.123000000,"abcd"",""abcd"
2022-08-30 11:47:22.607,123,123.123000000,"abcd"",""abcd"
2022-08-30 11:52:40.548,123,123.123000000,"abcd','abcd"
2022-08-30 11:52:40.549,123,123.123000000,
`
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists `test_Upload`")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	assert.NoError(t, testtools.EnsureDBCreated("test_Upload"))

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists `test_Upload`.`T2`(ts timestamp,n1 int,n2 double,n3 binary(30))")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	payload := &bytes.Buffer{}
	writer := multipart.NewWriter(payload)
	part1, _ := writer.CreateFormFile("data", filepath.Base("sql.csv"))
	_, err := io.Copy(part1, strings.NewReader(data))
	if err != nil {
		t.Error(err)
		return
	}
	err = writer.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	req, _ = http.NewRequest(http.MethodPost, "/rest/upload?db=test_Upload&table=T2&batch=10", payload)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	req.Header.Set("Content-Type", writer.FormDataContentType())
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("select n1,n2,n3 from `test_Upload`.`T2`")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	var result TDEngineRestfulRespDoc
	err = json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Code)
	assert.Equal(t, 6, len(result.Data))
	for i := 0; i < 6; i++ {
		result.Data[i] = append(result.Data[i], float64(123))
		result.Data[i] = append(result.Data[i], 123.123)
	}
	result.Data[0][2] = "abcd,abcd"
	result.Data[1][2] = "a\"bcd,abcd"
	result.Data[2][2] = "abcd\",\"abcd"
	result.Data[3][2] = "abcd\",\"abcd"
	result.Data[4][2] = "abcd','abcd"
	result.Data[5][2] = nil

	w = httptest.NewRecorder()
	body = strings.NewReader("drop database if exists `test_Upload`")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

func TestPrecision(t *testing.T) {
	config.Conf.RestfulRowLimit = -1
	now := time.Now().Unix()
	nowT := time.Unix(now, 0)
	{
		w := httptest.NewRecorder()
		body := strings.NewReader("create database if not exists test_pc_ms precision 'ms'")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		assert.NoError(t, testtools.EnsureDBCreated("test_pc_ms"))

		w = httptest.NewRecorder()
		body = strings.NewReader("create table if not exists t1(ts timestamp,v1 bool)")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_ms", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader(fmt.Sprintf(`insert into t1 values ('%s',true)`, nowT.Format(time.RFC3339Nano)))
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_ms", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader(`select * from t1 limit 1`)
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_ms", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		var result TDEngineRestfulRespDoc
		err := json.Unmarshal(w.Body.Bytes(), &result)
		assert.NoError(t, err)
		spT := strings.Split(result.Data[0][0].(string), ".")
		assert.Equal(t, 2, len(spT))
		assert.Equal(t, "000Z", spT[1])
		w = httptest.NewRecorder()
		body = strings.NewReader(`drop database test_pc_ms`)
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}
	{
		w := httptest.NewRecorder()
		body := strings.NewReader("create database if not exists test_pc_us precision 'us'")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		assert.NoError(t, testtools.EnsureDBCreated("test_pc_us"))
		w = httptest.NewRecorder()
		body = strings.NewReader("create table if not exists t1(ts timestamp,v1 bool)")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_us", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader(fmt.Sprintf(`insert into t1 values ('%s',true)`, nowT.Format(time.RFC3339Nano)))
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_us", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader(`select * from t1 limit 1`)
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_us", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		var result TDEngineRestfulRespDoc
		err := json.Unmarshal(w.Body.Bytes(), &result)
		assert.NoError(t, err)
		spT := strings.Split(result.Data[0][0].(string), ".")
		assert.Equal(t, 2, len(spT))
		assert.Equal(t, "000000Z", spT[1])
		w = httptest.NewRecorder()
		body = strings.NewReader(`drop database test_pc_us`)
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}
	{
		w := httptest.NewRecorder()
		body := strings.NewReader("create database if not exists test_pc_ns precision 'ns'")
		req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		assert.NoError(t, testtools.EnsureDBCreated("test_pc_ns"))

		w = httptest.NewRecorder()
		body = strings.NewReader("create table if not exists t1(ts timestamp,v1 bool)")
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_ns", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader(fmt.Sprintf(`insert into t1 values ('%s',true)`, nowT.Format(time.RFC3339Nano)))
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_ns", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		w = httptest.NewRecorder()
		body = strings.NewReader(`select * from t1 limit 1`)
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_pc_ns", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		var result TDEngineRestfulRespDoc
		err := json.Unmarshal(w.Body.Bytes(), &result)
		assert.NoError(t, err)
		spT := strings.Split(result.Data[0][0].(string), ".")
		assert.Equal(t, 2, len(spT))
		assert.Equal(t, "000000000Z", spT[1])
		w = httptest.NewRecorder()
		body = strings.NewReader(`drop database test_pc_ns`)
		req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}
}

func TestTimeZone(t *testing.T) {
	config.Conf.RestfulRowLimit = -1
	now := time.Now().Unix()
	nowT := time.Unix(now, 0)
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists test_tz")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	assert.NoError(t, testtools.EnsureDBCreated("test_tz"))
	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists t1(ts timestamp,v1 bool)")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_tz", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	body = strings.NewReader(fmt.Sprintf(`insert into t1 values ('%s',true)`, nowT.Format(time.RFC3339Nano)))
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql/test_tz", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	//Asia/Shanghai
	testZone := map[string][]string{
		"UTC":           {"000Z"},
		"Asia/Shanghai": {"000+08:00"},
		"Europe/Moscow": {"000+03:00"},
		//Daylight Saving Time
		"America/New_York": {"000-04:00", "000-05:00"},
	}
	for zone, timeZone := range testZone {
		w := httptest.NewRecorder()
		body := strings.NewReader(`select * from t1 limit 1`)
		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("/rest/sql/test_tz?tz=%s", zone), body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
		var result TDEngineRestfulRespDoc
		err := json.Unmarshal(w.Body.Bytes(), &result)
		assert.NoError(t, err)
		spT := strings.Split(result.Data[0][0].(string), ".")
		assert.Equal(t, 2, len(spT))
		assert.Contains(t, timeZone, spT[1])
	}
	w = httptest.NewRecorder()
	body = strings.NewReader(`drop database test_tz`)
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
}

func TestBadRequest(t *testing.T) {
	config.Conf.RestfulRowLimit = -1
	config.Conf.HttpCodeServerError = true
	w := httptest.NewRecorder()
	body := strings.NewReader("wrong sql")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)

	config.Conf.HttpCodeServerError = false
	w = httptest.NewRecorder()
	body = strings.NewReader("wrong sql")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestInternalError(t *testing.T) {
	config.Conf.RestfulRowLimit = -1
	config.Conf.HttpCodeServerError = true
	w := httptest.NewRecorder()
	body := strings.NewReader("CREATE MNODE ON DNODE 1")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)

	config.Conf.HttpCodeServerError = false
	w = httptest.NewRecorder()
	body = strings.NewReader("CREATE MNODE ON DNODE 1")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestSetConnectionOptions(t *testing.T) {
	config.Conf.RestfulRowLimit = -1
	w := httptest.NewRecorder()
	body := strings.NewReader("create database if not exists rest_test_options")
	url := "/rest/sql?app=rest_test_options&ip=192.168.100.1&conn_tz=Europe/Moscow&tz=Asia/Shanghai"
	req, _ := http.NewRequest(http.MethodPost, url, body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	checkResp(t, w)
	assert.NoError(t, testtools.EnsureDBCreated("rest_test_options"))
	defer func() {
		body := strings.NewReader("drop database if exists rest_test_options")
		req, _ := http.NewRequest(http.MethodPost, url, body)
		req.RemoteAddr = testtools.GetRandomRemoteAddr()
		req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		checkResp(t, w)
	}()

	w = httptest.NewRecorder()
	body = strings.NewReader("create table if not exists rest_test_options.t1(ts timestamp,v1 bool)")
	req.Body = io.NopCloser(body)
	router.ServeHTTP(w, req)
	checkResp(t, w)

	w = httptest.NewRecorder()
	ts := "2024-12-04 12:34:56.789"
	body = strings.NewReader(fmt.Sprintf(`insert into rest_test_options.t1 values ('%s',true)`, ts))
	req.Body = io.NopCloser(body)
	router.ServeHTTP(w, req)
	checkResp(t, w)

	w = httptest.NewRecorder()
	body = strings.NewReader(`select * from rest_test_options.t1 where ts = '2024-12-04 12:34:56.789'`)
	req.Body = io.NopCloser(body)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	var result TDEngineRestfulRespDoc
	err := json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Code)
	assert.Equal(t, 1, len(result.Data))

	location, err := time.LoadLocation("Europe/Moscow")
	assert.NoError(t, err)
	expectTime, err := time.ParseInLocation("2006-01-02 15:04:05.000", ts, location)
	assert.NoError(t, err)
	expectTimeStr := expectTime.Format(layout.LayoutMillSecond)
	assert.Equal(t, expectTimeStr, result.Data[0][0])
	t.Log(expectTimeStr, result.Data[0][0])

	// wrong timezone
	wrongTZUrl := "/rest/sql?app=rest_test_options&ip=192.168.100.1&tz=xxx"
	body = strings.NewReader(`select * from rest_test_options.t1 where ts = '2024-12-04 12:34:56.789'`)
	req, _ = http.NewRequest(http.MethodPost, wrongTZUrl, body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code)

	// wrong conn_tz
	wrongConnTZUrl := "/rest/sql?app=rest_test_options&ip=192.168.100.1&conn_tz=xxx"
	body = strings.NewReader(`select * from rest_test_options.t1 where ts = '2024-12-04 12:34:56.789'`)
	req, _ = http.NewRequest(http.MethodPost, wrongConnTZUrl, body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code)
	// wrong ip
	wrongIPUrl := "/rest/sql?app=rest_test_options&ip=xxx.xxx.xxx.xxx&conn_tz=Europe/Moscow&tz=Asia/Shanghai"
	req, _ = http.NewRequest(http.MethodPost, wrongIPUrl, body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	w = httptest.NewRecorder()
	body = strings.NewReader(`select * from rest_test_options.t1 where ts = '2024-12-04 12:34:56.789'`)
	req.Body = io.NopCloser(body)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	err = json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)
	// todo: engine should check the ip address
	//assert.NotEqual(t, 0, result.Code)
}

func checkResp(t *testing.T, w *httptest.ResponseRecorder) {
	assert.Equal(t, 200, w.Code)
	var result TDEngineRestfulRespDoc
	err := json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Code)
}

func Test_avoidNegativeDuration(t *testing.T) {
	type args struct {
		duration int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "negative",
			args: args{
				duration: -1,
			},
			want: 0,
		},
		{
			name: "positive",
			args: args{
				duration: 1,
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, avoidNegativeDuration(tt.args.duration), "avoidNegativeDuration(%v)", tt.args.duration)
		})
	}
}

func TestLimitQuery(t *testing.T) {
	config.Conf.Request.QueryLimitEnable = true
	config.Conf.Request.Default.QueryMaxWait = 0
	defer func() {
		config.Conf.Request.QueryLimitEnable = false
		limiter.GlobalLimiterMap.Clear()
	}()
	limiter.GlobalLimiterMap.Range(func(key, value interface{}) bool {
		t.Logf("key:%v,value:%v", key, value)
		return true
	})
	limiter.GlobalLimiterMap.Clear()
	wg := sync.WaitGroup{}
	wg.Add(10)
	// 10 concurrent queries, unlimited query wait time
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			body := strings.NewReader("select 2")
			req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
			req.RemoteAddr = testtools.GetRandomRemoteAddr()
			req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
			router.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code, w.Body.String())
		}()
	}
	wg.Wait()
	limiter.GlobalLimiterMap.Clear()
	config.Conf.Request.Default.QueryMaxWait = 1
	limited := false
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			body := strings.NewReader("select 2")
			req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
			req.RemoteAddr = testtools.GetRandomRemoteAddr()
			req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
			router.ServeHTTP(w, req)
			assert.Contains(t, []int{200, 503}, w.Code)
			if w.Code == 503 {
				limited = true
			}
		}()
	}
	wg.Wait()
	assert.True(t, limited)

	wg.Add(10)
	// 10 concurrent queries, unlimited sql
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			body := strings.NewReader("select 1")
			req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
			req.RemoteAddr = testtools.GetRandomRemoteAddr()
			req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
			router.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code)
		}()
	}
	wg.Wait()
	wg.Add(10)
	// 10 concurrent queries, unlimited sql regex
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			body := strings.NewReader("select * from information_schema.ins_databases")
			req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
			req.RemoteAddr = testtools.GetRandomRemoteAddr()
			req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
			router.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code)
		}()
	}
	wg.Wait()
}

func TestInfAndNan(t *testing.T) {
	checkResp(t, executeSQL("create database if not exists rest_test_inf_nan"))
	assert.NoError(t, testtools.EnsureDBCreated("rest_test_inf_nan"))
	defer func() {
		checkResp(t, executeSQL("drop database if exists rest_test_inf_nan"))
	}()
	logger := logrus.New().WithField("test", "TestInfAndNan")

	conn, err := syncinterface.TaosConnect("", "root", "taosdata", "", 0, logger, false)
	assert.NoError(t, err)
	defer syncinterface.TaosClose(conn, logger, false)
	res := syncinterface.TaosQuery(conn, "use rest_test_inf_nan", logger, false)
	defer func() {
		syncinterface.TaosSyncQueryFree(res, logger, false)
	}()
	code := syncinterface.TaosError(res, logger, false)
	assert.Equal(t, 0, code)
	now := time.Now()
	line := fmt.Sprintf(
		"measurement,host=host1 field1=1i,field2=2.0 %d\n"+
			"measurement,host=host1 field1=1i,field2=nan %d\n"+
			"measurement,host=host1 field1=1i,field2=inf %d",
		now.UnixNano(),
		now.Add(time.Second).UnixNano(),
		now.Add(time.Second*2).UnixNano(),
	)

	_, schemalessRes := syncinterface.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn, line, 1, "ns", 0, 0, "", logger, false)
	defer func() {
		syncinterface.TaosSyncQueryFree(schemalessRes, logger, false)
	}()
	code = syncinterface.TaosError(schemalessRes, logger, false)
	assert.Equal(t, 0, code)
	w := executeSQL("select _ts,field2 from rest_test_inf_nan.measurement order by _ts asc")
	assert.Equal(t, 200, w.Code)
	var result TDEngineRestfulRespDoc
	resp := w.Body.Bytes()
	t.Log(string(resp))
	err = json.Unmarshal(resp, &result)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Code)
	assert.Equal(t, 3, len(result.Data))
	assert.Equal(t, 2.0, result.Data[0][1])
	assert.Nil(t, result.Data[1][1])
	assert.Nil(t, result.Data[2][1])
}

func TestRejectSQL(t *testing.T) {
	v := viper.New()
	v.Set("rejectQuerySqlRegex", []string{"(?i)^drop\\s+database\\s+.*", "(?i)^drop\\s+table\\s+.*", "(?i)^alter\\s+table\\s+.*", `(?i)^select\s+.*from\s+testdb.*`})
	err := config.Conf.Reject.SetValue(v)
	require.NoError(t, err)
	defer func() {
		err = config.Conf.Reject.SetValue(viper.New())
		require.NoError(t, err)
	}()
	w := httptest.NewRecorder()
	body := strings.NewReader("DROP DATABASE testdb")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("DROP taBle testdb.stb")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("Alter table testdb.stb add column c1 int")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader(" select * from testdb.testtb ")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)

	w = httptest.NewRecorder()
	body = strings.NewReader("select * from information_schema.ins_databases")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

}

func TestTokenConnect(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("token test only for enterprise edition")
		return
	}
	w := httptest.NewRecorder()
	body := strings.NewReader("show databases")
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Bearer wrong")
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	var result TDEngineRestfulRespDoc
	err := json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, result.Code)

	w = executeSQL("create token test_token_restful from user root")
	err = json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Code)
	assert.Equal(t, 1, len(result.Data))
	token := result.Data[0][0].(string)
	assert.NoError(t, testtools.EnsureTokenCreated("test_token_restful"))

	defer func() {
		w = executeSQL("drop token test_token_restful")
		err = json.Unmarshal(w.Body.Bytes(), &result)
		assert.NoError(t, err)
		assert.Equal(t, 0, result.Code)
	}()

	w = httptest.NewRecorder()
	body = strings.NewReader("show databases")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	err = json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.Code)

	// with token param in url
	w = httptest.NewRecorder()
	body = strings.NewReader("show databases")
	req, _ = http.NewRequest(http.MethodPost, "/rest/sql?token=xxx", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func executeSQL(sql string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	body := strings.NewReader(sql)
	req, _ := http.NewRequest(http.MethodPost, "/rest/sql", body)
	req.RemoteAddr = testtools.GetRandomRemoteAddr()
	req.Header.Set("Authorization", "Taosd /KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
	router.ServeHTTP(w, req)
	return w
}
