package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/testutil"
	"github.com/taosdata/taoskeeper/util"
)

func TestAudit(t *testing.T) {
	cfg := config.GetCfg()
	cfg.Audit = config.Audit{
		Database: config.Database{
			Name: "keeper_test_audit",
		},
		Enable: true,
	}

	dbname := config.Conf.Audit.Database.Name
	defer func() {
		config.Conf.Audit.Database.Name = dbname
	}()
	config.Conf.Audit.Database.Name = "keeper_test_audit"

	audit := NewAudit(cfg)
	audit.Init(router)

	longDetails := strings.Repeat("0123456789", 5000)

	cases := []struct {
		name   string
		ts     int64
		detail string
		data   string
		expect string
	}{
		{
			name:   "1",
			ts:     1699839716440000000,
			data:   `{"timestamp": "1699839716440000000", "cluster_id": "cluster_id", "user": "user", "operation": "operation", "db":"dbnamea", "resource":"resourcenamea", "client_add": "localhost:30000", "details": "detail"}`,
			expect: "detail",
		},
		{
			name:   "2",
			ts:     1699839716441000000,
			data:   `{"timestamp": "1699839716441000000", "cluster_id": "cluster_id", "user": "user", "operation": "operation", "db":"dbnamea", "resource":"resourcenamea", "client_add": "localhost:30000", "details": "` + longDetails + `"}`,
			expect: longDetails[:50000],
		},
		{
			name:   "3",
			ts:     1699839716442000000,
			data:   "{\"timestamp\": \"1699839716442000000\", \"cluster_id\": \"cluster_id\", \"user\": \"user\", \"operation\": \"operation\", \"db\":\"dbnameb\", \"resource\":\"resourcenameb\", \"client_add\": \"localhost:30000\", \"details\": \"create database `meter` buffer 32 cachemodel 'none' duration 50d keep 3650d single_stable 0 wal_retention_period 3600 precision 'ms'\"}",
			expect: "create database `meter` buffer 32 cachemodel 'none' duration 50d keep 3650d single_stable 0 wal_retention_period 3600 precision 'ms'",
		},
		{
			name:   "4",
			ts:     1699839716443000000,
			data:   "{\"timestamp\": \"1699839716443000000\", \"cluster_id\": \"cluster_id\", \"user\": \"user\", \"operation\": \"operation\", \"db\":\"dbnamec\", \"resource\":\"resourcenamec\", \"client_add\": \"2002:09ba:0b4e:0006:be24:11ff:fe9c:03dd\", \"details\": \"detail\"}",
			expect: "detail",
		},
	}

	cases2 := []struct {
		name   string
		ts     int64
		detail string
		data   string
		expect string
	}{
		{
			name:   "1",
			ts:     1699839716445000000,
			data:   `{"timestamp":1699839716445, "cluster_id": "cluster_id", "user": "user", "operation": "operation", "db":"dbnamea", "resource":"resourcenamea", "client_add": "localhost:30000", "details": "details"}`,
			expect: "details",
		},
		{
			name:   "2",
			ts:     1699839716446000000,
			data:   `{"timestamp":1699839716446, "cluster_id": "cluster_id", "user": "user", "operation": "operation", "db":"dbnamea", "resource":"resourcenamea", "client_add": "2002:09ba:0b4e:0006:be24:11ff:fe9c:03dd", "details": "details"}`,
			expect: "details",
		},
	}

	conn, err := db.NewConnectorWithDb(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.Audit.Database.Name, cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer func() {
		_, _ = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", cfg.Audit.Database.Name), util.GetQidOwn(config.Conf.InstanceID))
	}()

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			body := strings.NewReader(c.data)
			req, _ := http.NewRequest(http.MethodPost, "/audit_v2", body)
			router.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code)

			data, err := conn.Query(context.Background(), fmt.Sprintf("select ts, details from %s.operations where ts=%d", cfg.Audit.Database.Name, c.ts), util.GetQidOwn(config.Conf.InstanceID))
			assert.NoError(t, err)
			assert.Equal(t, 1, len(data.Data))
			assert.Equal(t, c.expect, data.Data[0][1])
		})
	}

	for _, c := range cases2 {
		t.Run(c.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			body := strings.NewReader(c.data)
			req, _ := http.NewRequest(http.MethodPost, "/audit", body)
			router.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code)

			data, err := conn.Query(context.Background(), fmt.Sprintf("select ts, details from %s.operations where ts=%d", cfg.Audit.Database.Name, c.ts), util.GetQidOwn(config.Conf.InstanceID))
			assert.NoError(t, err)
			assert.Equal(t, 1, len(data.Data))
			assert.Equal(t, c.expect, data.Data[0][1])
		})
	}

	for _, c := range cases2 {
		t.Run(c.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			body := strings.NewReader(c.data)
			req, _ := http.NewRequest(http.MethodPost, "/audit", body)
			router.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code)

			data, err := conn.Query(context.Background(), fmt.Sprintf("select ts, details from %s.operations where ts=%d", cfg.Audit.Database.Name, c.ts), util.GetQidOwn(config.Conf.InstanceID))
			assert.NoError(t, err)
			assert.Equal(t, 1, len(data.Data))
			assert.Equal(t, c.expect, data.Data[0][1])
		})
	}

	MAX_SQL_LEN = 300
	// test audit batch
	input := `{"records":[{"timestamp":"1702548856940013848","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45166","db":"test","resource":"","details":"d630302"},{"timestamp":"1702548856939746458","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45230","db":"test","resource":"","details":"d130277"},{"timestamp":"1702548856939586665","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:50288","db":"test","resource":"","details":"d5268"},{"timestamp":"1702548856939528940","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:50222","db":"test","resource":"","details":"d255282"},{"timestamp":"1702548856939336371","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45126","db":"test","resource":"","details":"d755297"},{"timestamp":"1702548856939075131","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45122","db":"test","resource":"","details":"d380325"},{"timestamp":"1702548856938640661","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45152","db":"test","resource":"","details":"d255281"},{"timestamp":"1702548856938505795","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45122","db":"test","resource":"","details":"d130276"},{"timestamp":"1702548856938363319","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45178","db":"test","resource":"","details":"d755296"},{"timestamp":"1702548856938201478","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45166","db":"test","resource":"","details":"d380324"},{"timestamp":"1702548856937740618","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:50288","db":"test","resource":"","details":"d5266"}]}`

	defer func() {
		_, _ = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", cfg.Audit.Database.Name), util.GetQidOwn(config.Conf.InstanceID))
	}()

	t.Run("testbatch", func(t *testing.T) {
		//test empty array
		w1 := httptest.NewRecorder()
		body1 := strings.NewReader(`{"records": []}`)

		req1, _ := http.NewRequest(http.MethodPost, "/audit-batch", body1)
		router.ServeHTTP(w1, req1)
		assert.Equal(t, 200, w1.Code)

		//test 2 items array
		w := httptest.NewRecorder()
		body := strings.NewReader(input)
		req, _ := http.NewRequest(http.MethodPost, "/audit-batch", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		data, err := conn.Query(context.Background(), "select ts, details from "+cfg.Audit.Database.Name+".operations where cluster_id='8468922059162439502'", util.GetQidOwn(config.Conf.InstanceID))
		assert.NoError(t, err)
		assert.Equal(t, 11, len(data.Data))
	})
}

func TestNewAudit(t *testing.T) {
	cfg := config.Config{
		TDengine: config.TDengineRestful{
			Username: testutil.TestUsername(),
			Password: testutil.TestPassword(),
			Host:     "localhost",
			Port:     6041,
			Usessl:   false,
		},
		Audit: config.Audit{
			Database: config.Database{
				Name: "",
			},
			Enable: true,
		},
	}

	audit := NewAudit(&cfg)
	assert.Equal(t, "audit", audit.db)
}

func Test_handleDetails(t *testing.T) {
	details := handleDetails("\"")
	assert.Equal(t, "\\\"", details)

	details = handleDetails(strings.Repeat("a", 60000))
	assert.Equal(t, 50000, len(details))
}

func TestAudit_handleBatchFunc_NoConnection_Returns500(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	a := &Audit{conn: nil}
	r.POST("/audit-batch", a.handleBatchFunc())

	patch := gomonkey.ApplyPrivateMethod(reflect.TypeOf(a), "prepareConnectionAndTable", func(_ *Audit, c *gin.Context) bool {
		return true
	})
	defer patch.Reset()

	req := httptest.NewRequest(http.MethodPost, "/audit-batch", strings.NewReader(`{"records":[]}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)

	var body map[string]string
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "no connection", body["error"])
}

type errorReader struct{ err error }

func (e errorReader) Read(p []byte) (int, error) { return 0, e.err }

func TestAudit_handleBatchFunc_GetRawDataError_Returns400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	a := &Audit{conn: &db.Connector{}}
	r.POST("/audit-batch", a.handleBatchFunc())

	patch := gomonkey.ApplyPrivateMethod(reflect.TypeOf(a), "prepareConnectionAndTable", func(_ *Audit, c *gin.Context) bool {
		return true
	})
	defer patch.Reset()

	boom := errors.New("boom")
	req := httptest.NewRequest(http.MethodPost, "/audit-batch", errorReader{err: boom})
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusBadRequest)
	}

	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error: %v, raw=%q", err, w.Body.String())
	}
	want := "get audit data error. " + boom.Error()
	if body["error"] != want {
		t.Fatalf("error=%q, want %q", body["error"], want)
	}
}

func TestAudit_handleBatchFunc_TraceLogsWhenEnabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	old := logger.Logger.GetLevel()
	logger.Logger.SetLevel(logrus.TraceLevel)
	defer logger.Logger.SetLevel(old)

	a := &Audit{conn: &db.Connector{}}
	r.POST("/audit-batch", a.handleBatchFunc())

	patch := gomonkey.ApplyPrivateMethod(reflect.TypeOf(a), "prepareConnectionAndTable", func(_ *Audit, c *gin.Context) bool {
		return true
	})
	defer patch.Reset()

	req := httptest.NewRequest(http.MethodPost, "/audit-batch", strings.NewReader(`{"records":[]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-QID", "123")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusOK)
	}
}

func TestAudit_handleBatchFunc_ParseError_Returns400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	a := &Audit{conn: &db.Connector{}}
	r.POST("/audit-batch", a.handleBatchFunc())

	patch := gomonkey.ApplyPrivateMethod(reflect.TypeOf(a), "prepareConnectionAndTable", func(_ *Audit, c *gin.Context) bool {
		return true
	})
	defer patch.Reset()

	req := httptest.NewRequest(http.MethodPost, "/audit-batch", strings.NewReader(`{`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusBadRequest)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response error: %v, raw=%q", err, w.Body.String())
	}
	if !strings.HasPrefix(body["error"], "parse audit data error: ") {
		t.Fatalf("error prefix mismatch, got %q", body["error"])
	}
}

func TestAudit_handleBatchFunc_TraceNoRecords_LogsAndReturnsOK(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	old := logger.Logger.GetLevel()
	logger.Logger.SetLevel(logrus.TraceLevel)
	defer logger.Logger.SetLevel(old)

	a := &Audit{conn: &db.Connector{}}
	r.POST("/audit-batch", a.handleBatchFunc())

	patch := gomonkey.ApplyPrivateMethod(reflect.TypeOf(a), "prepareConnectionAndTable", func(_ *Audit, c *gin.Context) bool {
		return true
	})
	defer patch.Reset()

	req := httptest.NewRequest(http.MethodPost, "/audit-batch", strings.NewReader(`{"records":[]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-QID", "1")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusOK)
	}
}

func TestAudit_handleFunc_NoConnection_Returns500(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	a := &Audit{conn: nil}
	r.POST("/audit", a.handleFunc())

	patch := gomonkey.ApplyPrivateMethod(reflect.TypeOf(a), "prepareConnectionAndTable", func(_ *Audit, c *gin.Context) bool {
		return true
	})
	defer patch.Reset()

	req := httptest.NewRequest(http.MethodPost, "/audit", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-QID", "1")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusInternalServerError)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error: %v, raw=%q", err, w.Body.String())
	}
	if body["error"] != "no connection" {
		t.Fatalf(`error=%q, want "no connection"`, body["error"])
	}
}

func TestAudit_handleFunc_GetRawDataError_Returns400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	a := &Audit{conn: &db.Connector{}}
	r.POST("/audit", a.handleFunc())

	patch := gomonkey.ApplyPrivateMethod(reflect.TypeOf(a), "prepareConnectionAndTable", func(_ *Audit, c *gin.Context) bool {
		return true
	})
	defer patch.Reset()

	boom := errors.New("boom")
	req := httptest.NewRequest(http.MethodPost, "/audit", errorReader{err: boom})
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusBadRequest)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error: %v, raw=%q", err, w.Body.String())
	}
	want := "get audit data error. " + boom.Error()
	if body["error"] != want {
		t.Fatalf("error=%q, want %q", body["error"], want)
	}
}

func TestAudit_handleFunc_TraceLogsWhenEnabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	old := logger.Logger.GetLevel()
	logger.Logger.SetLevel(logrus.TraceLevel)
	defer logger.Logger.SetLevel(old)

	a := &Audit{conn: &db.Connector{}}
	r.POST("/audit", a.handleFunc())

	patch := gomonkey.ApplyPrivateMethod(reflect.TypeOf(a), "prepareConnectionAndTable", func(_ *Audit, c *gin.Context) bool {
		return true
	})
	defer patch.Reset()

	req := httptest.NewRequest(http.MethodPost, "/audit", strings.NewReader(`{`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-QID", "123")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusBadRequest)
	}
}

func TestAudit_handleFunc_ParseOldAuditError_ReturnsBadRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	a := &Audit{conn: &db.Connector{}}
	r.POST("/audit", a.handleFunc())

	patch := gomonkey.ApplyPrivateMethod(reflect.TypeOf(a), "prepareConnectionAndTable", func(_ *Audit, c *gin.Context) bool {
		return true
	})
	defer patch.Reset()

	body := `{
        "timestamp": 1690000000,
        "cluster_id": "c1",
        "user": 123,
        "operation": "op",
        "db": "d",
        "resource": "r",
        "client_add": "127.0.0.1",
        "details": "x"
    }`

	req := httptest.NewRequest(http.MethodPost, "/audit", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusBadRequest)
	}
	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response error: %v, raw=%q", err, w.Body.String())
	}
	if !strings.HasPrefix(resp["error"], "parse audit data error: ") {
		t.Fatalf("error prefix mismatch, got %q", resp["error"])
	}
}

func TestAudit_handleFunc_ParseStringTimestamp_Error_ReturnsBadRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	a := &Audit{conn: &db.Connector{}}
	r.POST("/audit", a.handleFunc())

	patch := gomonkey.ApplyPrivateMethod(reflect.TypeOf(a), "prepareConnectionAndTable", func(_ *Audit, c *gin.Context) bool {
		return true
	})
	defer patch.Reset()

	body := `{
        "timestamp": "1690000000000000000",
        "cluster_id": "c1",
        "user": 123,
        "operation": "op",
        "db": "d",
        "resource": "r",
        "client_add": "127.0.0.1",
        "details": "x"
    }`

	req := httptest.NewRequest(http.MethodPost, "/audit", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusBadRequest)
	}

	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response error: %v, raw=%q", err, w.Body.String())
	}
	if !strings.HasPrefix(resp["error"], "parse audit data error: ") {
		t.Fatalf("error prefix mismatch, got %q", resp["error"])
	}
}

func TestAudit_createSTables(t *testing.T) {
	cfg := config.GetCfg()

	conn1, err := db.NewConnector(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer conn1.Close()

	_, err = conn1.Query(context.Background(), "drop database if exists test_1766474758", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn1.Query(context.Background(), "create database test_1766474758", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	conn2, err := db.NewConnectorWithDb(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, "test_1766474758", cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer conn2.Close()

	audit := &Audit{
		conn: conn2,
	}

	err = audit.createSTable()
	assert.NoError(t, err)

	_, err = conn2.Query(context.Background(), "drop table operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	sql := `create stable operations(
		ts timestamp,
		user_name varchar(25),
		operation varchar(20),
		db varchar(65),
		resource varchar(193),
		client_address varchar(32),
		details varchar(50000)
	) tags (
		cluster_id varchar(64)
	)`
	_, err = conn2.Query(context.Background(), sql, util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	err = audit.createSTable()
	assert.NoError(t, err)

	_, err = conn2.Query(context.Background(), "drop database if exists test_1766474758", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
}

func TestAuditInfo_Unmarshal_NoExtraFields(t *testing.T) {
	jsonStr := `{
        "timestamp": "1699839716442000000",
        "cluster_id": "cluster_id",
        "user": "user",
        "operation": "opeation",
        "db": "db",
        "resource": "resource",
        "client_add": "127.0.0.1:3000",
        "details": "details"
    }`
	var info AuditInfo
	err := json.Unmarshal([]byte(jsonStr), &info)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), info.AffectedRows)
	assert.Equal(t, 0.0, info.Duration)
}

func TestAuditInfo_Unmarshal_WithExtraFields(t *testing.T) {
	jsonStr := `{
        "timestamp": "1699839716442000000",
        "cluster_id": "cluster_id",
        "user": "user",
        "operation": "opeation",
        "db": "db",
        "resource": "resource",
        "client_add": "127.0.0.1:3000",
        "details": "details",
		"affected_rows": 123,
        "duration": 4.56
    }`
	var info AuditInfo
	err := json.Unmarshal([]byte(jsonStr), &info)
	assert.NoError(t, err)
	assert.Equal(t, uint64(123), info.AffectedRows)
	assert.Equal(t, 4.56, info.Duration)
}

func TestAuditInfo_Unmarshal_NullFields(t *testing.T) {
	jsonStr := `{
        "timestamp": "1699839716442000000",
        "cluster_id": "cluster_id",
        "user": "user",
        "operation": "opeation",
        "db": "db",
        "resource": "resource",
        "client_add": "127.0.0.1:3000",
        "details": "details",
		"affected_rows": null,
		"duration": null
    }`
	var info AuditInfo
	err := json.Unmarshal([]byte(jsonStr), &info)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), info.AffectedRows)
	assert.Equal(t, 0.0, info.Duration)
}

func TestAuditInfo_Unmarshal_DurationExtremeValues(t *testing.T) {
	jsonStr := `{
        "timestamp": "1699839716442000000",
        "cluster_id": "cluster_id",
        "user": "user",
        "operation": "opeation",
        "db": "db",
        "resource": "resource",
        "client_add": "127.0.0.1:3000",
        "details": "details",
        "duration": 1e100
    }`
	var info AuditInfo
	err := json.Unmarshal([]byte(jsonStr), &info)
	assert.NoError(t, err)
	assert.Equal(t, 1e100, info.Duration)
}

func TestAuditInfo_AffectedRowsAndDurationVariants(t *testing.T) {
	cfg := config.GetCfg()
	cfg.Audit = config.Audit{
		Enable: true,
		Database: config.Database{
			Name: "test_1766482953",
		},
	}

	dbname := config.Conf.Audit.Database.Name
	defer func() {
		config.Conf.Audit.Database.Name = dbname
	}()
	config.Conf.Audit.Database.Name = "test_1766482953"

	gin.SetMode(gin.TestMode)
	router := gin.New()

	audit := NewAudit(cfg)
	audit.Init(router)

	cases := []struct {
		name                 string
		ts                   int64
		data                 string
		expect_affected_rows uint64
		expect_duration      float64
	}{
		{
			name:                 "no extra fields",
			ts:                   1699839716440000000,
			data:                 `{"timestamp":"1699839716440000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"}`,
			expect_affected_rows: 0,
			expect_duration:      0.0,
		},
		{
			name:                 "with extra fields",
			ts:                   1699839716441000000,
			data:                 `{"timestamp":"1699839716441000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":12345,"duration":12.345}`,
			expect_affected_rows: 12345,
			expect_duration:      12.345,
		},
		{
			name:                 "with null fields",
			ts:                   1699839716442000000,
			data:                 `{"timestamp":"1699839716442000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":null,"duration":null}`,
			expect_affected_rows: 0,
			expect_duration:      0.0,
		},
		{
			name:                 "duration extreme values",
			ts:                   1699839716443000000,
			data:                 `{"timestamp":"1699839716443000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":999999,"duration":1e+100}`,
			expect_affected_rows: 999999,
			expect_duration:      1e+100,
		},
	}

	conn, err := db.NewConnector(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer conn.Close()

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			body := strings.NewReader(c.data)
			req, _ := http.NewRequest(http.MethodPost, "/audit_v2", body)
			router.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code)

			data, err := conn.Query(context.Background(),
				fmt.Sprintf("select ts, affected_rows, `duration` from test_1766482953.operations where ts = %d",
					c.ts), util.GetQidOwn(config.Conf.InstanceID))
			assert.NoError(t, err)
			assert.Equal(t, 1, len(data.Data))
			assert.Equal(t, c.expect_affected_rows, data.Data[0][1])
			assert.Equal(t, c.expect_duration, data.Data[0][2])
		})
	}

	t.Run("testbatch", func(t *testing.T) {
		w := httptest.NewRecorder()
		body := strings.NewReader(`{"records": []}`)
		req, _ := http.NewRequest(http.MethodPost, "/audit-batch", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		w = httptest.NewRecorder()
		records := `{"records":[{"timestamp":"1699839716450000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"},{"timestamp":"1699839716451000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":12345,"duration":12.345},{"timestamp":"1699839716452000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":null,"duration":null},{"timestamp":"1699839716453000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":999999,"duration":1e+100}]}`
		body = strings.NewReader(records)
		req, _ = http.NewRequest(http.MethodPost, "/audit-batch", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		data, err := conn.Query(context.Background(),
			"select ts from test_1766482953.operations where cluster_id='cluster_id_batch'",
			util.GetQidOwn(config.Conf.InstanceID))
		assert.NoError(t, err)
		assert.Equal(t, 4, len(data.Data))
	})

	_, err = conn.Query(context.Background(), "drop database if exists test_1766482953", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
}

func TestAuditV2DefaultDB(t *testing.T) {
	cfg := config.GetCfg()
	cfg.Audit = config.Audit{
		Enable: true,
		Database: config.Database{
			Name: "test_1766564902",
		},
	}

	dbname := config.Conf.Audit.Database.Name
	defer func() {
		config.Conf.Audit.Database.Name = dbname
	}()
	config.Conf.Audit.Database.Name = "test_1766564902"

	gin.SetMode(gin.TestMode)
	router := gin.New()

	audit := NewAudit(cfg)
	audit.Init(router)

	records := []string{
		`{"timestamp":"1699839716442000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
		`{"timestamp":"1699839716452000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
		`{"timestamp":"1699839716462000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
		`{"timestamp":"1699839716472000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
	}
	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit_v2", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	conn, err := db.NewConnector(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer conn.Close()

	data, err := conn.Query(context.Background(), "select * from test_1766564902.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))

	_, err = conn.Query(context.Background(), "drop database if exists test_1766564902", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
}

func TestAuditDefaultDB(t *testing.T) {
	cfg := config.GetCfg()
	cfg.Audit = config.Audit{
		Enable: true,
		Database: config.Database{
			Name: "test_1766566175",
		},
	}

	dbname := config.Conf.Audit.Database.Name
	defer func() {
		config.Conf.Audit.Database.Name = dbname
	}()
	config.Conf.Audit.Database.Name = "test_1766566175"

	gin.SetMode(gin.TestMode)
	router := gin.New()

	audit := NewAudit(cfg)
	audit.Init(router)

	records := []string{
		`{"timestamp":"1699839716442000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"}`,
		`{"timestamp":"1699839716452000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"}`,
		`{"timestamp":"1699839716462000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"}`,
		`{"timestamp":"1699839716472000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"}`,
	}
	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	conn, err := db.NewConnector(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer conn.Close()

	data, err := conn.Query(context.Background(), "select * from test_1766566175.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))

	_, err = conn.Query(context.Background(), "drop database if exists test_1766566175", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
}

func TestAuditBatchDefaultDB(t *testing.T) {
	cfg := config.GetCfg()
	cfg.Audit = config.Audit{
		Enable: true,
		Database: config.Database{
			Name: "test_1766566446",
		},
	}

	dbname := config.Conf.Audit.Database.Name
	defer func() {
		config.Conf.Audit.Database.Name = dbname
	}()
	config.Conf.Audit.Database.Name = "test_1766566446"

	gin.SetMode(gin.TestMode)
	router := gin.New()

	audit := NewAudit(cfg)
	audit.Init(router)

	w := httptest.NewRecorder()
	records := `{"records":[{"timestamp":"1699839716450000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"},{"timestamp":"1699839716451000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":12345,"duration":12.345},{"timestamp":"1699839716452000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":null,"duration":null},{"timestamp":"1699839716453000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":999999,"duration":1e+100}]}`
	req, _ := http.NewRequest(http.MethodPost, "/audit-batch", strings.NewReader(records))
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	conn, err := db.NewConnector(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer conn.Close()

	data, err := conn.Query(context.Background(), "select * from test_1766566446.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))

	_, err = conn.Query(context.Background(), "drop database if exists test_1766566446", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
}

func TestAuditV2CustomDB(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	cfg := config.GetCfg()
	audit := NewAudit(cfg)
	audit.Init(router)

	conn, err := db.NewConnector(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer conn.Close()

	_, err = conn.Exec(context.Background(), "drop database if exists test_1766567690", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "create database test_1766567690 precision 'ns'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "drop database if exists test_1766719286", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "create database test_1766719286 precision 'ns'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	records := []string{
		`{"timestamp":"1699839716442000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
		`{"timestamp":"1699839716452000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
		`{"timestamp":"1699839716462000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
		`{"timestamp":"1699839716472000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit_v2?db=test_1766567690", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit_v2?db=test_1766719286", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit_v2?db=test_1766567690", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit_v2?db=test_1766719286", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	data, err := conn.Query(context.Background(), "select * from test_1766567690.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))
	_, err = conn.Query(context.Background(), "drop database if exists test_1766567690", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	data, err = conn.Query(context.Background(), "select * from test_1766719286.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))
	_, err = conn.Query(context.Background(), "drop database if exists test_1766719286", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
}

func TestAuditCustomDB(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	cfg := config.GetCfg()
	audit := NewAudit(cfg)
	audit.Init(router)

	conn, err := db.NewConnector(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer conn.Close()

	_, err = conn.Exec(context.Background(), "drop database if exists test_1766568746", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "create database test_1766568746 precision 'ns'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "drop database if exists test_1766719484", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "create database test_1766719484 precision 'ns'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	records := []string{
		`{"timestamp":"1699839716442000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"}`,
		`{"timestamp":"1699839716452000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"}`,
		`{"timestamp":"1699839716462000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"}`,
		`{"timestamp":"1699839716472000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"}`,
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit?db=test_1766568746", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit?db=test_1766719484", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit?db=test_1766568746", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit?db=test_1766719484", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	data, err := conn.Query(context.Background(), "select * from test_1766568746.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))
	_, err = conn.Query(context.Background(), "drop database if exists test_1766568746", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	data, err = conn.Query(context.Background(), "select * from test_1766719484.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))
	_, err = conn.Query(context.Background(), "drop database if exists test_1766719484", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
}

func TestAuditBatchCustomDB(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	cfg := config.GetCfg()
	audit := NewAudit(cfg)
	audit.Init(router)

	conn, err := db.NewConnector(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer conn.Close()

	_, err = conn.Exec(context.Background(), "drop database if exists test_1766568859", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "create database test_1766568859 precision 'ns'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "drop database if exists test_1766719552", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "create database test_1766719552 precision 'ns'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	records := `{"records":[{"timestamp":"1699839716450000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"},{"timestamp":"1699839716451000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":12345,"duration":12.345},{"timestamp":"1699839716452000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":null,"duration":null},{"timestamp":"1699839716453000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":999999,"duration":1e+100}]}`

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/audit-batch?db=test_1766568859", strings.NewReader(records))
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost, "/audit-batch?db=test_1766719552", strings.NewReader(records))
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost, "/audit-batch?db=test_1766568859", strings.NewReader(records))
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost, "/audit-batch?db=test_1766719552", strings.NewReader(records))
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	data, err := conn.Query(context.Background(), "select * from test_1766568859.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))
	_, err = conn.Query(context.Background(), "drop database if exists test_1766568859", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	data, err = conn.Query(context.Background(), "select * from test_1766719552.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))
	_, err = conn.Query(context.Background(), "drop database if exists test_1766719552", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
}

func TestAudit_createDBSql(t *testing.T) {
	tests := []struct {
		name      string
		db        string
		dbOptions map[string]interface{}
		want      string
	}{
		{
			name:      "no options",
			db:        "testdb",
			dbOptions: map[string]interface{}{},
			want:      "create database if not exists testdb precision 'ns' ",
		},
		{
			name: "string option",
			db:   "testdb",
			dbOptions: map[string]interface{}{
				"buffer": "32",
			},
			want: "create database if not exists testdb precision 'ns' buffer '32' ",
		},
		{
			name: "int option",
			db:   "testdb",
			dbOptions: map[string]interface{}{
				"duration": 50,
			},
			want: "create database if not exists testdb precision 'ns' duration 50 ",
		},
		{
			name: "float option",
			db:   "testdb",
			dbOptions: map[string]interface{}{
				"cache": 1.5,
			},
			want: "create database if not exists testdb precision 'ns' cache 1.5 ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			audit := &Audit{
				db:        tt.db,
				dbOptions: tt.dbOptions,
			}
			sql := audit.createDBSql()
			assert.Equal(t, tt.want, sql)
		})
	}
}

func IsEnterpriseTest() bool {
	if _, ok := os.LookupEnv("TEST_ENTERPRISE"); ok {
		return true
	}
	return false
}

func TestAuditV2CustomDBWithToken(t *testing.T) {
	if !IsEnterpriseTest() {
		t.Skip("only for TDengine Enterprise")
	}

	gin.SetMode(gin.TestMode)
	router := gin.New()

	cfg := config.GetCfg()
	audit := NewAudit(cfg)
	audit.Init(router)

	conn, err := db.NewConnector(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer conn.Close()

	_, err = conn.Exec(context.Background(), "drop database if exists test_1766991210", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "create database test_1766991210 precision 'ns'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "drop database if exists test_1766991212", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "create database test_1766991212 precision 'ns'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	conn.Exec(context.Background(), "drop user audit_v2_user", util.GetQidOwn(cfg.InstanceID))
	_, err = conn.Exec(context.Background(), "create user audit_v2_user pass 'token_pass_1'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "grant use,create table on database test_1766991210 to audit_v2_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "grant use,create table on database test_1766991212 to audit_v2_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "grant all on test_1766991210.* to audit_v2_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "grant all on test_1766991212.* to audit_v2_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)

	time.Sleep(10 * time.Second)

	data, err := conn.Query(context.Background(), "create token test_audit_v2_token from user audit_v2_user", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	token := data.Data[0][0].(string)

	records := []string{
		`{"timestamp":"1699839716442000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
		`{"timestamp":"1699839716452000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
		`{"timestamp":"1699839716462000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
		`{"timestamp":"1699839716472000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("/audit_v2?db=test_1766991210&token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("/audit_v2?db=test_1766991212&token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("/audit_v2?db=test_1766991210&token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("/audit_v2?db=test_1766991212&token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	data, err = conn.Query(context.Background(), "select * from test_1766991210.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))
	_, err = conn.Query(context.Background(), "drop database if exists test_1766991210", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	data, err = conn.Query(context.Background(), "select * from test_1766991212.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))
	_, err = conn.Query(context.Background(), "drop database if exists test_1766991212", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "drop user audit_v2_user", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
}

func TestAuditCustomDBWithToken(t *testing.T) {
	if !IsEnterpriseTest() {
		t.Skip("only for TDengine Enterprise")
	}

	gin.SetMode(gin.TestMode)
	router := gin.New()

	cfg := config.GetCfg()
	audit := NewAudit(cfg)
	audit.Init(router)

	conn, err := db.NewConnector(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer conn.Close()

	_, err = conn.Exec(context.Background(), "drop database if exists test_1766991791", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "create database test_1766991791 precision 'ns'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "drop database if exists test_1766991792", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "create database test_1766991792 precision 'ns'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	conn.Exec(context.Background(), "drop user audit_user", util.GetQidOwn(cfg.InstanceID))
	_, err = conn.Exec(context.Background(), "create user audit_user pass 'token_pass_1'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "grant use,create table on database test_1766991791 to audit_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "grant use,create table on database test_1766991792 to audit_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "grant all on test_1766991791.* to audit_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "grant all on test_1766991792.* to audit_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)

	time.Sleep(10 * time.Second)

	data, err := conn.Query(context.Background(), "create token test_audit_token from user audit_user", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	token := data.Data[0][0].(string)

	records := []string{
		`{"timestamp":"1699839716442000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"}`,
		`{"timestamp":"1699839716452000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"}`,
		`{"timestamp":"1699839716462000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"}`,
		`{"timestamp":"1699839716472000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"}`,
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("/audit?db=test_1766991791&token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("/audit?db=test_1766991792&token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("/audit?db=test_1766991791&token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("/audit?db=test_1766991792&token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	data, err = conn.Query(context.Background(), "select * from test_1766991791.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))
	_, err = conn.Query(context.Background(), "drop database if exists test_1766991791", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	data, err = conn.Query(context.Background(), "select * from test_1766991792.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))
	_, err = conn.Query(context.Background(), "drop database if exists test_1766991792", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "drop user audit_user", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
}

func TestAuditBatchCustomDBWithToken(t *testing.T) {
	if !IsEnterpriseTest() {
		t.Skip("only for TDengine Enterprise")
	}

	gin.SetMode(gin.TestMode)
	router := gin.New()

	cfg := config.GetCfg()
	audit := NewAudit(cfg)
	audit.Init(router)

	conn, err := db.NewConnector(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer conn.Close()

	_, err = conn.Exec(context.Background(), "drop database if exists test_1766992132", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "create database test_1766992132 precision 'ns'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "drop database if exists test_1766992133", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "create database test_1766992133 precision 'ns'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	conn.Exec(context.Background(), "drop user audit_batch_user", util.GetQidOwn(cfg.InstanceID))
	_, err = conn.Exec(context.Background(), "create user audit_batch_user pass 'token_pass_1'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "grant use,create table on database test_1766992132 to audit_batch_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "grant use,create table on database test_1766992133 to audit_batch_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "grant all on test_1766992132.* to audit_batch_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "grant all on test_1766992133.* to audit_batch_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)

	time.Sleep(10 * time.Second)

	data, err := conn.Query(context.Background(), "create token test_audit_batch_token from user audit_batch_user", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	token := data.Data[0][0].(string)

	records := `{"records":[{"timestamp":"1699839716450000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details"},{"timestamp":"1699839716451000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":12345,"duration":12.345},{"timestamp":"1699839716452000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":null,"duration":null},{"timestamp":"1699839716453000000","cluster_id":"cluster_id_batch","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":999999,"duration":1e+100}]}`

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost,
		fmt.Sprintf("/audit-batch?db=test_1766992132&token=%s", token), strings.NewReader(records))
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost,
		fmt.Sprintf("/audit-batch?db=test_1766992133&token=%s", token), strings.NewReader(records))
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost,
		fmt.Sprintf("/audit-batch?db=test_1766992132&token=%s", token), strings.NewReader(records))
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost,
		fmt.Sprintf("/audit-batch?db=test_1766992133&token=%s", token), strings.NewReader(records))
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	data, err = conn.Query(context.Background(), "select * from test_1766992132.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))
	_, err = conn.Query(context.Background(), "drop database if exists test_1766992132", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	data, err = conn.Query(context.Background(), "select * from test_1766992133.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))
	_, err = conn.Query(context.Background(), "drop database if exists test_1766992133", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "drop user audit_batch_user", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
}

func TestAuditV2SwitchDBAndToken(t *testing.T) {
	if !IsEnterpriseTest() {
		t.Skip("only for TDengine Enterprise")
	}

	gin.SetMode(gin.TestMode)
	router := gin.New()

	cfg := config.GetCfg()
	cfg.Audit.Database.Name = "test_1767001978"

	dbname := config.Conf.Audit.Database.Name
	defer func() {
		config.Conf.Audit.Database.Name = dbname
	}()
	config.Conf.Audit.Database.Name = "test_1767001978"

	audit := NewAudit(cfg)
	audit.Init(router)

	conn, err := db.NewConnector(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer conn.Close()

	_, err = conn.Exec(context.Background(), "drop database if exists test_1767007899", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "create database test_1767007899 precision 'ns'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	conn.Exec(context.Background(), "drop user sw_token_user", util.GetQidOwn(cfg.InstanceID))
	_, err = conn.Exec(context.Background(), "create user sw_token_user pass 'token_pass_1'", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "grant use,create table on database test_1767007899 to sw_token_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "grant all on test_1767007899.* to sw_token_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)

	time.Sleep(10 * time.Second)

	records := []string{
		`{"timestamp":"1699839716442000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
		`{"timestamp":"1699839716452000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
		`{"timestamp":"1699839716462000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
		`{"timestamp":"1699839716472000000","cluster_id":"cluster_id","user":"user","operation":"operation","db":"db","resource":"resource","client_add":"127.0.0.1:3000","details":"details","affected_rows":1,"duration":1.23}`,
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit_v2", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	_, err = conn.Exec(context.Background(), "grant use,create table on database test_1767001978 to sw_token_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	_, err = conn.Exec(context.Background(), "grant all on test_1767001978.* to sw_token_user", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)

	time.Sleep(10 * time.Second)

	data, err := conn.Query(context.Background(), "create token test_sw_token from user sw_token_user", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	token := data.Data[0][0].(string)

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("/audit_v2?token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit_v2", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("/audit_v2?token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit_v2?db=test_1767007899", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("/audit_v2?db=test_1767001978&token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("/audit_v2?db=test_1767007899&token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit_v2?db=test_1767001978", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("/audit_v2?token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit_v2", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("/audit_v2?token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("/audit_v2?db=test_1767007899&token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost,
			fmt.Sprintf("/audit_v2?db=test_1767001978&token=%s", token), strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	for _, body := range records {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/audit_v2?db=test_1767007899", strings.NewReader(body))
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)
	}

	data, err = conn.Query(context.Background(), "select * from test_1767001978.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))
	_, err = conn.Query(context.Background(), "drop database if exists tetest_1767001978st_1766991210", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	data, err = conn.Query(context.Background(), "select * from test_1767007899.operations", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 4, len(data.Data))
	_, err = conn.Query(context.Background(), "drop database if exists test_1767007899", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)

	_, err = conn.Exec(context.Background(), "drop user sw_token_user", util.GetQidOwn(cfg.InstanceID))
	assert.NoError(t, err)
}
