package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/util"
)

func TestAudit(t *testing.T) {
	cfg := config.GetCfg()
	cfg.Audit = config.AuditConfig{
		Database: config.Database{
			Name: "keeper_test_audit",
		},
		Enable: true,
	}

	a, err := NewAudit(cfg)
	assert.NoError(t, err)
	err = a.Init(router)
	assert.NoError(t, err)

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
			Username: "root",
			Password: "taosdata",
			Host:     "localhost",
			Port:     6041,
			Usessl:   false,
		},
		Audit: config.AuditConfig{
			Database: config.Database{
				Name: "",
			},
			Enable: true,
		},
	}

	a, err := NewAudit(&cfg)
	assert.NoError(t, err)
	assert.Equal(t, "audit", a.db)
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
