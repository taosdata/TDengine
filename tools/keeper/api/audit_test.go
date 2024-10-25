package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/util"
)

func TestAudit(t *testing.T) {
	cfg := util.GetCfg()
	cfg.Audit = config.AuditConfig{
		Database: config.Database{
			Name: "keepter_test_audit",
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
	}
	conn, err := db.NewConnectorWithDb(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.Audit.Database.Name, cfg.TDengine.Usessl)
	assert.NoError(t, err)
	defer func() {
		_, _ = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", cfg.Audit.Database.Name))
	}()

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			body := strings.NewReader(c.data)
			req, _ := http.NewRequest(http.MethodPost, "/audit_v2", body)
			router.ServeHTTP(w, req)
			assert.Equal(t, 200, w.Code)

			data, err := conn.Query(context.Background(), fmt.Sprintf("select ts, details from %s.operations where ts=%d", cfg.Audit.Database.Name, c.ts))
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

			data, err := conn.Query(context.Background(), fmt.Sprintf("select ts, details from %s.operations where ts=%d", cfg.Audit.Database.Name, c.ts))
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

			data, err := conn.Query(context.Background(), fmt.Sprintf("select ts, details from %s.operations where ts=%d", cfg.Audit.Database.Name, c.ts))
			assert.NoError(t, err)
			assert.Equal(t, 1, len(data.Data))
			assert.Equal(t, c.expect, data.Data[0][1])
		})
	}

	MAX_SQL_LEN = 300
	// test audit batch
	input := `{"records":[{"timestamp":"1702548856940013848","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45166","db":"test","resource":"","details":"d630302"},{"timestamp":"1702548856939746458","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45230","db":"test","resource":"","details":"d130277"},{"timestamp":"1702548856939586665","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:50288","db":"test","resource":"","details":"d5268"},{"timestamp":"1702548856939528940","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:50222","db":"test","resource":"","details":"d255282"},{"timestamp":"1702548856939336371","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45126","db":"test","resource":"","details":"d755297"},{"timestamp":"1702548856939075131","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45122","db":"test","resource":"","details":"d380325"},{"timestamp":"1702548856938640661","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45152","db":"test","resource":"","details":"d255281"},{"timestamp":"1702548856938505795","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45122","db":"test","resource":"","details":"d130276"},{"timestamp":"1702548856938363319","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45178","db":"test","resource":"","details":"d755296"},{"timestamp":"1702548856938201478","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:45166","db":"test","resource":"","details":"d380324"},{"timestamp":"1702548856937740618","cluster_id":"8468922059162439502","user":"root","operation":"createTable","client_add":"173.50.0.7:50288","db":"test","resource":"","details":"d5266"}]}`

	defer func() {
		_, _ = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", cfg.Audit.Database.Name))
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

		data, err := conn.Query(context.Background(), "select ts, details from "+cfg.Audit.Database.Name+".operations where cluster_id='8468922059162439502'")
		assert.NoError(t, err)
		assert.Equal(t, 11, len(data.Data))
	})
}
