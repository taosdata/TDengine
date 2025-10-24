package api

import (
	"context"
	"crypto/md5"
	"encoding/hex"
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

func TestAdapter2(t *testing.T) {
	c := &config.Config{
		InstanceID: 64,
		Port:       6043,
		TDengine: config.TDengineRestful{
			Host:     "127.0.0.1",
			Port:     6041,
			Username: "root",
			Password: "taosdata",
			Usessl:   false,
		},
		Metrics: config.MetricsConfig{
			Database: config.Database{
				Name:    "adapter_report_test",
				Options: map[string]interface{}{},
			},
		},
	}
	gm := NewGeneralMetric(c)
	a := NewAdapter(c, gm)
	err := a.Init(router)
	assert.NoError(t, err)

	w := httptest.NewRecorder()
	body := strings.NewReader(" {\"ts\": 1696928323, \"metrics\": {\"rest_total\": 10, \"rest_query\":  2, " +
		"\"rest_write\": 5, \"rest_other\": 3, \"rest_in_process\": 1, \"rest_fail\": 5, \"rest_success\": 3, " +
		"\"rest_query_success\": 1, \"rest_query_fail\": 2, \"rest_write_success\": 2, \"rest_write_fail\": 3, " +
		"\"rest_other_success\": 1, \"rest_other_fail\": 2, \"rest_query_in_process\": 1, \"rest_write_in_process\": 2, " +
		"\"ws_total\": 10, \"ws_query\": 2, \"ws_write\": 3, \"ws_other\": 5, \"ws_in_process\": 1, \"ws_success\": 3, " +
		"\"ws_fail\": 3, \"ws_query_success\": 1, \"ws_query_fail\": 1, \"ws_write_success\": 2, \"ws_write_fail\": 2, " +
		"\"ws_other_success\": 1, \"ws_other_fail\": 2, \"ws_query_in_process\": 1, \"ws_write_in_process\": 2 }, " +
		"\"endpoint\": \"adapter-1:6041\"}")
	req, _ := http.NewRequest(http.MethodPost, "/adapter_report", body)
	req.Header.Set("X-QID", "0x1234567890ABCD00")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	conn, err := db.NewConnectorWithDb(c.TDengine.Username, c.TDengine.Password, c.TDengine.Host, c.TDengine.Port, c.Metrics.Database.Name, c.TDengine.Usessl)
	defer func() {
		_, _ = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", c.Metrics.Database.Name), util.GetQidOwn(config.Conf.InstanceID))
	}()

	assert.NoError(t, err)
	data, err := conn.Query(context.Background(), "select * from adapter_requests where req_type=0", util.GetQidOwn(config.Conf.InstanceID))

	assert.NoError(t, err)
	assert.Equal(t, 1, len(data.Data))
	assert.Equal(t, uint32(10), data.Data[0][1])
	assert.Equal(t, uint32(2), data.Data[0][2])
	assert.Equal(t, uint32(5), data.Data[0][3])
	assert.Equal(t, uint32(3), data.Data[0][4])
	assert.Equal(t, uint32(1), data.Data[0][5])
	assert.Equal(t, uint32(3), data.Data[0][6])
	assert.Equal(t, uint32(5), data.Data[0][7])
	assert.Equal(t, uint32(1), data.Data[0][8])
	assert.Equal(t, uint32(2), data.Data[0][9])
	assert.Equal(t, uint32(2), data.Data[0][10])
	assert.Equal(t, uint32(3), data.Data[0][11])
	assert.Equal(t, uint32(1), data.Data[0][12])
	assert.Equal(t, uint32(2), data.Data[0][13])
	assert.Equal(t, uint32(1), data.Data[0][14])
	assert.Equal(t, uint32(2), data.Data[0][15])

	data, err = conn.Query(context.Background(), "select * from adapter_requests where req_type=1", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data.Data))
	assert.Equal(t, uint32(10), data.Data[0][1])
	assert.Equal(t, uint32(2), data.Data[0][2])
	assert.Equal(t, uint32(3), data.Data[0][3])
	assert.Equal(t, uint32(5), data.Data[0][4])
	assert.Equal(t, uint32(1), data.Data[0][5])
	assert.Equal(t, uint32(3), data.Data[0][6])
	assert.Equal(t, uint32(3), data.Data[0][7])
	assert.Equal(t, uint32(1), data.Data[0][8])
	assert.Equal(t, uint32(1), data.Data[0][9])
	assert.Equal(t, uint32(2), data.Data[0][10])
	assert.Equal(t, uint32(2), data.Data[0][11])
	assert.Equal(t, uint32(1), data.Data[0][12])
	assert.Equal(t, uint32(2), data.Data[0][13])
	assert.Equal(t, uint32(1), data.Data[0][14])
	assert.Equal(t, uint32(2), data.Data[0][15])

	// Test with new general metric
	body2 := strings.NewReader("{\"ts\":1743660120,\"metrics\":{\"rest_fail\":0,\"rest_in_process\":0,\"rest_other\":0,\"rest_other_fail\":0,\"rest_other_in_process\":0,\"rest_other_success\":0,\"rest_query\":5429,\"rest_query_fail\":0,\"rest_query_in_process\":0,\"rest_query_success\":5429,\"rest_success\":5429,\"rest_total\":5429,\"rest_write\":0,\"rest_write_fail\":0,\"rest_write_in_process\":0,\"rest_write_success\":0,\"ws_fail\":0,\"ws_in_process\":0,\"ws_other\":0,\"ws_other_fail\":0,\"ws_other_in_process\":0,\"ws_other_success\":0,\"ws_query\":0,\"ws_query_fail\":0,\"ws_query_in_process\":0,\"ws_query_success\":0,\"ws_success\":0,\"ws_total\":0,\"ws_write\":0,\"ws_write_fail\":0,\"ws_write_in_process\":0,\"ws_write_success\":0},\"endpoint\":\"max:6041\",\"extra_metrics\":[{\"ts\":\"1743660120000\",\"protocol\":2,\"tables\":[{\"name\":\"adapter_status\",\"metric_groups\":[{\"tags\":[{\"name\":\"endpoint\",\"value\":\"max:6041\"}],\"metrics\":[{\"name\":\"go_heap_sys\",\"value\":69156864},{\"name\":\"go_heap_inuse\",\"value\":59449344},{\"name\":\"go_stack_sys\",\"value\":1966080},{\"name\":\"go_stack_inuse\",\"value\":1966080},{\"name\":\"rss\",\"value\":113594368},{\"name\":\"ws_query_conn\",\"value\":0},{\"name\":\"ws_stmt_conn\",\"value\":0},{\"name\":\"ws_sml_conn\",\"value\":0},{\"name\":\"ws_ws_conn\",\"value\":0},{\"name\":\"ws_tmq_conn\",\"value\":0},{\"name\":\"async_c_limit\",\"value\":8},{\"name\":\"async_c_inflight\",\"value\":0},{\"name\":\"sync_c_limit\",\"value\":8},{\"name\":\"sync_c_inflight\",\"value\":0}]}]},{\"name\":\"adapter_conn_pool\",\"metric_groups\":[{\"tags\":[{\"name\":\"endpoint\",\"value\":\"max:6041\"},{\"name\":\"user\",\"value\":\"root\"}],\"metrics\":[{\"name\":\"conn_pool_total\",\"value\":16},{\"name\":\"conn_pool_in_use\",\"value\":1}]}]}]}]}")

	req2, _ := http.NewRequest(http.MethodPost, "/adapter_report", body2)
	req2.Header.Set("X-QID", "0x1234567890ABCD00")
	router.ServeHTTP(w, req2)
	assert.Equal(t, 200, w.Code)

	// Test with new general metric
	body3 := strings.NewReader("{\"ts\":1743660121,\"metrics\":{\"rest_fail\":0,\"rest_in_process\":0,\"rest_other\":0,\"rest_other_fail\":0,\"rest_other_in_process\":0,\"rest_other_success\":0,\"rest_query\":5429,\"rest_query_fail\":0,\"rest_query_in_process\":0,\"rest_query_success\":5429,\"rest_success\":5429,\"rest_total\":5429,\"rest_write\":0,\"rest_write_fail\":0,\"rest_write_in_process\":0,\"rest_write_success\":0,\"ws_fail\":0,\"ws_in_process\":0,\"ws_other\":0,\"ws_other_fail\":0,\"ws_other_in_process\":0,\"ws_other_success\":0,\"ws_query\":0,\"ws_query_fail\":0,\"ws_query_in_process\":0,\"ws_query_success\":0,\"ws_success\":0,\"ws_total\":0,\"ws_write\":0,\"ws_write_fail\":0,\"ws_write_in_process\":0,\"ws_write_success\":0},\"endpoint\":\"max:6041\",\"extra_metrics\":[{\"ts\":\"1743660120001\",\"protocol\":2,\"tables\":[{\"name\":\"adapter_status\",\"metric_groups\":[{\"tags\":[{\"name\":\"endpoint\",\"value\":\"maxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmax:6041\"}],\"metrics\":[{\"name\":\"go_heap_sys\",\"value\":69156864},{\"name\":\"go_heap_inuse\",\"value\":59449344},{\"name\":\"go_stack_sys\",\"value\":1966080},{\"name\":\"go_stack_inuse\",\"value\":1966080},{\"name\":\"rss\",\"value\":113594368},{\"name\":\"ws_query_conn\",\"value\":0},{\"name\":\"ws_stmt_conn\",\"value\":0},{\"name\":\"ws_sml_conn\",\"value\":0},{\"name\":\"ws_ws_conn\",\"value\":0},{\"name\":\"ws_tmq_conn\",\"value\":0},{\"name\":\"async_c_limit\",\"value\":8},{\"name\":\"async_c_inflight\",\"value\":0},{\"name\":\"sync_c_limit\",\"value\":8},{\"name\":\"sync_c_inflight\",\"value\":0}]}]},{\"name\":\"adapter_conn_pool\",\"metric_groups\":[{\"tags\":[{\"name\":\"endpoint\",\"value\":\"maxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmaxmax:6041\"},{\"name\":\"user\",\"value\":\"root\"}],\"metrics\":[{\"name\":\"conn_pool_total\",\"value\":16},{\"name\":\"conn_pool_in_use\",\"value\":1}]}]}]}]}")

	req3, _ := http.NewRequest(http.MethodPost, "/adapter_report", body3)
	req3.Header.Set("X-QID", "0x1234567890ABCD00")
	router.ServeHTTP(w, req3)
	assert.Equal(t, 200, w.Code)

	data, err = conn.Query(context.Background(), "select count(*) from adapter_status", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data.Data))
	assert.Equal(t, int64(2), data.Data[0][0])

	data, err = conn.Query(context.Background(), "select count(*) from adapter_conn_pool", util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data.Data))
	assert.Equal(t, int64(2), data.Data[0][0])

	conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", c.Metrics.Database.Name), util.GetQidOwn(config.Conf.InstanceID))
}

func Test_adapterTableSql(t *testing.T) {
	conn, _ := db.NewConnector("root", "taosdata", "127.0.0.1", 6041, false)
	defer conn.Close()

	dbName := "db_202412031446"
	conn.Exec(context.Background(), "create database "+dbName, util.GetQidOwn(config.Conf.InstanceID))
	defer conn.Exec(context.Background(), "drop database "+dbName, util.GetQidOwn(config.Conf.InstanceID))

	conn, _ = db.NewConnectorWithDb("root", "taosdata", "127.0.0.1", 6041, dbName, false)
	defer conn.Close()

	conn.Exec(context.Background(), adapterTableSql, util.GetQidOwn(config.Conf.InstanceID))

	testCases := []struct {
		ep      string
		wantErr bool
	}{
		{"", false},
		{"hello", false},
		{strings.Repeat("a", 128), false},
		{strings.Repeat("a", 255), false},
		{strings.Repeat("a", 256), true},
	}

	for i, tc := range testCases {
		sql := fmt.Sprintf("create table d%d using adapter_requests tags ('%s', 0)", i, tc.ep)
		_, err := conn.Exec(context.Background(), sql, util.GetQidOwn(config.Conf.InstanceID))
		if tc.wantErr {
			assert.Error(t, err) // [0x2653] Value too long for column/tag: endpoint
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestAdapter_tableName(t *testing.T) {
	a := &Adapter{}
	endpoint := strings.Repeat("x", util.MAX_TABLE_NAME_LEN)

	gotRest := a.tableName(endpoint, rest)
	sumRest := md5.Sum([]byte(fmt.Sprintf("%s%d", endpoint, rest)))
	wantRest := fmt.Sprintf("adapter_req_%s", hex.EncodeToString(sumRest[:]))
	assert.Equal(t, wantRest, gotRest)

	gotWS := a.tableName(endpoint, ws)
	sumWS := md5.Sum([]byte(fmt.Sprintf("%s%d", endpoint, ws)))
	wantWS := fmt.Sprintf("adapter_req_%s", hex.EncodeToString(sumWS[:]))
	assert.Equal(t, wantWS, gotWS)

	assert.NotEqual(t, gotRest, gotWS)
}

func TestAdapter_handleFunc_NoConnection_Returns500(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	a := &Adapter{conn: nil}
	r.POST("/adapter_report", a.handleFunc())

	req := httptest.NewRequest(http.MethodPost, "/adapter_report", strings.NewReader(`{}`))
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

type badReader struct{ err error }

func (b badReader) Read(p []byte) (int, error) { return 0, b.err }

func TestAdapter_handleFunc_GetRawDataError_Returns400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	a := &Adapter{conn: &db.Connector{}}
	r.POST("/adapter_report", a.handleFunc())

	req := httptest.NewRequest(http.MethodPost, "/adapter_report", badReader{err: errors.New("boom")})
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
	if body["error"] != "get adapter report data error. boom" {
		t.Fatalf("error=%q, want %q", body["error"], "get adapter report data error. boom")
	}
}

func TestAdapter_handleFunc_TraceReceiveData_LogsWhenEnabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	old := logger.Logger.GetLevel()
	logger.Logger.SetLevel(logrus.TraceLevel)
	defer logger.Logger.SetLevel(old)

	a := &Adapter{conn: &db.Connector{}}
	r.POST("/adapter_report", a.handleFunc())

	req := httptest.NewRequest(http.MethodPost, "/adapter_report", strings.NewReader(`{`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-QID", "123")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestAdapter_handleFunc_ParseError_ReturnsBadRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	a := &Adapter{conn: &db.Connector{}}
	r.POST("/adapter_report", a.handleFunc())

	req := httptest.NewRequest(http.MethodPost, "/adapter_report", strings.NewReader(`{`)) // invalid JSON
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-QID", "1")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusBadRequest)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response error: %v, raw=%q", err, w.Body.String())
	}
	if !strings.HasPrefix(body["error"], "parse adapter report data error: ") {
		t.Fatalf("error prefix mismatch, got %q", body["error"])
	}
}

func TestAdapter_createTable_NoConnection_ReturnsErrNoConnection(t *testing.T) {
	a := &Adapter{conn: nil}

	err := a.createTable()
	if !errors.Is(err, errNoConnection) {
		t.Fatalf("expected errNoConnection, got %v", err)
	}
}
