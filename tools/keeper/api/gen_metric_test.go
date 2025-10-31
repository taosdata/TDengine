package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/util"
)

var router_inited bool = false

func TestClusterBasic(t *testing.T) {
	cfg := config.GetCfg()

	CreateDatabase(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl, cfg.Metrics.Database.Name, cfg.Metrics.Database.Options)

	gm := NewGeneralMetric(cfg)
	if !router_inited {
		err := gm.Init(router)
		assert.NoError(t, err)
		router_inited = true
	}

	testcfg := struct {
		name   string
		ts     int64
		tbname string
		data   string
		expect string
	}{
		name:   "1",
		tbname: "taosd_cluster_basic",
		ts:     1705655770381,
		data:   `{"ts":"1705655770381","cluster_id":"7648966395564416484","protocol":2,"first_ep":"ssfood06:6130","first_ep_dnode_id":1,"cluster_version":"3.2.1.0.alp"}`,
		expect: "7648966395564416484",
	}

	conn, err := db.NewConnectorWithDb(gm.username, gm.password, gm.host, gm.port, gm.database, gm.usessl)
	assert.NoError(t, err)
	defer func() {
		_, _ = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", gm.database), util.GetQidOwn(config.Conf.InstanceID))
	}()

	t.Run(testcfg.name, func(t *testing.T) {
		w := httptest.NewRecorder()
		body := strings.NewReader(testcfg.data)
		req, _ := http.NewRequest(http.MethodPost, "/taosd-cluster-basic", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		data, err := conn.Query(context.Background(), fmt.Sprintf("select ts, cluster_id from %s.%s where ts=%d", gm.database, testcfg.tbname, testcfg.ts), util.GetQidOwn(config.Conf.InstanceID))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(data.Data))
		assert.Equal(t, testcfg.expect, data.Data[0][1])
	})

	testcfg = struct {
		name   string
		ts     int64
		tbname string
		data   string
		expect string
	}{
		name:   "1",
		tbname: "taos_slow_sql_detail",
		ts:     1703226836762,
		data: `[{
			"start_ts": "1703226836762",
			"request_id": "1",
			"query_time": 100,
			"code": 0,
			"error_info": "",
			"type": 1,
			"rows_num": 5,
			"sql": "select * from abc;",
			"process_name": "abc",
			"process_id": "123",
			"db": "dbname",
			"user": "root",
			"ip": "127.0.0.1",
			"cluster_id": "1234567"
			},
			{
			"start_ts": "1703226836763",
			"request_id": "2",
			"query_time": 100,
			"code": 0,
			"error_info": "",
			"type": 1,
			"rows_num": 5,
			"sql": "insert into abc ('a', 'b') values ('aaa', 'bbb');",
			"process_name": "abc",
			"process_id": "123",
			"db": "dbname",
			"user": "root",
			"ip": "127.0.0.1",
			"cluster_id": "1234567"
			}]`,
		expect: "1234567",
	}

	conn, err = db.NewConnectorWithDb(gm.username, gm.password, gm.host, gm.port, gm.database, gm.usessl)
	assert.NoError(t, err)
	defer func() {
		_, _ = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", gm.database), util.GetQidOwn(config.Conf.InstanceID))
	}()

	t.Run(testcfg.name, func(t *testing.T) {
		MAX_SQL_LEN = 1000000
		w := httptest.NewRecorder()
		body := strings.NewReader(testcfg.data)
		req, _ := http.NewRequest(http.MethodPost, "/slow-sql-detail-batch", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		data, err := conn.Query(context.Background(), fmt.Sprintf("select start_ts, cluster_id from %s.%s where start_ts=%d", gm.database, testcfg.tbname, testcfg.ts), util.GetQidOwn(config.Conf.InstanceID))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(data.Data))
		assert.Equal(t, testcfg.expect, data.Data[0][1])
	})
}

func TestGenMetric(t *testing.T) {
	cfg := config.GetCfg()

	CreateDatabase(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl, cfg.Metrics.Database.Name, cfg.Metrics.Database.Options)

	gm := NewGeneralMetric(cfg)
	if !router_inited {
		err := gm.Init(router)
		assert.NoError(t, err)
		router_inited = true
	}

	testcfg := struct {
		name   string
		ts     []int64
		tbname []string
		data   string
		expect string
	}{
		name:   "1",
		tbname: []string{"taosd_cluster_info", "taosd_dnodes_info"},
		ts:     []int64{1703226836761, 1703226836762},
		data: `[{
			"ts": "1703226836761",
			"protocol": 2,
			"tables": [{
				"name": "taosd_cluster_info",
				"metric_groups": [{
					"tags": [{
						"name": "cluster_id",
						"value": "1397715317673023180"
					}],
					"metrics": [{
						"name": "dbs_total",
						"value": 1
					}, {
						"name": "master_uptime",
						"value": 0
					}]
				}]
			}, {
				"name": "taosd_dnodes_info",
				"metric_groups": [{
					"tags": [{
						"name": "cluster_id",
						"value": "1397715317673023180"
					}, {
						"name": "dnode_id",
						"value": "1"
					}, {
						"name": "dnode_ep",
						"value": "ssfood06:6130"
					}],
					"metrics": [{
						"name": "uptime",
						"value": 0
					}, {
						"name": "cpu_engine",
						"value": 0
					}]
				}]
			}]
		}, {
			"ts": "1703226836762",
			"protocol": 2,
			"tables": [{
				"name": "taosd_cluster_info",
				"metric_groups": [{
					"tags": [{
						"name": "cluster_id",
						"value": "1397715317673023180"
					}],
					"metrics": [{
						"name": "dbs_total",
						"value": 1
					}, {
						"name": "master_uptime",
						"value": 0
					}]
				}]
			}, {
				"name": "taosd_dnodes_info",
				"metric_groups": [{
					"tags": [{
						"name": "cluster_id",
						"value": "1397715317673023180"
					}, {
						"name": "dnode_id",
						"value": "1"
					}, {
						"name": "dnode_ep",
						"value": ", =\"ssfood06:6130"
					}],
					"metrics": [{
						"name": "uptime",
						"value": 0
					}, {
						"name": "cpu_engine",
						"value": 0
					}]
				}]
			}]
		}]`,
		expect: "1397715317673023180",
	}

	conn, err := db.NewConnectorWithDb(gm.username, gm.password, gm.host, gm.port, gm.database, gm.usessl)
	assert.NoError(t, err)
	defer func() {
		_, _ = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", gm.database), util.GetQidOwn(config.Conf.InstanceID))
	}()

	t.Run(testcfg.name, func(t *testing.T) {
		w := httptest.NewRecorder()
		body := strings.NewReader(testcfg.data)
		req, _ := http.NewRequest(http.MethodPost, "/general-metric", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		for _, tbname := range testcfg.tbname {
			for _, ts := range testcfg.ts {
				data, err := conn.Query(context.Background(), fmt.Sprintf("select _ts, cluster_id from %s.%s where _ts=%d", gm.database, tbname, ts), util.GetQidOwn(config.Conf.InstanceID))
				assert.NoError(t, err)
				assert.Equal(t, 1, len(data.Data))
				assert.Equal(t, testcfg.expect, data.Data[0][1])
			}
		}
	})
}

func TestGetSubTableName(t *testing.T) {
	tests := []struct {
		stbName string
		tagMap  map[string]string
		want    string
	}{
		{
			stbName: "taosx_sys",
			tagMap:  map[string]string{"taosx_id": "123"},
			want:    "sys_123",
		},
		{
			stbName: "taosx_agent",
			tagMap:  map[string]string{"taosx_id": "123", "agent_id": "456"},
			want:    "agent_123_456",
		},
		{
			stbName: "taosx_connector",
			tagMap:  map[string]string{"taosx_id": "123", "ds_name": "ds", "task_id": "789"},
			want:    "connector_123_ds_789",
		},
		{
			stbName: "taosx_task_example",
			tagMap:  map[string]string{"taosx_id": "123", "task_id": "789"},
			want:    "task_123_example_789",
		},
		{
			stbName: "taosd_cluster_info",
			tagMap:  map[string]string{"cluster_id": "123"},
			want:    "cluster_123",
		},
		{
			stbName: "taosd_vgroups_info",
			tagMap:  map[string]string{"cluster_id": "123", "vgroup_id": "456", "database_name": "db"},
			want:    "vginfo_db_vgroup_456_cluster_123",
		},
		{
			stbName: "taosd_dnodes_info",
			tagMap:  map[string]string{"cluster_id": "123", "dnode_id": "123"},
			want:    "dinfo_123_cluster_123",
		},
		{
			stbName: "taosd_dnodes_status",
			tagMap:  map[string]string{"cluster_id": "123", "dnode_id": "123"},
			want:    "dstatus_123_cluster_123",
		},
		{
			stbName: "taosd_dnodes_log_dirs",
			tagMap:  map[string]string{"cluster_id": "123", "dnode_id": "123", "data_dir_name": "log"},
			want:    "dlog_123_log_cluster_123",
		},
		{
			stbName: "taosd_dnodes_log_dirs",
			tagMap:  map[string]string{"cluster_id": "123", "dnode_id": "123", "data_dir_name": "loglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglogloglog"},
			want:    "dlog_123_9cdc719961a632a27603cd5ed9f1aee2_cluster_123",
		},
		{
			stbName: "taosd_dnodes_data_dirs",
			tagMap:  map[string]string{"cluster_id": "123", "dnode_id": "123", "data_dir_name": "data", "data_dir_level": "5"},
			want:    "ddata_123_data_level_5_cluster_123",
		},
		{
			stbName: "taosd_dnodes_data_dirs",
			tagMap:  map[string]string{"cluster_id": "123", "dnode_id": "123", "data_dir_name": "datadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadatadata", "data_dir_level": "5"},
			want:    "ddata_123_03bf8dffdf6b97e08f347c6ae795998b_level_5_cluster_123",
		},
		{
			stbName: "taosd_mnodes_info",
			tagMap:  map[string]string{"cluster_id": "123", "mnode_id": "12"},
			want:    "minfo_12_cluster_123",
		},
		{
			stbName: "taosd_vnodes_info",
			tagMap:  map[string]string{"cluster_id": "123", "database_name": "db", "vgroup_id": "456", "dnode_id": "789"},
			want:    "vninfo_db_dnode_789_vgroup_456_cluster_123",
		},
		{
			stbName: "taosd_sql_req",
			tagMap:  map[string]string{"username": "user", "sql_type": "select", "result": "success", "dnode_id": "123", "vgroup_id": "456", "cluster_id": "123"},
			want:    "taosdsql_user_select_success_123_vgroup_456_cluster_123",
		},
		{
			stbName: "taos_sql_req",
			tagMap:  map[string]string{"username": "user", "sql_type": "select", "result": "success", "cluster_id": "123"},
			want:    "taossql_user_select_success_cluster_123",
		},
		{
			stbName: "taos_slow_sql",
			tagMap:  map[string]string{"username": "user", "duration": "100ms", "result": "success", "cluster_id": "123"},
			want:    "slowsql_user_100ms_success_cluster_123",
		},
		{
			stbName: "explorer_sys",
			tagMap:  map[string]string{"endpoint": "0015.novalocal:6060"},
			want:    "explorer_sys_0015_novalocal_6060",
		},
		{
			stbName: "explorer_sys2",
			tagMap:  map[string]string{"endpoint": "0015.novalocal:6060"},
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.stbName, func(t *testing.T) {
			if got := get_sub_table_name_valid(tt.stbName, tt.tagMap); got != tt.want {
				panic(fmt.Sprintf("get_sub_table_name() = %v, want %v", got, tt.want))
			}
		})
	}
}

func Test_createSTables(t *testing.T) {
	conn, _ := db.NewConnector("root", "taosdata", "127.0.0.1", 6041, false)
	defer conn.Close()

	dbName := "db_202412031527"
	conn.Exec(context.Background(), "create database "+dbName, util.GetQidOwn(config.Conf.InstanceID))
	defer conn.Exec(context.Background(), "drop database "+dbName, util.GetQidOwn(config.Conf.InstanceID))

	conn, _ = db.NewConnectorWithDb("root", "taosdata", "127.0.0.1", 6041, dbName, false)
	defer conn.Close()

	gm := GeneralMetric{conn: conn}
	gm.createSTables()

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

	conn.Exec(context.Background(),
		"create table d0 using taosd_cluster_basic tags('cluster_id')", util.GetQidOwn(config.Conf.InstanceID))

	for _, tc := range testCases {
		sql := fmt.Sprintf("insert into d0 (ts, first_ep) values(%d, '%s')", time.Now().UnixMilli(), tc.ep)
		_, err := conn.Exec(context.Background(), sql, util.GetQidOwn(config.Conf.InstanceID))
		if tc.wantErr {
			assert.Error(t, err) // [0x2653] Value too long for column/tag: endpoint
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestGeneralMetric_createSTables(t *testing.T) {
	g := &GeneralMetric{}
	err := g.createSTables()
	assert.Error(t, err)
}

func Test_contains(t *testing.T) {
	res := contains([]string{"a", "b", "c"}, "b")
	assert.True(t, res)

	res = contains([]string{"a", "b", "c"}, "d")
	assert.False(t, res)
}

func Test_get_sub_table_name(t *testing.T) {
	res := get_sub_table_name("taosx_abc", nil)
	assert.Equal(t, "", res)

	res = get_sub_table_name("taosd_write_metrics", map[string]string{"database_name": "test", "vgroup_id": "1", "cluster_id": "1"})
	assert.Equal(t, "taosd_write_metric_test_vgroup_1_cluster_1", res)

	res = get_sub_table_name("taosd_dnodes_metrics", map[string]string{"dnode_ep": "ep", "cluster_id": "1"})
	assert.Equal(t, "dnode_metric_ep_cluster_1", res)

	res = get_sub_table_name("adapter_c_interface", map[string]string{"endpoint": "ep"})
	assert.Equal(t, "adapter_c_if_ep", res)

	ep := strings.Repeat("a", 300)
	res = get_sub_table_name("adapter_c_interface", map[string]string{"endpoint": ep})
	assert.Equal(t, fmt.Sprintf("adapter_c_if_%s", strings.Repeat("a", 177)), res)

	res = get_sub_table_name("abc", nil)
	assert.Equal(t, "", res)
}

func Test_checkKeysExist(t *testing.T) {
	res := checkKeysExist(map[string]string{"a": "1", "b": "2"}, "a", "c")
	assert.False(t, res)
}

func Test_writeTags(t *testing.T) {
	stb := "stb_ut1"
	Store(stb, ColumnSeq{tagNames: []string{"a", "b"}})

	tags := []Tag{
		{Name: "a", Value: ""},
		{Name: STABLE_NAME_KEY, Value: "subtbl"},
	}

	var buf bytes.Buffer
	writeTags(tags, stb, &buf)

	got := buf.String()
	want := ",a=unknown,b=unknown"
	if got != want {
		t.Fatalf("unexpected tags output, got %q, want %q", got, want)
	}
}

func Test_writeTags_ReturnsEarlyWhenStableNameKeyPresent(t *testing.T) {
	stb := "stb_ut_return"
	Store(stb, ColumnSeq{tagNames: []string{"x"}})

	tags := []Tag{
		{Name: "x", Value: "v"},
		{Name: STABLE_NAME_KEY, Value: "custom_sub"},
	}

	var buf bytes.Buffer
	writeTags(tags, stb, &buf)

	got := buf.String()
	assert.Equal(t, ",x=v,priv_stn=custom_sub", got)
}

func Test_writeTags_LogErrorWhenNoSubTableName(t *testing.T) {
	var buf bytes.Buffer
	stb := "random_does_not_exist_12345"
	tags := []Tag{{Name: "foo", Value: "bar"}}

	writeTags(tags, stb, &buf)

	out := buf.String()
	if strings.Contains(out, STABLE_NAME_KEY) {
		t.Fatalf("should not append %q when sub table name is missing, got %q", STABLE_NAME_KEY, out)
	}
}

func TestGeneralMetric_handleSlowSqlDetailBatch_NoConnection_Returns500(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	gm := &GeneralMetric{conn: nil}
	r.POST("/slow-sql-detail-batch", gm.handleSlowSqlDetailBatch())

	req := httptest.NewRequest(http.MethodPost, "/slow-sql-detail-batch", strings.NewReader(`[]`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-QID", "123")

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

func TestGeneralMetric_handleSlowSqlDetailBatch_GetRawDataError_Returns400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	gm := &GeneralMetric{conn: &db.Connector{}}
	r.POST("/slow-sql-detail-batch", gm.handleSlowSqlDetailBatch())

	boom := errors.New("boom")
	req := httptest.NewRequest(http.MethodPost, "/slow-sql-detail-batch", errReader{err: boom})
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusBadRequest)
	}

	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error: %v; raw=%q", err, w.Body.String())
	}
	want := "get taos slow sql detail data error. " + boom.Error()
	if body["error"] != want {
		t.Fatalf("error message mismatch: got %q, want %q", body["error"], want)
	}
}

func TestGeneralMetric_handleSlowSqlDetailBatch_TraceLogsWhenEnabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	old := logger.Logger.GetLevel()
	logger.Logger.SetLevel(logrus.TraceLevel)
	defer logger.Logger.SetLevel(old)

	gm := &GeneralMetric{conn: &db.Connector{}}
	r.POST("/slow-sql-detail-batch", gm.handleSlowSqlDetailBatch())

	req := httptest.NewRequest(http.MethodPost, "/slow-sql-detail-batch", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusBadRequest)
	}
}

func TestGeneralMetric_handleSlowSqlDetailBatch_ParseError_Returns400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	gm := &GeneralMetric{conn: &db.Connector{}}
	r.POST("/slow-sql-detail-batch", gm.handleSlowSqlDetailBatch())

	req := httptest.NewRequest(http.MethodPost, "/slow-sql-detail-batch", strings.NewReader(`{}`))
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
	errMsg := body["error"]
	if !strings.HasPrefix(errMsg, "parse taos slow sql detail error: ") {
		t.Fatalf("error prefix mismatch, got %q", errMsg)
	}
}

func TestGeneralMetric_handleSlowSqlDetailBatch_EmptyStartTs_IsSkippedAndOK(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	gm := &GeneralMetric{conn: &db.Connector{}}
	r.POST("/slow-sql-detail-batch", gm.handleSlowSqlDetailBatch())

	body := `[{"start_ts": ""}]`
	req := httptest.NewRequest(http.MethodPost, "/slow-sql-detail-batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-QID", "123")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusOK)
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response error: %v, raw=%q", err, w.Body.String())
	}
	if len(resp) != 0 {
		t.Fatalf("expected empty object {}, got %v", resp)
	}
}

func TestGeneralMetric_handleTaosdClusterBasic(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	gm := &GeneralMetric{conn: nil}
	r.POST("/taosd-cluster-basic", gm.handleTaosdClusterBasic())

	req := httptest.NewRequest(http.MethodPost, "/taosd-cluster-basic", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-QID", "123")

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
		t.Fatalf(`error field = %q, want "no connection"`, body["error"])
	}
}

func TestGeneralMetric_handleTaosdClusterBasic_GetRawDataError_Returns400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	gm := &GeneralMetric{conn: &db.Connector{}}
	r.POST("/taosd-cluster-basic", gm.handleTaosdClusterBasic())

	boom := errors.New("boom")
	req := httptest.NewRequest(http.MethodPost, "/taosd-cluster-basic", errReader{err: boom})
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusBadRequest)
	}

	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error: %v; raw=%q", err, w.Body.String())
	}
	want := "get general metric data error. " + boom.Error()
	if body["error"] != want {
		t.Fatalf("error message mismatch: got %q, want %q", body["error"], want)
	}
}

func TestGeneralMetric_handleTaosdClusterBasic_TraceLogsWhenEnabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	old := logger.Logger.GetLevel()
	logger.Logger.SetLevel(logrus.TraceLevel)
	defer logger.Logger.SetLevel(old)

	gm := &GeneralMetric{conn: &db.Connector{}}
	r.POST("/taosd-cluster-basic", gm.handleTaosdClusterBasic())

	req := httptest.NewRequest(http.MethodPost, "/taosd-cluster-basic", strings.NewReader(`[]`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusBadRequest)
	}
}

func TestGeneralMetric_handleTaosdClusterBasic_ParseError_Returns400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	gm := &GeneralMetric{conn: &db.Connector{}}
	r.POST("/taosd-cluster-basic", gm.handleTaosdClusterBasic())

	req := httptest.NewRequest(http.MethodPost, "/taosd-cluster-basic", strings.NewReader(`[]`))
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
	errMsg := body["error"]
	if !strings.HasPrefix(errMsg, "parse general metric data error: ") {
		t.Fatalf("error prefix mismatch, got %q", errMsg)
	}
	if !strings.Contains(errMsg, "cannot unmarshal") {
		t.Fatalf("unexpected error detail: %q", errMsg)
	}
}

func TestGeneralMetric_handleBatchMetrics(t *testing.T) {
	gm := &GeneralMetric{}

	gm.handleBatchMetrics(nil, 0)

	request := []StableArrayInfo{{
		Ts: "",
	}}
	gm.handleBatchMetrics(request, 0)

	request = []StableArrayInfo{{
		Ts: "123",
		Tables: []StableInfo{{
			Name: "",
		}},
	}}
	gm.handleBatchMetrics(request, 0)
}

func TestGeneralMetric_handleFunc(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	gm := &GeneralMetric{client: nil}
	r.POST("/general-metric", gm.handleFunc())

	req := httptest.NewRequest(http.MethodPost, "/general-metric", strings.NewReader(`[]`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-QID", "123")

	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusInternalServerError)
	}

	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error: %v; raw=%q", err, w.Body.String())
	}
	if body["error"] != "no connection" {
		t.Fatalf(`error=%q, want "no connection"`, body["error"])
	}
}

type errReader struct{ err error }

func (e errReader) Read(p []byte) (int, error) { return 0, e.err }

func TestGeneralMetric_handleFunc_ReadBodyError_Returns400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	gm := &GeneralMetric{client: &http.Client{}}
	r.POST("/general-metric", gm.handleFunc())

	boom := errors.New("boom")
	req := httptest.NewRequest(http.MethodPost, "/general-metric", errReader{err: boom})
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-QID", "123")

	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusBadRequest)
	}

	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error: %v; raw=%q", err, w.Body.String())
	}
	got := body["error"]
	want := "get general metric data error. " + boom.Error()
	if got != want {
		t.Fatalf("error message mismatch: got %q, want %q", got, want)
	}
}

func TestGeneralMetric_handleFunc_TraceLogsWhenEnabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	old := logger.Logger.GetLevel()
	logger.Logger.SetLevel(logrus.TraceLevel)
	defer logger.Logger.SetLevel(old)

	gm := &GeneralMetric{client: &http.Client{}}
	r.POST("/general-metric", gm.handleFunc())

	body := `[{"ts":"1","tables":[]}]`
	req := httptest.NewRequest(http.MethodPost, "/general-metric", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusOK)
	}
}

func TestGeneralMetric_handleFunc_ParseError_Returns400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	gm := &GeneralMetric{client: &http.Client{}}
	r.POST("/general-metric", gm.handleFunc())

	req := httptest.NewRequest(http.MethodPost, "/general-metric", strings.NewReader(`{}`))
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

	errMsg := body["error"]
	if !strings.HasPrefix(errMsg, "parse general metric data error: ") {
		t.Fatalf("error prefix mismatch, got %q", errMsg)
	}
	if !strings.Contains(errMsg, "cannot unmarshal object into Go value of type []api.StableArrayInfo") {
		t.Fatalf("unexpected parse error detail: %q", errMsg)
	}
}

func TestGeneralMetric_handleFunc_EmptyRequest_ReturnsOK(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	gm := &GeneralMetric{client: &http.Client{}}
	r.POST("/general-metric", gm.handleFunc())

	req := httptest.NewRequest(http.MethodPost, "/general-metric", strings.NewReader(`[]`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusOK)
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal error: %v; raw=%q", err, w.Body.String())
	}
	if len(body) != 0 {
		t.Fatalf("expect empty object {}, got: %v", body)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestGeneralMetric_handleFunc_ProcessRecordsError_Returns400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       io.NopCloser(bytes.NewBufferString("boom")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	client := &http.Client{Transport: rt}

	gm := &GeneralMetric{
		client:   client,
		username: "u",
		password: "p",
		url: &url.URL{
			Scheme:   "http",
			Host:     "example.com",
			Path:     "/influxdb/v1/write",
			RawQuery: "db=test&precision=ms&table_name_key=" + STABLE_NAME_KEY,
		},
	}

	r.POST("/general-metric", gm.handleFunc())

	body := `[{"ts":"1700000000000","tables":[{"name":"t","metric_groups":[{}]}]}]`
	req := httptest.NewRequest(http.MethodPost, "/general-metric", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-QID", "1")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want=%d", w.Code, http.StatusBadRequest)
	}

	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal error: %v; raw=%q", err, w.Body.String())
	}
	errMsg := resp["error"]
	if !strings.HasPrefix(errMsg, "process records error. ") {
		t.Fatalf("error prefix mismatch: %q", errMsg)
	}
	if !strings.Contains(errMsg, "unexpected status code 500:body:boom") {
		t.Fatalf("unexpected error detail: %q", errMsg)
	}
}

func TestNewGeneralMetric(t *testing.T) {
	var conf config.Config
	jsonCfg := `{
        "tdengine": {
            "usessl": true,
            "host": "127.0.0.1",
            "port": 6041,
            "username": "u",
            "password": "p"
        },
        "metrics": {
            "database": { "name": "db" }
        }
    }`
	if err := json.Unmarshal([]byte(jsonCfg), &conf); err != nil {
		t.Fatalf("unmarshal config error: %v", err)
	}

	gm := NewGeneralMetric(&conf)
	if gm == nil || gm.url == nil {
		t.Fatalf("NewGeneralMetric returned nil or url is nil")
	}
	if gm.url.Scheme != "https" {
		t.Fatalf("scheme = %q, want %q", gm.url.Scheme, "https")
	}
}

func TestGeneralMetric_lineWriteBody(t *testing.T) {
	old := logger.Logger.GetLevel()
	logger.Logger.SetLevel(logrus.TraceLevel)
	defer logger.Logger.SetLevel(old)

	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(bytes.NewBuffer(nil)),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	client := &http.Client{Transport: rt}

	gm := &GeneralMetric{
		client:   client,
		username: "u",
		password: "p",
		url: &url.URL{
			Scheme:   "http",
			Host:     "example.com",
			Path:     "/influxdb/v1/write",
			RawQuery: "db=test&precision=ms&table_name_key=" + STABLE_NAME_KEY,
		},
	}

	var buf bytes.Buffer
	buf.WriteString("m  1\n")

	if err := gm.lineWriteBody(&buf, 42); err != nil {
		t.Fatalf("lineWriteBody returned error: %v", err)
	}
}
