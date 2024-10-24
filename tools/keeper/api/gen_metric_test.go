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
	"github.com/taosdata/taoskeeper/util"
)

var router_inited bool = false

func TestClusterBasic(t *testing.T) {
	cfg := util.GetCfg()

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
		_, _ = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", gm.database), util.GetQidOwn())
	}()

	t.Run(testcfg.name, func(t *testing.T) {
		w := httptest.NewRecorder()
		body := strings.NewReader(testcfg.data)
		req, _ := http.NewRequest(http.MethodPost, "/taosd-cluster-basic", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		data, err := conn.Query(context.Background(), fmt.Sprintf("select ts, cluster_id from %s.%s where ts=%d", gm.database, testcfg.tbname, testcfg.ts), util.GetQidOwn())
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
		_, _ = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", gm.database), util.GetQidOwn())
	}()

	t.Run(testcfg.name, func(t *testing.T) {
		MAX_SQL_LEN = 1000000
		w := httptest.NewRecorder()
		body := strings.NewReader(testcfg.data)
		req, _ := http.NewRequest(http.MethodPost, "/slow-sql-detail-batch", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		data, err := conn.Query(context.Background(), fmt.Sprintf("select start_ts, cluster_id from %s.%s where start_ts=%d", gm.database, testcfg.tbname, testcfg.ts), util.GetQidOwn())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(data.Data))
		assert.Equal(t, testcfg.expect, data.Data[0][1])
	})
}

func TestGenMetric(t *testing.T) {
	cfg := util.GetCfg()

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
		_, _ = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", gm.database), util.GetQidOwn())
	}()

	t.Run(testcfg.name, func(t *testing.T) {
		w := httptest.NewRecorder()
		body := strings.NewReader(testcfg.data)
		req, _ := http.NewRequest(http.MethodPost, "/general-metric", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		for _, tbname := range testcfg.tbname {
			for _, ts := range testcfg.ts {
				data, err := conn.Query(context.Background(), fmt.Sprintf("select _ts, cluster_id from %s.%s where _ts=%d", gm.database, tbname, ts), util.GetQidOwn())
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
	}

	for _, tt := range tests {
		t.Run(tt.stbName, func(t *testing.T) {
			if got := get_sub_table_name_valid(tt.stbName, tt.tagMap); got != tt.want {
				panic(fmt.Sprintf("get_sub_table_name() = %v, want %v", got, tt.want))
			}
		})
	}
}
