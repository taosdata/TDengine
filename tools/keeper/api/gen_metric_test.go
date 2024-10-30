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
		_, _ = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", gm.database))
	}()

	t.Run(testcfg.name, func(t *testing.T) {
		w := httptest.NewRecorder()
		body := strings.NewReader(testcfg.data)
		req, _ := http.NewRequest(http.MethodPost, "/taosd-cluster-basic", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		data, err := conn.Query(context.Background(), fmt.Sprintf("select ts, cluster_id from %s.%s where ts=%d", gm.database, testcfg.tbname, testcfg.ts))
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
		_, _ = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", gm.database))
	}()

	t.Run(testcfg.name, func(t *testing.T) {
		w := httptest.NewRecorder()
		body := strings.NewReader(testcfg.data)
		req, _ := http.NewRequest(http.MethodPost, "/general-metric", body)
		router.ServeHTTP(w, req)
		assert.Equal(t, 200, w.Code)

		for _, tbname := range testcfg.tbname {
			for _, ts := range testcfg.ts {
				data, err := conn.Query(context.Background(), fmt.Sprintf("select _ts, cluster_id from %s.%s where _ts=%d", gm.database, tbname, ts))
				assert.NoError(t, err)
				assert.Equal(t, 1, len(data.Data))
				assert.Equal(t, testcfg.expect, data.Data[0][1])
			}
		}
	})
}
