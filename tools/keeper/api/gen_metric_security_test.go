// Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/util"
)

// setupGeneralMetricTest prepares the metrics database and the stables.
// createSTables normally runs inside Init, which only the first test in this
// package executes (router_inited guard); later tests must create the stables
// themselves after re-creating the database. The database is intentionally
// NOT dropped here: these tests run before the existing gen_metric tests
// which rely on the stables created by the first Init.
func setupGeneralMetricTest(t *testing.T) (*GeneralMetric, *db.Connector) {
	cfg := config.GetCfg()
	CreateDatabase(cfg.TDengine.Username, cfg.TDengine.Password, cfg.TDengine.Host, cfg.TDengine.Port, cfg.TDengine.Usessl, cfg.Metrics.Database.Name, cfg.Metrics.Database.Options)

	gm := NewGeneralMetric(cfg)
	if !router_inited {
		assert.NoError(t, gm.Init(router))
		router_inited = true
	}

	conn, err := db.NewConnectorWithDb(gm.username, gm.password, gm.host, gm.port, gm.database, gm.usessl)
	assert.NoError(t, err)
	if gm.conn == nil {
		gm.conn = conn
	}
	assert.NoError(t, gm.createSTables())
	return gm, conn
}

// The sql column of taos_slow_sql_detail must round-trip bytes exactly: a
// backslash in the original statement used to be consumed by the server as an
// escape character, silently corrupting the stored statement.
func TestSlowSqlDetailBatch_BackslashRoundTrip(t *testing.T) {
	gm, conn := setupGeneralMetricTest(t)

	// this test asserts on the statement built from a single request, so the
	// shared MAX_SQL_LEN must hold its default here. Save and restore it so the
	// value cannot leak into other tests either way.
	maxSqlLen := MAX_SQL_LEN
	MAX_SQL_LEN = 1000000
	t.Cleanup(func() { MAX_SQL_LEN = maxSqlLen })
	rawSql := `select * from t where a = 'x\'); drop table y; --'`
	startTs := int64(1759999999001)
	body := fmt.Sprintf(`[{
		"start_ts": "%d",
		"request_id": "97531",
		"query_time": 100,
		"code": 0,
		"error_info": "",
		"type": 1,
		"rows_num": 5,
		"sql": %s,
		"process_name": "abc",
		"process_id": "123",
		"db": "dbname",
		"user": "root",
		"ip": "127.0.0.1",
		"cluster_id": "sec123"
	}]`, startTs, strconv.Quote(rawSql))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/slow-sql-detail-batch", strings.NewReader(body))
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	data, err := conn.Query(context.Background(), fmt.Sprintf("select sql from %s.taos_slow_sql_detail where start_ts=%d", gm.database, startTs), util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	if assert.Equal(t, 1, len(data.Data)) {
		assert.Equal(t, rawSql, data.Data[0][0])
	}
}

// A non-numeric ts in the taosd-cluster-basic payload is rejected with 400
// instead of being concatenated into the insert statement.
func TestClusterBasic_InvalidTs_Returns400(t *testing.T) {
	setupGeneralMetricTest(t)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"ts":"1) ,('x","cluster_id":"c1","protocol":2,"first_ep":"ep:6030","first_ep_dnode_id":1,"cluster_version":"3.0"}`)
	req, _ := http.NewRequest(http.MethodPost, "/taosd-cluster-basic", body)
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// Every request-controlled string of taosd-cluster-basic must be neutralized
// in the built SQL, like the audit and report endpoints.
func TestClusterBasic_InjectionFieldsAreNeutralized(t *testing.T) {
	evilCluster := `c'); drop table operations; --`
	evilEp := `ep\'); drop table operations; --`
	evilVersion := `v'); delete from operations; --`

	sql := buildClusterBasicSQL("keeper", ClusterBasic{
		ClusterId:      evilCluster,
		FirstEp:        evilEp,
		FirstEpDnodeId: 1,
		ClusterVersion: evilVersion,
	}, 1699839716440)

	// no raw payload survives: quotes are doubled, backslashes escaped
	assert.NotContains(t, sql, evilCluster)
	assert.NotContains(t, sql, evilEp)
	assert.NotContains(t, sql, evilVersion)
	assert.Contains(t, sql, "'"+util.EscapeSQLString(evilCluster)+"'")
	assert.Contains(t, sql, "'"+util.EscapeTrunc(evilEp, 255)+"'")
	assert.Contains(t, sql, "'"+util.EscapeTrunc(evilVersion, 20)+"'")
	// the sub-table name suffix is an identifier: no quote or backslash
	name := sql[strings.Index(sql, "taosd_cluster_basic_"):strings.Index(sql, " using ")]
	assert.NotContains(t, name, "'")
	assert.NotContains(t, name, "\\")
}

// In a slow-sql batch, records with a non-numeric start_ts are skipped while
// the valid records are still inserted.
func TestSlowSqlDetailBatch_SkipsInvalidRecords(t *testing.T) {
	gm, conn := setupGeneralMetricTest(t)

	// this test asserts on the statement built from a single request, so the
	// shared MAX_SQL_LEN must hold its default here. Save and restore it so the
	// value cannot leak into other tests either way.
	maxSqlLen := MAX_SQL_LEN
	MAX_SQL_LEN = 1000000
	t.Cleanup(func() { MAX_SQL_LEN = maxSqlLen })
	validStartTs := int64(1759999999002)
	record := func(startTs, requestId string) string {
		return fmt.Sprintf(`{
			"start_ts": %s,
			"request_id": %s,
			"query_time": 100,
			"code": 0,
			"error_info": "",
			"type": 1,
			"rows_num": 5,
			"sql": "select 1;",
			"process_name": "abc",
			"process_id": "123",
			"db": "dbname",
			"user": "root",
			"ip": "127.0.0.1",
			"cluster_id": "sec456"
		}`, strconv.Quote(startTs), strconv.Quote(requestId))
	}
	body := "[" + record("not-a-ts", "97541") + "," + record(strconv.FormatInt(validStartTs, 10), "97542") + "]"

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/slow-sql-detail-batch", strings.NewReader(body))
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	data, err := conn.Query(context.Background(), fmt.Sprintf("select request_id from %s.taos_slow_sql_detail where request_id=97542", gm.database), util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(data.Data))

	data, err = conn.Query(context.Background(), fmt.Sprintf("select request_id from %s.taos_slow_sql_detail where request_id=97541", gm.database), util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
	assert.Equal(t, 0, len(data.Data))
}
