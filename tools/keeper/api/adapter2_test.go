package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	a := NewAdapter(c)
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
		_, _ = conn.Query(context.Background(), "drop database if exists adapter_report_test", util.GetQidOwn())
	}()

	assert.NoError(t, err)
	data, err := conn.Query(context.Background(), "select * from adapter_report_test.adapter_requests where req_type=0", util.GetQidOwn())

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

	data, err = conn.Query(context.Background(), "select * from adapter_report_test.adapter_requests where req_type=1", util.GetQidOwn())
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

	conn.Exec(context.Background(), "drop database "+c.Metrics.Database.Name, util.GetQidOwn())
}
