package api

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/process"
)

func TestMetricParser_ParseGeneralMetric(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{})

	// general-metric request data
	requestBody := `[{
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
					"value": 100
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
				}],
				"metrics": [{
					"name": "uptime",
					"value": 200
				}]
			}]
		}]
	}]`

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/general-metric", strings.NewReader(requestBody))
	c.Request.Header.Set("Content-Type", "application/json")

	// Parse and store in memory
	err := parser.ParseAndStore(c, []byte(requestBody))
	assert.NoError(t, err)

	// Verify data has been stored in memory
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 2, len(allData))

	// Verify cluster_info table
	clusterInfo := findMetricByTableName(allData, "taosd_cluster_info")
	assert.NotNil(t, clusterInfo)
	assert.Equal(t, "1397715317673023180", clusterInfo.Tags["cluster_id"])
	assert.Equal(t, float64(1), clusterInfo.Metrics["dbs_total"])
	assert.Equal(t, float64(100), clusterInfo.Metrics["master_uptime"])

	// Verify dnodes_info table
	dnodesInfo := findMetricByTableName(allData, "taosd_dnodes_info")
	assert.NotNil(t, dnodesInfo)
	assert.Equal(t, "1397715317673023180", dnodesInfo.Tags["cluster_id"])
	assert.Equal(t, "1", dnodesInfo.Tags["dnode_id"])
	assert.Equal(t, float64(200), dnodesInfo.Metrics["uptime"])
}

func TestMetricParser_ParseClusterBasic(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{})

	requestBody := `{
		"ts": "1705655770381",
		"cluster_id": "7648966395564416484",
		"protocol": 2,
		"first_ep": "ssfood06:6130",
		"first_ep_dnode_id": 1,
		"cluster_version": "3.2.1.0"
	}`

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/taosd-cluster-basic", strings.NewReader(requestBody))

	err := parser.ParseAndStore(c, []byte(requestBody))
	assert.NoError(t, err)

	// Verify data
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 1, len(allData))

	data := allData[0]
	assert.Equal(t, "taosd_cluster_basic", data.TableName)
	assert.Equal(t, "7648966395564416484", data.Tags["cluster_id"])
	assert.Equal(t, "ssfood06:6130", data.Tags["first_ep"])
	assert.Equal(t, float64(1), data.Metrics["first_ep_dnode_id"])
}

func TestMetricParser_ParseAdapterReport(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{})

	requestBody := `{
		"ts": 1743660120,
		"metrics": {
			"rest_total": 5429,
			"rest_query": 5429,
			"rest_query_success": 5429,
			"ws_total": 0
		},
		"endpoint": "adapter-1:6041",
		"extra_metrics": [{
			"ts": "1743660120000",
			"protocol": 2,
			"tables": [{
				"name": "adapter_status",
				"metric_groups": [{
					"tags": [{
						"name": "endpoint",
						"value": "adapter-1:6041"
					}],
					"metrics": [{
						"name": "go_heap_sys",
						"value": 69156864
					}]
				}]
			}]
		}]
	}`

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/adapter_report", strings.NewReader(requestBody))

	err := parser.ParseAndStore(c, []byte(requestBody))
	assert.NoError(t, err)

	// Verify data: should have adapter_requests (rest + ws) and adapter_status
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 3, len(allData)) // rest, ws, adapter_status

	// Verify adapter_requests (rest)
	restRequest := findMetricByTags(allData, "adapter_requests", map[string]string{
		"endpoint": "adapter-1:6041",
		"req_type": "0",
	})
	assert.NotNil(t, restRequest)
	assert.Equal(t, float64(5429), restRequest.Metrics["total"])
	assert.Equal(t, float64(5429), restRequest.Metrics["query"])

	// Verify adapter_requests (ws)
	wsRequest := findMetricByTags(allData, "adapter_requests", map[string]string{
		"endpoint": "adapter-1:6041",
		"req_type": "1",
	})
	assert.NotNil(t, wsRequest)
	assert.Equal(t, float64(0), wsRequest.Metrics["total"])

	// Verify adapter_status
	adapterStatus := findMetricByTags(allData, "adapter_status", map[string]string{
		"endpoint": "adapter-1:6041",
	})
	assert.NotNil(t, adapterStatus)
	assert.Equal(t, float64(69156864), adapterStatus.Metrics["go_heap_sys"])
}

func TestMetricParser_SkipSlowSqlDetail(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{})

	requestBody := `[{
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
	}]`

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/slow-sql-detail-batch", strings.NewReader(requestBody))

	err := parser.ParseAndStore(c, []byte(requestBody))
	assert.NoError(t, err)

	// Slow SQL details should not be cached
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 0, len(allData))
}

func TestMetricParser_FilterUnsupportedTables(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{})

	// Including supported and unsupported tables
	requestBody := `[{
		"ts": "1703226836761",
		"protocol": 2,
		"tables": [{
			"name": "taosd_cluster_info",
			"metric_groups": [{
				"tags": [{"name": "cluster_id", "value": "123"}],
				"metrics": [{"name": "dbs_total", "value": 1}]
			}]
		}, {
			"name": "taosx_sys",
			"metric_groups": [{
				"tags": [{"name": "taosx_id", "value": "456"}],
				"metrics": [{"name": "status", "value": 1}]
			}]
		}, {
			"name": "taos_sql_req",
			"metric_groups": [{
				"tags": [{"name": "cluster_id", "value": "789"}],
				"metrics": [{"name": "count", "value": 10}]
			}]
		}]
	}]`

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/general-metric", strings.NewReader(requestBody))

	err := parser.ParseAndStore(c, []byte(requestBody))
	assert.NoError(t, err)

	// Only taosd_cluster_info and taos_sql_req are cached
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 2, len(allData))

	tableNames := make(map[string]bool)
	for _, data := range allData {
		tableNames[data.TableName] = true
	}

	assert.True(t, tableNames["taosd_cluster_info"])
	assert.True(t, tableNames["taos_sql_req"])
	assert.False(t, tableNames["taosx_sys"])
}

func TestMetricParser_InvalidJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{})

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/general-metric", strings.NewReader("invalid json"))

	err := parser.ParseAndStore(c, []byte("invalid json"))
	assert.Error(t, err)
}

func TestMetricParser_WriteMetricsConfigDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{}) // No additional tables configured, write_metrics is ignored

	requestBody := `[{
		"ts": "1703226836761",
		"protocol": 2,
		"tables": [{
			"name": "taosd_write_metrics",
			"metric_groups": [{
				"tags": [{"name": "cluster_id", "value": "123"}],
				"metrics": [{"name": "commit_count", "value": 1}]
			}]
		}]
	}]`

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/general-metric", strings.NewReader(requestBody))

	err := parser.ParseAndStore(c, []byte(requestBody))
	assert.NoError(t, err)

	// write_metrics should not be cached (default excluded)
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 0, len(allData))
}

func TestMetricParser_WriteMetricsConfigEnabled(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{"taosd_write_metrics"}) // Configure additional tables

	requestBody := `[{
		"ts": "1703226836761",
		"protocol": 2,
		"tables": [{
			"name": "taosd_write_metrics",
			"metric_groups": [{
				"tags": [{"name": "cluster_id", "value": "123"}],
				"metrics": [{"name": "commit_count", "value": 1}]
			}]
		}]
	}]`

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/general-metric", strings.NewReader(requestBody))

	err := parser.ParseAndStore(c, []byte(requestBody))
	assert.NoError(t, err)

	// write_metrics should be cached (user explicitly configured)
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 1, len(allData))
	assert.Equal(t, "taosd_write_metrics", allData[0].TableName)
	assert.Equal(t, float64(1), allData[0].Metrics["commit_count"])
}

func TestMetricParser_MultipleIncludeTables(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	// Configure multiple additional tables
	parser := NewMetricParser(store, []string{"taosd_write_metrics", "adapter_c_interface"})

	// Test write_metrics
	requestBody1 := `[{
		"ts": "1703226836761",
		"protocol": 2,
		"tables": [{
			"name": "taosd_write_metrics",
			"metric_groups": [{
				"tags": [{"name": "cluster_id", "value": "123"}],
				"metrics": [{"name": "commit_count", "value": 1}]
			}]
		}]
	}]`

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/general-metric", strings.NewReader(requestBody1))

	err := parser.ParseAndStore(c, []byte(requestBody1))
	assert.NoError(t, err)

	// Test adapter_c_interface
	requestBody2 := `[{
		"ts": "1703226836761",
		"protocol": 2,
		"tables": [{
			"name": "adapter_c_interface",
			"metric_groups": [{
				"tags": [{"name": "endpoint", "value": "test"}],
				"metrics": [{"name": "total", "value": 100}]
			}]
		}]
	}]`

	c2, _ := gin.CreateTestContext(httptest.NewRecorder())
	c2.Request = httptest.NewRequest("POST", "/general-metric", strings.NewReader(requestBody2))

	err = parser.ParseAndStore(c2, []byte(requestBody2))
	assert.NoError(t, err)

	// Both tables should be cached
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 2, len(allData))

	tableNames := make(map[string]bool)
	for _, data := range allData {
		tableNames[data.TableName] = true
	}
	assert.True(t, tableNames["taosd_write_metrics"])
	assert.True(t, tableNames["adapter_c_interface"])
}

// Helper function: find metrics by table name and tags
func findMetricByTableName(dataList []*process.MetricData, tableName string) *process.MetricData {
	for _, data := range dataList {
		if data.TableName == tableName {
			return data
		}
	}
	return nil
}

// Helper function: find metrics by table name and tags
func findMetricByTags(dataList []*process.MetricData, tableName string, tags map[string]string) *process.MetricData {
	for _, data := range dataList {
		if data.TableName != tableName {
			continue
		}

		matched := true
		for k, v := range tags {
			if data.Tags[k] != v {
				matched = false
				break
			}
		}

		if matched {
			return data
		}
	}
	return nil
}
