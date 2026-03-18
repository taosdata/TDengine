package api

import (
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/process"
)

// TestMetricsV2_EndToEnd tests end-to-end flow: send data -> cache -> v2 API returns
func TestMetricsV2_EndToEnd(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 1. Create router and middleware
	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{}) // Use default config (write_metrics not cached)

	router := gin.New()
	router.Use(MetricCacheMiddleware(parser))

	// 2. Register general-metric endpoint
	router.POST("/general-metric", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// 3. Register metrics/v2 endpoint
	router.GET("/metrics/v2", func(c *gin.Context) {
		maxAge := 30 * time.Second
		collector := process.NewMemoryStoreCollector(store, maxAge)

		reg := prometheus.NewRegistry()
		reg.MustRegister(collector)

		handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
		handler.ServeHTTP(c.Writer, c.Request)
	})

	// 4. Send data to /general-metric (using current timestamp)
	currentTs := time.Now().UnixMilli()
	requestBody := `[{
		"ts": "` + strconv.FormatInt(currentTs, 10) + `",
		"protocol": 2,
		"tables": [{
			"name": "taosd_cluster_info",
			"metric_groups": [{
				"tags": [{"name": "cluster_id", "value": "123"}],
				"metrics": [
					{"name": "dbs_total", "value": 5},
					{"name": "master_uptime", "value": 100}
				]
			}]
		}]
	}]`

	req := httptest.NewRequest("POST", "/general-metric", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)

	// 5. Call /metrics/v2 to verify returned data
	req2 := httptest.NewRequest("GET", "/metrics/v2", nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	assert.Equal(t, 200, w2.Code)

	responseBody := w2.Body.String()

	// Verify Prometheus format data contains expected metrics
	assert.Contains(t, responseBody, "taosd_cluster_info_dbs_total")
	assert.Contains(t, responseBody, "taosd_cluster_info_master_uptime")
	assert.Contains(t, responseBody, `cluster_id="123"`)
	assert.Contains(t, responseBody, "5")
	assert.Contains(t, responseBody, "100")
}

// TestMetricsV2_WithWriteMetricsDisabled tests that write_metrics is not cached by default
func TestMetricsV2_WithWriteMetricsDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{}) // No additional tables configured

	router := gin.New()
	router.Use(MetricCacheMiddleware(parser))
	router.POST("/general-metric", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})
	router.GET("/metrics/v2", func(c *gin.Context) {
		maxAge := 30 * time.Second
		collector := process.NewMemoryStoreCollector(store, maxAge)

		reg := prometheus.NewRegistry()
		reg.MustRegister(collector)

		handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
		handler.ServeHTTP(c.Writer, c.Request)
	})

	// Send write_metrics data
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

	req := httptest.NewRequest("POST", "/general-metric", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)

	// Verify v2 API does not return write_metrics
	req2 := httptest.NewRequest("GET", "/metrics/v2", nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	assert.Equal(t, 200, w2.Code)
	assert.NotContains(t, w2.Body.String(), "taosd_write_metrics_commit_count")
}

// TestMetricsV2_WithWriteMetricsEnabled tests enabling write_metrics via configuration
func TestMetricsV2_WithWriteMetricsEnabled(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{"taosd_write_metrics"}) // Configure additional tables

	router := gin.New()
	router.Use(MetricCacheMiddleware(parser))
	router.POST("/general-metric", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})
	router.GET("/metrics/v2", func(c *gin.Context) {
		maxAge := 30 * time.Second
		collector := process.NewMemoryStoreCollector(store, maxAge)

		reg := prometheus.NewRegistry()
		reg.MustRegister(collector)

		handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
		handler.ServeHTTP(c.Writer, c.Request)
	})

	// Send write_metrics data (using current timestamp)
	currentTs := time.Now().UnixMilli()
	requestBody := `[{
		"ts": "` + strconv.FormatInt(currentTs, 10) + `",
		"protocol": 2,
		"tables": [{
			"name": "taosd_write_metrics",
			"metric_groups": [{
				"tags": [{"name": "cluster_id", "value": "123"}],
				"metrics": [{"name": "commit_count", "value": 42}]
			}]
		}]
	}]`

	req := httptest.NewRequest("POST", "/general-metric", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)

	// Verify v2 API returns write_metrics
	req2 := httptest.NewRequest("GET", "/metrics/v2", nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	assert.Equal(t, 200, w2.Code)
	assert.Contains(t, w2.Body.String(), "taosd_write_metrics_commit_count")
	assert.Contains(t, w2.Body.String(), `cluster_id="123"`)
	assert.Contains(t, w2.Body.String(), "42")
}

// TestMetricsV2_ExcludedTables tests that default excluded tables are filtered
func TestMetricsV2_ExcludedTables(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{}) // Default config

	router := gin.New()
	router.Use(MetricCacheMiddleware(parser))
	router.POST("/general-metric", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})
	router.POST("/slow-sql-detail-batch", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})
	router.GET("/metrics/v2", func(c *gin.Context) {
		maxAge := 30 * time.Second
		collector := process.NewMemoryStoreCollector(store, maxAge)

		reg := prometheus.NewRegistry()
		reg.MustRegister(collector)

		handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
		handler.ServeHTTP(c.Writer, c.Request)
	})

	// Test adapter_c_interface (excluded by default, ~200 metrics)
	requestBody1 := `[{
		"ts": "1703226836761",
		"protocol": 2,
		"tables": [{
			"name": "adapter_c_interface",
			"metric_groups": [{
				"tags": [{"name": "endpoint", "value": "test:6041"}],
				"metrics": [{"name": "total", "value": 100}]
			}]
		}]
	}]`

	req1 := httptest.NewRequest("POST", "/general-metric", strings.NewReader(requestBody1))
	req1.Header.Set("Content-Type", "application/json")
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)

	assert.Equal(t, 200, w1.Code)

	req2 := httptest.NewRequest("GET", "/metrics/v2", nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	assert.Equal(t, 200, w2.Code)
	assert.NotContains(t, w2.Body.String(), "adapter_c_interface_total")

	// Test taos_slow_sql_detail (excluded by default, query-type data)
	requestBody2 := `[{
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

	req3 := httptest.NewRequest("POST", "/slow-sql-detail-batch", strings.NewReader(requestBody2))
	req3.Header.Set("Content-Type", "application/json")
	w3 := httptest.NewRecorder()
	router.ServeHTTP(w3, req3)

	assert.Equal(t, 200, w3.Code)

	// Verify slow SQL details are not cached in memory
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 0, len(allData))
}

// TestMetricsV2_ExpiredData tests that expired data is not returned
func TestMetricsV2_ExpiredData(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()

	// 1. Store data from 30 seconds ago (simulate data reporting stopped)
	oldTimestamp := time.Now().Add(-30 * time.Second)
	expiredTags := map[string]string{"cluster_id": "123"}
	expiredMetrics := map[string]float64{"dbs_total": 5, "master_uptime": 100}
	store.SetWithTimestamp("taosd_cluster_info", expiredTags, expiredMetrics, oldTimestamp)

	// 2. When maxAge=25s, should not get data
	collector1 := process.NewMemoryStoreCollector(store, 25*time.Second)
	reg1 := prometheus.NewRegistry()
	reg1.MustRegister(collector1)

	ch1 := make(chan prometheus.Metric, 100)
	go collector1.Collect(ch1)
	metricCount1 := 0
	timeout1 := time.After(100 * time.Millisecond)
loop1:
	for {
		select {
		case <-ch1:
			metricCount1++
		case <-timeout1:
			break loop1
		}
	}
	assert.Equal(t, 0, metricCount1, "maxAge=25s should return 0 metrics for 30s old data")

	// 3. When maxAge=35s, should get data
	collector2 := process.NewMemoryStoreCollector(store, 35*time.Second)
	reg2 := prometheus.NewRegistry()
	reg2.MustRegister(collector2)

	ch2 := make(chan prometheus.Metric, 100)
	go collector2.Collect(ch2)
	metricCount2 := 0
	timeout2 := time.After(100 * time.Millisecond)
loop2:
	for {
		select {
		case <-ch2:
			metricCount2++
		case <-timeout2:
			break loop2
		}
	}
	assert.Equal(t, 2, metricCount2, "maxAge=35s should return 2 metrics for 30s old data")
}
