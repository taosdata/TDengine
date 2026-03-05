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

func TestMetricCacheMiddleware_ShouldCachePath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{
			name:     "general-metric should cache",
			path:     "/general-metric",
			expected: true,
		},
		{
			name:     "taosd-cluster-basic should cache",
			path:     "/taosd-cluster-basic",
			expected: true,
		},
		{
			name:     "slow-sql-detail-batch should cache",
			path:     "/slow-sql-detail-batch",
			expected: true,
		},
		{
			name:     "adapter_report should cache",
			path:     "/adapter_report",
			expected: true,
		},
		{
			name:     "other paths should not cache",
			path:     "/metrics",
			expected: false,
		},
		{
			name:     "health check should not cache",
			path:     "/check_health",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldCachePath(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMetricCacheMiddleware_InterceptAndCache(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{})

	// Setup router and middleware
	router := gin.New()
	router.Use(MetricCacheMiddleware(parser))
	router.POST("/general-metric", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// Send request
	requestBody := `[{
		"ts": "1703226836761",
		"protocol": 2,
		"tables": [{
			"name": "taosd_cluster_info",
			"metric_groups": [{
				"tags": [{"name": "cluster_id", "value": "123"}],
				"metrics": [{"name": "dbs_total", "value": 1}]
			}]
		}]
	}]`

	req := httptest.NewRequest("POST", "/general-metric", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// Verify response
	assert.Equal(t, 200, w.Code)

	// Synchronous parsing, immediately available
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 1, len(allData))
	assert.Equal(t, "taosd_cluster_info", allData[0].TableName)
}

func TestMetricCacheMiddleware_SkipNonPostRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{})

	router := gin.New()
	router.Use(MetricCacheMiddleware(parser))
	router.GET("/general-metric", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	req := httptest.NewRequest("GET", "/general-metric", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)

	// GET requests should not cache data
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 0, len(allData))
}

func TestMetricCacheMiddleware_SkipNonMatchingPaths(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{})

	router := gin.New()
	router.Use(MetricCacheMiddleware(parser))
	router.POST("/other-path", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	requestBody := `{"test": "data"}`
	req := httptest.NewRequest("POST", "/other-path", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)

	// Non-matching paths should not cache
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 0, len(allData))
}

func TestMetricCacheMiddleware_PreserveRequestBody(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store, _ := process.NewMemoryStore(5 * time.Minute)
	defer store.Close()
	parser := NewMetricParser(store, []string{})

	router := gin.New()
	router.Use(MetricCacheMiddleware(parser))
	router.POST("/general-metric", func(c *gin.Context) {
		// Handler can read request body normally
		c.JSON(200, gin.H{"status": "ok"})
	})

	requestBody := `[{
		"ts": "1703226836761",
		"protocol": 2,
		"tables": [{
			"name": "taosd_cluster_info",
			"metric_groups": [{
				"tags": [{"name": "cluster_id", "value": "123"}],
				"metrics": [{"name": "dbs_total", "value": 1}]
			}]
		}]
	}]`

	req := httptest.NewRequest("POST", "/general-metric", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// Verify handler responds normally (request body correctly restored)
	assert.Equal(t, 200, w.Code)
}
