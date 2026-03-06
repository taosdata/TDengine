package api

import (
	"bytes"
	"io"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taoskeeper/infrastructure/log"
)

var middlewareLogger = log.GetLogger("METRIC_MIDDLEWARE")

// MetricCacheMiddleware AOP middleware (synchronous version)
func MetricCacheMiddleware(parser *MetricParser) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Fast path 1: non-POST request
		if c.Request.Method != "POST" {
			c.Next()
			return
		}

		// Fast path 2: path not matched
		path := c.Request.URL.Path
		if !shouldCachePath(path) {
			c.Next()
			return
		}

		// Read request body
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.Next()
			return
		}
		c.Request.Body = io.NopCloser(bytes.NewBuffer(body))

		// Synchronous parsing (~5µs only, negligible latency)
		if err := parser.ParseAndStore(c, body); err != nil {
			middlewareLogger.Debugf("Failed to parse metrics: %v", err)
		}

		c.Next()
	}
}

// shouldCachePath matches paths
var cachePathMap = map[string]bool{
	"/general-metric":        true,
	"/taosd-cluster-basic":   true,
	"/slow-sql-detail-batch": true,
	"/adapter_report":        true,
}

func shouldCachePath(path string) bool {
	for prefix := range cachePathMap {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}
