package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/taosdata/taoskeeper/infrastructure/log"
	"github.com/taosdata/taoskeeper/process"
)

var perfLogger = log.GetLogger("NODE_EXPORTER")

type NodeExporter struct {
	processor   *process.Processor
	memoryStore *process.MemoryStore
	reporter    *Reporter
}

func NewNodeExporter(processor *process.Processor, memoryStore *process.MemoryStore, reporter *Reporter) *NodeExporter {
	return &NodeExporter{
		processor:   processor,
		memoryStore: memoryStore,
		reporter:    reporter,
	}
}

func (z *NodeExporter) Init(c gin.IRouter) {
	// v1: Legacy mode (database)
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(z.processor)
	c.GET("metrics", z.myMiddleware(promhttp.HandlerFor(reg, promhttp.HandlerOpts{})))

	// v2: Memory mode
	c.GET("metrics/v2", z.serveMetricsV2)
}

func (z *NodeExporter) myMiddleware(next http.Handler) gin.HandlerFunc {
	return func(c *gin.Context) {
		z.processor.Process()
		next.ServeHTTP(c.Writer, c.Request)
	}
}

// serveMetricsV2 v2: Memory mode + Real-time performance_schema
// Query parameters:
//   - max_age: Max data age in seconds (default 30, e.g., 30, 60, 300)
func (z *NodeExporter) serveMetricsV2(c *gin.Context) {
	// Parse max_age parameter (default 30 seconds)
	maxAge := 30 * time.Second
	if maxAgeStr := c.Query("max_age"); maxAgeStr != "" {
		if seconds, err := strconv.Atoi(maxAgeStr); err == nil && seconds > 0 {
			maxAge = time.Duration(seconds) * time.Second
		}
	}

	reg := prometheus.NewPedanticRegistry()

	// 1. Register memory cache collector (cached metrics)
	memoryCollector := process.NewMemoryStoreCollector(z.memoryStore, maxAge)
	reg.MustRegister(memoryCollector)

	// 2. Register performance_schema collector (real-time query)
	perfCollector := NewPerformanceCollector(z.reporter)
	reg.MustRegister(perfCollector)

	// Log if memory cache is empty (for debugging)
	stats := z.memoryStore.GetStats()
	if totalMetrics, ok := stats["total_metrics"].(int); ok && totalMetrics == 0 {
		perfLogger.Trace("Memory cache is empty, ensure data is sent to /general-metric or /adapter_report")
	}

	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(c.Writer, c.Request)
}
