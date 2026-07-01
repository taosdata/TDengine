package api

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/log"
)

// verySlowQueryThreshold defines the threshold for very slow queries (300ms)
const verySlowQueryThreshold = 300 * time.Millisecond

// PerformanceCollector collects performance_schema metrics in real-time
type PerformanceCollector struct {
	reporter *Reporter
}

// NewPerformanceCollector creates a performance_schema collector
func NewPerformanceCollector(reporter *Reporter) *PerformanceCollector {
	return &PerformanceCollector{
		reporter: reporter,
	}
}

// Describe implements prometheus.Collector interface
func (p *PerformanceCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc(
		"taosd_perf_slow_queries_total",
		"Total slow queries from performance_schema.perf_queries",
		nil, nil,
	)

	ch <- prometheus.NewDesc(
		"taosd_perf_very_slow_queries_total",
		"Slow queries with exec_time > 300ms from performance_schema.perf_queries",
		nil, nil,
	)
}

// Collect implements prometheus.Collector interface
func (p *PerformanceCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn := p.reporter.tryGetConn()
	if conn == nil {
		log.GetLogger("PERF_COLLECTOR").Trace("Database connection unavailable, skip performance_schema query")
		return
	}
	defer conn.Close() // Close connection to prevent resource leak

	p.collectSlowQueries(ctx, conn, ch)
	p.collectVerySlowQueries(ctx, conn, ch)
}

// collectSlowQueries queries total slow query count
func (p *PerformanceCollector) collectSlowQueries(ctx context.Context, conn *db.Connector, ch chan<- prometheus.Metric) {
	query := "SELECT COUNT(*) as slow_count FROM performance_schema.perf_queries"

	data, err := conn.Query(ctx, query, 0)
	if err != nil {
		log.GetLogger("PERF_COLLECTOR").Tracef("Failed to query perf_queries: %v", err)
		return
	}

	var count int64 = 0
	if len(data.Data) > 0 && len(data.Data[0]) > 0 {
		count = toInt64(data.Data[0][0])
	}

	desc := prometheus.NewDesc(
		"taosd_perf_slow_queries_total",
		"Total slow queries from performance_schema.perf_queries",
		nil, nil,
	)

	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(count))
	log.GetLogger("PERF_COLLECTOR").Tracef("Collected perf_slow_queries: %d", count)
}

// collectVerySlowQueries queries very slow query count (>300ms exec time)
func (p *PerformanceCollector) collectVerySlowQueries(ctx context.Context, conn *db.Connector, ch chan<- prometheus.Metric) {
	query := fmt.Sprintf(`
		SELECT COUNT(*) as slow_count
		FROM performance_schema.perf_queries
		WHERE exec_usec > %d
	`, verySlowQueryThreshold.Microseconds())

	data, err := conn.Query(ctx, query, 0)
	if err != nil {
		log.GetLogger("PERF_COLLECTOR").Tracef("Failed to query perf_queries (exec_usec > 300ms): %v", err)
		return
	}

	var count int64 = 0
	if len(data.Data) > 0 && len(data.Data[0]) > 0 {
		count = toInt64(data.Data[0][0])
	}

	desc := prometheus.NewDesc(
		"taosd_perf_very_slow_queries_total",
		"Slow queries with exec_time > 300ms from performance_schema.perf_queries",
		nil, nil,
	)

	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(count))
	log.GetLogger("PERF_COLLECTOR").Tracef("Collected perf_very_slow_queries: %d", count)
}

// toInt64 converts interface{} to int64
func toInt64(v interface{}) int64 {
	if v == nil {
		return 0
	}

	switch val := v.(type) {
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case uint:
		return int64(val)
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		if val > uint64(math.MaxInt64) {
			log.GetLogger("PERF_COLLECTOR").Warnf("uint64 value %d exceeds int64 max, using MaxInt64", val)
			return math.MaxInt64
		}
		return int64(val)
	case float32:
		return int64(val)
	case float64:
		return int64(val)
	case string:
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0
		}
		return i
	default:
		return 0
	}
}
