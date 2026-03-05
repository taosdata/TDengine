package api

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taoskeeper/infrastructure/log"
	"github.com/taosdata/taoskeeper/process"
)

var parserLogger = log.GetLogger("METRIC_PARSER")

// Only cache tables with following prefixes
var supportedPrefixes = []string{
	"taosd_",
	"taos_",
	"adapter_",
}

// Tables excluded by default (high cardinality or query-type data)
var excludedTables = map[string]bool{
	"taos_slow_sql_detail": true, // Query-type data
	"adapter_c_interface":    true, // ~200 C interface metrics
	"taosd_write_metrics":     true, // Write performance metrics (high cardinality)
}

// MetricParser parses and caches metrics
type MetricParser struct {
	memoryStore   *process.MemoryStore
	includeTables map[string]bool // User-configured additional tables (restore from exclusion list)
}

// NewMetricParser creates a metric parser
func NewMetricParser(store *process.MemoryStore, includeTables []string) *MetricParser {
	includeMap := make(map[string]bool)
	for _, table := range includeTables {
		includeMap[table] = true
	}
	return &MetricParser{
		memoryStore:   store,
		includeTables: includeMap,
	}
}

// isSupportedTable checks if table is supported for caching
func (p *MetricParser) isSupportedTable(tableName string) bool {
	for _, prefix := range supportedPrefixes {
		if strings.HasPrefix(tableName, prefix) {
			if excludedTables[tableName] {
				return p.includeTables[tableName]
			}
			return true
		}
	}
	return false
}

// ParseAndStore parses and stores metrics to memory
func (p *MetricParser) ParseAndStore(c *gin.Context, body []byte) error {
	path := c.Request.URL.Path

	switch {
	case path == "/general-metric":
		return p.parseGeneralMetric(c, body)
	case path == "/taosd-cluster-basic":
		return p.parseClusterBasic(c, body)
	case path == "/slow-sql-detail-batch":
		return p.parseSlowSqlDetail(c, body)
	case path == "/adapter_report":
		return p.parseAdapterReport(c, body)
	default:
		return nil
	}
}

// parseGeneralMetric parses general-metric data with automatic field support
func (p *MetricParser) parseGeneralMetric(c *gin.Context, body []byte) error {
	var request []struct {
		Ts     string `json:"ts"`
		Tables []struct {
			Name         string `json:"name"`
			MetricGroups []struct {
				Tags    []struct {
					Name  string `json:"name"`
					Value string `json:"value"`
				} `json:"tags"`
				Metrics []struct {
					Name  string  `json:"name"`
					Value float64 `json:"value"`
				} `json:"metrics"`
			} `json:"metric_groups"`
		} `json:"tables"`
	}

	if err := json.Unmarshal(body, &request); err != nil {
		return err
	}

	cachedCount := 0
	for _, stableArrayInfo := range request {
		// Parse timestamp from string (milliseconds since epoch)
		tsMillis, err := strconv.ParseInt(stableArrayInfo.Ts, 10, 64)
		if err != nil {
			parserLogger.Warnf("Failed to parse timestamp %s: %v", stableArrayInfo.Ts, err)
			continue
		}
		timestamp := time.UnixMilli(tsMillis)

		for _, table := range stableArrayInfo.Tables {
			tableName := strings.ToLower(table.Name)

			if !p.isSupportedTable(tableName) {
				continue
			}

			for _, metricGroup := range table.MetricGroups {
				tags := make(map[string]string)
				for _, tag := range metricGroup.Tags {
					tags[tag.Name] = tag.Value
				}

				metrics := make(map[string]float64)
				for _, metric := range metricGroup.Metrics {
					metrics[metric.Name] = metric.Value
				}

				p.memoryStore.SetWithTimestamp(tableName, tags, metrics, timestamp)
				cachedCount++
				parserLogger.Tracef("Cached metric: table=%s, tags=%d, metrics=%d",
					tableName, len(tags), len(metrics))
			}
		}
	}

	if cachedCount > 0 {
		parserLogger.Tracef("general-metric: cached %d metric groups", cachedCount)
	}
	return nil
}

// parseClusterBasic parses cluster basic information
func (p *MetricParser) parseClusterBasic(c *gin.Context, body []byte) error {
	var request struct {
		ClusterId      string `json:"cluster_id"`
		FirstEp        string `json:"first_ep"`
		FirstEpDnodeId int32  `json:"first_ep_dnode_id"`
		Ts             string `json:"ts"`
		ClusterVersion string `json:"cluster_version"`
	}

	if err := json.Unmarshal(body, &request); err != nil {
		return err
	}

	// Parse timestamp from string (milliseconds since epoch)
	tsMillis, err := strconv.ParseInt(request.Ts, 10, 64)
	if err != nil {
		parserLogger.Warnf("Failed to parse timestamp %s: %v", request.Ts, err)
		return err
	}
	timestamp := time.UnixMilli(tsMillis)

	tags := map[string]string{
		"cluster_id":      request.ClusterId,
		"first_ep":        request.FirstEp,
		"cluster_version": request.ClusterVersion,
	}
	metrics := map[string]float64{
		"first_ep_dnode_id": float64(request.FirstEpDnodeId),
	}

	p.memoryStore.SetWithTimestamp("taosd_cluster_basic", tags, metrics, timestamp)
	parserLogger.Tracef("Cached cluster basic info: cluster_id=%s, version=%s", request.ClusterId, request.ClusterVersion)
	return nil
}

// parseSlowSqlDetail skips caching slow SQL detail records
func (p *MetricParser) parseSlowSqlDetail(c *gin.Context, body []byte) error {
	parserLogger.Trace("Skipped slow SQL detail (historical query data, not real-time metric)")
	return nil
}

// parseAdapterReport parses adapter report with universal handling
func (p *MetricParser) parseAdapterReport(c *gin.Context, body []byte) error {
	var request struct {
		Ts           int64             `json:"ts"`
		Metrics      AdapterMetrics    `json:"metrics"`
		Endpoint     string            `json:"endpoint"`
		ExtraMetrics []StableArrayInfo `json:"extra_metrics"`
	}

	if err := json.Unmarshal(body, &request); err != nil {
		return err
	}

	// Parse timestamp (seconds since epoch)
	timestamp := time.Unix(request.Ts, 0)

	// 1. Cache adapter_requests (rest and ws)
	p.cacheAdapterRequests(request.Endpoint, 0, &request.Metrics, timestamp)
	p.cacheAdapterRequests(request.Endpoint, 1, &request.Metrics, timestamp)

	// 2. Cache extra_metrics tables with automatic field support
	if len(request.ExtraMetrics) > 0 {
		for _, stableArrayInfo := range request.ExtraMetrics {
			for _, table := range stableArrayInfo.Tables {
				tableName := strings.ToLower(table.Name)
				if !p.isSupportedTable(tableName) {
					continue
				}

				for _, metricGroup := range table.MetricGroups {
					tags := make(map[string]string)
					for _, tag := range metricGroup.Tags {
						tags[tag.Name] = tag.Value
					}

					metrics := make(map[string]float64)
					for _, metric := range metricGroup.Metrics {
						metrics[metric.Name] = metric.Value
					}

					p.memoryStore.SetWithTimestamp(tableName, tags, metrics, timestamp)
					parserLogger.Tracef("Cached adapter metric: table=%s", tableName)
				}
			}
		}
	}

	parserLogger.Tracef("Cached adapter report: endpoint=%s", request.Endpoint)
	return nil
}

// cacheAdapterRequests caches adapter_requests with automatic field mapping
func (p *MetricParser) cacheAdapterRequests(endpoint string, reqType int, metrics *AdapterMetrics, timestamp time.Time) {
	tags := map[string]string{
		"endpoint": endpoint,
		"req_type": strconv.Itoa(reqType),
	}

	prefix := "Rest"
	if reqType == 1 {
		prefix = "WS"
	}

	metricMap := make(map[string]float64)
	v := reflect.ValueOf(metrics).Elem()
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i)

		if strings.HasPrefix(field.Name, prefix) && field.Type.Kind() == reflect.Int {
			metricName := strings.ToLower(strings.TrimPrefix(field.Name, prefix))
			metricMap[metricName] = float64(value.Int())
		}
	}

	p.memoryStore.SetWithTimestamp("adapter_requests", tags, metricMap, timestamp)
}

