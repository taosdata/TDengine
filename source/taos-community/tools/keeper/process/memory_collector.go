package process

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// MemoryStoreCollector Prometheus Collector for memory store
type MemoryStoreCollector struct {
	store  *MemoryStore
	maxAge time.Duration
}

// NewMemoryStoreCollector creates a collector for memory store
func NewMemoryStoreCollector(store *MemoryStore, maxAge time.Duration) *MemoryStoreCollector {
	return &MemoryStoreCollector{
		store:  store,
		maxAge: maxAge,
	}
}

// Describe implements Prometheus Collector interface
// Dynamic descriptors; no static descriptions to send
func (m *MemoryStoreCollector) Describe(ch chan<- *prometheus.Desc) {
	// No-op: metrics are dynamically generated in Collect()
}

// Collect implements Prometheus Collector interface
func (m *MemoryStoreCollector) Collect(ch chan<- prometheus.Metric) {
	now := time.Now()
	minTimestamp := now.Add(-m.maxAge)

	// Get valid data using filter method
	allData := m.store.GetAllFiltered(minTimestamp)

	for _, data := range allData {
		labels := make(prometheus.Labels)
		for k, v := range data.Tags {
			labels[k] = v
		}

		for metricName, value := range data.Metrics {
			// Build full metric name: table_metric
			fqName := data.TableName + "_" + metricName
			desc := prometheus.NewDesc(fqName, "Metric from "+data.TableName, nil, labels)
			metric, err := prometheus.NewConstMetric(desc, prometheus.GaugeValue, value)
			if err == nil {
				ch <- metric
			}
		}
	}
}
