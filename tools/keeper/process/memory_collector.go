// Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

package process

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// rejectedEntriesDesc describes the rejection counter. It is a package-level
// variable so every scrape reuses one consistent descriptor.
var rejectedEntriesDesc = prometheus.NewDesc("taoskeeper_memory_store_rejected_total",
	"Metric entries rejected by the memory store because it was full: either the entry count or the byte budget was reached, or a refreshed series no longer fit the byte budget",
	nil, nil)

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

	// Always expose the rejection counter, its zero value included. A counter
	// that only appears once it is non-zero cannot be alerted on beforehand,
	// and the series has no history to compute a rate from when it does.
	metric, err := prometheus.NewConstMetric(rejectedEntriesDesc, prometheus.CounterValue, float64(m.store.RejectedTotal()))
	if err == nil {
		ch <- metric
	}
}
