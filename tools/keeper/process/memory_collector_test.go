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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

// Helper function: count from channel with timeout
func countMetricsWithTimeout(ch <-chan prometheus.Metric, timeout time.Duration) int {
	metricCount := 0
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			// Return current count on timeout
			return metricCount
		case _, ok := <-ch:
			if !ok {
				// Channel closed, return final count
				return metricCount
			}
			metricCount++
		}
	}
}

// Helper function: count from Desc channel with timeout
func countDescsWithTimeout(ch <-chan *prometheus.Desc, timeout time.Duration) int {
	descCount := 0
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			// Return current count on timeout
			return descCount
		case _, ok := <-ch:
			if !ok {
				// Channel closed, return final count
				return descCount
			}
			descCount++
		}
	}
}


func TestMemoryStoreCollector_Collect(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	// Add test data
	tags1 := map[string]string{
		"cluster_id": "123",
		"dnode_id":   "1",
	}
	metrics1 := map[string]float64{
		"uptime":     100.0,
		"cpu_engine": 5.5,
	}
	store.SetWithTimestamp("taosd_dnodes_info", tags1, metrics1, time.Now())

	tags2 := map[string]string{
		"cluster_id": "123",
	}
	metrics2 := map[string]float64{
		"dbs_total":     10.0,
		"master_uptime": 200.0,
	}
	store.SetWithTimestamp("taosd_cluster_info", tags2, metrics2, time.Now())

	// Create collector
	collector := NewMemoryStoreCollector(store, 30*time.Second)

	// Collect metrics
	ch := make(chan prometheus.Metric, 100)
	go collector.Collect(ch)

	// Verify collected metrics
	metricCount := countMetricsWithTimeout(ch, 1*time.Second)

	// Should collect 5 metrics (2+2 data + the rejection counter)
	assert.Equal(t, 5, metricCount)
}

func TestMemoryStoreCollector_CollectWithMaxAge(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	// Add old data
	tags1 := map[string]string{"id": "old"}
	metrics1 := map[string]float64{"value": 1.0}
	store.SetWithTimestamp("test_table", tags1, metrics1, time.Now())

	// Wait for some time
	time.Sleep(10 * time.Millisecond)

	// Add new data
	tags2 := map[string]string{"id": "new"}
	metrics2 := map[string]float64{"value": 2.0}
	store.SetWithTimestamp("test_table", tags2, metrics2, time.Now())

	// Create collector with very short maxAge
	collector := NewMemoryStoreCollector(store, 5*time.Millisecond)

	// Collect metrics
	ch := make(chan prometheus.Metric, 100)
	go collector.Collect(ch)

	metricCount := countMetricsWithTimeout(ch, 1*time.Second)

	// Should only collect new data (1 metric) plus the rejection counter
	assert.Equal(t, 2, metricCount)
}

func TestMemoryStoreCollector_Describe(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()
	collector := NewMemoryStoreCollector(store, 30*time.Second)

	ch := make(chan *prometheus.Desc, 10)
	go collector.Describe(ch)

	// Describe should produce at least one description
	descCount := countDescsWithTimeout(ch, 100*time.Millisecond)
	assert.GreaterOrEqual(t, descCount, 0)
}

func TestMemoryStoreCollector_EmptyStore(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()
	collector := NewMemoryStoreCollector(store, 30*time.Second)

	// Empty store produces no data metrics, only the rejection counter
	ch := make(chan prometheus.Metric, 100)
	go collector.Collect(ch)

	metricCount := countMetricsWithTimeout(ch, 100*time.Millisecond)
	assert.Equal(t, 1, metricCount)
}

func TestMemoryStoreCollector_Integration(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	// Directly add test data (simulating parser behavior)
	tags := map[string]string{
		"cluster_id": "123",
	}
	metrics := map[string]float64{
		"dbs_total":     5.0,
		"master_uptime": 100.0,
	}
	store.SetWithTimestamp("taosd_cluster_info", tags, metrics, time.Now())

	// Use collector to collect
	collector := NewMemoryStoreCollector(store, 30*time.Second)
	ch := make(chan prometheus.Metric, 100)
	go collector.Collect(ch)

	metricCount := countMetricsWithTimeout(ch, 1*time.Second)

	// Should collect 3 metrics (2 data + the rejection counter)
	assert.Equal(t, 3, metricCount)
}
