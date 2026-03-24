package process

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemoryStore_SetAndGet(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	// Test storage and retrieval
	tags := map[string]string{
		"cluster_id": "123",
		"dnode_id":   "1",
	}
	metrics := map[string]float64{
		"uptime":     100.0,
		"cpu_engine": 5.5,
	}

	store.SetWithTimestamp("taosd_dnodes_info", tags, metrics, time.Now())

	// Verify storage
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 1, len(allData))

	data := allData[0]
	assert.Equal(t, "taosd_dnodes_info", data.TableName)
	assert.Equal(t, 2, len(data.Tags))
	assert.Equal(t, "123", data.Tags["cluster_id"])
	assert.Equal(t, "1", data.Tags["dnode_id"])
	assert.Equal(t, 2, len(data.Metrics))
	assert.Equal(t, 100.0, data.Metrics["uptime"])
	assert.Equal(t, 5.5, data.Metrics["cpu_engine"])
}

func TestMemoryStore_UpdateExisting(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	tags := map[string]string{"cluster_id": "123"}

	// First insert: value1 only
	timestamp1 := time.Now()
	metrics1 := map[string]float64{"value1": 10.0}
	store.SetWithTimestamp("test_table", tags, metrics1, timestamp1)

	// Verify first insert
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 1, len(allData))
	data := allData[0]
	assert.Equal(t, 1, len(data.Metrics))
	assert.Equal(t, 10.0, data.Metrics["value1"])
	assert.True(t, data.Timestamp.Equal(timestamp1) || data.Timestamp.After(timestamp1))

	// Second insert: value2 only (should replace value1, not merge)
	timestamp2 := time.Now().Add(1 * time.Second)
	metrics2 := map[string]float64{"value2": 20.0}
	store.SetWithTimestamp("test_table", tags, metrics2, timestamp2)

	// Verify replacement (not merge)
	allData = store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 1, len(allData)) // Still only one entry
	data = allData[0]
	assert.Equal(t, 1, len(data.Metrics)) // Only value2, value1 is gone!
	assert.Equal(t, 20.0, data.Metrics["value2"])
	_, exists := data.Metrics["value1"]
	assert.False(t, exists, "value1 should not exist after replacement")
	assert.Equal(t, timestamp2, data.Timestamp) // Timestamp updated
}

func TestMemoryStore_GetAllFiltered(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	tags1 := map[string]string{"id": "1"}
	metrics1 := map[string]float64{"value": 10.0}
	store.SetWithTimestamp("table1", tags1, metrics1, time.Now())

	// Wait to ensure different timestamps
	time.Sleep(10 * time.Millisecond)

	tags2 := map[string]string{"id": "2"}
	metrics2 := map[string]float64{"value": 20.0}
	store.SetWithTimestamp("table2", tags2, metrics2, time.Now())

	// Get all data
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 2, len(allData))

	// Filter: only return recent data
	filteredData := store.GetAllFiltered(time.Now().Add(-5 * time.Millisecond))
	assert.Equal(t, 1, len(filteredData))
	assert.Equal(t, "table2", filteredData[0].TableName)
}

func TestMemoryStore_BuildKey(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	tags1 := map[string]string{"a": "1", "b": "2"}
	key1 := store.buildKey("test", tags1)

	tags2 := map[string]string{"b": "2", "a": "1"} // Different order
	key2 := store.buildKey("test", tags2)

	// Key should be independent of order
	assert.Equal(t, key1, key2)
}

func TestMemoryStore_GetStats(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	// Add first metric
	tags := map[string]string{"id": "1"}
	metrics := map[string]float64{"value": 10.0}
	store.SetWithTimestamp("test_table", tags, metrics, time.Now())

	// Wait a bit to ensure different timestamp
	time.Sleep(10 * time.Millisecond)

	// Add second metric with a specific timestamp
	specificTime := time.Now().Add(-1 * time.Hour)
	store.SetWithTimestamp("test_table2", map[string]string{"id": "2"}, map[string]float64{"value": 20.0}, specificTime)

	stats := store.GetStats()
	assert.Equal(t, 2, stats["total_metrics"])

	// last_update should be the most recent timestamp (first metric, not the old one)
	lastUpdateStr, ok := stats["last_update"].(string)
	assert.True(t, ok, "last_update should be a string")
	assert.NotEmpty(t, lastUpdateStr)

	lastUpdate, err := time.Parse(time.RFC3339, lastUpdateStr)
	assert.NoError(t, err)

	// The most recent data should be recent (within last second), not 1 hour ago
	assert.True(t, time.Since(lastUpdate) < time.Second, "last_update should be recent, not the old timestamp")
}

func TestMemoryStore_ConcurrentAccess(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	// Concurrent writes
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(n int) {
			tags := map[string]string{"id": fmt.Sprintf("%d", n)}
			metrics := map[string]float64{"value": float64(n)}
			store.SetWithTimestamp("test", tags, metrics, time.Now())
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 100; i++ {
		<-done
	}

	// Verify data consistency
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 100, len(allData))
}

func TestMemoryStore_TTLExpiration(t *testing.T) {
	// Create store with minimum TTL (1 minute)
	store, _ := NewMemoryStore(1 * time.Minute)
	defer store.Close()

	// Add test data with old timestamp (older than TTL)
	tags := map[string]string{"id": "test"}
	metrics := map[string]float64{"value": 100.0}
	oldTimestamp := time.Now().Add(-61 * time.Second)
	store.SetWithTimestamp("test_table", tags, metrics, oldTimestamp)

	// Verify data is stored immediately
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 1, len(allData))

	// Manually trigger cleanup to verify TTL logic works
	store.cleanup()

	// Verify expired data has been cleaned up
	allData = store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 0, len(allData), "Expired data should be automatically cleaned up")
}

func TestMemoryStore_TTLWithMultipleEntries(t *testing.T) {
	// Create store with minimum TTL (1 minute)
	store, _ := NewMemoryStore(1 * time.Minute)
	defer store.Close()

	// Add first batch of data with old timestamp
	tags1 := map[string]string{"batch": "1"}
	metrics1 := map[string]float64{"value": 1.0}
	oldTimestamp := time.Now().Add(-65 * time.Second)
	store.SetWithTimestamp("test_table", tags1, metrics1, oldTimestamp)

	// Add second batch of data (recent)
	tags2 := map[string]string{"batch": "2"}
	metrics2 := map[string]float64{"value": 2.0}
	store.SetWithTimestamp("test_table", tags2, metrics2, time.Now())

	// Verify both entries exist
	allData := store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 2, len(allData))

	// Manually trigger cleanup - first batch should expire
	store.cleanup()

	// Only second batch should remain
	allData = store.GetAllFiltered(time.Unix(0, 0))
	assert.Equal(t, 1, len(allData), "Only recent data should remain after TTL cleanup")
	assert.Equal(t, "2", allData[0].Tags["batch"])
}
