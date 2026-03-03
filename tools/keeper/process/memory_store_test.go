package process

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemoryStore_SetAndGet(t *testing.T) {
	store := NewMemoryStore(5 * time.Minute)
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

	store.Set("taosd_dnodes_info", tags, metrics)

	// Verify storage
	allData := store.GetAll()
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
	store := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	tags := map[string]string{"cluster_id": "123"}
	metrics1 := map[string]float64{"value1": 10.0}
	store.Set("test_table", tags, metrics1)

	// Update the same metric
	metrics2 := map[string]float64{"value2": 20.0}
	store.Set("test_table", tags, metrics2)

	// Should update, not insert new
	allData := store.GetAll()
	assert.Equal(t, 1, len(allData))

	data := allData[0]
	assert.Equal(t, 2, len(data.Metrics))
	assert.Equal(t, 10.0, data.Metrics["value1"])
	assert.Equal(t, 20.0, data.Metrics["value2"])
}

func TestMemoryStore_GetAllFiltered(t *testing.T) {
	store := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	tags1 := map[string]string{"id": "1"}
	metrics1 := map[string]float64{"value": 10.0}
	store.Set("table1", tags1, metrics1)

	// Wait to ensure different timestamps
	time.Sleep(10 * time.Millisecond)

	tags2 := map[string]string{"id": "2"}
	metrics2 := map[string]float64{"value": 20.0}
	store.Set("table2", tags2, metrics2)

	// Get all data
	allData := store.GetAll()
	assert.Equal(t, 2, len(allData))

	// Filter: only return recent data
	filteredData := store.GetAllFiltered(time.Now().Add(-5 * time.Millisecond))
	assert.Equal(t, 1, len(filteredData))
	assert.Equal(t, "table2", filteredData[0].TableName)
}

func TestMemoryStore_GetAllFilteredLazyCleanup(t *testing.T) {
	// Create store with TTL=200ms for testing
	store := NewMemoryStore(200 * time.Millisecond)
	defer store.Close()

	// Add old data (simulating data that's been in memory for a while)
	tags1 := map[string]string{"id": "old"}
	metrics1 := map[string]float64{"value": 1.0}
	store.Set("test_table", tags1, metrics1)

	// Wait 50ms
	time.Sleep(50 * time.Millisecond)

	// Add new data
	tags2 := map[string]string{"id": "new"}
	metrics2 := map[string]float64{"value": 2.0}
	store.Set("test_table", tags2, metrics2)

	// Verify both entries exist initially
	allData := store.GetAll()
	assert.Equal(t, 2, len(allData))

	// Scenario 1: Use maxAge=100ms
	// - "old" data (age=50ms) < maxAge → return
	// - "new" data (age=0ms) < maxAge → return
	time.Sleep(10 * time.Millisecond) // old data age ~60ms, new data age ~10ms
	filteredData1 := store.GetAllFiltered(time.Now().Add(-100 * time.Millisecond))
	assert.Equal(t, 2, len(filteredData1), "Should return both data entries (within maxAge)")

	// Verify both still exist in memory (none deleted because within TTL)
	allData = store.GetAll()
	assert.Equal(t, 2, len(allData), "Data within TTL should not be deleted")

	// Scenario 2: Wait for data to exceed maxAge but still within TTL
	time.Sleep(100 * time.Millisecond) // old data age ~160ms, new data age ~110ms

	// Call GetAllFiltered with maxAge=30ms
	// - "old" data (age~160ms) > maxAge (30ms), < TTL (200ms) → skip, not deleted
	// - "new" data (age~110ms) > maxAge (30ms), < TTL (200ms) → skip, not deleted
	filteredData2 := store.GetAllFiltered(time.Now().Add(-30 * time.Millisecond))
	assert.Equal(t, 0, len(filteredData2), "Should return no data (both exceeded maxAge)")

	// Verify both still exist in memory (within TTL)
	allData = store.GetAll()
	assert.Equal(t, 2, len(allData), "Data within TTL should not be deleted even if exceeded maxAge")

	// Scenario 3: Wait for all data to exceed TTL
	time.Sleep(100 * time.Millisecond) // old data age~260ms (>200ms TTL), new data age~210ms (>200ms TTL)

	// Call GetAllFiltered with maxAge=30ms
	// - "old" data (age~260ms) > maxAge, > TTL → should be deleted
	// - "new" data (age~210ms) > maxAge, > TTL → should be deleted
	filteredData3 := store.GetAllFiltered(time.Now().Add(-30 * time.Millisecond))
	assert.Equal(t, 0, len(filteredData3), "No data within maxAge")

	// Verify expired data was deleted
	allData = store.GetAll()
	assert.Equal(t, 0, len(allData), "Expired data (beyond TTL) should be deleted during GetAllFiltered")
}

func TestMemoryStore_GetAllFilteredWithDifferentAges(t *testing.T) {
	// Test the three-tier logic: maxAge vs TTL
	store := NewMemoryStore(300 * time.Millisecond)
	defer store.Close()

	// Add three data entries with different ages
	tags1 := map[string]string{"id": "data1"}
	metrics1 := map[string]float64{"value": 1.0}
	store.Set("test_table", tags1, metrics1)

	time.Sleep(100 * time.Millisecond)

	tags2 := map[string]string{"id": "data2"}
	metrics2 := map[string]float64{"value": 2.0}
	store.Set("test_table", tags2, metrics2)

	time.Sleep(100 * time.Millisecond) // data1 age~200ms, data2 age~100ms

	tags3 := map[string]string{"id": "data3"}
	metrics3 := map[string]float64{"value": 3.0}
	store.Set("test_table", tags3, metrics3) // data3 age~0ms

	// Test 1: maxAge=150ms
	// - data1 (age~200ms) > maxAge, < TTL (300ms) → skip, keep in memory
	// - data2 (age~100ms) < maxAge → return
	// - data3 (age~0ms) < maxAge → return
	filteredData := store.GetAllFiltered(time.Now().Add(-150 * time.Millisecond))
	assert.Equal(t, 2, len(filteredData), "Should return data2 and data3 (within maxAge)")

	// Verify all 3 still in memory (none exceeded TTL)
	allData := store.GetAll()
	assert.Equal(t, 3, len(allData), "All data should remain in memory (within TTL)")

	// Test 2: Wait for data1 to exceed TTL, then call GetAllFiltered
	time.Sleep(120 * time.Millisecond) // data1 age~320ms (>300ms TTL), data2 age~220ms, data3 age~120ms

	// Use maxAge=100ms (more lenient to include data2 and data3)
	// - data1 (age~320ms) > maxAge, > TTL → should be deleted
	// - data2 (age~220ms) > maxAge, < TTL (300ms) → skip (not returned, not deleted)
	// - data3 (age~120ms) > maxAge, < TTL (300ms) → skip (not returned, not deleted)
	filteredData = store.GetAllFiltered(time.Now().Add(-100 * time.Millisecond))
	assert.Equal(t, 0, len(filteredData), "Should return no data (all exceeded maxAge)")

	// Verify data1 was deleted (exceeded TTL), but data2 and data3 remain
	allData = store.GetAll()
	assert.Equal(t, 2, len(allData), "data1 should be deleted (exceeded TTL), data2 and data3 should remain")

	// Verify remaining data is data2 and data3 (not data1)
	ids := make([]string, 2)
	for i, data := range allData {
		ids[i] = data.Tags["id"]
	}
	assert.NotContains(t, ids, "data1", "Deleted data1 should not remain")
	assert.Contains(t, ids, "data2")
	assert.Contains(t, ids, "data3")
}

func TestMemoryStore_BuildKey(t *testing.T) {
	store := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	tags1 := map[string]string{"a": "1", "b": "2"}
	key1 := store.buildKey("test", tags1)

	tags2 := map[string]string{"b": "2", "a": "1"} // Different order
	key2 := store.buildKey("test", tags2)

	// Key should be independent of order
	assert.Equal(t, key1, key2)
}

func TestMemoryStore_GetStats(t *testing.T) {
	store := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	// Add first metric
	tags := map[string]string{"id": "1"}
	metrics := map[string]float64{"value": 10.0}
	store.Set("test_table", tags, metrics)

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
	store := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	// Concurrent writes
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(n int) {
			tags := map[string]string{"id": fmt.Sprintf("%d", n)}
			metrics := map[string]float64{"value": float64(n)}
			store.Set("test", tags, metrics)
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 100; i++ {
		<-done
	}

	// Verify data consistency
	allData := store.GetAll()
	assert.Equal(t, 100, len(allData))
}

func TestMemoryStore_TTLExpiration(t *testing.T) {
	// Create store with very short TTL (100ms)
	store := NewMemoryStore(100 * time.Millisecond)
	defer store.Close()

	// Add test data
	tags := map[string]string{"id": "test"}
	metrics := map[string]float64{"value": 100.0}
	store.Set("test_table", tags, metrics)

	// Verify data is stored immediately
	allData := store.GetAll()
	assert.Equal(t, 1, len(allData))

	// Wait for data to expire
	time.Sleep(150 * time.Millisecond)

	// Manually trigger cleanup to verify TTL logic works
	store.cleanup()

	// Verify expired data has been cleaned up
	allData = store.GetAll()
	assert.Equal(t, 0, len(allData), "Expired data should be automatically cleaned up")
}

func TestMemoryStore_TTLWithMultipleEntries(t *testing.T) {
	// Create store with short TTL
	store := NewMemoryStore(200 * time.Millisecond)
	defer store.Close()

	// Add first batch of data
	tags1 := map[string]string{"batch": "1"}
	metrics1 := map[string]float64{"value": 1.0}
	store.Set("test_table", tags1, metrics1)

	// Wait a bit
	time.Sleep(50 * time.Millisecond)

	// Add second batch of data (newer)
	tags2 := map[string]string{"batch": "2"}
	metrics2 := map[string]float64{"value": 2.0}
	store.Set("test_table", tags2, metrics2)

	// Verify both entries exist
	allData := store.GetAll()
	assert.Equal(t, 2, len(allData))

	// Wait for first batch to expire but second batch still valid
	time.Sleep(160 * time.Millisecond) // Total: 50ms + 160ms = 210ms

	// Manually trigger cleanup
	store.cleanup()

	// Only second batch should remain
	allData = store.GetAll()
	assert.Equal(t, 1, len(allData), "Only recent data should remain after TTL cleanup")
	assert.Equal(t, "2", allData[0].Tags["batch"])
}
