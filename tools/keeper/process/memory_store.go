package process

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// MetricData stores metric data in memory
type MetricData struct {
	TableName string                 // Table name
	Tags      map[string]string      // Tags
	Metrics   map[string]float64     // Metric name -> Value
	Timestamp time.Time              // Update timestamp
}

// MemoryStore is an in-memory cache
type MemoryStore struct {
	mu    sync.RWMutex
	store map[string]*MetricData // key: tableName:serializedTags
	ttl   time.Duration           // Time-to-live for cached data
	done  chan struct{}           // Channel to signal cleanupLoop to stop
	closeOnce sync.Once           // Ensure Close is idempotent
}

// NewMemoryStore creates an in-memory store with specified TTL
// TTL must be at least 1 minute (will return error if less)
func NewMemoryStore(ttl time.Duration) (*MemoryStore, error) {
	if ttl < time.Minute {
		return nil, fmt.Errorf("MemoryStore TTL must be at least 1 minute, got %v", ttl)
	}

	store := &MemoryStore{
		store: make(map[string]*MetricData),
		ttl:   ttl,
		done:  make(chan struct{}),
	}
	// Start cleanup goroutine
	go store.cleanupLoop()
	return store, nil
}


// GetAllFiltered retrieves filtered metrics (used by v2)
// Returns deep copies to prevent data races
// Uses read lock for better concurrency - no side effects
func (m *MemoryStore) GetAllFiltered(minTimestamp time.Time) []*MetricData {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*MetricData, 0, len(m.store))
	for _, data := range m.store {
		// Only return data within the specified time range
		if data.Timestamp.After(minTimestamp) {
			// Deep copy to prevent data races
			dataCopy := &MetricData{
				TableName: data.TableName,
				Timestamp: data.Timestamp,
				Tags:      make(map[string]string, len(data.Tags)),
				Metrics:   make(map[string]float64, len(data.Metrics)),
			}
			for k, v := range data.Tags {
				dataCopy.Tags[k] = v
			}
			for k, v := range data.Metrics {
				dataCopy.Metrics[k] = v
			}
			result = append(result, dataCopy)
		}
	}
	return result
}

// cleanupLoop periodically cleans up expired data
func (m *MemoryStore) cleanupLoop() {
	// Check and cleanup expired data every 5 minutes
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanup()
		case <-m.done:
			return
		}
	}
}

// cleanup removes expired data (called by cleanupLoop or tests)
func (m *MemoryStore) cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for key, data := range m.store {
		if now.Sub(data.Timestamp) > m.ttl {
			delete(m.store, key)
		}
	}
}

// buildKey builds cache key
func (m *MemoryStore) buildKey(tableName string, tags map[string]string) string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	// Sort to ensure key consistency
	sort.Strings(keys)

	var builder strings.Builder
	builder.WriteString(tableName)
	builder.WriteByte(':')

	for i, k := range keys {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(k)
		builder.WriteByte('=')
		builder.WriteString(tags[k])
	}

	return builder.String()
}

// GetStats retrieves statistics
func (m *MemoryStore) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Find the most recent update timestamp
	var lastUpdate time.Time
	for _, data := range m.store {
		if data.Timestamp.After(lastUpdate) {
			lastUpdate = data.Timestamp
		}
	}

	var lastUpdateStr string
	if !lastUpdate.IsZero() {
		lastUpdateStr = lastUpdate.Format(time.RFC3339)
	}

	return map[string]interface{}{
		"total_metrics": len(m.store),
		"last_update":   lastUpdateStr,
	}
}

// SetWithTimestamp sets data with specified timestamp
// Use the timestamp from the request to ensure accurate metric timing
// If the key already exists, the entire metric map will be replaced (not merged)
// This ensures data consistency and accurate timestamp semantics
func (m *MemoryStore) SetWithTimestamp(tableName string, tags map[string]string, metrics map[string]float64, timestamp time.Time) {
	key := m.buildKey(tableName, tags)
	m.mu.Lock()
	defer m.mu.Unlock()

	// Copy tags and metrics to avoid external modification
	tagsCopy := make(map[string]string, len(tags))
	for k, v := range tags {
		tagsCopy[k] = v
	}
	metricsCopy := make(map[string]float64, len(metrics))
	for k, v := range metrics {
		metricsCopy[k] = v
	}

	// Always replace the entire MetricData object
	// This ensures timestamp accurately reflects when these metrics were collected
	m.store[key] = &MetricData{
		TableName: tableName,
		Tags:      tagsCopy,
		Metrics:   metricsCopy,
		Timestamp: timestamp,
	}
}

// Close stops the cleanup goroutine to prevent goroutine leaks
// Should be called when the store is no longer needed
// Idempotent: safe to call multiple times
func (m *MemoryStore) Close() {
	m.closeOnce.Do(func() {
		close(m.done)
	})
}
