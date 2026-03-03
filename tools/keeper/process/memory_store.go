package process

import (
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
}

// NewMemoryStore creates an in-memory store with specified TTL
func NewMemoryStore(ttl time.Duration) *MemoryStore {
	store := &MemoryStore{
		store: make(map[string]*MetricData),
		ttl:   ttl,
		done:  make(chan struct{}),
	}
	// Start cleanup goroutine
	go store.cleanupLoop()
	return store
}

// Set stores or updates metrics
func (m *MemoryStore) Set(tableName string, tags map[string]string, metrics map[string]float64) {
	key := m.buildKey(tableName, tags)
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.store[key]; ok {
		// Update existing metric values
		for k, v := range metrics {
			existing.Metrics[k] = v
		}
		existing.Timestamp = time.Now()
	} else {
		// Create new metric
		// Copy tags and metrics to avoid external modification
		tagsCopy := make(map[string]string, len(tags))
		for k, v := range tags {
			tagsCopy[k] = v
		}
		metricsCopy := make(map[string]float64, len(metrics))
		for k, v := range metrics {
			metricsCopy[k] = v
		}

		m.store[key] = &MetricData{
			TableName: tableName,
			Tags:      tagsCopy,
			Metrics:   metricsCopy,
			Timestamp: time.Now(),
		}
	}
}

// GetAll retrieves all metrics (returns a deep copy to prevent data races)
func (m *MemoryStore) GetAll() []*MetricData {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*MetricData, 0, len(m.store))
	for _, data := range m.store {
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
	return result
}

// GetAllFiltered retrieves filtered metrics (used by v2)
// Also performs lazy cleanup: removes data that exceeds TTL
// Returns deep copies to prevent data races
func (m *MemoryStore) GetAllFiltered(minTimestamp time.Time) []*MetricData {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	result := make([]*MetricData, 0, len(m.store))
	expiredKeys := make([]string, 0)

	for key, data := range m.store {
		// Check if data is within maxAge
		if data.Timestamp.After(minTimestamp) {
			// Within maxAge: return this data (deep copy)
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
		} else {
			// Exceeds maxAge: check if it exceeds TTL
			if now.Sub(data.Timestamp) > m.ttl {
				// Exceeds TTL: mark for deletion
				expiredKeys = append(expiredKeys, key)
			}
			// Else: within TTL but outside maxAge, just skip (don't return)
		}
	}

	// Delete expired data
	for _, key := range expiredKeys {
		delete(m.store, key)
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

// SetWithTimestamp sets data with specified timestamp (mainly for testing)
func (m *MemoryStore) SetWithTimestamp(tableName string, tags map[string]string, metrics map[string]float64, timestamp time.Time) {
	key := m.buildKey(tableName, tags)
	m.mu.Lock()
	defer m.mu.Unlock()

	// Copy tags and metrics
	tagsCopy := make(map[string]string, len(tags))
	for k, v := range tags {
		tagsCopy[k] = v
	}
	metricsCopy := make(map[string]float64, len(metrics))
	for k, v := range metrics {
		metricsCopy[k] = v
	}

	m.store[key] = &MetricData{
		TableName: tableName,
		Tags:      tagsCopy,
		Metrics:   metricsCopy,
		Timestamp: timestamp,
	}
}

// Close stops the cleanup goroutine to prevent goroutine leaks
// Should be called when the store is no longer needed
func (m *MemoryStore) Close() {
	close(m.done)
}
