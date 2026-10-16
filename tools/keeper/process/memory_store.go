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
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MetricData stores metric data in memory
type MetricData struct {
	TableName  string             // Table name
	Tags       map[string]string  // Tags
	Metrics    map[string]float64 // Metric name -> Value
	Timestamp  time.Time          // Metric timestamp from the request
	ReceivedAt time.Time          // Time the entry was stored locally, used for eviction
	size       int                // approximate heap this entry occupies, see entrySize
}

const (
	// defaultMaxTotalEntries caps how many series the cache may hold. The TTL
	// alone only bounds how long an entry lives, not how many the
	// unauthenticated ingestion endpoints can create, so this is a store-level
	// backstop against that abuse: once reached, new series are rejected.
	defaultMaxTotalEntries = 1000000

	// defaultMaxTotalBytes caps the heap the cache may hold, in approximate
	// bytes. A count cap cannot bound memory on its own: tag names and values
	// are stored verbatim and are what the cache key is built from, so the
	// sender chooses how large an entry is. Both limits must hold to admit.
	defaultMaxTotalBytes = 512 << 20 // 512 MiB
)

// MemoryStore is an in-memory cache
type MemoryStore struct {
	mu              sync.RWMutex
	store           map[string]*MetricData // key: tableName:serializedTags
	ttl             time.Duration          // Time-to-live for cached data
	done            chan struct{}          // Channel to signal cleanupLoop to stop
	closeOnce       sync.Once              // Ensure Close is idempotent
	maxTotalEntries int
	maxTotalBytes   int
	totalBytes      int           // sum of the size of every entry, guarded by mu
	sweepEvery      time.Duration // how often expired entries are swept
	lastSweep       time.Time     // guarded by mu
	rejectedTotal   atomic.Uint64 // entries rejected because the store is full
}

// NewMemoryStore creates an in-memory store with specified TTL
// TTL must be at least 1 minute (will return error if less)
func NewMemoryStore(ttl time.Duration) (*MemoryStore, error) {
	if ttl < time.Minute {
		return nil, fmt.Errorf("MemoryStore TTL must be at least 1 minute, got %v", ttl)
	}

	store := &MemoryStore{
		store:           make(map[string]*MetricData),
		ttl:             ttl,
		done:            make(chan struct{}),
		maxTotalEntries: defaultMaxTotalEntries,
		maxTotalBytes:   defaultMaxTotalBytes,
		sweepEvery:      sweepInterval(ttl),
	}
	// Start cleanup goroutine
	go store.cleanupLoop()
	return store, nil
}

// sweepInterval keeps the sweep no more than half a TTL behind the entries it
// reclaims, so an expired entry cannot hold a cap slot for long after it is
// logically gone. The TTL floor is one minute and the ceiling matches the
// previous fixed cadence, which suits the default TTL.
func sweepInterval(ttl time.Duration) time.Duration {
	interval := ttl / 2
	if interval > 5*time.Minute {
		interval = 5 * time.Minute
	}
	if interval < time.Minute {
		interval = time.Minute
	}
	return interval
}

// entrySize estimates the heap an entry occupies. The per-item margins cover
// the strings' headers and the map buckets; the constant covers the MetricData
// struct and the two map headers.
// entrySize estimates the heap an entry occupies. key already begins with the
// table name (see buildKey), so it is counted once.
func entrySize(key string, tags map[string]string, metrics map[string]float64) int {
	size := len(key) + 96
	for k, v := range tags {
		size += len(k) + len(v) + 32
	}
	for k := range metrics {
		size += len(k) + 24
	}
	return size
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
			result = append(result, &MetricData{
				TableName:  data.TableName,
				Timestamp:  data.Timestamp,
				ReceivedAt: data.ReceivedAt,
				size:       data.size,
				Tags:       copyTags(data.Tags),
				Metrics:    copyMetrics(data.Metrics),
			})
		}
	}
	return result
}

// cleanupLoop periodically cleans up expired data
func (m *MemoryStore) cleanupLoop() {
	ticker := time.NewTicker(m.sweepEvery)
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
	m.sweepLocked()
}

// sweepLocked removes expired data. Callers must hold the write lock.
// Eviction is based on ReceivedAt (local insertion time), not the caller-supplied
// metric timestamp, so entries stamped with a far-future ts still age out.
func (m *MemoryStore) sweepLocked() {
	now := time.Now()
	for key, data := range m.store {
		if now.Sub(data.ReceivedAt) > m.ttl {
			m.totalBytes -= data.size
			delete(m.store, key)
		}
	}
	m.lastSweep = now
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
		"total_metrics":    len(m.store),
		"total_bytes":      m.totalBytes,
		"last_update":      lastUpdateStr,
		"rejected_metrics": m.rejectedTotal.Load(),
	}
}

// RejectedTotal returns how many entries were rejected because the store was full.
func (m *MemoryStore) RejectedTotal() uint64 {
	return m.rejectedTotal.Load()
}

// SetWithTimestamp sets data with specified timestamp
// Use the timestamp from the request to ensure accurate metric timing
// If the key already exists, the entire metric map will be replaced (not merged)
// This ensures data consistency and accurate timestamp semantics
// New entries are rejected once the store is full.
func (m *MemoryStore) SetWithTimestamp(tableName string, tags map[string]string, metrics map[string]float64, timestamp time.Time) {
	key := m.buildKey(tableName, tags)
	size := entrySize(key, tags, metrics)

	m.mu.Lock()
	defer m.mu.Unlock()

	// A refresh and a new series are admitted the same way, because both add
	// bytes to what the store holds: a refresh is measured against the byte
	// budget too, or a sender that keeps one set of tags fixed and grows the
	// payload would walk past it without ever being compared.
	delta, exists := m.writeDeltaLocked(key, size)
	if !m.admitsLocked(delta, exists) {
		// Sweep before refusing. The ticker can be half a TTL behind, and
		// without this a full store keeps refusing writes until the next tick
		// even when every entry has already expired. Rate-limited to one sweep
		// per interval so a store kept full cannot turn every write into a full
		// scan.
		if time.Since(m.lastSweep) >= m.sweepEvery {
			m.sweepLocked()
			// the sweep may have removed the entry being refreshed, which turns
			// this write back into a new series
			delta, exists = m.writeDeltaLocked(key, size)
		}
		if !m.admitsLocked(delta, exists) {
			m.rejectedTotal.Add(1)
			return
		}
	}

	m.totalBytes += delta
	// Always replace the entire MetricData object
	// This ensures timestamp accurately reflects when these metrics were collected
	m.store[key] = &MetricData{
		TableName:  tableName,
		Tags:       copyTags(tags),
		Metrics:    copyMetrics(metrics),
		Timestamp:  timestamp,
		ReceivedAt: time.Now(),
		size:       size,
	}
}

// writeDeltaLocked reports what storing `size` bytes under key would add to
// totalBytes, and whether that key already holds an entry. Callers must hold
// the write lock.
func (m *MemoryStore) writeDeltaLocked(key string, size int) (delta int, exists bool) {
	old, exists := m.store[key]
	if !exists {
		return size, false
	}
	return size - old.size, true
}

// admitsLocked reports whether a write adding delta bytes is within the store's
// limits. Callers must hold the write lock. A refresh replaces an entry's own
// bytes rather than taking a slot, so only the byte budget applies to it; a new
// series has to fit the entry count as well.
func (m *MemoryStore) admitsLocked(delta int, exists bool) bool {
	if !exists && len(m.store) >= m.maxTotalEntries {
		return false
	}
	return m.totalBytes+delta <= m.maxTotalBytes
}

// copyTags and copyMetrics keep the stored entry independent of the caller's
// maps, which the parser reuses across metric groups.
func copyTags(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[k] = v
	}
	return out
}

func copyMetrics(metrics map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(metrics))
	for k, v := range metrics {
		out[k] = v
	}
	return out
}

// Close stops the cleanup goroutine to prevent goroutine leaks
// Should be called when the store is no longer needed
// Idempotent: safe to call multiple times
func (m *MemoryStore) Close() {
	m.closeOnce.Do(func() {
		close(m.done)
	})
}
