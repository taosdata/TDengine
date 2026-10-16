// Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

// The store is bounded by bytes as well as by entries. A count cap alone cannot
// bound memory because the sender chooses the entry size: tag names and values
// are stored verbatim and form the cache key.
func TestMemoryStore_RejectsWhenByteBudgetExceeded(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()
	store.maxTotalBytes = 4096

	// each iteration is a distinct series, so they accumulate against the budget
	for i := 0; i < 100 && store.RejectedTotal() == 0; i++ {
		store.SetWithTimestamp("taosd_x",
			map[string]string{"id": fmt.Sprintf("%s-%d", strings.Repeat("x", 500), i)},
			map[string]float64{"v": 1}, time.Now())
	}

	assert.NotZero(t, store.RejectedTotal(), "the byte budget must start rejecting")
	assert.LessOrEqual(t, store.totalBytes, store.maxTotalBytes, "the budget must hold")
	assert.Equal(t, store.totalBytes, store.GetStats()["total_bytes"])
}

// Overwriting an entry replaces its size in the running total, so a store that
// keeps being refreshed does not drift away from its real footprint. The key is
// built from the tags, so an overwrite keeps the same tags and changes what is
// stored under them.
func TestMemoryStore_OverwriteAdjustsTotalBytes(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	tags := map[string]string{"id": "a"}
	store.SetWithTimestamp("t1", tags, map[string]float64{"v": 1}, time.Now())
	small := store.totalBytes
	assert.NotZero(t, small)

	big := make(map[string]float64, 200)
	for i := 0; i < 200; i++ {
		big[fmt.Sprintf("metric_%03d", i)] = 1
	}
	store.SetWithTimestamp("t1", tags, big, time.Now())
	assert.Greater(t, store.totalBytes, small, "a larger payload must grow the total")
	assert.Len(t, store.GetAllFiltered(time.Unix(0, 0)), 1, "the overwrite must not add an entry")

	store.SetWithTimestamp("t1", tags, map[string]float64{"v": 1}, time.Now())
	assert.Equal(t, small, store.totalBytes, "a smaller payload must shrink the total back")
}

// A refresh is admitted on the same budget as a new entry. Otherwise a sender
// that keeps one set of tags fixed and grows the payload walks past the byte
// limit without ever being compared against it, which is exactly the growth the
// limit exists to stop.
func TestMemoryStore_RefreshRespectsByteBudget(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()
	store.maxTotalBytes = 4096

	tags := map[string]string{"id": "a"}
	store.SetWithTimestamp("taosd_x", tags, map[string]float64{"v": 1}, time.Now())
	small := store.totalBytes

	big := make(map[string]float64, 400)
	for i := 0; i < 400; i++ {
		big[fmt.Sprintf("metric_%03d", i)] = 1
	}
	for i := 0; i < 20; i++ {
		store.SetWithTimestamp("taosd_x", tags, big, time.Now())
	}

	assert.LessOrEqual(t, store.totalBytes, store.maxTotalBytes, "a hot key must not walk past the budget")
	assert.NotZero(t, store.RejectedTotal(), "the oversized refresh must be counted")
	assert.Equal(t, small, store.totalBytes, "the refused refresh leaves the previous entry in place")
}

// A refresh gets the same sweep-then-retry as a new series. When the store is
// over budget only because expired entries have not been swept yet, a refresh
// that fits afterwards must not be refused — the two admission paths share one
// rule, so the sweep cannot be skipped just because the key already exists.
func TestMemoryStore_RefreshSweepsBeforeRejecting(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()
	store.maxTotalBytes = 4096

	keep := map[string]string{"id": "keep"}
	keepKey := store.buildKey("taosd_x", keep)
	store.SetWithTimestamp("taosd_x", keep, map[string]float64{"v": 1}, time.Now())
	keepSize := store.totalBytes

	// unrelated entries fill most of the budget and are expired before the refresh
	for i := 0; i < 3; i++ {
		store.SetWithTimestamp("taosd_y", map[string]string{"id": fmt.Sprintf("junk-%d", i)},
			map[string]float64{"v": 1}, time.Now())
	}
	occupied := store.totalBytes

	// a payload that does not fit alongside the junk but fits once it is swept
	var big map[string]float64
	for n := 20; ; n += 10 {
		big = make(map[string]float64, n)
		for i := 0; i < n; i++ {
			big[fmt.Sprintf("metric_%03d", i)] = 1
		}
		s := entrySize(keepKey, keep, big)
		if occupied-keepSize+s > store.maxTotalBytes && keepSize+s <= store.maxTotalBytes {
			break
		}
	}

	store.mu.Lock()
	for k, d := range store.store {
		if k != keepKey {
			d.ReceivedAt = time.Now().Add(-10 * time.Minute)
		}
	}
	store.lastSweep = time.Now().Add(-time.Hour) // let the on-demand sweep run
	store.mu.Unlock()

	before := store.RejectedTotal()
	store.SetWithTimestamp("taosd_x", keep, big, time.Now())
	assert.Equal(t, before, store.RejectedTotal(), "the refresh must be admitted once the junk is swept")
	// the refresh replaces the entry, so the store now holds only its new size
	assert.Equal(t, entrySize(keepKey, keep, big), store.totalBytes)
}

// Expired entries are swept on demand before admission is refused: the ticker
// can be half a TTL behind, so without this a store full of entries that have
// already expired keeps rejecting new series until the next tick.
func TestMemoryStore_SweepsExpiredBeforeRejecting(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()
	store.maxTotalEntries = 2

	store.SetWithTimestamp("t1", map[string]string{"id": "a"}, map[string]float64{"v": 1}, time.Now())
	store.SetWithTimestamp("t2", map[string]string{"id": "b"}, map[string]float64{"v": 1}, time.Now())

	expire := func() {
		store.mu.Lock()
		for _, d := range store.store {
			d.ReceivedAt = time.Now().Add(-10 * time.Minute)
		}
		store.lastSweep = time.Now().Add(-time.Hour) // let the on-demand sweep run
		store.mu.Unlock()
	}

	// control: entries that are still live keep the store closed
	store.SetWithTimestamp("t3", map[string]string{"id": "c"}, map[string]float64{"v": 1}, time.Now())
	assert.Equal(t, uint64(1), store.RejectedTotal())
	assert.Len(t, store.GetAllFiltered(time.Unix(0, 0)), 2)

	expire()
	store.SetWithTimestamp("t4", map[string]string{"id": "d"}, map[string]float64{"v": 1}, time.Now())
	assert.Equal(t, uint64(1), store.RejectedTotal(), "the sweep must make room instead of rejecting")
	assert.Equal(t, 1, len(store.GetAllFiltered(time.Unix(0, 0))))
}

// Once entries have been rejected, the collector exposes the store-level
// rejected counter in addition to the data metrics.
func TestMemoryStoreCollector_ExposesRejectedTotal(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()
	store.maxTotalEntries = 1

	store.SetWithTimestamp("t1", map[string]string{"id": "a"}, map[string]float64{"v": 1}, time.Now())
	store.SetWithTimestamp("t1", map[string]string{"id": "b"}, map[string]float64{"v": 1}, time.Now()) // rejected
	assert.Equal(t, uint64(1), store.GetStats()["rejected_metrics"])
	assert.Equal(t, uint64(1), store.RejectedTotal())

	collector := NewMemoryStoreCollector(store, time.Hour)
	ch := make(chan prometheus.Metric, 10)
	go func() {
		collector.Collect(ch)
		close(ch)
	}()
	// 1 data metric + 1 rejected counter
	assert.Equal(t, 2, countMetricsWithTimeout(ch, 2*time.Second))
}

// The deep copy returned by GetAllFiltered carries ReceivedAt, so eviction
// accounting based on it stays correct for consumers.
func TestMemoryStore_GetAllFilteredCopiesReceivedAt(t *testing.T) {
	store, _ := NewMemoryStore(5 * time.Minute)
	defer store.Close()

	before := time.Now()
	store.SetWithTimestamp("t1", map[string]string{"id": "a"}, map[string]float64{"v": 1}, time.Now())
	after := time.Now()

	all := store.GetAllFiltered(time.Unix(0, 0))
	assert.Len(t, all, 1)
	assert.False(t, all[0].ReceivedAt.IsZero())
	assert.False(t, all[0].ReceivedAt.Before(before))
	assert.False(t, all[0].ReceivedAt.After(after))
}
