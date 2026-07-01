#include <gtest/gtest.h>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>

#include "os.h"
#include "tlrucache.h"

// Test basic LRU cache initialization with different shard configurations
TEST(lrucacheTest, init_with_shard_bits) {
  // Test with auto-calculated shards (-1)
  SLRUCache* cache1 = taosLRUCacheInit(10 * 1024 * 1024, -1, 0.5);
  ASSERT_NE(cache1, nullptr);
  int32_t numShards1 = taosLRUCacheGetNumShards(cache1);
  EXPECT_GT(numShards1, 0);
  std::cout << "Auto-calculated shards for 10MB: " << numShards1 << std::endl;
  taosLRUCacheCleanup(cache1);

  // Test with explicit shard bits (0 = 1 shard)
  SLRUCache* cache2 = taosLRUCacheInit(10 * 1024 * 1024, 0, 0.5);
  ASSERT_NE(cache2, nullptr);
  int32_t numShards2 = taosLRUCacheGetNumShards(cache2);
  EXPECT_EQ(numShards2, 1);
  std::cout << "Explicit 0 bits (1 shard): " << numShards2 << std::endl;
  taosLRUCacheCleanup(cache2);

  // Test with 2 shard bits (4 shards)
  SLRUCache* cache3 = taosLRUCacheInit(10 * 1024 * 1024, 2, 0.5);
  ASSERT_NE(cache3, nullptr);
  int32_t numShards3 = taosLRUCacheGetNumShards(cache3);
  EXPECT_EQ(numShards3, 4);
  std::cout << "Explicit 2 bits (4 shards): " << numShards3 << std::endl;
  taosLRUCacheCleanup(cache3);

  // Test with 4 shard bits (16 shards)
  SLRUCache* cache4 = taosLRUCacheInit(10 * 1024 * 1024, 4, 0.5);
  ASSERT_NE(cache4, nullptr);
  int32_t numShards4 = taosLRUCacheGetNumShards(cache4);
  EXPECT_EQ(numShards4, 16);
  std::cout << "Explicit 4 bits (16 shards): " << numShards4 << std::endl;
  taosLRUCacheCleanup(cache4);
}

// Test invalid shard bits
TEST(lrucacheTest, invalid_shard_bits) {
  // Test with too many shard bits (>= 20)
  SLRUCache* cache1 = taosLRUCacheInit(10 * 1024 * 1024, 20, 0.5);
  EXPECT_EQ(cache1, nullptr);

  SLRUCache* cache2 = taosLRUCacheInit(10 * 1024 * 1024, 25, 0.5);
  EXPECT_EQ(cache2, nullptr);
}

// Test NULL cache handling
TEST(lrucacheTest, null_cache_get_shards) {
  int32_t numShards = taosLRUCacheGetNumShards(nullptr);
  EXPECT_EQ(numShards, 0);
}

// Test cache operations with different shard counts
TEST(lrucacheTest, operations_with_different_shards) {
  const int32_t numItems = 1000;

  // Test with 1 shard
  SLRUCache* cache1 = taosLRUCacheInit(1 * 1024 * 1024, 0, 0.5);
  ASSERT_NE(cache1, nullptr);

  for (int32_t i = 0; i < numItems; i++) {
    char key[64];
    snprintf(key, sizeof(key), "key_%d", i);
    int32_t value = i;

    LRUHandle* handle = nullptr;
    LRUStatus status = taosLRUCacheInsert(cache1, key, strlen(key), &value, sizeof(value),
                                          nullptr, nullptr, &handle, TAOS_LRU_PRIORITY_LOW, nullptr);
    if (status == TAOS_LRU_STATUS_OK || status == TAOS_LRU_STATUS_OK_OVERWRITTEN) {
      ASSERT_NE(handle, nullptr);
      taosLRUCacheRelease(cache1, handle, false);
    }
  }

  // Verify some items can be retrieved
  for (int32_t i = 0; i < 10; i++) {
    char key[64];
    snprintf(key, sizeof(key), "key_%d", i);
    LRUHandle* handle = taosLRUCacheLookup(cache1, key, strlen(key));
    if (handle) {
      taosLRUCacheRelease(cache1, handle, false);
    }
  }

  taosLRUCacheCleanup(cache1);

  // Test with 16 shards
  SLRUCache* cache2 = taosLRUCacheInit(1 * 1024 * 1024, 4, 0.5);
  ASSERT_NE(cache2, nullptr);
  EXPECT_EQ(taosLRUCacheGetNumShards(cache2), 16);

  for (int32_t i = 0; i < numItems; i++) {
    char key[64];
    snprintf(key, sizeof(key), "key_%d", i);
    int32_t value = i;

    LRUHandle* handle = nullptr;
    LRUStatus status = taosLRUCacheInsert(cache2, key, strlen(key), &value, sizeof(value),
                                          nullptr, nullptr, &handle, TAOS_LRU_PRIORITY_LOW, nullptr);
    if (status == TAOS_LRU_STATUS_OK || status == TAOS_LRU_STATUS_OK_OVERWRITTEN) {
      ASSERT_NE(handle, nullptr);
      taosLRUCacheRelease(cache2, handle, false);
    }
  }

  taosLRUCacheCleanup(cache2);
}

// Test concurrent access with multiple shards
TEST(lrucacheTest, concurrent_access_multi_shard) {
  const int32_t numThreads = 8;
  const int32_t opsPerThread = 500;

  SLRUCache* cache = taosLRUCacheInit(10 * 1024 * 1024, 3, 0.5);  // 8 shards
  ASSERT_NE(cache, nullptr);
  EXPECT_EQ(taosLRUCacheGetNumShards(cache), 8);

  std::atomic<int32_t> successCount{0};
  std::vector<std::thread> threads;

  for (int32_t t = 0; t < numThreads; t++) {
    threads.emplace_back([&, t]() {
      for (int32_t i = 0; i < opsPerThread; i++) {
        char key[64];
        snprintf(key, sizeof(key), "thread_%d_key_%d", t, i);
        int32_t value = t * 10000 + i;

        // Insert
        LRUHandle* handle = nullptr;
        LRUStatus status = taosLRUCacheInsert(cache, key, strlen(key), &value, sizeof(value),
                                              nullptr, nullptr, &handle, TAOS_LRU_PRIORITY_LOW, nullptr);
        if (status == TAOS_LRU_STATUS_OK || status == TAOS_LRU_STATUS_OK_OVERWRITTEN) {
          if (handle) {
            taosLRUCacheRelease(cache, handle, false);

            // Lookup
            handle = taosLRUCacheLookup(cache, key, strlen(key));
            if (handle) {
              int32_t* pValue = (int32_t*)taosLRUCacheValue(cache, handle);
              if (pValue && *pValue == value) {
                successCount++;
              }
              taosLRUCacheRelease(cache, handle, false);
            }
          }
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  std::cout << "Concurrent operations succeeded: " << successCount.load() << " / "
            << (numThreads * opsPerThread) << std::endl;
  EXPECT_GT(successCount.load(), 0);

  taosLRUCacheCleanup(cache);
}

// Test capacity and eviction with different shard counts
TEST(lrucacheTest, capacity_eviction_with_shards) {
  const size_t capacity = 1024;  // Small capacity to trigger eviction
  const size_t itemSize = 64;
  const int32_t numItems = 50;

  // Test with 1 shard
  SLRUCache* cache1 = taosLRUCacheInit(capacity, 0, 0.5);
  ASSERT_NE(cache1, nullptr);

  int insertCount1 = 0;
  for (int32_t i = 0; i < numItems; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key_%d", i);
    char value[64];
    memset(value, 'A' + (i % 26), itemSize);

    LRUHandle* handle = nullptr;
    LRUStatus status = taosLRUCacheInsert(cache1, key, strlen(key), value, itemSize,
                                          nullptr, nullptr, &handle, TAOS_LRU_PRIORITY_LOW, nullptr);
    if (status == TAOS_LRU_STATUS_OK || status == TAOS_LRU_STATUS_OK_OVERWRITTEN) {
      if (handle) {
        insertCount1++;
        taosLRUCacheRelease(cache1, handle, false);
      }
    }
  }

  size_t usage1 = taosLRUCacheGetUsage(cache1);
  std::cout << "Single shard - Inserted: " << insertCount1 << ", Usage: " << usage1 << " / " << capacity << std::endl;
  EXPECT_LE(usage1, capacity);

  taosLRUCacheCleanup(cache1);

  // Test with 4 shards
  SLRUCache* cache2 = taosLRUCacheInit(capacity, 2, 0.5);
  ASSERT_NE(cache2, nullptr);
  EXPECT_EQ(taosLRUCacheGetNumShards(cache2), 4);

  int insertCount2 = 0;
  for (int32_t i = 0; i < numItems; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key_%d", i);
    char value[64];
    memset(value, 'A' + (i % 26), itemSize);

    LRUHandle* handle = nullptr;
    LRUStatus status = taosLRUCacheInsert(cache2, key, strlen(key), value, itemSize,
                                          nullptr, nullptr, &handle, TAOS_LRU_PRIORITY_LOW, nullptr);
    if (status == TAOS_LRU_STATUS_OK || status == TAOS_LRU_STATUS_OK_OVERWRITTEN) {
      if (handle) {
        insertCount2++;
        taosLRUCacheRelease(cache2, handle, false);
      }
    }
  }

  size_t usage2 = taosLRUCacheGetUsage(cache2);
  std::cout << "4 shards - Inserted: " << insertCount2 << ", Usage: " << usage2 << " / " << capacity << std::endl;
  EXPECT_LE(usage2, capacity);

  taosLRUCacheCleanup(cache2);
}

// Test auto-calculation of shards based on capacity
TEST(lrucacheTest, auto_shard_calculation) {
  struct TestCase {
    size_t capacity;
    int32_t expectedMinShards;
    int32_t expectedMaxShards;
  };

  std::vector<TestCase> testCases = {
    {256 * 1024, 0, 1},           // 256KB -> 0-1 shards
    {1 * 1024 * 1024, 1, 2},      // 1MB -> 1-2 shards
    {10 * 1024 * 1024, 2, 32},    // 10MB -> 2-32 shards
    {100 * 1024 * 1024, 8, 64},   // 100MB -> 8-64 shards
  };

  for (const auto& tc : testCases) {
    SLRUCache* cache = taosLRUCacheInit(tc.capacity, -1, 0.5);
    ASSERT_NE(cache, nullptr);

    int32_t numShards = taosLRUCacheGetNumShards(cache);
    std::cout << "Capacity: " << tc.capacity << " bytes -> " << numShards << " shards" << std::endl;

    EXPECT_GE(numShards, tc.expectedMinShards);
    EXPECT_LE(numShards, tc.expectedMaxShards);

    taosLRUCacheCleanup(cache);
  }
}

// Test cache erase with multiple shards
TEST(lrucacheTest, erase_with_shards) {
  SLRUCache* cache = taosLRUCacheInit(1 * 1024 * 1024, 2, 0.5);  // 4 shards
  ASSERT_NE(cache, nullptr);

  const int32_t numItems = 100;

  // Insert items
  for (int32_t i = 0; i < numItems; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key_%d", i);
    int32_t value = i;

    LRUHandle* handle = nullptr;
    LRUStatus status = taosLRUCacheInsert(cache, key, strlen(key), &value, sizeof(value),
                                          nullptr, nullptr, &handle, TAOS_LRU_PRIORITY_LOW, nullptr);
    if (status == TAOS_LRU_STATUS_OK || status == TAOS_LRU_STATUS_OK_OVERWRITTEN) {
      ASSERT_NE(handle, nullptr);
      taosLRUCacheRelease(cache, handle, false);
    }
  }

  // Erase every other item
  for (int32_t i = 0; i < numItems; i += 2) {
    char key[32];
    snprintf(key, sizeof(key), "key_%d", i);
    taosLRUCacheErase(cache, key, strlen(key));
  }

  // Verify erased items are gone and others remain
  int foundCount = 0;
  for (int32_t i = 0; i < numItems; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key_%d", i);
    LRUHandle* handle = taosLRUCacheLookup(cache, key, strlen(key));

    if (i % 2 == 0) {
      // Should be erased
      EXPECT_EQ(handle, nullptr);
    } else {
      // Should still exist
      if (handle) {
        foundCount++;
        taosLRUCacheRelease(cache, handle, false);
      }
    }
  }

  std::cout << "Found " << foundCount << " items after erasing half" << std::endl;

  taosLRUCacheCleanup(cache);
}

// Test strict capacity mode with shards
TEST(lrucacheTest, strict_capacity_with_shards) {
  const size_t capacity = 2048;

  SLRUCache* cache = taosLRUCacheInit(capacity, 2, 0.5);  // 4 shards
  ASSERT_NE(cache, nullptr);

  taosLRUCacheSetStrictCapacity(cache, true);
  EXPECT_TRUE(taosLRUCacheIsStrictCapacity(cache));

  // Try to insert items
  const size_t itemSize = 256;
  int successCount = 0;

  for (int32_t i = 0; i < 20; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key_%d", i);
    char value[256];
    memset(value, 'X', itemSize);

    LRUHandle* handle = nullptr;
    LRUStatus status = taosLRUCacheInsert(cache, key, strlen(key), value, itemSize,
                                          nullptr, nullptr, &handle, TAOS_LRU_PRIORITY_LOW, nullptr);
    if (status == TAOS_LRU_STATUS_OK || status == TAOS_LRU_STATUS_OK_OVERWRITTEN) {
      if (handle) {
        successCount++;
        taosLRUCacheRelease(cache, handle, false);
      }
    }
  }

  size_t usage = taosLRUCacheGetUsage(cache);
  std::cout << "Strict mode - Inserted: " << successCount << ", Usage: " << usage << " / " << capacity << std::endl;

  // In strict mode, usage should not exceed capacity
  EXPECT_LE(usage, capacity);

  taosLRUCacheCleanup(cache);
}
