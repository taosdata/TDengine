/**
 * @file metricsTest.cpp
 * @author TDengine Team
 * @brief metrics module tests
 * @version 1.0
 * @date 2024-01-01
 *
 * @copyright Copyright (c) 2024
 *
 */

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <vector>
#include <random>
#include "os.h"
#include "libs/metrics/metrics.h"
#include "tglobal.h"

class MetricsTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    // Initialize global variables needed for metrics
    tsEnableMonitor = true;
    strcpy(tsMonitorFqdn, "localhost");
    tsMonitorPort = 8080;
    tsEnableMetrics = true;
    tsMetricsPrintLog = false;
    
    // Initialize metrics manager
    int32_t code = initMetricsManager();
    ASSERT_EQ(code, 0);
  }

  static void TearDownTestSuite() {
    cleanupMetrics();
    taosMsleep(100);
  }

 public:
  void SetUp() override {}
  void TearDown() override {}

  void PrepareRawWriteMetrics(SRawWriteMetrics *pMetrics, int32_t multiplier = 1);
  void PrepareRawDnodeMetrics(SRawDnodeMetrics *pMetrics);
  SHashObj *CreateValidVgroupsHash(int32_t *vgIds, int32_t count);
  void SimulateHighLoadMetrics(int32_t vgId, int32_t iterations);
};

void MetricsTest::PrepareRawWriteMetrics(SRawWriteMetrics *pMetrics, int32_t multiplier) {
  snprintf(pMetrics->dbname, sizeof(pMetrics->dbname), "test_db_%d", multiplier);
  pMetrics->total_requests = 100 * multiplier;
  pMetrics->total_rows = 1000 * multiplier;
  pMetrics->total_bytes = 10000 * multiplier;
  pMetrics->fetch_batch_meta_time = 5000 * multiplier; // microseconds
  pMetrics->fetch_batch_meta_count = 50 * multiplier;
  pMetrics->preprocess_time = 2000 * multiplier; // microseconds
  pMetrics->wal_write_bytes = 8000 * multiplier;
  pMetrics->wal_write_time = 3000 * multiplier; // microseconds
  pMetrics->apply_bytes = 7000 * multiplier;
  pMetrics->apply_time = 4000 * multiplier; // microseconds
  pMetrics->commit_count = 20 * multiplier;
  pMetrics->commit_time = 6000 * multiplier; // microseconds
  pMetrics->memtable_wait_time = 1000 * multiplier; // microseconds
  pMetrics->block_commit_count = 10 * multiplier;
  pMetrics->blocked_commit_time = 2500 * multiplier; // microseconds
  pMetrics->merge_count = 5 * multiplier;
  pMetrics->merge_time = 8000 * multiplier; // microseconds
  pMetrics->last_cache_commit_time = 3500 * multiplier; // microseconds
  pMetrics->last_cache_commit_count = 15 * multiplier;
}

void MetricsTest::PrepareRawDnodeMetrics(SRawDnodeMetrics *pMetrics) {
  pMetrics->rpcQueueMemoryAllowed = 1024 * 1024 * 100; // 100MB
  pMetrics->rpcQueueMemoryUsed = 1024 * 1024 * 50;     // 50MB
  pMetrics->applyMemoryAllowed = 1024 * 1024 * 200;    // 200MB
  pMetrics->applyMemoryUsed = 1024 * 1024 * 80;        // 80MB
}

void MetricsTest::SimulateHighLoadMetrics(int32_t vgId, int32_t iterations) {
  for (int32_t i = 0; i < iterations; i++) {
    SRawWriteMetrics rawMetrics = {0};
    PrepareRawWriteMetrics(&rawMetrics, i + 1);
    
    char dbname[32];
    snprintf(dbname, sizeof(dbname), "load_test_db_%d", i);
    int32_t code = addWriteMetrics(vgId, 1, 123456789, dbname, &rawMetrics);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
}

SHashObj *MetricsTest::CreateValidVgroupsHash(int32_t *vgIds, int32_t count) {
  SHashObj *pValidVgroups = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  EXPECT_NE(pValidVgroups, nullptr);
  
  for (int32_t i = 0; i < count; i++) {
    char dummy = 1;
    int32_t ret = taosHashPut(pValidVgroups, &vgIds[i], sizeof(int32_t), &dummy, sizeof(char));
    EXPECT_EQ(ret, 0);
  }
  
  return pValidVgroups;
}

TEST_F(MetricsTest, InitAndCleanup) {
  // Test that we can get metrics even when no data has been added
  SVnodeMetricsCounters *pCounters = getVnodeMetricsCounters(999);
  ASSERT_EQ(pCounters, nullptr); // Should be null for non-existent vgId
}

TEST_F(MetricsTest, AddWriteMetrics) {
  SRawWriteMetrics rawMetrics = {0};
  PrepareRawWriteMetrics(&rawMetrics);
  
  int32_t vgId = 100;
  int32_t dnodeId = 1;
  int64_t clusterId = 123456789;
  
  // Add write metrics
  int32_t code = addWriteMetrics(vgId, dnodeId, clusterId, "test_db_1", &rawMetrics);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  // Retrieve and validate the metrics counters exist
  SVnodeMetricsCounters *pCounters = getVnodeMetricsCounters(vgId);
  ASSERT_NE(pCounters, nullptr);
  ASSERT_EQ(pCounters->vgId, vgId);
  ASSERT_EQ(pCounters->dnodeId, dnodeId);
  ASSERT_EQ(pCounters->clusterId, clusterId);
}

TEST_F(MetricsTest, UpdateWriteMetrics) {
  SRawWriteMetrics rawMetrics = {0};
  PrepareRawWriteMetrics(&rawMetrics, 2); // Use multiplier 2
  
  int32_t vgId = 100; // Same vgId as previous test
  int32_t dnodeId = 1;
  int64_t clusterId = 123456789;
  
  // Update existing write metrics
  int32_t code = addWriteMetrics(vgId, dnodeId, clusterId, "test_db_2", &rawMetrics);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  // Retrieve and validate the updated metrics
  SVnodeMetricsCounters *pCounters = getVnodeMetricsCounters(vgId);
  ASSERT_NE(pCounters, nullptr);
  ASSERT_EQ(pCounters->vgId, vgId);
}

TEST_F(MetricsTest, AddDnodeMetrics) {
  SRawDnodeMetrics rawMetrics = {0};
  PrepareRawDnodeMetrics(&rawMetrics);
  
  // Add dnode metrics
  int32_t code = addDnodeMetrics(&rawMetrics);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
}

TEST_F(MetricsTest, MultipleVgroups) {
  // Add metrics for multiple vgroups
  for (int32_t i = 200; i <= 205; i++) {
    SRawWriteMetrics rawMetrics = {0};
    PrepareRawWriteMetrics(&rawMetrics, i - 199); // Different multipliers
    
    char dbname[32];
    snprintf(dbname, sizeof(dbname), "test_db_%d", i - 199);
    int32_t code = addWriteMetrics(i, 1, 123456789, dbname, &rawMetrics);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  
  // Verify all vgroups have metrics
  for (int32_t i = 200; i <= 205; i++) {
    SVnodeMetricsCounters *pCounters = getVnodeMetricsCounters(i);
    ASSERT_NE(pCounters, nullptr);
    ASSERT_EQ(pCounters->vgId, i);
  }
}

TEST_F(MetricsTest, CleanExpiredMetrics) {
  // Add metrics for vgroups 100, 200, 202, 204
  int32_t activeVgIds[] = {100, 200, 202, 204};
  for (int32_t i = 0; i < 4; i++) {
    SRawWriteMetrics rawMetrics = {0};
    PrepareRawWriteMetrics(&rawMetrics);
    int32_t code = addWriteMetrics(activeVgIds[i], 1, 123456789, "test_db", &rawMetrics);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  
  // Verify all vgroups exist
  ASSERT_NE(getVnodeMetricsCounters(100), nullptr);
  ASSERT_NE(getVnodeMetricsCounters(200), nullptr);
  ASSERT_NE(getVnodeMetricsCounters(202), nullptr);
  ASSERT_NE(getVnodeMetricsCounters(204), nullptr);
  
  // Create valid vgroups hash with only some of the vgroups
  int32_t validVgIds[] = {100, 200, 204}; // 202 is missing
  SHashObj *pValidVgroups = CreateValidVgroupsHash(validVgIds, 3);
  
  // Clean expired metrics
  int32_t code = cleanExpiredWriteMetrics(pValidVgroups);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  // Verify that vgId 202 metrics were removed, others remain
  ASSERT_NE(getVnodeMetricsCounters(100), nullptr);
  ASSERT_NE(getVnodeMetricsCounters(200), nullptr);
  ASSERT_EQ(getVnodeMetricsCounters(202), nullptr); // Should be removed
  ASSERT_NE(getVnodeMetricsCounters(204), nullptr);
  
  taosHashCleanup(pValidVgroups);
}

TEST_F(MetricsTest, HighLoadSimulation) {
  int32_t vgId = 1001;
  int32_t iterations = 10;
  
  // Simulate high load
  SimulateHighLoadMetrics(vgId, iterations);
  
  // Verify metrics exist
  SVnodeMetricsCounters *pCounters = getVnodeMetricsCounters(vgId);
  ASSERT_NE(pCounters, nullptr);
  ASSERT_EQ(pCounters->vgId, vgId);
}

TEST_F(MetricsTest, ConcurrentAccess) {
  const int32_t numThreads = 4;
  const int32_t iterationsPerThread = 5;
  std::vector<std::thread> threads;
  
  // Create multiple threads adding metrics concurrently
  for (int32_t t = 0; t < numThreads; t++) {
    threads.emplace_back([t, iterationsPerThread]() {
      for (int32_t i = 0; i < iterationsPerThread; i++) {
        int32_t vgId = 2000 + t * 100 + i; // Unique vgId for each thread/iteration
        SRawWriteMetrics rawMetrics = {0};
        
        // Prepare different metrics for each thread
        snprintf(rawMetrics.dbname, sizeof(rawMetrics.dbname), "thread_%d_db_%d", t, i);
        rawMetrics.total_requests = (t + 1) * (i + 1) * 10;
        rawMetrics.total_rows = (t + 1) * (i + 1) * 100;
        rawMetrics.total_bytes = (t + 1) * (i + 1) * 1000;
        
        int32_t code = addWriteMetrics(vgId, t + 1, 123456789 + t, rawMetrics.dbname, &rawMetrics);
        ASSERT_EQ(code, TSDB_CODE_SUCCESS);
      }
    });
  }
  
  // Wait for all threads to complete
  for (auto& thread : threads) {
    thread.join();
  }
  
  // Verify all metrics were created
  for (int32_t t = 0; t < numThreads; t++) {
    for (int32_t i = 0; i < iterationsPerThread; i++) {
      int32_t vgId = 2000 + t * 100 + i;
      SVnodeMetricsCounters *pCounters = getVnodeMetricsCounters(vgId);
      ASSERT_NE(pCounters, nullptr);
      ASSERT_EQ(pCounters->vgId, vgId);
      ASSERT_EQ(pCounters->dnodeId, t + 1);
      ASSERT_EQ(pCounters->clusterId, 123456789 + t);
    }
  }
}

TEST_F(MetricsTest, InvalidParameters) {
  // Test with null raw metrics
  int32_t code = addWriteMetrics(500, 1, 123456789, "test_db", nullptr);
  ASSERT_EQ(code, TSDB_CODE_INVALID_PARA);
  
  // Test with null dnode metrics
  code = addDnodeMetrics(nullptr);
  ASSERT_EQ(code, TSDB_CODE_INVALID_PARA);
  
  // Verify no metrics were created
  SVnodeMetricsCounters *pCounters = getVnodeMetricsCounters(500);
  ASSERT_EQ(pCounters, nullptr);
}

TEST_F(MetricsTest, LargeVgIdRange) {
  // Test with large vgId values
  int32_t largeVgIds[] = {999999, 1000000, 2147483647}; // Including INT32_MAX
  
  for (int32_t i = 0; i < 3; i++) {
    SRawWriteMetrics rawMetrics = {0};
    PrepareRawWriteMetrics(&rawMetrics, i + 1);
    
    int32_t code = addWriteMetrics(largeVgIds[i], 1, 123456789, "large_vg_test", &rawMetrics);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    
    SVnodeMetricsCounters *pCounters = getVnodeMetricsCounters(largeVgIds[i]);
    ASSERT_NE(pCounters, nullptr);
    ASSERT_EQ(pCounters->vgId, largeVgIds[i]);
  }
} 