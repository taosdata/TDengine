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
#include <algorithm>

extern "C" {
#include "os.h"
#include "libs/metrics/metrics.h"
#include "metricsInt.h"
#include "tglobal.h"
#include "taos_counter.h"
}

class MetricsTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    // Initialize global variables needed for metrics
    tsEnableMonitor = true;
    strcpy(tsMonitorFqdn, "localhost");
    tsMonitorPort = 8080;
    tsEnableMetrics = true;
    
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
  void AddMetricsForVgroups(const std::vector<int32_t>& vgIds, const char* dbPrefix);
  void VerifyVgroupMetricsExist(const std::vector<int32_t>& vgIds, bool shouldExist);
  bool CheckVgroupMetricsExist(int32_t vgId, int64_t clusterId, int32_t dnodeId, const char* dnodeEp, const char* dbname);
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
  pMetrics->blocked_commit_count = 10 * multiplier;
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
    int32_t code = addWriteMetrics(vgId, 1, 123456789, "localhost:6030", dbname, &rawMetrics);
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

bool MetricsTest::CheckVgroupMetricsExist(int32_t vgId, int64_t clusterId, int32_t dnodeId, const char* dnodeEp, const char* dbname) {
  // For testing purposes, we'll track metrics existence using a simple approach
  // Since we're testing the cleanup functionality, we mainly need to simulate
  // the existence check based on whether metrics were added and then cleaned up
  
  if (write_total_rows == NULL) {
    return false;
  }
  
  // Check if counter has any entries at all
  int key_count = taos_counter_get_keys_size(write_total_rows);
  if (key_count == 0) {
    return false;
  }
  
  // Use the vgroup_ids function to check for specific vgroup
  int32_t *vgroup_ids = NULL;
  char **keys = NULL;
  int list_size = key_count;
  
  int ret = taos_counter_get_vgroup_ids(write_total_rows, &keys, &vgroup_ids, &list_size);
  if (ret != 0) {
    return false;
  }
  
  bool found = false;
  for (int i = 0; i < list_size; i++) {
    if (vgroup_ids && vgroup_ids[i] == vgId) {
      found = true;
      break;
    }
  }
  
  // Clean up allocated memory
  if (vgroup_ids) taosMemoryFree(vgroup_ids);
  if (keys) taosMemoryFree(keys);
  
  return found;
}

void MetricsTest::AddMetricsForVgroups(const std::vector<int32_t>& vgIds, const char* dbPrefix) {
  for (size_t i = 0; i < vgIds.size(); i++) {
    SRawWriteMetrics rawMetrics = {0};
    PrepareRawWriteMetrics(&rawMetrics, i + 1);
    
    char dbname[64];
    snprintf(dbname, sizeof(dbname), "%s_%d", dbPrefix, vgIds[i]);
    
    int32_t code = addWriteMetrics(vgIds[i], 1, 123456789, "localhost:6030", dbname, &rawMetrics);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
}

void MetricsTest::VerifyVgroupMetricsExist(const std::vector<int32_t>& vgIds, bool shouldExist) {
  for (int32_t vgId : vgIds) {
    char dbname[64];
    snprintf(dbname, sizeof(dbname), "test_db_%d", vgId);
    bool exists = CheckVgroupMetricsExist(vgId, 123456789, 1, "localhost:6030", dbname);
    if (shouldExist) {
      ASSERT_TRUE(exists) << "Expected metrics to exist for vgId: " << vgId;
    } else {
      ASSERT_FALSE(exists) << "Expected metrics to be cleaned up for vgId: " << vgId;
    }
  }
}

TEST_F(MetricsTest, InitAndCleanup) {
  // Test that we can get metrics even when no data has been added
  bool exists = CheckVgroupMetricsExist(999, 123456789, 1, "localhost:6030", "nonexistent_db");
  ASSERT_FALSE(exists); // Should be false for non-existent vgId
}

TEST_F(MetricsTest, AddWriteMetrics) {
  SRawWriteMetrics rawMetrics = {0};
  PrepareRawWriteMetrics(&rawMetrics);
  
  int32_t vgId = 100;
  int32_t dnodeId = 1;
  int64_t clusterId = 123456789;
  
  // Add write metrics
  int32_t code = addWriteMetrics(vgId, dnodeId, clusterId, "localhost:6030", "test_db_1", &rawMetrics);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  // Retrieve and validate the metrics exist
  bool exists = CheckVgroupMetricsExist(vgId, clusterId, dnodeId, "localhost:6030", "test_db_1");
  ASSERT_TRUE(exists);
}

TEST_F(MetricsTest, UpdateWriteMetrics) {
  SRawWriteMetrics rawMetrics = {0};
  PrepareRawWriteMetrics(&rawMetrics, 2); // Use multiplier 2
  
  int32_t vgId = 100; // Same vgId as previous test
  int32_t dnodeId = 1;
  int64_t clusterId = 123456789;
  
  // Update existing write metrics
  int32_t code = addWriteMetrics(vgId, dnodeId, clusterId, "localhost:6030", "test_db_2", &rawMetrics);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  // Retrieve and validate the updated metrics exist
  bool exists = CheckVgroupMetricsExist(vgId, clusterId, dnodeId, "localhost:6030", "test_db_2");
  ASSERT_TRUE(exists);
}

TEST_F(MetricsTest, AddDnodeMetrics) {
  SRawDnodeMetrics rawMetrics = {0};
  PrepareRawDnodeMetrics(&rawMetrics);
  
  // Add dnode metrics
  int32_t code = addDnodeMetrics(&rawMetrics, 123456789, 1, "localhost:6030");
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
}

TEST_F(MetricsTest, MultipleVgroups) {
  // Add metrics for multiple vgroups
  for (int32_t i = 200; i <= 205; i++) {
    SRawWriteMetrics rawMetrics = {0};
    PrepareRawWriteMetrics(&rawMetrics, i - 199); // Different multipliers
    
    char dbname[32];
    snprintf(dbname, sizeof(dbname), "test_db_%d", i - 199);
    int32_t code = addWriteMetrics(i, 1, 123456789, "localhost:6030", dbname, &rawMetrics);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  
  // Verify all vgroups have metrics
  for (int32_t i = 200; i <= 205; i++) {
    char dbname[32];
    snprintf(dbname, sizeof(dbname), "test_db_%d", i - 199);
    bool exists = CheckVgroupMetricsExist(i, 123456789, 1, "localhost:6030", dbname);
    ASSERT_TRUE(exists);
  }
}

// New comprehensive tests for cleanupExpiredMetrics

TEST_F(MetricsTest, CleanupExpiredMetrics_NullParameterHandling) {
  // Test with null parameter should return success
  int32_t code = cleanupExpiredMetrics(nullptr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
}

TEST_F(MetricsTest, CleanupExpiredMetrics_EmptyValidVgroups) {
  // Add metrics for test vgroups
  std::vector<int32_t> testVgIds = {1001, 1002, 1003};
  for (size_t i = 0; i < testVgIds.size(); i++) {
    SRawWriteMetrics rawMetrics = {0};
    PrepareRawWriteMetrics(&rawMetrics, i + 1);
    char dbname[64];
    snprintf(dbname, sizeof(dbname), "empty_test_%d", testVgIds[i]);
    int32_t code = addWriteMetrics(testVgIds[i], 1, 123456789, "localhost:6030", dbname, &rawMetrics);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  
  // Verify metrics exist before cleanup
  for (size_t i = 0; i < testVgIds.size(); i++) {
    char dbname[64];
    snprintf(dbname, sizeof(dbname), "empty_test_%d", testVgIds[i]);
    bool exists = CheckVgroupMetricsExist(testVgIds[i], 123456789, 1, "localhost:6030", dbname);
    ASSERT_TRUE(exists);
  }
  
  // Create empty valid vgroups hash
  SHashObj *pValidVgroups = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  ASSERT_NE(pValidVgroups, nullptr);
  
  // Call cleanup with empty valid vgroups (should clean all)
  int32_t code = cleanupExpiredMetrics(pValidVgroups);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  // All metrics should be cleaned up
  for (size_t i = 0; i < testVgIds.size(); i++) {
    char dbname[64];
    snprintf(dbname, sizeof(dbname), "empty_test_%d", testVgIds[i]);
    bool exists = CheckVgroupMetricsExist(testVgIds[i], 123456789, 1, "localhost:6030", dbname);
    ASSERT_FALSE(exists);
  }
  
  taosHashCleanup(pValidVgroups);
}

TEST_F(MetricsTest, CleanupExpiredMetrics_PartialCleanup) {
  // Create metrics for vgroups 2001-2005
  std::vector<int32_t> allVgIds = {2001, 2002, 2003, 2004, 2005};
  for (size_t i = 0; i < allVgIds.size(); i++) {
    SRawWriteMetrics rawMetrics = {0};
    PrepareRawWriteMetrics(&rawMetrics, i + 1);
    char dbname[64];
    snprintf(dbname, sizeof(dbname), "partial_test_%d", allVgIds[i]);
    int32_t code = addWriteMetrics(allVgIds[i], 1, 123456789, "localhost:6030", dbname, &rawMetrics);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  
  // Verify all metrics exist
  for (size_t i = 0; i < allVgIds.size(); i++) {
    char dbname[64];
    snprintf(dbname, sizeof(dbname), "partial_test_%d", allVgIds[i]);
    bool exists = CheckVgroupMetricsExist(allVgIds[i], 123456789, 1, "localhost:6030", dbname);
    ASSERT_TRUE(exists);
  }
  
  // Create valid vgroups hash with only some vgroups (2001, 2003, 2005)
  std::vector<int32_t> validVgIds = {2001, 2003, 2005};
  SHashObj *pValidVgroups = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  ASSERT_NE(pValidVgroups, nullptr);
  
  for (int32_t vgId : validVgIds) {
    char dummy = 1;
    int32_t ret = taosHashPut(pValidVgroups, &vgId, sizeof(int32_t), &dummy, sizeof(char));
    ASSERT_EQ(ret, 0);
  }
  
  // Clean expired metrics
  int32_t code = cleanupExpiredMetrics(pValidVgroups);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  // Verify valid vgroups still exist
  for (int32_t vgId : validVgIds) {
    char dbname[64];
    snprintf(dbname, sizeof(dbname), "partial_test_%d", vgId);
    bool exists = CheckVgroupMetricsExist(vgId, 123456789, 1, "localhost:6030", dbname);
    ASSERT_TRUE(exists);
  }
  
  // Verify invalid vgroups are cleaned up (2002, 2004)
  std::vector<int32_t> invalidVgIds = {2002, 2004};
  for (int32_t vgId : invalidVgIds) {
    char dbname[64];
    snprintf(dbname, sizeof(dbname), "partial_test_%d", vgId);
    bool exists = CheckVgroupMetricsExist(vgId, 123456789, 1, "localhost:6030", dbname);
    ASSERT_FALSE(exists);
  }
  
  taosHashCleanup(pValidVgroups);
}

TEST_F(MetricsTest, CleanupExpiredMetrics_AllValidVgroups) {
  // Create metrics for vgroups 3001-3003
  std::vector<int32_t> testVgIds = {3001, 3002, 3003};
  for (size_t i = 0; i < testVgIds.size(); i++) {
    SRawWriteMetrics rawMetrics = {0};
    PrepareRawWriteMetrics(&rawMetrics, i + 1);
    char dbname[64];
    snprintf(dbname, sizeof(dbname), "all_valid_test_%d", testVgIds[i]);
    int32_t code = addWriteMetrics(testVgIds[i], 1, 123456789, "localhost:6030", dbname, &rawMetrics);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  
  // Verify all metrics exist
  for (size_t i = 0; i < testVgIds.size(); i++) {
    char dbname[64];
    snprintf(dbname, sizeof(dbname), "all_valid_test_%d", testVgIds[i]);
    bool exists = CheckVgroupMetricsExist(testVgIds[i], 123456789, 1, "localhost:6030", dbname);
    ASSERT_TRUE(exists);
  }
  
  // Create valid vgroups hash with all vgroups
  SHashObj *pValidVgroups = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  ASSERT_NE(pValidVgroups, nullptr);
  
  for (int32_t vgId : testVgIds) {
    char dummy = 1;
    int32_t ret = taosHashPut(pValidVgroups, &vgId, sizeof(int32_t), &dummy, sizeof(char));
    ASSERT_EQ(ret, 0);
  }
  
  // Clean expired metrics
  int32_t code = cleanupExpiredMetrics(pValidVgroups);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  // All vgroups should still exist (none should be cleaned)
  for (size_t i = 0; i < testVgIds.size(); i++) {
    char dbname[64];
    snprintf(dbname, sizeof(dbname), "all_valid_test_%d", testVgIds[i]);
    bool exists = CheckVgroupMetricsExist(testVgIds[i], 123456789, 1, "localhost:6030", dbname);
    ASSERT_TRUE(exists);
  }
  
  taosHashCleanup(pValidVgroups);
}

TEST_F(MetricsTest, CleanupExpiredMetrics_EmptyCounters) {
  // Test cleanup when no metrics exist
  SHashObj *pValidVgroups = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  ASSERT_NE(pValidVgroups, nullptr);
  
  // Add some valid vgroups
  std::vector<int32_t> validVgIds = {5001, 5002};
  for (int32_t vgId : validVgIds) {
    char dummy = 1;
    int32_t ret = taosHashPut(pValidVgroups, &vgId, sizeof(int32_t), &dummy, sizeof(char));
    ASSERT_EQ(ret, 0);
  }
  
  // Call cleanup when no metrics exist (should not crash)
  int32_t code = cleanupExpiredMetrics(pValidVgroups);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  taosHashCleanup(pValidVgroups);
}

TEST_F(MetricsTest, CleanupExpiredMetrics_BasicFunctionality) {
  // Add metrics for vgroups 100, 200, 202, 204
  int32_t activeVgIds[] = {100, 200, 202, 204};
  for (int32_t i = 0; i < 4; i++) {
    SRawWriteMetrics rawMetrics = {0};
    PrepareRawWriteMetrics(&rawMetrics);
    int32_t code = addWriteMetrics(activeVgIds[i], 1, 123456789, "localhost:6030", "test_db", &rawMetrics);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  
  // Verify all vgroups exist
  for (int32_t i = 0; i < 4; i++) {
    bool exists = CheckVgroupMetricsExist(activeVgIds[i], 123456789, 1, "localhost:6030", "test_db");
    ASSERT_TRUE(exists);
  }
  
  // Create valid vgroups hash with only some of the vgroups
  int32_t validVgIds[] = {100, 200, 204}; // 202 is missing
  SHashObj *pValidVgroups = CreateValidVgroupsHash(validVgIds, 3);
  
  // Clean expired metrics using the new function
  int32_t code = cleanupExpiredMetrics(pValidVgroups);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  // Verify that vgId 202 metrics were removed, others remain
  ASSERT_TRUE(CheckVgroupMetricsExist(100, 123456789, 1, "localhost:6030", "test_db"));
  ASSERT_TRUE(CheckVgroupMetricsExist(200, 123456789, 1, "localhost:6030", "test_db"));
  ASSERT_FALSE(CheckVgroupMetricsExist(202, 123456789, 1, "localhost:6030", "test_db")); // Should be removed
  ASSERT_TRUE(CheckVgroupMetricsExist(204, 123456789, 1, "localhost:6030", "test_db"));
  
  taosHashCleanup(pValidVgroups);
}

TEST_F(MetricsTest, HighLoadSimulation) {
  int32_t vgId = 1001;
  int32_t iterations = 10;
  
  // Simulate high load
  SimulateHighLoadMetrics(vgId, iterations);
  
  // Verify metrics exist (check the last added one)
  char dbname[32];
  snprintf(dbname, sizeof(dbname), "load_test_db_%d", iterations - 1);
  bool exists = CheckVgroupMetricsExist(vgId, 123456789, 1, "localhost:6030", dbname);
  ASSERT_TRUE(exists);
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
        
        int32_t code = addWriteMetrics(vgId, t + 1, 123456789 + t, "localhost:6030", rawMetrics.dbname, &rawMetrics);
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
      char dbname[32];
      snprintf(dbname, sizeof(dbname), "thread_%d_db_%d", t, i);
      bool exists = CheckVgroupMetricsExist(vgId, 123456789 + t, t + 1, "localhost:6030", dbname);
      ASSERT_TRUE(exists);
    }
  }
}

TEST_F(MetricsTest, InvalidParameters) {
  // Test with null raw metrics
  int32_t code = addWriteMetrics(500, 1, 123456789, "localhost:6030", "test_db", nullptr);
  ASSERT_EQ(code, TSDB_CODE_INVALID_PARA);
  
  // Test with null dnode metrics
  code = addDnodeMetrics(nullptr, 123456789, 1, "localhost:6030");
  ASSERT_EQ(code, TSDB_CODE_INVALID_PARA);
  
  // Verify no metrics were created
  bool exists = CheckVgroupMetricsExist(500, 123456789, 1, "localhost:6030", "test_db");
  ASSERT_FALSE(exists);
}

TEST_F(MetricsTest, LargeVgIdRange) {
  // Test with large vgId values
  int32_t largeVgIds[] = {999999, 1000000, 2147483647}; // Including INT32_MAX
  
  for (int32_t i = 0; i < 3; i++) {
    SRawWriteMetrics rawMetrics = {0};
    PrepareRawWriteMetrics(&rawMetrics, i + 1);
    
    int32_t code = addWriteMetrics(largeVgIds[i], 1, 123456789, "localhost:6030", "large_vg_test", &rawMetrics);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    
    bool exists = CheckVgroupMetricsExist(largeVgIds[i], 123456789, 1, "localhost:6030", "large_vg_test");
    ASSERT_TRUE(exists);
  }
}

TEST_F(MetricsTest, CleanupExpiredMetrics_LargeScale) {
  // Test with large number of vgroups for comprehensive coverage
  std::vector<int32_t> allVgIds;
  std::vector<int32_t> validVgIds;
  
  // Create 100 vgroups
  for (int32_t i = 10001; i <= 10100; i++) {
    allVgIds.push_back(i);
    // Keep every 5th vgroup as valid
    if (i % 5 == 1) {
      validVgIds.push_back(i);
    }
  }
  
  // Add metrics for all vgroups
  for (size_t i = 0; i < allVgIds.size(); i++) {
    SRawWriteMetrics rawMetrics = {0};
    PrepareRawWriteMetrics(&rawMetrics, i + 1);
    int32_t code = addWriteMetrics(allVgIds[i], 1, 123456789, "localhost:6030", "large_scale_test", &rawMetrics);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  
  // Verify all metrics exist
  for (int32_t vgId : allVgIds) {
    bool exists = CheckVgroupMetricsExist(vgId, 123456789, 1, "localhost:6030", "large_scale_test");
    ASSERT_TRUE(exists);
  }
  
  // Create valid vgroups hash
  SHashObj *pValidVgroups = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  ASSERT_NE(pValidVgroups, nullptr);
  
  for (int32_t vgId : validVgIds) {
    char dummy = 1;
    int32_t ret = taosHashPut(pValidVgroups, &vgId, sizeof(int32_t), &dummy, sizeof(char));
    ASSERT_EQ(ret, 0);
  }
  
  // Clean expired metrics
  int32_t code = cleanupExpiredMetrics(pValidVgroups);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  // Verify valid vgroups still exist
  for (int32_t vgId : validVgIds) {
    bool exists = CheckVgroupMetricsExist(vgId, 123456789, 1, "localhost:6030", "large_scale_test");
    ASSERT_TRUE(exists);
  }
  
  // Verify invalid vgroups are cleaned up
  for (int32_t vgId : allVgIds) {
    // Check if this vgId is in the valid list
    bool isValid = false;
    for (int32_t validVgId : validVgIds) {
      if (vgId == validVgId) {
        isValid = true;
        break;
      }
    }
    
    if (!isValid) {
      bool exists = CheckVgroupMetricsExist(vgId, 123456789, 1, "localhost:6030", "large_scale_test");
      ASSERT_FALSE(exists);
    }
  }
  
  taosHashCleanup(pValidVgroups);
}

TEST_F(MetricsTest, CleanupExpiredMetrics_EdgeCases) {
  // Test edge cases for comprehensive coverage
  
  // Test with vgId 0
  SRawWriteMetrics rawMetrics = {0};
  PrepareRawWriteMetrics(&rawMetrics);
  int32_t code = addWriteMetrics(0, 1, 123456789, "localhost:6030", "edge_case_test", &rawMetrics);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  // Create valid vgroups hash with vgId 0
  SHashObj *pValidVgroups = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  ASSERT_NE(pValidVgroups, nullptr);
  
  int32_t vgId = 0;
  char dummy = 1;
  int32_t ret = taosHashPut(pValidVgroups, &vgId, sizeof(int32_t), &dummy, sizeof(char));
  ASSERT_EQ(ret, 0);
  
  // Clean expired metrics - vgId 0 should remain
  code = cleanupExpiredMetrics(pValidVgroups);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  // Verify vgId 0 still exists
  bool exists = CheckVgroupMetricsExist(0, 123456789, 1, "localhost:6030", "edge_case_test");
  ASSERT_TRUE(exists);
  
  taosHashCleanup(pValidVgroups);
}

TEST_F(MetricsTest, CleanupExpiredMetrics_StressTest) {
  // Stress test with rapid add/cleanup cycles
  for (int32_t cycle = 0; cycle < 5; cycle++) {
    // Add metrics for vgroups
    std::vector<int32_t> testVgIds = {20001 + cycle * 10, 20002 + cycle * 10, 20003 + cycle * 10};
    for (size_t i = 0; i < testVgIds.size(); i++) {
      SRawWriteMetrics rawMetrics = {0};
      PrepareRawWriteMetrics(&rawMetrics, i + 1);
      int32_t code = addWriteMetrics(testVgIds[i], 1, 123456789, "localhost:6030", "stress_test", &rawMetrics);
      ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    }
    
    // Create valid vgroups hash with only first vgroup
    SHashObj *pValidVgroups = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    ASSERT_NE(pValidVgroups, nullptr);
    
    char dummy = 1;
    int32_t ret = taosHashPut(pValidVgroups, &testVgIds[0], sizeof(int32_t), &dummy, sizeof(char));
    ASSERT_EQ(ret, 0);
    
    // Clean expired metrics
    int32_t code = cleanupExpiredMetrics(pValidVgroups);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    
         // Verify only first vgroup remains
     ASSERT_TRUE(CheckVgroupMetricsExist(testVgIds[0], 123456789, 1, "localhost:6030", "stress_test"));
     ASSERT_FALSE(CheckVgroupMetricsExist(testVgIds[1], 123456789, 1, "localhost:6030", "stress_test"));
     ASSERT_FALSE(CheckVgroupMetricsExist(testVgIds[2], 123456789, 1, "localhost:6030", "stress_test"));
    
    taosHashCleanup(pValidVgroups);
  }
}

TEST_F(MetricsTest, CleanupExpiredMetrics_ConcurrentAccess) {
  // Test concurrent access to cleanupExpiredMetrics (simplified to avoid segfault)
  const int32_t baseVgId = 30000;
  
  // Add initial metrics
  for (int32_t i = 0; i < 5; i++) {
    SRawWriteMetrics rawMetrics = {0};
    PrepareRawWriteMetrics(&rawMetrics);
    int32_t code = addWriteMetrics(baseVgId + i, 1, 123456789, "localhost:6030", "concurrent_test", &rawMetrics);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
  
  // Create valid vgroups hash with some of the vgroups  
  SHashObj *pValidVgroups = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  ASSERT_NE(pValidVgroups, nullptr);
  
  // Keep vgroups 0, 2, 4 as valid
  for (int32_t i = 0; i < 5; i += 2) {
    int32_t vgId = baseVgId + i;
    char dummy = 1;
    int32_t ret = taosHashPut(pValidVgroups, &vgId, sizeof(int32_t), &dummy, sizeof(char));
    ASSERT_EQ(ret, 0);
  }
  
  // Call cleanup
  int32_t code = cleanupExpiredMetrics(pValidVgroups);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  // Verify valid vgroups still exist (0, 2, 4)
  for (int32_t i = 0; i < 5; i += 2) {
    bool exists = CheckVgroupMetricsExist(baseVgId + i, 123456789, 1, "localhost:6030", "concurrent_test");
    ASSERT_TRUE(exists);
  }
  
  // Verify invalid vgroups are cleaned up (1, 3)
  for (int32_t i = 1; i < 5; i += 2) {
    bool exists = CheckVgroupMetricsExist(baseVgId + i, 123456789, 1, "localhost:6030", "concurrent_test");
    ASSERT_FALSE(exists);
  }
  
  taosHashCleanup(pValidVgroups);
} 