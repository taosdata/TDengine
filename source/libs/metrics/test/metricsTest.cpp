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
  void ValidateWriteMetrics(SWriteMetricsEx *pMetrics, int32_t vgId, int32_t dnodeId, int64_t clusterId);
  void ValidateDnodeMetrics(SDnodeMetricsEx *pMetrics);
  SHashObj *CreateValidVgroupsHash(int32_t *vgIds, int32_t count);
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
}

void MetricsTest::PrepareRawDnodeMetrics(SRawDnodeMetrics *pMetrics) {
  pMetrics->rpcQueueMemoryAllowed = 1024 * 1024 * 100; // 100MB
  pMetrics->rpcQueueMemoryUsed = 1024 * 1024 * 50;     // 50MB
  pMetrics->applyMemoryAllowed = 1024 * 1024 * 200;    // 200MB
  pMetrics->applyMemoryUsed = 1024 * 1024 * 80;        // 80MB
}

void MetricsTest::ValidateWriteMetrics(SWriteMetricsEx *pMetrics, int32_t vgId, int32_t dnodeId, int64_t clusterId) {
  ASSERT_NE(pMetrics, nullptr);
  ASSERT_EQ(pMetrics->vgId, vgId);
  ASSERT_EQ(pMetrics->dnodeId, dnodeId);
  ASSERT_EQ(pMetrics->clusterId, clusterId);
  ASSERT_STRNE(pMetrics->dbname, "");  // Database name should not be empty
  
  ASSERT_EQ(getMetricInt64(&pMetrics->total_requests), 100);
  ASSERT_EQ(getMetricInt64(&pMetrics->total_rows), 1000);
  ASSERT_EQ(getMetricInt64(&pMetrics->total_bytes), 10000);
  ASSERT_EQ(getMetricInt64(&pMetrics->fetch_batch_meta_time), 5); // converted to ms
  ASSERT_EQ(getMetricInt64(&pMetrics->fetch_batch_meta_count), 50);
  ASSERT_EQ(getMetricInt64(&pMetrics->preprocess_time), 2); // converted to ms
}

void MetricsTest::ValidateDnodeMetrics(SDnodeMetricsEx *pMetrics) {
  ASSERT_NE(pMetrics, nullptr);
  ASSERT_EQ(getMetricInt64(&pMetrics->rpcQueueMemoryAllowed), 1024 * 1024 * 100);
  ASSERT_EQ(getMetricInt64(&pMetrics->rpcQueueMemoryUsed), 1024 * 1024 * 50);
  ASSERT_EQ(getMetricInt64(&pMetrics->applyMemoryAllowed), 1024 * 1024 * 200);
  ASSERT_EQ(getMetricInt64(&pMetrics->applyMemoryUsed), 1024 * 1024 * 80);
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
  // Test that metrics manager is properly initialized
  SDnodeMetricsEx *pDnodeMetrics = getDnodeMetrics();
  ASSERT_NE(pDnodeMetrics, nullptr);
  
  // Test that we can get metrics even when no data has been added
  SWriteMetricsEx *pWriteMetrics = getWriteMetricsByVgId(999);
  ASSERT_EQ(pWriteMetrics, nullptr); // Should be null for non-existent vgId
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
  
  // Retrieve and validate the metrics
  SWriteMetricsEx *pMetrics = getWriteMetricsByVgId(vgId);
  ValidateWriteMetrics(pMetrics, vgId, dnodeId, clusterId);
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
  SWriteMetricsEx *pMetrics = getWriteMetricsByVgId(vgId);
  ASSERT_NE(pMetrics, nullptr);
  ASSERT_EQ(getMetricInt64(&pMetrics->total_requests), 200); // Should be updated to 200
  ASSERT_EQ(getMetricInt64(&pMetrics->total_rows), 2000);    // Should be updated to 2000
}

TEST_F(MetricsTest, AddDnodeMetrics) {
  SRawDnodeMetrics rawMetrics = {0};
  PrepareRawDnodeMetrics(&rawMetrics);
  
  // Add dnode metrics
  int32_t code = addDnodeMetrics(&rawMetrics);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  // Retrieve and validate the metrics
  SDnodeMetricsEx *pMetrics = getDnodeMetrics();
  ValidateDnodeMetrics(pMetrics);
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
    SWriteMetricsEx *pMetrics = getWriteMetricsByVgId(i);
    ASSERT_NE(pMetrics, nullptr);
    ASSERT_EQ(pMetrics->vgId, i);
  }
}

TEST_F(MetricsTest, CleanExpiredMetrics) {
  // Create valid vgroups hash with only some of the vgroups
  int32_t validVgIds[] = {100, 200, 202, 204}; // Some from previous tests
  SHashObj *pValidVgroups = CreateValidVgroupsHash(validVgIds, 4);
  
  // Clean expired metrics
  cleanExpiredWriteMetrics(pValidVgroups);
  
  // Check that valid vgroups still exist
  ASSERT_NE(getWriteMetricsByVgId(100), nullptr);
  ASSERT_NE(getWriteMetricsByVgId(200), nullptr);
  ASSERT_NE(getWriteMetricsByVgId(202), nullptr);
  ASSERT_NE(getWriteMetricsByVgId(204), nullptr);
  
  // Check that invalid vgroups are removed
  ASSERT_EQ(getWriteMetricsByVgId(201), nullptr);
  ASSERT_EQ(getWriteMetricsByVgId(203), nullptr);
  ASSERT_EQ(getWriteMetricsByVgId(205), nullptr);
  
  taosHashCleanup(pValidVgroups);
}

TEST_F(MetricsTest, ReportMetrics) {
  // This test mainly checks that report functions don't crash
  // Since we can't easily test HTTP requests in unit tests,
  // we just ensure the functions can be called without errors
  
  // Disable actual network reporting for testing
  bool originalEnableMonitor = tsEnableMonitor;
  tsEnableMonitor = false;
  
  reportWriteMetrics();  // Should not crash
  reportDnodeMetrics();  // Should not crash
  
  // Restore original setting
  tsEnableMonitor = originalEnableMonitor;
}

TEST_F(MetricsTest, ErrorHandling) {
  // Test with null parameters
  int32_t code = addWriteMetrics(999, 1, 123456789, "test_db", nullptr);
  ASSERT_EQ(code, TSDB_CODE_INVALID_PARA);
  
  code = addDnodeMetrics(nullptr);
  ASSERT_EQ(code, TSDB_CODE_INVALID_PARA);
  
  // Test cleanup with null parameters
  cleanExpiredWriteMetrics(nullptr); // Should not crash
}

TEST_F(MetricsTest, MetricTypes) {
  // Test different metric types
  SMetric metric1, metric2, metric3;
  
  // Test INT64 metric
  initMetric(&metric1, METRIC_TYPE_INT64, METRIC_LEVEL_HIGH);
  setMetricInt64(&metric1, 12345);
  ASSERT_EQ(getMetricInt64(&metric1), 12345);
  
  // Test DOUBLE metric  
  initMetric(&metric2, METRIC_TYPE_DOUBLE, METRIC_LEVEL_HIGH);
  setMetricDouble(&metric2, 123.45);
  ASSERT_DOUBLE_EQ(getMetricDouble(&metric2), 123.45);
  
  // Test STRING metric
  initMetric(&metric3, METRIC_TYPE_STRING, METRIC_LEVEL_HIGH);
  setMetricString(&metric3, "test_string");
  ASSERT_STREQ(getMetricString(&metric3), "test_string");
  
  // Cleanup string metric
  setMetricString(&metric3, nullptr);
} 