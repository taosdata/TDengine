/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include "../inc/perfCollector.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void test_query_performance_collection() {
  printf("Testing Query Performance Collection...\n");

  SPerfCollector       *pCollector = NULL;
  SPerfCollectionConfig config = {.enable_query_metrics = true,
                                  .enable_system_metrics = true,
                                  .enable_cache_metrics = true,
                                  .enable_rpc_metrics = true,
                                  .collection_interval_ms = 1000,
                                  .max_retained_records = 100,
                                  .sampling_rate = 100};

  int32_t result = perfCollectorInit(&pCollector, &config);
  if (result != 0) {
    printf("Failed to initialize performance collector: %d\n", result);
    return;
  }

  const char *test_sql = "SELECT COUNT(*) FROM test_table WHERE ts > NOW() - 1h";
  const char *test_user = "test_user";
  const char *test_db = "test_db";
  int64_t     start_time = perfCollectorGetCurrentTimestamp();

  result = perfCollectorRecordQueryStart(pCollector, test_sql, test_user, test_db, 0, 1, start_time);
  if (result != 0) {
    printf("Failed to record query start: %d\n", result);
  } else {
    printf("Successfully recorded query start\n");
  }

  sleep(1);
  int64_t end_time = perfCollectorGetCurrentTimestamp();
  int32_t affected_rows = 0;
  int32_t error_code = 0;

  result = perfCollectorRecordQueryEnd(pCollector, test_sql, end_time, affected_rows, error_code);
  if (result != 0) {
    printf("Failed to record query end: %d\n", result);
  } else {
    printf("Successfully recorded query end\n");
  }

  char *jsonStr = NULL;
  result = perfCollectorExportToJson(pCollector, &jsonStr);
  if (result == 0 && jsonStr != NULL) {
    printf("Performance data exported to JSON:\n%s\n", jsonStr);
    free(jsonStr);
  }

  perfCollectorCleanup(pCollector);
  printf("Query performance collection test completed\n");
}

void test_system_metrics() {
  printf("Testing System Metrics Collection...\n");

  SPerfCollector       *pCollector = NULL;
  SPerfCollectionConfig config = {.enable_system_metrics = true, .collection_interval_ms = 1000};

  int32_t result = perfCollectorInit(&pCollector, &config);
  if (result != 0) {
    printf("Failed to initialize performance collector: %d\n", result);
    return;
  }

  SSystemPerfMetrics systemMetrics = {.cpu_usage_percent = 75,
                                      .memory_usage_bytes = 2LL * 1024 * 1024 * 1024,
                                      .disk_usage_bytes = 100LL * 1024 * 1024 * 1024,
                                      .network_in_bytes = 1024LL * 1024,
                                      .network_out_bytes = 512LL * 1024,
                                      .disk_read_bytes = 10LL * 1024 * 1024,
                                      .disk_write_bytes = 5LL * 1024 * 1024,
                                      .active_connections = 25,
                                      .active_queries = 5,
                                      .queued_requests = 2,
                                      .timestamp = perfCollectorGetCurrentTimestamp()};

  result = perfCollectorUpdateSystemMetrics(pCollector, &systemMetrics);
  if (result != 0) {
    printf("Failed to update system metrics: %d\n", result);
  } else {
    printf("Successfully updated system metrics\n");
  }

  SSystemPerfMetrics retrievedMetrics;
  result = perfCollectorGetSystemMetrics(pCollector, &retrievedMetrics);
  if (result == 0) {
    printf("Retrieved system metrics:\n");
    printf("  CPU Usage: %ld%%\n", retrievedMetrics.cpu_usage_percent);
    printf("  Memory Usage: %ld bytes\n", retrievedMetrics.memory_usage_bytes);
    printf("  Active Connections: %ld\n", retrievedMetrics.active_connections);
  }

  perfCollectorCleanup(pCollector);
  printf("System metrics test completed\n");
}

void test_cache_metrics() {
  printf("Testing Cache Metrics Collection...\n");

  SPerfCollector       *pCollector = NULL;
  SPerfCollectionConfig config = {.enable_cache_metrics = true, .collection_interval_ms = 1000};

  int32_t result = perfCollectorInit(&pCollector, &config);
  if (result != 0) {
    printf("Failed to initialize performance collector: %d\n", result);
    return;
  }

  SCachePerfMetrics cacheMetrics = {.cache_hits = 1000,
                                    .cache_misses = 100,
                                    .cache_size_bytes = 64LL * 1024 * 1024,
                                    .cache_max_size_bytes = 128LL * 1024 * 1024,
                                    .hit_ratio = 0.909,
                                    .evict_count = 50,
                                    .timestamp = perfCollectorGetCurrentTimestamp()};

  result = perfCollectorUpdateCacheMetrics(pCollector, &cacheMetrics);
  if (result != 0) {
    printf("Failed to update cache metrics: %d\n", result);
  } else {
    printf("Successfully updated cache metrics\n");
  }

  SCachePerfMetrics retrievedMetrics;
  result = perfCollectorGetCacheMetrics(pCollector, &retrievedMetrics);
  if (result == 0) {
    printf("Retrieved cache metrics:\n");
    printf("  Cache Hits: %ld\n", retrievedMetrics.cache_hits);
    printf("  Cache Misses: %ld\n", retrievedMetrics.cache_misses);
    printf("  Hit Ratio: %.3f\n", retrievedMetrics.hit_ratio);
  }

  perfCollectorCleanup(pCollector);
  printf("Cache metrics test completed\n");
}

void test_rpc_metrics() {
  printf("Testing RPC Metrics Collection...\n");

  SPerfCollector       *pCollector = NULL;
  SPerfCollectionConfig config = {.enable_rpc_metrics = true, .collection_interval_ms = 1000};

  int32_t result = perfCollectorInit(&pCollector, &config);
  if (result != 0) {
    printf("Failed to initialize performance collector: %d\n", result);
    return;
  }

  SRpcPerfMetrics rpcMetrics = {.total_requests = 5000,
                                .success_requests = 4950,
                                .failed_requests = 50,
                                .total_time_us = 1000000,
                                .avg_time_us = 200,
                                .max_time_us = 5000,
                                .min_time_us = 10,
                                .bytes_sent = 10LL * 1024 * 1024,
                                .bytes_received = 50LL * 1024 * 1024,
                                .active_connections = 15,
                                .timestamp = perfCollectorGetCurrentTimestamp()};

  result = perfCollectorUpdateRpcMetrics(pCollector, &rpcMetrics);
  if (result != 0) {
    printf("Failed to update RPC metrics: %d\n", result);
  } else {
    printf("Successfully updated RPC metrics\n");
  }

  SRpcPerfMetrics retrievedMetrics;
  result = perfCollectorGetRpcMetrics(pCollector, &retrievedMetrics);
  if (result == 0) {
    printf("Retrieved RPC metrics:\n");
    printf("  Total Requests: %ld\n", retrievedMetrics.total_requests);
    printf("  Success Requests: %ld\n", retrievedMetrics.success_requests);
    printf("  Average Time: %ld us\n", retrievedMetrics.avg_time_us);
    printf("  Active Connections: %d\n", retrievedMetrics.active_connections);
  }

  perfCollectorCleanup(pCollector);
  printf("RPC metrics test completed\n");
}

int main() {
  printf("Performance Collector Test Suite\n");
  printf("Version: %s\n", perfCollectorGetVersion());

  test_query_performance_collection();
  printf("\n");

  test_system_metrics();
  printf("\n");

  test_cache_metrics();
  printf("\n");

  test_rpc_metrics();
  printf("\n");

  printf("All tests completed successfully\n");
  return 0;
}