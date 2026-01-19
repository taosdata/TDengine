/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "../inc/perfCollector.h"

void demonstrate_api_usage() {
  printf("=== TDengine Performance Data Collection API Demo ===\n");

  SPerfCollector       *pCollector = NULL;
  SPerfCollectionConfig config = {.enable_query_metrics = true,
                                  .enable_system_metrics = true,
                                  .enable_cache_metrics = true,
                                  .enable_rpc_metrics = true,
                                  .collection_interval_ms = 1000,
                                  .max_retained_records = 1000,
                                  .sampling_rate = 100};

  int32_t result = perfCollectorInit(&pCollector, &config);
  if (result != 0) {
    printf("Failed to initialize performance collector: %d\n", result);
    return;
  }

  printf("Performance collector initialized successfully\n");
  printf("Version: %s\n", perfCollectorGetVersion());
  printf("Health Status: %d\n", perfCollectorGetHealthStatus(pCollector));

  // Demonstrate query performance recording
  printf("\n--- Query Performance Demo ---\n");
  const char *test_sql = "SELECT COUNT(*) FROM sensor_data WHERE ts > NOW() - 1h";
  const char *test_user = "demo_user";
  const char *test_db = "demo_db";
  int64_t     start_time = perfCollectorGetCurrentTimestamp();

  result = perfCollectorRecordQueryStart(pCollector, test_sql, test_user, test_db, 0, 1, start_time);
  if (result == 0) {
    printf("✓ Query start recorded successfully\n");
  } else {
    printf("✗ Failed to record query start: %d\n", result);
  }

  // Simulate query execution
  usleep(50000);  // 50ms
  int64_t end_time = perfCollectorGetCurrentTimestamp();
  int32_t affected_rows = 1000;
  int32_t error_code = 0;

  result = perfCollectorRecordQueryEnd(pCollector, test_sql, end_time, affected_rows, error_code);
  if (result == 0) {
    printf("✓ Query end recorded successfully\n");
  } else {
    printf("✗ Failed to record query end: %d\n", result);
  }

  // Demonstrate system metrics updating
  printf("\n--- System Metrics Demo ---\n");
  SSystemPerfMetrics system_metrics = {.cpu_usage_percent = 85,
                                       .memory_usage_bytes = 4LL * 1024 * 1024 * 1024,  // 4GB
                                       .disk_usage_bytes = 100LL * 1024 * 1024 * 1024,  // 100GB
                                       .network_in_bytes = 10LL * 1024 * 1024,
                                       .network_out_bytes = 5LL * 1024 * 1024,
                                       .disk_read_bytes = 50LL * 1024 * 1024,
                                       .disk_write_bytes = 25LL * 1024 * 1024,
                                       .active_connections = 50,
                                       .active_queries = 10,
                                       .queued_requests = 5,
                                       .timestamp = perfCollectorGetCurrentTimestamp()};

  result = perfCollectorUpdateSystemMetrics(pCollector, &system_metrics);
  if (result == 0) {
    printf("✓ System metrics updated successfully\n");
  } else {
    printf("✗ Failed to update system metrics: %d\n", result);
  }

  // Retrieve and display system metrics
  SSystemPerfMetrics retrieved_system;
  result = perfCollectorGetSystemMetrics(pCollector, &retrieved_system);
  if (result == 0) {
    printf("✓ Retrieved system metrics:\n");
    printf("  CPU Usage: %ld%%\n", retrieved_system.cpu_usage_percent);
    printf("  Memory Usage: %ld bytes (%.2f GB)\n", retrieved_system.memory_usage_bytes,
           (double)retrieved_system.memory_usage_bytes / (1024.0 * 1024.0 * 1024.0));
    printf("  Active Connections: %ld\n", retrieved_system.active_connections);
  } else {
    printf("✗ Failed to retrieve system metrics: %d\n", result);
  }

  // Demonstrate cache metrics
  printf("\n--- Cache Metrics Demo ---\n");
  SCachePerfMetrics cache_metrics = {.cache_hits = 10000,
                                     .cache_misses = 1000,
                                     .cache_size_bytes = 64LL * 1024 * 1024,       // 64MB
                                     .cache_max_size_bytes = 128LL * 1024 * 1024,  // 128MB
                                     .hit_ratio = 0.909,
                                     .evict_count = 50,
                                     .timestamp = perfCollectorGetCurrentTimestamp()};

  result = perfCollectorUpdateCacheMetrics(pCollector, &cache_metrics);
  if (result == 0) {
    printf("✓ Cache metrics updated successfully\n");
  } else {
    printf("✗ Failed to update cache metrics: %d\n", result);
  }

  // Retrieve and display cache metrics
  SCachePerfMetrics retrieved_cache;
  result = perfCollectorGetCacheMetrics(pCollector, &retrieved_cache);
  if (result == 0) {
    printf("✓ Retrieved cache metrics:\n");
    printf("  Cache Hits: %ld\n", retrieved_cache.cache_hits);
    printf("  Cache Misses: %ld\n", retrieved_cache.cache_misses);
    printf("  Hit Ratio: %.3f\n", retrieved_cache.hit_ratio);
    printf("  Cache Size: %ld bytes (%.2f MB)\n", retrieved_cache.cache_size_bytes,
           (double)retrieved_cache.cache_size_bytes / (1024.0 * 1024.0));
  } else {
    printf("✗ Failed to retrieve cache metrics: %d\n", result);
  }

  // Demonstrate RPC metrics
  printf("\n--- RPC Metrics Demo ---\n");
  SRpcPerfMetrics rpc_metrics = {.total_requests = 10000,
                                 .success_requests = 9850,
                                 .failed_requests = 150,
                                 .total_time_us = 2000000,
                                 .avg_time_us = 200,
                                 .max_time_us = 5000,
                                 .min_time_us = 10,
                                 .bytes_sent = 50LL * 1024 * 1024,
                                 .bytes_received = 200LL * 1024 * 1024,
                                 .active_connections = 25,
                                 .timestamp = perfCollectorGetCurrentTimestamp()};

  result = perfCollectorUpdateRpcMetrics(pCollector, &rpc_metrics);
  if (result == 0) {
    printf("✓ RPC metrics updated successfully\n");
  } else {
    printf("✗ Failed to update RPC metrics: %d\n", result);
  }

  // Retrieve and display RPC metrics
  SRpcPerfMetrics retrieved_rpc;
  result = perfCollectorGetRpcMetrics(pCollector, &retrieved_rpc);
  if (result == 0) {
    printf("✓ Retrieved RPC metrics:\n");
    printf("  Total Requests: %ld\n", retrieved_rpc.total_requests);
    printf("  Success Rate: %.2f%%\n", (double)retrieved_rpc.success_requests / retrieved_rpc.total_requests * 100.0);
    printf("  Average Response Time: %ld μs\n", retrieved_rpc.avg_time_us);
    printf("  Active Connections: %d\n", retrieved_rpc.active_connections);
  } else {
    printf("✗ Failed to retrieve RPC metrics: %d\n", result);
  }

  // Demonstrate JSON export
  printf("\n--- JSON Export Demo ---\n");
  char *json_output = NULL;
  result = perfCollectorExportToJson(pCollector, &json_output);
  if (result == 0 && json_output != NULL) {
    printf("✓ Performance data exported to JSON:\n%s\n", json_output);
    free(json_output);
  } else {
    printf("✗ Failed to export to JSON: %d\n", result);
  }

  // Demonstrate metrics reset
  printf("\n--- Metrics Reset Demo ---\n");
  result = perfCollectorResetMetrics(pCollector);
  if (result == 0) {
    printf("✓ Metrics reset successfully\n");
  } else {
    printf("✗ Failed to reset metrics: %d\n", result);
  }

  // Cleanup
  perfCollectorCleanup(pCollector);
  printf("\n=== Demo completed successfully ===\n");
}

int main() {
  demonstrate_api_usage();
  return 0;
}