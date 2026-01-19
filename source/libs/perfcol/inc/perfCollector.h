/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 */

#ifndef _TD_PERF_COLLECTOR_H
#define _TD_PERF_COLLECTOR_H

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#define TSDB_QUERY_SQL_LEN 1048576
#define TSDB_USER_LEN      128
#define TSDB_DB_NAME_LEN   65

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  PERF_TYPE_QUERY = 0,
  PERF_TYPE_WRITE = 1,
  PERF_TYPE_READ = 2,
  PERF_TYPE_SYSTEM = 3,
  PERF_TYPE_NETWORK = 4,
  PERF_TYPE_STORAGE = 5,
  PERF_TYPE_MEMORY = 6
} EPerfType;

typedef struct {
  char    sql[TSDB_QUERY_SQL_LEN];
  int64_t start_time;
  int64_t end_time;
  int64_t parse_time;
  int64_t plan_time;
  int64_t exec_time;
  int64_t total_rows;
  int64_t scanned_rows;
  int32_t affected_rows;
  int32_t error_code;
  char    user[TSDB_USER_LEN];
  char    db[TSDB_DB_NAME_LEN];
  int8_t  query_type;
  int8_t  priority;
} SQueryPerfMetrics;

typedef struct {
  int64_t cpu_usage_percent;
  int64_t memory_usage_bytes;
  int64_t disk_usage_bytes;
  int64_t network_in_bytes;
  int64_t network_out_bytes;
  int64_t disk_read_bytes;
  int64_t disk_write_bytes;
  int64_t active_connections;
  int64_t active_queries;
  int64_t queued_requests;
  int64_t timestamp;
} SSystemPerfMetrics;

typedef struct {
  int64_t cache_hits;
  int64_t cache_misses;
  int64_t cache_size_bytes;
  int64_t cache_max_size_bytes;
  double  hit_ratio;
  int64_t evict_count;
  int64_t timestamp;
} SCachePerfMetrics;

typedef struct {
  int64_t total_requests;
  int64_t success_requests;
  int64_t failed_requests;
  int64_t total_time_us;
  int64_t avg_time_us;
  int64_t max_time_us;
  int64_t min_time_us;
  int64_t bytes_sent;
  int64_t bytes_received;
  int32_t active_connections;
  int64_t timestamp;
} SRpcPerfMetrics;

typedef struct {
  bool    enable_query_metrics;
  bool    enable_system_metrics;
  bool    enable_cache_metrics;
  bool    enable_rpc_metrics;
  int32_t collection_interval_ms;
  int32_t max_retained_records;
  int32_t sampling_rate;
} SPerfCollectionConfig;

typedef struct {
  SPerfCollectionConfig config;
  SQueryPerfMetrics    *queryMetrics;
  int32_t               queryMetricsCount;
  int32_t               queryMetricsCapacity;
  SSystemPerfMetrics    systemMetrics;
  SCachePerfMetrics     cacheMetrics;
  SRpcPerfMetrics       rpcMetrics;
  pthread_mutex_t       mutex;
  int                   initialized;
} SPerfCollector;

int32_t perfCollectorInit(SPerfCollector **ppCollector, const SPerfCollectionConfig *pConfig);
void    perfCollectorCleanup(SPerfCollector *pCollector);

int32_t perfCollectorRecordQueryStart(SPerfCollector *pCollector, const char *sql, const char *user, const char *db,
                                      int8_t queryType, int8_t priority, int64_t startTime);
int32_t perfCollectorRecordQueryEnd(SPerfCollector *pCollector, const char *sql, int64_t endTime, int32_t affectedRows,
                                    int32_t errorCode);

int32_t perfCollectorUpdateSystemMetrics(SPerfCollector *pCollector, const SSystemPerfMetrics *pMetrics);
int32_t perfCollectorUpdateCacheMetrics(SPerfCollector *pCollector, const SCachePerfMetrics *pMetrics);
int32_t perfCollectorUpdateRpcMetrics(SPerfCollector *pCollector, const SRpcPerfMetrics *pMetrics);

int32_t perfCollectorGetQueryMetrics(SPerfCollector *pCollector, SQueryPerfMetrics **ppMetrics, int32_t *pCount);
int32_t perfCollectorGetSystemMetrics(SPerfCollector *pCollector, SSystemPerfMetrics *pMetrics);
int32_t perfCollectorGetCacheMetrics(SPerfCollector *pCollector, SCachePerfMetrics *pMetrics);
int32_t perfCollectorGetRpcMetrics(SPerfCollector *pCollector, SRpcPerfMetrics *pMetrics);

int64_t     perfCollectorGetCurrentTimestamp(void);
const char *perfCollectorGetVersion(void);
int32_t     perfCollectorGetHealthStatus(SPerfCollector *pCollector);

int32_t perfCollectorExportToJson(SPerfCollector *pCollector, char **ppJsonStr);
int32_t perfCollectorResetMetrics(SPerfCollector *pCollector);

#ifdef __cplusplus
}
#endif

#endif /* _TD_PERF_COLLECTOR_H */