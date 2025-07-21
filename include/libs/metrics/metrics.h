/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _TD_METRICS_H
#define _TD_METRICS_H

#include "tdef.h"
#include "thash.h"

#ifdef __cplusplus
extern "C" {
#endif

// Metric collection level
typedef enum {
  METRIC_LEVEL_LOW = 0,  // Disabled by default, can be enabled for debugging
  METRIC_LEVEL_HIGH = 1  // Always collected
} EMetricLevel;

// Raw Dnode Metrics Structure (Input data)
typedef struct {
  int64_t rpcQueueMemoryAllowed;
  int64_t rpcQueueMemoryUsed;
  int64_t applyMemoryAllowed;
  int64_t applyMemoryUsed;
} SRawDnodeMetrics;

// Raw Write Metrics Structure (Input data)
typedef struct {
  char    dbname[TSDB_DB_NAME_LEN];  // Database name
  int64_t total_requests;
  int64_t total_rows;
  int64_t total_bytes;
  int64_t fetch_batch_meta_time;
  int64_t fetch_batch_meta_count;
  int64_t preprocess_time;
  int64_t wal_write_bytes;
  int64_t wal_write_time;
  int64_t apply_bytes;
  int64_t apply_time;
  int64_t commit_count;
  int64_t commit_time;
  int64_t memtable_wait_time;
  int64_t blocked_commit_count;
  int64_t blocked_commit_time;
  int64_t merge_count;
  int64_t merge_time;
  int64_t last_cache_commit_time;
  int64_t last_cache_commit_count;
} SRawWriteMetrics;

// Public API functions
int32_t initMetricsManager();
void    cleanupMetrics();

// Write metrics functions
int32_t addWriteMetrics(int32_t vgId, int32_t dnodeId, int64_t clusterId, const char *dnodeEp, const char *dbname,
                        const SRawWriteMetrics *pRawMetrics);

// Dnode metrics functions
int32_t addDnodeMetrics(const SRawDnodeMetrics *pRawMetrics, int64_t clusterId, int32_t dnodeId, const char *dnodeEp);

// Clean expired metrics based on valid vgroups (similar to vmCleanExpriedSamples)
int32_t cleanupExpiredMetrics(SHashObj *pValidVgroups);

// Metrics collection control macro with priority
// Only collect metrics when monitoring is disabled or not properly configured
// priority: 0 = detailed level (only when tsMetricsFlag=1), 1 = high level (always collected)
#define METRICS_UPDATE(field, priority, value)                                                 \
  do {                                                                                         \
    if ((tsEnableMonitor && tsMonitorFqdn[0] != 0 && tsMonitorPort != 0 && tsEnableMetrics) && \
        ((priority) == METRIC_LEVEL_HIGH || tsMetricsLevel == 1)) {                             \
      (void)atomic_add_fetch_64(&(field), (value));                                                  \
    }                                                                                          \
  } while (0)

// Timing block macro with priority - wraps a code block with timing
// priority: 0 = high level (always collected), 1 = detailed level (only when tsMetricsFlag=1)
#define METRICS_TIMING_BLOCK(field, priority, code_block)                                      \
  do {                                                                                         \
    if ((tsEnableMonitor && tsMonitorFqdn[0] != 0 && tsMonitorPort != 0 && tsEnableMetrics) && \
        ((priority) == METRIC_LEVEL_HIGH || tsMetricsLevel == 1)) {                             \
      int64_t start_time = taosGetTimestampMs();                                               \
      code_block;                                                                              \
      int64_t end_time = taosGetTimestampMs();                                                 \
      (void)atomic_add_fetch_64(&(field), end_time - start_time);                                    \
    } else {                                                                                   \
      code_block;                                                                              \
    }                                                                                          \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_METRICS_H_*/
