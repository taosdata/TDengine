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

#include "tarray.h"
#include "tdef.h"
#include "tlog.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum { METRIC_TYPE_INT64 = 1, METRIC_TYPE_DOUBLE = 2, METRIC_TYPE_STRING = 3 } EMetricType;

// Metric collection level
typedef enum {
  METRIC_LEVEL_LOW = 1,     // Always collected
  METRIC_LEVEL_MEDIUM = 2,  // Collected by default but can be disabled
  METRIC_LEVEL_HIGH = 3     // Disabled by default, can be enabled for debugging
} EMetricLevel;

// Metric categories
typedef enum {
  METRIC_CATEGORY_WRITE = 1,
  METRIC_CATEGORY_QUERY = 2,
  METRIC_CATEGORY_STREAM = 3,
  METRIC_CATEGORY_SYSTEM = 4,
  METRIC_CATEGORY_CUSTOM = 5
} EMetricCategory;

// Metric scope
typedef enum {
  METRIC_SCOPE_SYSTEM = 1,    // System-wide metric
  METRIC_SCOPE_DNODE = 2,     // Dnode-level metric
  METRIC_SCOPE_MNODE = 3,     // Mnode-level metric
  METRIC_SCOPE_VNODE = 4,     // Vnode-level metric
  METRIC_SCOPE_DATABASE = 5,  // Database-level metric
  METRIC_SCOPE_TABLE = 6      // Table-level metric
} EMetricScope;

// Metric definition structure
typedef struct {
  char            name[32];
  char            description[128];
  EMetricType     type;
  EMetricLevel    level;
  EMetricCategory category;
  EMetricScope    scope;
  char            unit[16];
} SMetricDef;

// Metric value union
typedef union {
  int64_t int_val;
  double  double_val;
  char   *str_val;
} SMetricValue;

// Metric instance structure
typedef struct {
  SMetricDef   definition;
  SMetricValue value;
  int64_t      collect_time;
  int32_t      vgId;  // vnode group ID
} SMetric;

// Write Metrics Extended Structure
typedef struct {
  SMetric total_requests;
  SMetric total_rows;
  SMetric total_bytes;
  SMetric avg_write_size;
  SMetric cache_hit_ratio;
  SMetric rpc_queue_wait;
  SMetric preprocess_time;
  SMetric wal_write_rate;
  SMetric sync_rate;
  SMetric apply_rate;
  SMetric memory_table_size;
  SMetric memory_table_rows;
  SMetric commit_count;
  SMetric auto_commit_count;
  SMetric forced_commit_count;
  SMetric stt_trigger_value;
  SMetric merge_count;
  SMetric avg_commit_time;
  SMetric avg_merge_time;
  SMetric blocked_commits;
  SMetric memtable_wait_time;
} SWriteMetricsEx;

// Query Metrics Extended Structure
typedef struct {
  SMetric total_queries;
  SMetric avg_query_time;
  SMetric slow_queries;
  SMetric query_errors;
  SMetric rows_scanned;
  SMetric rows_returned;
  SMetric data_scanned;
  SMetric cache_hits;
  SMetric cache_misses;
  SMetric index_read_time;
  SMetric data_read_time;
  SMetric executor_time;
  SMetric result_send_time;
  SMetric query_queue_wait;
  SMetric concurrent_queries;
} SQueryMetricsEx;

// Stream Metrics Extended Structure
typedef struct {
  SMetric total_streams;
  SMetric active_streams;
  SMetric stream_errors;
  SMetric rows_processed;
  SMetric avg_processing_time;
  SMetric stream_pauses;
  SMetric stream_resumes;
  SMetric stream_memory_usage;
} SStreamMetricsEx;

// Metrics Manager Structure
typedef struct {
  bool    is_initialized;
  bool    high_level_enabled;
  int64_t collection_interval;  // in milliseconds
  char   *log_path;             // Path to log metrics to
  void   *write_metrics;        // Global write metrics
  void   *query_metrics;        // Global query metrics
  void   *stream_metrics;       // Global stream metrics
  void   *custom_metrics;       // Global custom metrics
  void  **vnode_metrics;        // Array of per-vnode metrics [vgId => metrics]
  int32_t vnode_metrics_size;   // Size of vnode_metrics array
} SMetricsManager;

#ifdef __cplusplus
}
#endif

#endif /*_TD_METRICS_H_*/
