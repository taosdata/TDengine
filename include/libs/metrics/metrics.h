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
  METRIC_LEVEL_LOW = 1,  // Always collected
  METRIC_LEVEL_HIGH = 2  // Disabled by default, can be enabled for debugging
} EMetricLevel;

// Metric definition structure
typedef struct {
  EMetricType     type;
  EMetricLevel    level;
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
} SMetric;

// Write Metrics Extended Structure
typedef struct {
  int32_t vgId;
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
} SQueryMetricsEx;

// Stream Metrics Extended Structure
typedef struct {
} SStreamMetricsEx;

// Metrics Manager Structure
typedef struct {
  SArray   *pDnodeMetrics;
  SHashObj *pWriteMetrics;
  SHashObj *pQueryMetrics;
  SHashObj *pStreamMetrics;
} SMetricsManager;

// Metrics management functions
int32_t initMetricsManager();
void    cleanupMetrics();

// Metric manipulation functions
void        initMetric(SMetric *pMetric, EMetricType type, EMetricLevel level);
void        setMetricInt64(SMetric *pMetric, int64_t value);
void        setMetricDouble(SMetric *pMetric, double value);
void        setMetricString(SMetric *pMetric, const char *value);
int64_t     getMetricInt64(const SMetric *pMetric);
double      getMetricDouble(const SMetric *pMetric);
const char *getMetricString(const SMetric *pMetric);

// Write metrics functions
void             initWriteMetricsEx(SWriteMetricsEx *pMetrics);
int32_t          addWriteMetrics(SWriteMetricsEx *pMetrics);
SWriteMetricsEx *getWriteMetricsByVgId(int32_t vgId);

// Function type definition for vnode callbacks
typedef void *(*SVnodeMetricsLogFn)(void **pIter);
typedef int32_t (*VnodeGetMetricsFn)(void *pVnode, SWriteMetricsEx *pMetrics);
typedef int32_t (*MetricsLogCallback)(const char *jsonMetrics, void *param);

// Metrics logging functions
int32_t logAllVnodeMetrics(SVnodeMetricsLogFn fnGetVnode, VnodeGetMetricsFn fnGetMetrics);
void    resetAllVnodeMetrics();
int32_t formatMetricsToJson(int32_t vgId, char *buffer, int32_t bufferSize);
int32_t forEachMetric(MetricsLogCallback callback, void *param);
int32_t getAllMetricsJson(char **pJson);

#ifdef __cplusplus
}
#endif

#endif /*_TD_METRICS_H_*/
