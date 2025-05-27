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
  EMetricType  type;
  EMetricLevel level;
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

typedef struct {
  int64_t rpcQueueMemoryAllowed;
  int64_t rpcQueueMemoryUsed;
  int64_t applyMemoryAllowed;
  int64_t applyMemoryUsed;
} SDnodeMetrics;

typedef struct {
  int64_t rpcQueueMemoryAllowed;
  int64_t rpcQueueMemoryUsed;
  int64_t applyMemoryAllowed;
  int64_t applyMemoryUsed;
} SRawDnodeMetrics;

typedef struct {
  SMetric rpcQueueMemoryAllowed;
  SMetric rpcQueueMemoryUsed;
  SMetric applyMemoryAllowed;
  SMetric applyMemoryUsed;
} SDnodeMetricsEx;

// Raw Write Metrics Structure (Primitive Types)
typedef struct {
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
  int64_t blocked_commits;
  int64_t merge_count;
  int64_t merge_time;
} SRawWriteMetrics;

// Write Metrics Extended Structure (Formatted)
typedef struct {
  int32_t vgId;
  int32_t dnodeId;
  SMetric total_requests;
  SMetric total_rows;
  SMetric total_bytes;
  SMetric fetch_batch_meta_time;
  SMetric fetch_batch_meta_count;
  SMetric preprocess_time;
  SMetric wal_write_bytes;
  SMetric wal_write_time;
  SMetric apply_bytes;
  SMetric apply_time;
  SMetric commit_count;
  SMetric commit_time;
  SMetric memtable_wait_time;
  SMetric blocked_commits;
  SMetric merge_count;
  SMetric merge_time;
} SWriteMetricsEx;

// Query Metrics Extended Structure
typedef struct {
} SQueryMetricsEx;

// Stream Metrics Extended Structure
typedef struct {
} SStreamMetricsEx;

// Metrics Manager Structure
typedef struct {
  SDnodeMetrics *pDnodeMetrics;
  SHashObj      *pWriteMetrics;
  SHashObj      *pQueryMetrics;
  SHashObj      *pStreamMetrics;
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
int32_t          addWriteMetrics(int32_t vgId, int32_t dnodeId, const SRawWriteMetrics *pRawMetrics);
SWriteMetricsEx *getWriteMetricsByVgId(int32_t vgId);

// Dnode metrics functions
void             initDnodeMetricsEx(SDnodeMetricsEx *pMetrics);
int32_t          addDnodeMetrics(const SRawDnodeMetrics *pRawMetrics);
SDnodeMetricsEx *getDnodeMetrics();

// Function type definition for vnode callbacks
typedef void *(*SVnodeMetricsLogFn)(void **pIter);
typedef int32_t (*VnodeGetRawMetricsFn)(void *pVnode, SRawWriteMetrics *pRawMetrics);
typedef int32_t (*MetricsLogCallback)(const char *jsonMetrics, void *param);

// Metrics logging functions
int32_t logAllVnodeMetrics(SVnodeMetricsLogFn fnGetVnode, VnodeGetRawMetricsFn fnGetRawMetrics);
void    resetAllVnodeMetrics();
int32_t formatMetricsToJson(int32_t vgId, char *buffer, int32_t bufferSize);
int32_t forEachMetric(MetricsLogCallback callback, void *param);
int32_t getAllMetricsJson(char **pJson);

void reportWriteMetrics();
void reportDnodeMetrics();
void sendAllMetricsReport();

#ifdef __cplusplus
}
#endif

#endif /*_TD_METRICS_H_*/
