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
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include "../inc/tmetrics.h"
#include "os.h"
#include "osTimezone.h"
#include "taoserror.h"
#include "tdef.h"
#include "tlog.h"
#include "tsched.h"
#include "tthread.h"
#include "ttime.h"
#include "ttimer.h"

#ifndef TSDB_CODE_FAILED_TO_CREATE_THREAD
#define TSDB_CODE_FAILED_TO_CREATE_THREAD TSDB_CODE_APP_ERROR
#endif

static SMetricsManager gMetricsManager = {0};
static tmr_h           gMetricsTimer = NULL;
static TdThread        gMetricsThread;
static bool            gMetricsRunning = false;
static int64_t         gLastCollectTime = 0;

static void  tmetrics_collect_metrics_callback(void* param, void* tmrId);
static void* tmetrics_async_writer_fn(void* arg);
static SWriteMetricsEx* tmetrics_get_or_init_vnode_metrics(int32_t vgId);

static void tmetrics_init_metric_def(SMetric* metric, const char* name, const char* description, EMetricType type,
                                     EMetricLevel level, EMetricCategory category, EMetricScope scope, const char* unit,
                                     int32_t vgId) {
  strcpy(metric->definition.name, name);
  strcpy(metric->definition.description, description);
  metric->definition.type = type;
  metric->definition.level = level;
  metric->definition.category = category;
  metric->definition.scope = scope;
  strcpy(metric->definition.unit, unit);
  metric->value.int_val = 0;
  metric->value.double_val = 0.0;
  metric->collect_time = 0;
  metric->vgId = vgId;
}

int32_t tmetrics_init() {
  if (gMetricsManager.is_initialized) {
    return TSDB_CODE_SUCCESS;
  }

  memset(&gMetricsManager, 0, sizeof(SMetricsManager));

  gMetricsManager.high_level_enabled = false;
  gMetricsManager.collection_interval = 5000;
  gMetricsManager.log_path = taosStrdup("metrics.log");

  gMetricsManager.write_metrics = taosMemoryCalloc(1, sizeof(SWriteMetricsEx));
  gMetricsManager.query_metrics = taosMemoryCalloc(1, sizeof(SQueryMetricsEx));
  gMetricsManager.stream_metrics = taosMemoryCalloc(1, sizeof(SStreamMetricsEx));

  if (gMetricsManager.write_metrics == NULL || gMetricsManager.query_metrics == NULL ||
      gMetricsManager.stream_metrics == NULL) {
    tmetrics_cleanup();
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  gMetricsManager.vnode_metrics_size = 1024;
  gMetricsManager.vnode_metrics = taosMemoryCalloc(gMetricsManager.vnode_metrics_size, sizeof(void*));
  if (gMetricsManager.vnode_metrics == NULL) {
    tmetrics_cleanup();
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  tmetrics_init_write_metrics((SWriteMetricsEx*)gMetricsManager.write_metrics, 0);
  tmetrics_init_query_metrics((SQueryMetricsEx*)gMetricsManager.query_metrics);
  tmetrics_init_stream_metrics((SStreamMetricsEx*)gMetricsManager.stream_metrics);

  void* tmrHandle = taosTmrInit(10, 100, 3000, "metrics-tmr");
  if (tmrHandle == NULL) {
    tmetrics_cleanup();
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  gMetricsRunning = true;
  if (taosThreadCreate(&gMetricsThread, NULL, tmetrics_async_writer_fn, NULL) != 0) {
    tmetrics_cleanup();
    return TSDB_CODE_FAILED_TO_CREATE_THREAD;
  }

  gMetricsTimer = taosTmrStart(tmetrics_collect_metrics_callback, gMetricsManager.collection_interval, NULL, tmrHandle);
  if (gMetricsTimer == NULL) {
    gMetricsRunning = false;
    taosThreadJoin(gMetricsThread, NULL);
    tmetrics_cleanup();
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  gMetricsManager.is_initialized = true;

  return TSDB_CODE_SUCCESS;
}

void tmetrics_cleanup() {
  if (gMetricsTimer != NULL) {
    taosTmrStop(gMetricsTimer);
    gMetricsTimer = NULL;
  }

  if (gMetricsRunning) {
    gMetricsRunning = false;
    taosThreadJoin(gMetricsThread, NULL);
  }

  if (gMetricsManager.write_metrics) {
    taosMemoryFree(gMetricsManager.write_metrics);
  }

  if (gMetricsManager.query_metrics) {
    taosMemoryFree(gMetricsManager.query_metrics);
  }

  if (gMetricsManager.stream_metrics) {
    taosMemoryFree(gMetricsManager.stream_metrics);
  }

  if (gMetricsManager.custom_metrics) {
    taosMemoryFree(gMetricsManager.custom_metrics);
  }

  if (gMetricsManager.vnode_metrics) {
    for (int32_t i = 0; i < gMetricsManager.vnode_metrics_size; i++) {
      if (gMetricsManager.vnode_metrics[i]) {
        taosMemoryFree(gMetricsManager.vnode_metrics[i]);
      }
    }
    taosMemoryFree(gMetricsManager.vnode_metrics);
  }

  if (gMetricsManager.log_path) {
    taosMemoryFree(gMetricsManager.log_path);
  }

  memset(&gMetricsManager, 0, sizeof(SMetricsManager));
}

static void tmetrics_collect_metrics_callback(void* param, void* tmrId) {
  if (!gMetricsManager.is_initialized) return;

  int64_t now = taosGetTimestampMs();
  if (now - gLastCollectTime < gMetricsManager.collection_interval) {
    return;
  }

  if (gMetricsManager.write_metrics) {
    tmetrics_collect_write_metrics((SWriteMetricsEx*)gMetricsManager.write_metrics);
  }

  if (gMetricsManager.query_metrics) {
    tmetrics_collect_query_metrics((SQueryMetricsEx*)gMetricsManager.query_metrics);
  }

  if (gMetricsManager.stream_metrics) {
    tmetrics_collect_stream_metrics((SStreamMetricsEx*)gMetricsManager.stream_metrics);
  }

  gLastCollectTime = now;
}

static void* tmetrics_async_writer_fn(void* arg) {
  int64_t       lastWriteTime = 0;
  const int64_t writeInterval = 1000;

  while (gMetricsRunning) {
    int64_t now = taosGetTimestampMs();

    if (now - lastWriteTime >= writeInterval && gMetricsManager.log_path != NULL) {
      if (gMetricsManager.write_metrics) {
        tmetrics_log_write_metrics((SWriteMetricsEx*)gMetricsManager.write_metrics, gMetricsManager.log_path);
      }

      if (gMetricsManager.query_metrics) {
        tmetrics_log_query_metrics((SQueryMetricsEx*)gMetricsManager.query_metrics, gMetricsManager.log_path);
      }

      if (gMetricsManager.stream_metrics) {
        tmetrics_log_stream_metrics((SStreamMetricsEx*)gMetricsManager.stream_metrics, gMetricsManager.log_path);
      }

      tmetrics_log_all_vnodes(gMetricsManager.log_path);

      lastWriteTime = now;
    }

    taosMsleep(100);
  }

  return NULL;
}

bool tmetrics_is_high_level_enabled() { return gMetricsManager.high_level_enabled; }

void tmetrics_enable_high_level(bool enable) { gMetricsManager.high_level_enabled = enable; }

int32_t tmetrics_set_collection_interval(int64_t interval_ms) {
  if (interval_ms <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  gMetricsManager.collection_interval = interval_ms;
  if (gMetricsTimer != NULL) {
    taosTmrReset(tmetrics_collect_metrics_callback, interval_ms, NULL, NULL, &gMetricsTimer);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tmetrics_set_log_path(const char* path) {
  if (path == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (gMetricsManager.log_path) {
    taosMemoryFree(gMetricsManager.log_path);
  }

  gMetricsManager.log_path = taosStrdup(path);
  if (gMetricsManager.log_path == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

SWriteMetricsEx* tmetrics_get_write_metrics() {
  if (!gMetricsManager.is_initialized) {
    return NULL;
  }
  return (SWriteMetricsEx*)gMetricsManager.write_metrics;
}

void tmetrics_init_write_metrics(SWriteMetricsEx* metrics, int32_t vgId) {
  if (metrics == NULL) return;

  tmetrics_init_metric_def(&metrics->total_requests, "total_requests", "Total number of write requests received",
                           METRIC_TYPE_INT64, METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "count",
                           vgId);
  tmetrics_init_metric_def(&metrics->total_rows, "total_rows", "Total number of rows written", METRIC_TYPE_INT64,
                           METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "rows", vgId);
  tmetrics_init_metric_def(&metrics->total_bytes, "total_bytes", "Total bytes of data written", METRIC_TYPE_INT64,
                           METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "bytes", vgId);
  tmetrics_init_metric_def(&metrics->avg_write_size, "avg_write_size", "Average size of SQL write operations",
                           METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE,
                           "bytes/request", vgId);
  tmetrics_init_metric_def(&metrics->cache_hit_ratio, "cache_hit_ratio", "Cache hit ratio during write operations",
                           METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE,
                           "ratio, 0-1", vgId);
  tmetrics_init_metric_def(&metrics->rpc_queue_wait, "rpc_queue_wait", "Average time spent waiting in RPC queue",
                           METRIC_TYPE_INT64, METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "µs", vgId);
  tmetrics_init_metric_def(&metrics->preprocess_time, "preprocess_time",
                           "Average time spent preprocessing write requests", METRIC_TYPE_INT64, METRIC_LEVEL_LOW,
                           METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "µs", vgId);
  tmetrics_init_metric_def(&metrics->wal_write_rate, "wal_write_rate", "WAL write rate", METRIC_TYPE_DOUBLE,
                           METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "bytes/s", vgId);
  tmetrics_init_metric_def(&metrics->sync_rate, "sync_rate", "Message sync rate", METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW,
                           METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "msgs/s", vgId);
  tmetrics_init_metric_def(&metrics->apply_rate, "apply_rate", "Message apply rate", METRIC_TYPE_DOUBLE,
                           METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "msgs/s", vgId);
  tmetrics_init_metric_def(&metrics->memory_table_size, "memory_table_size", "Current size of memory table",
                           METRIC_TYPE_INT64, METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "bytes",
                           vgId);
  tmetrics_init_metric_def(&metrics->memory_table_rows, "memory_table_rows", "Current rows in memory table",
                           METRIC_TYPE_INT64, METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "rows",
                           vgId);
  tmetrics_init_metric_def(&metrics->commit_count, "commit_count", "Total number of commit operations",
                           METRIC_TYPE_INT64, METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "count",
                           vgId);
  tmetrics_init_metric_def(&metrics->auto_commit_count, "auto_commit_count", "Total number of auto commit operations",
                           METRIC_TYPE_INT64, METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "count",
                           vgId);
  tmetrics_init_metric_def(&metrics->forced_commit_count, "forced_commit_count",
                           "Total number of forced commit operations", METRIC_TYPE_INT64, METRIC_LEVEL_LOW,
                           METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "count", vgId);
  tmetrics_init_metric_def(&metrics->stt_trigger_value, "stt_trigger_value", "Current value of stt_trigger",
                           METRIC_TYPE_INT64, METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "value",
                           vgId);
  tmetrics_init_metric_def(&metrics->merge_count, "merge_count", "Number of file merges triggered by stt_trigger",
                           METRIC_TYPE_INT64, METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "count",
                           vgId);
  tmetrics_init_metric_def(&metrics->avg_commit_time, "avg_commit_time", "Average time spent on commit operations",
                           METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "ms", vgId);
  tmetrics_init_metric_def(&metrics->avg_merge_time, "avg_merge_time", "Average time spent on merge operations",
                           METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW, METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "ms", vgId);
  tmetrics_init_metric_def(&metrics->blocked_commits, "blocked_commits",
                           "Number of commit operations blocked by ongoing merges", METRIC_TYPE_INT64, METRIC_LEVEL_LOW,
                           METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "count", vgId);
  tmetrics_init_metric_def(&metrics->memtable_wait_time, "memtable_wait_time",
                           "Average time spent waiting for memtable lock", METRIC_TYPE_INT64, METRIC_LEVEL_HIGH,
                           METRIC_CATEGORY_WRITE, METRIC_SCOPE_VNODE, "µs", vgId);
}

void tmetrics_collect_write_metrics(SWriteMetricsEx* metrics) {
  if (metrics == NULL) return;

  int64_t now = taosGetTimestampMs();

  if (tmetrics_is_high_level_enabled()) {
    metrics->memtable_wait_time.value.int_val = 180;
    metrics->memtable_wait_time.collect_time = now;

    metrics->cache_hit_ratio.value.double_val = 0.85;
    metrics->rpc_queue_wait.value.int_val = 12;
    metrics->preprocess_time.value.int_val = 5;
    metrics->wal_write_rate.value.double_val = 1250.0;
    metrics->sync_rate.value.double_val = 350.5;
    metrics->apply_rate.value.double_val = 320.8;
    metrics->memory_table_size.value.int_val = 1024 * 1024 * 8;
  }
}

static void tmetrics_log_metric(TdFilePtr fp, const SMetric* metric) {
  if (metric->definition.type == METRIC_TYPE_INT64) {
    taosFprintfFile(fp, "    %s: %" PRId64 " (%s)\n", metric->definition.name, metric->value.int_val,
                    metric->definition.unit);
  } else if (metric->definition.type == METRIC_TYPE_DOUBLE) {
    taosFprintfFile(fp, "    %s: %.2f (%s)\n", metric->definition.name, metric->value.double_val,
                    metric->definition.unit);
  } else if (metric->definition.type == METRIC_TYPE_STRING) {
    taosFprintfFile(fp, "    %s: %s (%s)\n", metric->definition.name,
                    metric->value.str_val ? metric->value.str_val : "", metric->definition.unit);
  }
}

void tmetrics_log_write_metrics(SWriteMetricsEx* metrics, const char* log_file) {
  if (metrics == NULL || log_file == NULL) return;

  TdFilePtr fp = taosOpenFile(log_file, TD_FILE_APPEND | TD_FILE_CREATE | TD_FILE_WRITE);
  if (!fp) return;

  char    timeStr[26];
  int64_t now = taosGetTimestamp(TSDB_TIME_PRECISION_MILLI);
  taosFormatUtcTime(timeStr, sizeof(timeStr), now, TSDB_TIME_PRECISION_MILLI);

  if (metrics->total_requests.vgId > 0) {
    taosFprintfFile(fp, "[%s] Write Performance Metrics for vnode %d:\n", timeStr, metrics->total_requests.vgId);
  } else {
    taosFprintfFile(fp, "[%s] Global Write Performance Metrics:\n", timeStr);
  }

  tmetrics_log_metric(fp, &metrics->total_requests);
  tmetrics_log_metric(fp, &metrics->total_rows);
  tmetrics_log_metric(fp, &metrics->total_bytes);
  tmetrics_log_metric(fp, &metrics->avg_write_size);
  tmetrics_log_metric(fp, &metrics->commit_count);
  tmetrics_log_metric(fp, &metrics->merge_count);
  tmetrics_log_metric(fp, &metrics->avg_commit_time);
  tmetrics_log_metric(fp, &metrics->avg_merge_time);
  tmetrics_log_metric(fp, &metrics->blocked_commits);
  tmetrics_log_metric(fp, &metrics->stt_trigger_value);

  if (tmetrics_is_high_level_enabled()) {
    tmetrics_log_metric(fp, &metrics->memtable_wait_time);
    tmetrics_log_metric(fp, &metrics->cache_hit_ratio);
    tmetrics_log_metric(fp, &metrics->rpc_queue_wait);
    tmetrics_log_metric(fp, &metrics->preprocess_time);
    tmetrics_log_metric(fp, &metrics->wal_write_rate);
    tmetrics_log_metric(fp, &metrics->sync_rate);
    tmetrics_log_metric(fp, &metrics->apply_rate);
    tmetrics_log_metric(fp, &metrics->memory_table_size);
  }

  taosFprintfFile(fp, "\n");
  taosCloseFile(&fp);
}

SQueryMetricsEx* tmetrics_get_query_metrics() {
  if (!gMetricsManager.is_initialized) {
    return NULL;
  }
  return (SQueryMetricsEx*)gMetricsManager.query_metrics;
}

void tmetrics_init_query_metrics(SQueryMetricsEx* metrics) {
  if (metrics == NULL) return;
  // TODO: Initialize query metrics definitions
}

void tmetrics_collect_query_metrics(SQueryMetricsEx* metrics) {
  if (metrics == NULL) return;
  // TODO: Implement query metrics collection
}

void tmetrics_log_query_metrics(SQueryMetricsEx* metrics, const char* log_file) {
  if (metrics == NULL) return;
  // TODO: Implement query metrics logging
}

SStreamMetricsEx* tmetrics_get_stream_metrics() {
  if (!gMetricsManager.is_initialized) {
    return NULL;
  }
  return (SStreamMetricsEx*)gMetricsManager.stream_metrics;
}

void tmetrics_init_stream_metrics(SStreamMetricsEx* metrics) {
  if (metrics == NULL) return;
  // TODO: Initialize stream metrics definitions
}

void tmetrics_collect_stream_metrics(SStreamMetricsEx* metrics) {
  if (metrics == NULL) return;
  // TODO: Implement stream metrics collection
}

void tmetrics_log_stream_metrics(SStreamMetricsEx* metrics, const char* log_file) {
  if (metrics == NULL) return;
  // TODO: Implement stream metrics logging
}

int32_t tmetrics_register_custom_metric(SMetricDef* def, void* collector) { return TSDB_CODE_SUCCESS; }

int32_t tmetrics_unregister_custom_metric(const char* name) { return TSDB_CODE_SUCCESS; }

void tmetrics_record_write_begin(SWriteMetricsEx* metrics, int32_t vgId, int32_t rows, int32_t bytes) {
  if (metrics == NULL) metrics = tmetrics_get_or_init_vnode_metrics(vgId);
  if (metrics == NULL) return;

  __sync_fetch_and_add(&metrics->total_requests.value.int_val, 1);
  __sync_fetch_and_add(&metrics->total_rows.value.int_val, rows);
  __sync_fetch_and_add(&metrics->total_bytes.value.int_val, bytes);

  metrics->total_requests.collect_time = taosGetTimestampMs();
}

void tmetrics_record_write_end(SWriteMetricsEx* metrics, int32_t vgId, int64_t start_time, bool is_commit) {
  if (metrics == NULL) metrics = tmetrics_get_or_init_vnode_metrics(vgId);
  if (metrics == NULL) return;

  int64_t end_time = taosGetTimestampMs();
  int64_t elapsed = end_time - start_time;

  if (metrics->total_requests.value.int_val > 0) {
    double old_avg_size = metrics->avg_write_size.value.double_val;
    double current_size = (double)metrics->total_bytes.value.int_val / metrics->total_requests.value.int_val;
    metrics->avg_write_size.value.double_val = old_avg_size * 0.9 + current_size * 0.1;
  }

  if (is_commit) {
    __sync_fetch_and_add(&metrics->commit_count.value.int_val, 1);
    double old_avg_commit = metrics->avg_commit_time.value.double_val;
    metrics->avg_commit_time.value.double_val = old_avg_commit * 0.9 + ((double)elapsed) * 0.1;
  }

  metrics->avg_write_size.collect_time = end_time;
  if (is_commit) {
    metrics->commit_count.collect_time = end_time;
    metrics->avg_commit_time.collect_time = end_time;
  }
}

void tmetrics_record_merge(SWriteMetricsEx* metrics, int32_t vgId, int64_t merge_time) {
  if (metrics == NULL) metrics = tmetrics_get_or_init_vnode_metrics(vgId);
  if (metrics == NULL) return;

  __sync_fetch_and_add(&metrics->merge_count.value.int_val, 1);

  double old_avg = metrics->avg_merge_time.value.double_val;
  metrics->avg_merge_time.value.double_val = old_avg * 0.9 + ((double)merge_time) * 0.1;

  metrics->merge_count.collect_time = taosGetTimestampMs();
  metrics->avg_merge_time.collect_time = metrics->merge_count.collect_time;
}

void tmetrics_record_blocked_commit(SWriteMetricsEx* metrics, int32_t vgId) {
  if (metrics == NULL) metrics = tmetrics_get_or_init_vnode_metrics(vgId);
  if (metrics == NULL) return;

  __sync_fetch_and_add(&metrics->blocked_commits.value.int_val, 1);
  metrics->blocked_commits.collect_time = taosGetTimestampMs();
}

void tmetrics_update_stt_trigger(SWriteMetricsEx* metrics, int32_t vgId, int32_t value) {
  if (metrics == NULL) metrics = tmetrics_get_or_init_vnode_metrics(vgId);
  if (metrics == NULL) return;

  metrics->stt_trigger_value.value.int_val = value;
  metrics->stt_trigger_value.collect_time = taosGetTimestampMs();
}

static SWriteMetricsEx* tmetrics_get_or_init_vnode_metrics(int32_t vgId) {
  if (!gMetricsManager.is_initialized || vgId < 0 || vgId >= gMetricsManager.vnode_metrics_size) {
    return NULL;
  }

  if (gMetricsManager.vnode_metrics[vgId] == NULL) {
    SWriteMetricsEx* new_metrics = (SWriteMetricsEx*)taosMemoryCalloc(1, sizeof(SWriteMetricsEx));
    if (new_metrics == NULL) {
      return NULL;
    }
    tmetrics_init_write_metrics(new_metrics, vgId);

    // Use atomic compare-and-swap to safely assign if still NULL
    if (!__sync_bool_compare_and_swap(&gMetricsManager.vnode_metrics[vgId], NULL, new_metrics)) {
      // Another thread initialized it in the meantime, free our allocation
      taosMemoryFree(new_metrics);
    }
  }

  return (SWriteMetricsEx*)gMetricsManager.vnode_metrics[vgId];
}

SWriteMetricsEx* tmetrics_get_vnode_write_metrics(int32_t vgId) { return tmetrics_get_or_init_vnode_metrics(vgId); }

void tmetrics_collect_all_vnodes() {
  if (!gMetricsManager.is_initialized) return;

  for (int32_t i = 0; i < gMetricsManager.vnode_metrics_size; i++) {
    if (gMetricsManager.vnode_metrics[i]) {
      SWriteMetricsEx* metrics = (SWriteMetricsEx*)gMetricsManager.vnode_metrics[i];
      metrics->total_requests.collect_time = taosGetTimestampMs();
    }
  }
}

void tmetrics_log_all_vnodes(const char* log_file) {
  if (!gMetricsManager.is_initialized || !log_file) return;

  for (int32_t i = 0; i < gMetricsManager.vnode_metrics_size; i++) {
    if (gMetricsManager.vnode_metrics[i]) {
      SWriteMetricsEx* metrics = (SWriteMetricsEx*)gMetricsManager.vnode_metrics[i];
      if (metrics->total_requests.value.int_val > 0) {
        tmetrics_log_write_metrics(metrics, log_file);
      }
    }
  }
}