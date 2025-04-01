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

// Error code definition if not available
#ifndef TSDB_CODE_FAILED_TO_CREATE_THREAD
#define TSDB_CODE_FAILED_TO_CREATE_THREAD TSDB_CODE_APP_ERROR
#endif

// Global metrics manager instance
static SMetricsManager gMetricsManager = {0};
static tmr_h           gMetricsTimer = NULL;
static TdThread        gMetricsThread;
static bool            gMetricsRunning = false;
static int64_t         gLastCollectTime = 0;

static void  tmetrics_collect_metrics_callback(void* param, void* tmrId);
static void* tmetrics_async_writer_fn(void* arg);

int32_t tmetrics_init() {
  if (gMetricsManager.is_initialized) {
    return TSDB_CODE_SUCCESS;
  }

  memset(&gMetricsManager, 0, sizeof(SMetricsManager));

  gMetricsManager.high_level_enabled = false;
  gMetricsManager.collection_interval = 5000;  // Default 5 seconds
  gMetricsManager.log_path = taosStrdup("metrics.log");

  // Initialize global metrics
  gMetricsManager.write_metrics = taosMemoryCalloc(1, sizeof(SWriteMetricsEx));
  gMetricsManager.query_metrics = taosMemoryCalloc(1, sizeof(SQueryMetricsEx));
  gMetricsManager.stream_metrics = taosMemoryCalloc(1, sizeof(SStreamMetricsEx));

  if (gMetricsManager.write_metrics == NULL || gMetricsManager.query_metrics == NULL ||
      gMetricsManager.stream_metrics == NULL) {
    tmetrics_cleanup();
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // Initialize vnode metrics array with pre-allocated size
  gMetricsManager.vnode_metrics_size = 1024;  // Max 1024 vnodes
  gMetricsManager.vnode_metrics = taosMemoryCalloc(gMetricsManager.vnode_metrics_size, sizeof(void*));
  if (gMetricsManager.vnode_metrics == NULL) {
    tmetrics_cleanup();
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  tmetrics_init_write_metrics((SWriteMetricsEx*)gMetricsManager.write_metrics);
  tmetrics_init_query_metrics((SQueryMetricsEx*)gMetricsManager.query_metrics);
  tmetrics_init_stream_metrics((SStreamMetricsEx*)gMetricsManager.stream_metrics);

  // Initialize metrics timer
  void* tmrHandle = taosTmrInit(10, 100, 3000, "metrics-tmr");
  if (tmrHandle == NULL) {
    tmetrics_cleanup();
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // Start the metrics background thread
  gMetricsRunning = true;
  if (taosThreadCreate(&gMetricsThread, NULL, tmetrics_async_writer_fn, NULL) != 0) {
    tmetrics_cleanup();
    return TSDB_CODE_FAILED_TO_CREATE_THREAD;
  }

  // Start the metrics collection timer
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
  // Stop timer
  if (gMetricsTimer != NULL) {
    taosTmrStop(gMetricsTimer);
    gMetricsTimer = NULL;
  }

  // Stop background thread
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

// Callback for the timer to collect metrics
static void tmetrics_collect_metrics_callback(void* param, void* tmrId) {
  if (!gMetricsManager.is_initialized) return;

  int64_t now = taosGetTimestampMs();
  if (now - gLastCollectTime < gMetricsManager.collection_interval) {
    return;  // Avoid collecting too frequently
  }

  // Collect metrics into the shared structures
  // This is a lightweight operation that just copies data
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

// Background thread to asynchronously write metrics to log file
static void* tmetrics_async_writer_fn(void* arg) {
  int64_t       lastWriteTime = 0;
  const int64_t writeInterval = 1000;  // Write at most once per second

  while (gMetricsRunning) {
    int64_t now = taosGetTimestampMs();

    if (now - lastWriteTime >= writeInterval && gMetricsManager.log_path != NULL) {
      // Write metrics to log file only if significant time has passed
      // This avoids excessive disk I/O
      if (gMetricsManager.write_metrics) {
        tmetrics_log_write_metrics((SWriteMetricsEx*)gMetricsManager.write_metrics, gMetricsManager.log_path);
      }

      if (gMetricsManager.query_metrics) {
        tmetrics_log_query_metrics((SQueryMetricsEx*)gMetricsManager.query_metrics, gMetricsManager.log_path);
      }

      if (gMetricsManager.stream_metrics) {
        tmetrics_log_stream_metrics((SStreamMetricsEx*)gMetricsManager.stream_metrics, gMetricsManager.log_path);
      }

      lastWriteTime = now;
    }

    // Sleep to avoid busy waiting
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
  // Update timer if it's running
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

void tmetrics_init_write_metrics(SWriteMetricsEx* metrics) {
  if (metrics == NULL) return;

  strcpy(metrics->total_requests.definition.name, "total_requests");
  strcpy(metrics->total_requests.definition.description, "Total number of write requests received");
  metrics->total_requests.definition.type = METRIC_TYPE_INT64;
  metrics->total_requests.definition.level = METRIC_LEVEL_LOW;
  metrics->total_requests.definition.category = METRIC_CATEGORY_WRITE;
  metrics->total_requests.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->total_requests.definition.unit, "count");

  strcpy(metrics->total_rows.definition.name, "total_rows");
  strcpy(metrics->total_rows.definition.description, "Total number of rows written");
  metrics->total_rows.definition.type = METRIC_TYPE_INT64;
  metrics->total_rows.definition.level = METRIC_LEVEL_LOW;
  metrics->total_rows.definition.category = METRIC_CATEGORY_WRITE;
  metrics->total_rows.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->total_rows.definition.unit, "rows");

  strcpy(metrics->total_bytes.definition.name, "total_bytes");
  strcpy(metrics->total_bytes.definition.description, "Total bytes of data written");
  metrics->total_bytes.definition.type = METRIC_TYPE_INT64;
  metrics->total_bytes.definition.level = METRIC_LEVEL_LOW;
  metrics->total_bytes.definition.category = METRIC_CATEGORY_WRITE;
  metrics->total_bytes.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->total_bytes.definition.unit, "bytes");

  strcpy(metrics->avg_write_size.definition.name, "avg_write_size");
  strcpy(metrics->avg_write_size.definition.description, "Average size of SQL write operations");
  metrics->avg_write_size.definition.type = METRIC_TYPE_DOUBLE;
  metrics->avg_write_size.definition.level = METRIC_LEVEL_LOW;
  metrics->avg_write_size.definition.category = METRIC_CATEGORY_WRITE;
  metrics->avg_write_size.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->avg_write_size.definition.unit, "bytes/request");

  strcpy(metrics->cache_hit_ratio.definition.name, "cache_hit_ratio");
  strcpy(metrics->cache_hit_ratio.definition.description, "Cache hit ratio during write operations");
  metrics->cache_hit_ratio.definition.type = METRIC_TYPE_DOUBLE;
  metrics->cache_hit_ratio.definition.level = METRIC_LEVEL_LOW;
  metrics->cache_hit_ratio.definition.category = METRIC_CATEGORY_WRITE;
  metrics->cache_hit_ratio.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->cache_hit_ratio.definition.unit, "ratio, 0-1");

  strcpy(metrics->rpc_queue_wait.definition.name, "rpc_queue_wait");
  strcpy(metrics->rpc_queue_wait.definition.description, "Average time spent waiting in RPC queue");
  metrics->rpc_queue_wait.definition.type = METRIC_TYPE_INT64;
  metrics->rpc_queue_wait.definition.level = METRIC_LEVEL_LOW;
  metrics->rpc_queue_wait.definition.category = METRIC_CATEGORY_WRITE;
  metrics->rpc_queue_wait.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->rpc_queue_wait.definition.unit, "µs");

  strcpy(metrics->preprocess_time.definition.name, "preprocess_time");
  strcpy(metrics->preprocess_time.definition.description, "Average time spent preprocessing write requests");
  metrics->preprocess_time.definition.type = METRIC_TYPE_INT64;
  metrics->preprocess_time.definition.level = METRIC_LEVEL_LOW;
  metrics->preprocess_time.definition.category = METRIC_CATEGORY_WRITE;
  metrics->preprocess_time.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->preprocess_time.definition.unit, "µs");

  strcpy(metrics->wal_write_rate.definition.name, "wal_write_rate");
  strcpy(metrics->wal_write_rate.definition.description, "WAL write rate");
  metrics->wal_write_rate.definition.type = METRIC_TYPE_DOUBLE;
  metrics->wal_write_rate.definition.level = METRIC_LEVEL_LOW;
  metrics->wal_write_rate.definition.category = METRIC_CATEGORY_WRITE;
  metrics->wal_write_rate.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->wal_write_rate.definition.unit, "bytes/s");

  strcpy(metrics->sync_rate.definition.name, "sync_rate");
  strcpy(metrics->sync_rate.definition.description, "Message sync rate");
  metrics->sync_rate.definition.type = METRIC_TYPE_DOUBLE;
  metrics->sync_rate.definition.level = METRIC_LEVEL_LOW;
  metrics->sync_rate.definition.category = METRIC_CATEGORY_WRITE;
  metrics->sync_rate.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->sync_rate.definition.unit, "msgs/s");

  strcpy(metrics->apply_rate.definition.name, "apply_rate");
  strcpy(metrics->apply_rate.definition.description, "Message apply rate");
  metrics->apply_rate.definition.type = METRIC_TYPE_DOUBLE;
  metrics->apply_rate.definition.level = METRIC_LEVEL_LOW;
  metrics->apply_rate.definition.category = METRIC_CATEGORY_WRITE;
  metrics->apply_rate.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->apply_rate.definition.unit, "msgs/s");

  strcpy(metrics->memory_table_size.definition.name, "memory_table_size");
  strcpy(metrics->memory_table_size.definition.description, "Current size of memory table");
  metrics->memory_table_size.definition.type = METRIC_TYPE_INT64;
  metrics->memory_table_size.definition.level = METRIC_LEVEL_LOW;
  metrics->memory_table_size.definition.category = METRIC_CATEGORY_WRITE;
  metrics->memory_table_size.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->memory_table_size.definition.unit, "bytes");

  strcpy(metrics->commit_count.definition.name, "commit_count");
  strcpy(metrics->commit_count.definition.description, "Total number of commit operations");
  metrics->commit_count.definition.type = METRIC_TYPE_INT64;
  metrics->commit_count.definition.level = METRIC_LEVEL_LOW;
  metrics->commit_count.definition.category = METRIC_CATEGORY_WRITE;
  metrics->commit_count.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->commit_count.definition.unit, "count");

  strcpy(metrics->merge_count.definition.name, "merge_count");
  strcpy(metrics->merge_count.definition.description, "Number of file merges triggered by stt_trigger");
  metrics->merge_count.definition.type = METRIC_TYPE_INT64;
  metrics->merge_count.definition.level = METRIC_LEVEL_LOW;
  metrics->merge_count.definition.category = METRIC_CATEGORY_WRITE;
  metrics->merge_count.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->merge_count.definition.unit, "count");

  strcpy(metrics->avg_commit_time.definition.name, "avg_commit_time");
  strcpy(metrics->avg_commit_time.definition.description, "Average time spent on commit operations");
  metrics->avg_commit_time.definition.type = METRIC_TYPE_DOUBLE;
  metrics->avg_commit_time.definition.level = METRIC_LEVEL_LOW;
  metrics->avg_commit_time.definition.category = METRIC_CATEGORY_WRITE;
  metrics->avg_commit_time.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->avg_commit_time.definition.unit, "ms");

  strcpy(metrics->avg_merge_time.definition.name, "avg_merge_time");
  strcpy(metrics->avg_merge_time.definition.description, "Average time spent on merge operations");
  metrics->avg_merge_time.definition.type = METRIC_TYPE_DOUBLE;
  metrics->avg_merge_time.definition.level = METRIC_LEVEL_LOW;
  metrics->avg_merge_time.definition.category = METRIC_CATEGORY_WRITE;
  metrics->avg_merge_time.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->avg_merge_time.definition.unit, "ms");

  strcpy(metrics->blocked_commits.definition.name, "blocked_commits");
  strcpy(metrics->blocked_commits.definition.description, "Number of commit operations blocked by ongoing merges");
  metrics->blocked_commits.definition.type = METRIC_TYPE_INT64;
  metrics->blocked_commits.definition.level = METRIC_LEVEL_LOW;
  metrics->blocked_commits.definition.category = METRIC_CATEGORY_WRITE;
  metrics->blocked_commits.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->blocked_commits.definition.unit, "count");

  strcpy(metrics->memtable_wait_time.definition.name, "memtable_wait_time");
  strcpy(metrics->memtable_wait_time.definition.description, "Average time spent waiting for memtable lock");
  metrics->memtable_wait_time.definition.type = METRIC_TYPE_INT64;
  metrics->memtable_wait_time.definition.level = METRIC_LEVEL_HIGH;
  metrics->memtable_wait_time.definition.category = METRIC_CATEGORY_WRITE;
  metrics->memtable_wait_time.definition.scope = METRIC_SCOPE_VNODE;
  strcpy(metrics->memtable_wait_time.definition.unit, "µs");
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
    metrics->memory_table_size.value.int_val = 1024 * 1024 * 8;  // 8MB
  }
}

void tmetrics_log_write_metrics(SWriteMetricsEx* metrics, const char* log_file) {
  if (metrics == NULL) return;

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

  taosFprintfFile(fp, "    %s: %" PRId64 " (%s)\n", metrics->total_requests.definition.name,
                  metrics->total_requests.value.int_val, metrics->total_requests.definition.unit);

  taosFprintfFile(fp, "    %s: %" PRId64 " (%s)\n", metrics->total_rows.definition.name,
                  metrics->total_rows.value.int_val, metrics->total_rows.definition.unit);

  taosFprintfFile(fp, "    %s: %" PRId64 " (%s)\n", metrics->total_bytes.definition.name,
                  metrics->total_bytes.value.int_val, metrics->total_bytes.definition.unit);

  taosFprintfFile(fp, "    %s: %.2f (%s)\n", metrics->avg_write_size.definition.name,
                  metrics->avg_write_size.value.double_val, metrics->avg_write_size.definition.unit);

  // 添加更多相关指标
  taosFprintfFile(fp, "    %s: %" PRId64 " (%s)\n", metrics->commit_count.definition.name,
                  metrics->commit_count.value.int_val, metrics->commit_count.definition.unit);

  taosFprintfFile(fp, "    %s: %" PRId64 " (%s)\n", metrics->merge_count.definition.name,
                  metrics->merge_count.value.int_val, metrics->merge_count.definition.unit);

  taosFprintfFile(fp, "    %s: %.2f (%s)\n", metrics->avg_commit_time.definition.name,
                  metrics->avg_commit_time.value.double_val, metrics->avg_commit_time.definition.unit);

  taosFprintfFile(fp, "    %s: %.2f (%s)\n", metrics->avg_merge_time.definition.name,
                  metrics->avg_merge_time.value.double_val, metrics->avg_merge_time.definition.unit);

  taosFprintfFile(fp, "    %s: %" PRId64 " (%s)\n", metrics->blocked_commits.definition.name,
                  metrics->blocked_commits.value.int_val, metrics->blocked_commits.definition.unit);

  taosFprintfFile(fp, "    %s: %" PRId64 " (%s)\n", metrics->stt_trigger_value.definition.name,
                  metrics->stt_trigger_value.value.int_val, "value");

  if (tmetrics_is_high_level_enabled()) {
    taosFprintfFile(fp, "    %s: %" PRId64 " (%s)\n", metrics->memtable_wait_time.definition.name,
                    metrics->memtable_wait_time.value.int_val, metrics->memtable_wait_time.definition.unit);
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
}

void tmetrics_collect_query_metrics(SQueryMetricsEx* metrics) {
  if (metrics == NULL) return;
}

void tmetrics_log_query_metrics(SQueryMetricsEx* metrics, const char* log_file) {
  if (metrics == NULL) return;
}

SStreamMetricsEx* tmetrics_get_stream_metrics() {
  if (!gMetricsManager.is_initialized) {
    return NULL;
  }

  return (SStreamMetricsEx*)gMetricsManager.stream_metrics;
}

void tmetrics_init_stream_metrics(SStreamMetricsEx* metrics) {
  if (metrics == NULL) return;
}

void tmetrics_collect_stream_metrics(SStreamMetricsEx* metrics) {
  if (metrics == NULL) return;
}

void tmetrics_log_stream_metrics(SStreamMetricsEx* metrics, const char* log_file) {
  if (metrics == NULL) return;
}

int32_t tmetrics_register_custom_metric(SMetricDef* def, void* collector) { return TSDB_CODE_SUCCESS; }

int32_t tmetrics_unregister_custom_metric(const char* name) { return TSDB_CODE_SUCCESS; }

// Record write operation start with minimal overhead
void tmetrics_record_write_begin(SWriteMetricsEx* metrics, int32_t vgId, int32_t rows, int32_t bytes) {
  if (metrics == NULL) {
    metrics = tmetrics_get_vnode_write_metrics(vgId);
    if (metrics == NULL) return;
  }

  // Update counters atomically
  __sync_fetch_and_add(&metrics->total_requests.value.int_val, 1);
  __sync_fetch_and_add(&metrics->total_rows.value.int_val, rows);
  __sync_fetch_and_add(&metrics->total_bytes.value.int_val, bytes);

  metrics->total_requests.collect_time = taosGetTimestampMs();
}

// Record write operation end and calculate duration
void tmetrics_record_write_end(SWriteMetricsEx* metrics, int32_t vgId, int64_t start_time, bool is_commit) {
  if (metrics == NULL) {
    metrics = tmetrics_get_vnode_write_metrics(vgId);
    if (metrics == NULL) return;
  }

  int64_t end_time = taosGetTimestampMs();
  int64_t elapsed = end_time - start_time;

  // Update average write size using sliding window
  if (metrics->total_requests.value.int_val > 0) {
    double old_avg = metrics->avg_write_size.value.double_val;
    double new_avg =
        old_avg * 0.9 + ((double)metrics->total_bytes.value.int_val / metrics->total_requests.value.int_val) * 0.1;
    metrics->avg_write_size.value.double_val = new_avg;
  }

  if (is_commit) {
    __sync_fetch_and_add(&metrics->commit_count.value.int_val, 1);

    // Update average commit time using sliding window
    double old_avg = metrics->avg_commit_time.value.double_val;
    double new_avg = old_avg * 0.9 + ((double)elapsed) * 0.1;
    metrics->avg_commit_time.value.double_val = new_avg;
  }

  metrics->avg_write_size.collect_time = end_time;
}

// Record file merge operation
void tmetrics_record_merge(SWriteMetricsEx* metrics, int32_t vgId, int64_t merge_time) {
  if (metrics == NULL) {
    metrics = tmetrics_get_vnode_write_metrics(vgId);
    if (metrics == NULL) return;
  }

  __sync_fetch_and_add(&metrics->merge_count.value.int_val, 1);

  double old_avg = metrics->avg_merge_time.value.double_val;
  double new_avg = old_avg * 0.9 + ((double)merge_time) * 0.1;
  metrics->avg_merge_time.value.double_val = new_avg;

  metrics->merge_count.collect_time = taosGetTimestampMs();
}

// Record blocked commit
void tmetrics_record_blocked_commit(SWriteMetricsEx* metrics, int32_t vgId) {
  if (metrics == NULL) {
    metrics = tmetrics_get_vnode_write_metrics(vgId);
    if (metrics == NULL) return;
  }

  __sync_fetch_and_add(&metrics->blocked_commits.value.int_val, 1);
  metrics->blocked_commits.collect_time = taosGetTimestampMs();
}

// Update STT trigger value
void tmetrics_update_stt_trigger(SWriteMetricsEx* metrics, int32_t vgId, int32_t value) {
  if (metrics == NULL) {
    metrics = tmetrics_get_vnode_write_metrics(vgId);
    if (metrics == NULL) return;
  }

  metrics->stt_trigger_value.value.int_val = value;
  metrics->stt_trigger_value.collect_time = taosGetTimestampMs();
}

// Get metrics instance for specific vnode
SWriteMetricsEx* tmetrics_get_vnode_write_metrics(int32_t vgId) {
  if (!gMetricsManager.is_initialized || vgId < 0 || vgId >= gMetricsManager.vnode_metrics_size) {
    return NULL;
  }

  // Lazy initialization of vnode metrics
  if (gMetricsManager.vnode_metrics[vgId] == NULL) {
    gMetricsManager.vnode_metrics[vgId] = taosMemoryCalloc(1, sizeof(SWriteMetricsEx));
    if (gMetricsManager.vnode_metrics[vgId] == NULL) {
      return NULL;
    }

    tmetrics_init_write_metrics((SWriteMetricsEx*)gMetricsManager.vnode_metrics[vgId]);

    // Set vgId for all metrics
    SWriteMetricsEx* metrics = (SWriteMetricsEx*)gMetricsManager.vnode_metrics[vgId];
    metrics->total_requests.vgId = vgId;
    metrics->total_rows.vgId = vgId;
    metrics->total_bytes.vgId = vgId;
    metrics->avg_write_size.vgId = vgId;
    metrics->cache_hit_ratio.vgId = vgId;
    metrics->rpc_queue_wait.vgId = vgId;
    metrics->preprocess_time.vgId = vgId;
    metrics->wal_write_rate.vgId = vgId;
    metrics->sync_rate.vgId = vgId;
    metrics->apply_rate.vgId = vgId;
    metrics->memory_table_size.vgId = vgId;
    metrics->memory_table_rows.vgId = vgId;
    metrics->commit_count.vgId = vgId;
    metrics->auto_commit_count.vgId = vgId;
    metrics->forced_commit_count.vgId = vgId;
    metrics->stt_trigger_value.vgId = vgId;
    metrics->merge_count.vgId = vgId;
    metrics->avg_commit_time.vgId = vgId;
    metrics->avg_merge_time.vgId = vgId;
    metrics->blocked_commits.vgId = vgId;
    metrics->memtable_wait_time.vgId = vgId;
  }

  return (SWriteMetricsEx*)gMetricsManager.vnode_metrics[vgId];
}

// Collect and log metrics for all vnodes
void tmetrics_collect_all_vnodes() {
  if (!gMetricsManager.is_initialized) return;

  // Collect global metrics
  if (gMetricsManager.write_metrics) {
    tmetrics_collect_write_metrics((SWriteMetricsEx*)gMetricsManager.write_metrics);
  }

  // Collect metrics for all vnodes
  for (int32_t i = 0; i < gMetricsManager.vnode_metrics_size; i++) {
    if (gMetricsManager.vnode_metrics[i]) {
      SWriteMetricsEx* metrics = (SWriteMetricsEx*)gMetricsManager.vnode_metrics[i];
      metrics->total_requests.collect_time = taosGetTimestampMs();
    }
  }
}

// Log metrics for all active vnodes
void tmetrics_log_all_vnodes(const char* log_file) {
  if (!gMetricsManager.is_initialized || !log_file) return;

  // Log global metrics
  if (gMetricsManager.write_metrics) {
    tmetrics_log_write_metrics((SWriteMetricsEx*)gMetricsManager.write_metrics, log_file);
  }

  // Log metrics for active vnodes only
  for (int32_t i = 0; i < gMetricsManager.vnode_metrics_size; i++) {
    if (gMetricsManager.vnode_metrics[i]) {
      SWriteMetricsEx* metrics = (SWriteMetricsEx*)gMetricsManager.vnode_metrics[i];
      if (metrics->total_requests.value.int_val > 0) {
        tmetrics_log_write_metrics(metrics, log_file);
      }
    }
  }
}