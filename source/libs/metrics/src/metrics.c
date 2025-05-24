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
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include "metricsInt.h"
#include "os.h"
#include "osTimezone.h"
#include "taoserror.h"
#include "tdef.h"
#include "tglobal.h"
#include "thash.h"
#include "thttp.h"
#include "tjson.h"
#include "tlog.h"
#include "tsched.h"
#include "tthread.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

extern int32_t vnodeGetWriteMetricsEx(void *pVnode, SWriteMetricsEx *pMetrics);

// --- Global Manager ---

static SMetricsManager gMetricsManager = {0};

// --- Init/Destroy ---

static void destroyWriteMetricsEx(void *p) { taosMemoryFree(*(SWriteMetricsEx **)p); }

int32_t initMetricsManager() {
  int32_t code = 0;

  gMetricsManager.pDnodeMetrics = taosArrayInit(8, sizeof(SMetric));
  if (gMetricsManager.pDnodeMetrics == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // Initialize pWriteMetrics hash table for SWriteMetricsBundle*
  gMetricsManager.pWriteMetrics =
      taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (gMetricsManager.pWriteMetrics == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  gMetricsManager.pQueryMetrics =
      taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (gMetricsManager.pQueryMetrics == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  gMetricsManager.pStreamMetrics =
      taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (gMetricsManager.pStreamMetrics == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  taosHashSetFreeFp(gMetricsManager.pWriteMetrics, destroyWriteMetricsEx);

  return 0;

_err:
  destroyMetricsManager();
  return code;
}

static void destroyMetricsManager() {
  taosArrayDestroy(gMetricsManager.pDnodeMetrics);
  taosHashClear(gMetricsManager.pWriteMetrics);
  taosHashClear(gMetricsManager.pQueryMetrics);
  taosHashClear(gMetricsManager.pStreamMetrics);
}

void initMetric(SMetric *pMetric, EMetricType type, EMetricLevel level) {
  if (pMetric == NULL) return;

  pMetric->definition.type = type;
  pMetric->definition.level = level;

  switch (type) {
    case METRIC_TYPE_INT64:
      pMetric->value.int_val = 0;
      break;
    case METRIC_TYPE_DOUBLE:
      pMetric->value.double_val = 0.0;
      break;
    case METRIC_TYPE_STRING:
      pMetric->value.str_val = NULL;
      break;
    default:
      break;
  }
}

void setMetricInt64(SMetric *pMetric, int64_t value) {
  if (pMetric == NULL || pMetric->definition.type != METRIC_TYPE_INT64) return;
  pMetric->value.int_val = value;
}

void setMetricDouble(SMetric *pMetric, double value) {
  if (pMetric == NULL || pMetric->definition.type != METRIC_TYPE_DOUBLE) return;
  pMetric->value.double_val = value;
}

void setMetricString(SMetric *pMetric, const char *value) {
  if (pMetric == NULL || pMetric->definition.type != METRIC_TYPE_STRING) return;

  if (pMetric->value.str_val) {
    taosMemoryFree(pMetric->value.str_val);
  }

  if (value) {
    pMetric->value.str_val = strdup(value);
  } else {
    pMetric->value.str_val = NULL;
  }
}

int64_t getMetricInt64(const SMetric *pMetric) {
  if (pMetric == NULL || pMetric->definition.type != METRIC_TYPE_INT64) return 0;
  return pMetric->value.int_val;
}

double getMetricDouble(const SMetric *pMetric) {
  if (pMetric == NULL || pMetric->definition.type != METRIC_TYPE_DOUBLE) return 0.0;
  return pMetric->value.double_val;
}

const char *getMetricString(const SMetric *pMetric) {
  if (pMetric == NULL || pMetric->definition.type != METRIC_TYPE_STRING) return NULL;
  return pMetric->value.str_val;
}

// Initializes the FORMATTED structure (called once per bundle creation)
void initWriteMetricsEx(SWriteMetricsEx *pMetrics) {
  if (pMetrics == NULL) return;
  pMetrics->vgId = 0;
  initMetric(&pMetrics->total_requests, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->total_rows, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->total_bytes, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->avg_write_size, METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->cache_hit_ratio, METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->rpc_queue_wait, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->preprocess_time, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->fetch_batch_meta_time, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->fetch_batch_meta_count, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->wal_write_rate, METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->sync_rate, METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->apply_rate, METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->memory_table_size, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->memory_table_rows, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->commit_count, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->merge_count, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->avg_commit_time, METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->avg_merge_time, METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->blocked_commits, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->memtable_wait_time, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->wal_write_bytes, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->wal_write_time, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->sync_bytes, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->sync_time, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->apply_bytes, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->apply_time, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
}

void cleanupMetrics() { destroyMetricsManager(); }

static void updateFormattedFromRaw(SWriteMetricsEx *fmt, const SRawWriteMetrics *raw, int32_t vgId) {
  if (fmt == NULL || raw == NULL) return;

  fmt->vgId = vgId;

  setMetricInt64(&fmt->total_requests, raw->total_requests);
  setMetricInt64(&fmt->total_rows, raw->total_rows);
  setMetricInt64(&fmt->total_bytes, raw->total_bytes);
  setMetricDouble(&fmt->avg_write_size, raw->avg_write_size);
  setMetricDouble(&fmt->cache_hit_ratio, raw->cache_hit_ratio);
  setMetricInt64(&fmt->rpc_queue_wait, raw->rpc_queue_wait);
  setMetricInt64(&fmt->preprocess_time, raw->preprocess_time);
  setMetricInt64(&fmt->fetch_batch_meta_time, raw->fetch_batch_meta_time);
  setMetricInt64(&fmt->fetch_batch_meta_count, raw->fetch_batch_meta_count);

  setMetricInt64(&fmt->memory_table_size, raw->memory_table_size);
  setMetricInt64(&fmt->memory_table_rows, raw->memory_table_rows);
  setMetricInt64(&fmt->commit_count, raw->commit_count);
  setMetricInt64(&fmt->merge_count, raw->merge_count);
  setMetricInt64(&fmt->blocked_commits, raw->blocked_commits);
  setMetricInt64(&fmt->memtable_wait_time, raw->memtable_wait_time);

  setMetricInt64(&fmt->wal_write_bytes, raw->wal_write_bytes);
  setMetricInt64(&fmt->wal_write_time, raw->wal_write_time);
  setMetricInt64(&fmt->sync_bytes, raw->sync_bytes);
  setMetricInt64(&fmt->sync_time, raw->sync_time);
  setMetricInt64(&fmt->apply_bytes, raw->apply_bytes);
  setMetricInt64(&fmt->apply_time, raw->apply_time);
  double avg_commit_time = (raw->commit_count > 0) ? (raw->commit_time_sum / raw->commit_count) : 0.0;
  setMetricDouble(&fmt->avg_commit_time, avg_commit_time);
  double avg_merge_time = (raw->merge_count > 0) ? (raw->merge_time_sum / raw->merge_count) : 0.0;
  setMetricDouble(&fmt->avg_merge_time, avg_merge_time);
}

int32_t addWriteMetrics(int32_t vgId, const SRawWriteMetrics *pRawMetrics) {
  if (pRawMetrics == NULL || gMetricsManager.pWriteMetrics == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SWriteMetricsEx *pMetricEx = {0};
  int32_t          code = TSDB_CODE_SUCCESS;
  SWriteMetricsEx **ppMetricEx = NULL;

  ppMetricEx = (SWriteMetricsEx **)taosHashGet(gMetricsManager.pWriteMetrics, &vgId, sizeof(vgId));

  if (ppMetricEx == NULL) {
    pMetricEx = (SWriteMetricsEx *)taosMemoryMalloc(sizeof(SWriteMetricsEx));
    if (pMetricEx == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    initWriteMetricsEx(pMetricEx);
    updateFormattedFromRaw(pMetricEx, pRawMetrics, vgId);
    code = taosHashPut(gMetricsManager.pWriteMetrics, &vgId, sizeof(vgId), &pMetricEx, sizeof(SWriteMetricsEx *));
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(pMetricEx);
      pMetricEx = NULL;
      uError("VgId:%d Failed to add write metrics to hash table, code:%d", vgId, code);
      return code;
    }
  } else {
    pMetricEx = *ppMetricEx;
    updateFormattedFromRaw(pMetricEx, pRawMetrics, vgId);
  }
  return code;
}

static SJson *metricsToJson(SWriteMetricsEx *pMetrics) {
  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return NULL;

  char    buf[40] = {0};
  int64_t curTime = taosGetTimestampMs();
  if (taosFormatUtcTime(buf, sizeof(buf), curTime, TSDB_TIME_PRECISION_MILLI) != 0) {
    tjsonDelete(pJson);
    return NULL;
  }

  tjsonAddStringToObject(pJson, "ts", buf);
  tjsonAddDoubleToObject(pJson, "vgId", pMetrics->vgId);
  tjsonAddDoubleToObject(pJson, "total_requests", getMetricInt64(&pMetrics->total_requests));
  tjsonAddDoubleToObject(pJson, "total_rows", getMetricInt64(&pMetrics->total_rows));
  tjsonAddDoubleToObject(pJson, "total_bytes", getMetricInt64(&pMetrics->total_bytes));
  tjsonAddDoubleToObject(pJson, "avg_write_size", getMetricDouble(&pMetrics->avg_write_size));
  tjsonAddDoubleToObject(pJson, "rpc_queue_wait", getMetricInt64(&pMetrics->rpc_queue_wait));
  tjsonAddDoubleToObject(pJson, "preprocess_time", getMetricInt64(&pMetrics->preprocess_time));
  tjsonAddDoubleToObject(pJson, "fetch_batch_meta_time", getMetricInt64(&pMetrics->fetch_batch_meta_time));
  tjsonAddDoubleToObject(pJson, "fetch_batch_meta_count", getMetricInt64(&pMetrics->fetch_batch_meta_count));
  tjsonAddDoubleToObject(pJson, "memtable_wait_time", getMetricInt64(&pMetrics->memtable_wait_time));
  tjsonAddDoubleToObject(pJson, "memory_table_rows", getMetricInt64(&pMetrics->memory_table_rows));
  tjsonAddDoubleToObject(pJson, "memory_table_size", getMetricInt64(&pMetrics->memory_table_size));
  tjsonAddDoubleToObject(pJson, "cache_hit_ratio", getMetricDouble(&pMetrics->cache_hit_ratio));
  tjsonAddDoubleToObject(pJson, "wal_write_bytes", getMetricInt64(&pMetrics->wal_write_bytes));
  tjsonAddDoubleToObject(pJson, "wal_write_time", getMetricInt64(&pMetrics->wal_write_time));
  tjsonAddDoubleToObject(pJson, "sync_bytes", getMetricInt64(&pMetrics->sync_bytes));
  tjsonAddDoubleToObject(pJson, "sync_time", getMetricInt64(&pMetrics->sync_time));
  tjsonAddDoubleToObject(pJson, "apply_bytes", getMetricInt64(&pMetrics->apply_bytes));
  tjsonAddDoubleToObject(pJson, "apply_time", getMetricInt64(&pMetrics->apply_time));
  tjsonAddDoubleToObject(pJson, "commit_count", getMetricInt64(&pMetrics->commit_count));
  tjsonAddDoubleToObject(pJson, "avg_commit_time", getMetricDouble(&pMetrics->avg_commit_time));
  tjsonAddDoubleToObject(pJson, "blocked_commits", getMetricInt64(&pMetrics->blocked_commits));
  tjsonAddDoubleToObject(pJson, "merge_count", getMetricInt64(&pMetrics->merge_count));
  tjsonAddDoubleToObject(pJson, "avg_merge_time", getMetricDouble(&pMetrics->avg_merge_time));

  return pJson;
}

static void sendMetricsReport(SJson *pJson) {
  if (!tsEnableMonitor || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;

  char *pCont = tjsonToString(pJson);
  if (pCont != NULL) {
    EHttpCompFlag flag = HTTP_FLAT;
    char          tmp[100] = {0};
    snprintf(tmp, 100, "0x%" PRIxLEAST64, taosGetTimestampUs());
    if (taosSendHttpReportWithQID(tsMonitorFqdn, "/metrics", tsMonitorPort, pCont, strlen(pCont), flag, tmp) != 0) {
      uError("failed to send metrics msg");
    }
    taosMemoryFree(pCont);
  }
}

void reportWriteMetrics() {
  if (gMetricsManager.pWriteMetrics == NULL) {
    uError("Write metrics manager not initialized for reporting.");
    return;
  }

  void            *pIter = NULL;
  SWriteMetricsEx *pMetrics = NULL;
  void            *pData = NULL;

  while ((pData = taosHashIterate(gMetricsManager.pWriteMetrics, pIter)) != NULL) {
    pMetrics = *(SWriteMetricsEx **)pData;
    pIter = pData;

    if (pMetrics == NULL) {
      continue;
    }

    uInfo("VgId:%d Req:%" PRId64 " Rows:%" PRId64 " Bytes:%" PRId64
          " AvgSize:%.2f "
          "RPC:%.2fms Preproc:%.2fms FetchBatchMetaTime:%.2fms FetchBatchMetaCount:%" PRId64
          " "
          "MemWait:%.2fms MemRows:%" PRId64 " MemBytes:%" PRId64
          " CacheHit:%.2f%% "
          "WalWriteBytes:%" PRId64 " WalWriteTime:%" PRId64 " SyncBytes:%" PRId64 " SyncTime:%" PRId64
          " "
          "ApplyBytes:%" PRId64 " ApplyTime:%" PRId64 " Commits:%" PRId64 " CommitTime:%.2fms Blocked:%" PRId64
          " "
          "Merges:%" PRId64 " MergeTime:%.2fms",
          pMetrics->vgId, getMetricInt64(&pMetrics->total_requests), getMetricInt64(&pMetrics->total_rows),
          getMetricInt64(&pMetrics->total_bytes), getMetricDouble(&pMetrics->avg_write_size),
          (double)getMetricInt64(&pMetrics->rpc_queue_wait), (double)getMetricInt64(&pMetrics->preprocess_time),
          (double)getMetricInt64(&pMetrics->fetch_batch_meta_time), getMetricInt64(&pMetrics->fetch_batch_meta_count),
          (double)getMetricInt64(&pMetrics->memtable_wait_time), getMetricInt64(&pMetrics->memory_table_rows),
          getMetricInt64(&pMetrics->memory_table_size), getMetricDouble(&pMetrics->cache_hit_ratio) * 100.0,
          getMetricInt64(&pMetrics->wal_write_bytes), getMetricInt64(&pMetrics->wal_write_time),
          getMetricInt64(&pMetrics->sync_bytes), getMetricInt64(&pMetrics->sync_time),
          getMetricInt64(&pMetrics->apply_bytes), getMetricInt64(&pMetrics->apply_time),
          getMetricInt64(&pMetrics->commit_count), getMetricDouble(&pMetrics->avg_commit_time),
          getMetricInt64(&pMetrics->blocked_commits), getMetricInt64(&pMetrics->merge_count),
          getMetricDouble(&pMetrics->avg_merge_time));

    SJson *pJson = metricsToJson(pMetrics);
    if (pJson != NULL) {
      sendMetricsReport(pJson);
      tjsonDelete(pJson);
    }
  }
}

void sendAllMetricsReport() {
  if (!tsEnableMonitor || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;
  if (gMetricsManager.pWriteMetrics == NULL) return;

  SJson *pRootJson = tjsonCreateObject();
  if (pRootJson == NULL) return;

  char    buf[40] = {0};
  int64_t curTime = taosGetTimestampMs();
  if (taosFormatUtcTime(buf, sizeof(buf), curTime, TSDB_TIME_PRECISION_MILLI) != 0) {
    tjsonDelete(pRootJson);
    return;
  }

  tjsonAddStringToObject(pRootJson, "ts", buf);
  SJson *pMetricsArray = tjsonAddArrayToObject(pRootJson, "write_metrics");
  if (pMetricsArray == NULL) {
    tjsonDelete(pRootJson);
    return;
  }

  void            *pIter = NULL;
  SWriteMetricsEx *pMetrics = NULL;
  void            *pData = NULL;

  while ((pData = taosHashIterate(gMetricsManager.pWriteMetrics, pIter)) != NULL) {
    pMetrics = *(SWriteMetricsEx **)pData;
    pIter = pData;

    if (pMetrics == NULL) continue;

    SJson *pVgJson = tjsonCreateObject();
    if (pVgJson == NULL) continue;

    tjsonAddDoubleToObject(pVgJson, "vgId", pMetrics->vgId);
    tjsonAddDoubleToObject(pVgJson, "total_requests", getMetricInt64(&pMetrics->total_requests));
    tjsonAddDoubleToObject(pVgJson, "total_rows", getMetricInt64(&pMetrics->total_rows));
    tjsonAddDoubleToObject(pVgJson, "total_bytes", getMetricInt64(&pMetrics->total_bytes));
    tjsonAddDoubleToObject(pVgJson, "avg_write_size", getMetricDouble(&pMetrics->avg_write_size));
    tjsonAddDoubleToObject(pVgJson, "rpc_queue_wait", getMetricInt64(&pMetrics->rpc_queue_wait));
    tjsonAddDoubleToObject(pVgJson, "preprocess_time", getMetricInt64(&pMetrics->preprocess_time));
    tjsonAddDoubleToObject(pVgJson, "fetch_batch_meta_time", getMetricInt64(&pMetrics->fetch_batch_meta_time));
    tjsonAddDoubleToObject(pVgJson, "fetch_batch_meta_count", getMetricInt64(&pMetrics->fetch_batch_meta_count));
    tjsonAddDoubleToObject(pVgJson, "memtable_wait_time", getMetricInt64(&pMetrics->memtable_wait_time));
    tjsonAddDoubleToObject(pVgJson, "memory_table_rows", getMetricInt64(&pMetrics->memory_table_rows));
    tjsonAddDoubleToObject(pVgJson, "memory_table_size", getMetricInt64(&pMetrics->memory_table_size));
    tjsonAddDoubleToObject(pVgJson, "cache_hit_ratio", getMetricDouble(&pMetrics->cache_hit_ratio));
    tjsonAddDoubleToObject(pVgJson, "wal_write_bytes", getMetricInt64(&pMetrics->wal_write_bytes));
    tjsonAddDoubleToObject(pVgJson, "wal_write_time", getMetricInt64(&pMetrics->wal_write_time));
    tjsonAddDoubleToObject(pVgJson, "sync_bytes", getMetricInt64(&pMetrics->sync_bytes));
    tjsonAddDoubleToObject(pVgJson, "sync_time", getMetricInt64(&pMetrics->sync_time));
    tjsonAddDoubleToObject(pVgJson, "apply_bytes", getMetricInt64(&pMetrics->apply_bytes));
    tjsonAddDoubleToObject(pVgJson, "apply_time", getMetricInt64(&pMetrics->apply_time));
    tjsonAddDoubleToObject(pVgJson, "commit_count", getMetricInt64(&pMetrics->commit_count));
    tjsonAddDoubleToObject(pVgJson, "avg_commit_time", getMetricDouble(&pMetrics->avg_commit_time));
    tjsonAddDoubleToObject(pVgJson, "blocked_commits", getMetricInt64(&pMetrics->blocked_commits));
    tjsonAddDoubleToObject(pVgJson, "merge_count", getMetricInt64(&pMetrics->merge_count));
    tjsonAddDoubleToObject(pVgJson, "avg_merge_time", getMetricDouble(&pMetrics->avg_merge_time));

    if (tjsonAddItemToArray(pMetricsArray, pVgJson) != 0) {
      tjsonDelete(pVgJson);
    }
  }

  char *pCont = tjsonToString(pRootJson);
  if (pCont != NULL) {
    EHttpCompFlag flag = HTTP_FLAT;
    char          tmp[100] = {0};
    snprintf(tmp, 100, "0x%" PRIxLEAST64, taosGetTimestampUs());
    if (taosSendHttpReportWithQID(tsMonitorFqdn, "/metrics-batch", tsMonitorPort, pCont, strlen(pCont), flag, tmp) !=
        0) {
      uError("failed to send metrics batch msg");
    }
    taosMemoryFree(pCont);
  }

  tjsonDelete(pRootJson);
}
