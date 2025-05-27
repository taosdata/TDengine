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

static void destroyDnodeMetricsEx(void *p) { taosMemoryFree(*(SDnodeMetricsEx **)p); }

int32_t initMetricsManager() {
  int32_t code = 0;

  gMetricsManager.pDnodeMetrics = taosMemoryMalloc(sizeof(SDnodeMetricsEx));
  if (gMetricsManager.pDnodeMetrics == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  initDnodeMetricsEx((SDnodeMetricsEx *)gMetricsManager.pDnodeMetrics);

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
  if (gMetricsManager.pDnodeMetrics) {
    taosMemoryFree(gMetricsManager.pDnodeMetrics);
    gMetricsManager.pDnodeMetrics = NULL;
  }
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
  initMetric(&pMetrics->fetch_batch_meta_time, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->fetch_batch_meta_count, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->preprocess_time, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->wal_write_bytes, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->wal_write_time, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->apply_bytes, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->apply_time, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->commit_count, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->commit_time, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->memtable_wait_time, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->blocked_commits, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->merge_count, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->merge_time, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
}

void initDnodeMetricsEx(SDnodeMetricsEx *pMetrics) {
  if (pMetrics == NULL) return;
  initMetric(&pMetrics->rpcQueueMemoryAllowed, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->rpcQueueMemoryUsed, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->applyMemoryAllowed, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->applyMemoryUsed, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
}

void cleanupMetrics() { destroyMetricsManager(); }

static void updateFormattedFromRaw(SWriteMetricsEx *fmt, const SRawWriteMetrics *raw, int32_t vgId, int32_t dnodeId) {
  if (fmt == NULL || raw == NULL) return;

  fmt->vgId = vgId;
  fmt->dnodeId = dnodeId;

  setMetricInt64(&fmt->total_requests, raw->total_requests);
  setMetricInt64(&fmt->total_rows, raw->total_rows);
  setMetricInt64(&fmt->total_bytes, raw->total_bytes);
  setMetricInt64(&fmt->fetch_batch_meta_time, raw->fetch_batch_meta_time);
  setMetricInt64(&fmt->fetch_batch_meta_count, raw->fetch_batch_meta_count);
  setMetricInt64(&fmt->preprocess_time, raw->preprocess_time);
  setMetricInt64(&fmt->wal_write_bytes, raw->wal_write_bytes);
  setMetricInt64(&fmt->wal_write_time, raw->wal_write_time);
  setMetricInt64(&fmt->apply_bytes, raw->apply_bytes);
  setMetricInt64(&fmt->apply_time, raw->apply_time);
  setMetricInt64(&fmt->commit_count, raw->commit_count);
  setMetricInt64(&fmt->commit_time, raw->commit_time);
  setMetricInt64(&fmt->memtable_wait_time, raw->memtable_wait_time);
  setMetricInt64(&fmt->blocked_commits, raw->blocked_commits);
  setMetricInt64(&fmt->merge_count, raw->merge_count);
  setMetricInt64(&fmt->merge_time, raw->merge_time);
}

static void updateDnodeFormattedFromRaw(SDnodeMetricsEx *fmt, const SRawDnodeMetrics *raw) {
  if (fmt == NULL || raw == NULL) return;

  setMetricInt64(&fmt->rpcQueueMemoryAllowed, raw->rpcQueueMemoryAllowed);
  setMetricInt64(&fmt->rpcQueueMemoryUsed, raw->rpcQueueMemoryUsed);
  setMetricInt64(&fmt->applyMemoryAllowed, raw->applyMemoryAllowed);
  setMetricInt64(&fmt->applyMemoryUsed, raw->applyMemoryUsed);
}

int32_t addWriteMetrics(int32_t vgId, int32_t dnodeId, const SRawWriteMetrics *pRawMetrics) {
  if (pRawMetrics == NULL || gMetricsManager.pWriteMetrics == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SWriteMetricsEx  *pMetricEx = {0};
  int32_t           code = TSDB_CODE_SUCCESS;
  SWriteMetricsEx **ppMetricEx = NULL;

  ppMetricEx = (SWriteMetricsEx **)taosHashGet(gMetricsManager.pWriteMetrics, &vgId, sizeof(vgId));

  if (ppMetricEx == NULL) {
    pMetricEx = (SWriteMetricsEx *)taosMemoryMalloc(sizeof(SWriteMetricsEx));
    if (pMetricEx == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    initWriteMetricsEx(pMetricEx);
    updateFormattedFromRaw(pMetricEx, pRawMetrics, vgId, dnodeId);
    code = taosHashPut(gMetricsManager.pWriteMetrics, &vgId, sizeof(vgId), &pMetricEx, sizeof(SWriteMetricsEx *));
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(pMetricEx);
      pMetricEx = NULL;
      uError("VgId:%d Failed to add write metrics to hash table, code:%d", vgId, code);
      return code;
    }
  } else {
    pMetricEx = *ppMetricEx;
    updateFormattedFromRaw(pMetricEx, pRawMetrics, vgId, dnodeId);
  }
  return code;
}

int32_t addDnodeMetrics(const SRawDnodeMetrics *pRawMetrics) {
  if (pRawMetrics == NULL || gMetricsManager.pDnodeMetrics == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SDnodeMetricsEx *pMetricEx = (SDnodeMetricsEx *)gMetricsManager.pDnodeMetrics;
  updateDnodeFormattedFromRaw(pMetricEx, pRawMetrics);
  return TSDB_CODE_SUCCESS;
}

SDnodeMetricsEx *getDnodeMetrics() { return (SDnodeMetricsEx *)gMetricsManager.pDnodeMetrics; }

SWriteMetricsEx *getWriteMetricsByVgId(int32_t vgId) {
  if (gMetricsManager.pWriteMetrics == NULL) {
    return NULL;
  }

  SWriteMetricsEx **ppMetricEx = (SWriteMetricsEx **)taosHashGet(gMetricsManager.pWriteMetrics, &vgId, sizeof(vgId));
  if (ppMetricEx == NULL) {
    return NULL;
  }

  return *ppMetricEx;
}

static SJson *writeMetricsToJson(SWriteMetricsEx *pMetrics) {
  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return NULL;

  char    buf[40] = {0};
  int64_t curTime = taosGetTimestampMs();
  if (taosFormatUtcTime(buf, sizeof(buf), curTime, TSDB_TIME_PRECISION_MILLI) != 0) {
    tjsonDelete(pJson);
    return NULL;
  }

  tjsonAddStringToObject(pJson, "ts", buf);
  tjsonAddDoubleToObject(pJson, "dnodeId", pMetrics->dnodeId);
  tjsonAddDoubleToObject(pJson, "vgId", pMetrics->vgId);
  tjsonAddDoubleToObject(pJson, "total_requests", getMetricInt64(&pMetrics->total_requests));
  tjsonAddDoubleToObject(pJson, "total_rows", getMetricInt64(&pMetrics->total_rows));
  tjsonAddDoubleToObject(pJson, "total_bytes", getMetricInt64(&pMetrics->total_bytes));
  tjsonAddDoubleToObject(pJson, "fetch_batch_meta_time", getMetricInt64(&pMetrics->fetch_batch_meta_time));
  tjsonAddDoubleToObject(pJson, "fetch_batch_meta_count", getMetricInt64(&pMetrics->fetch_batch_meta_count));
  tjsonAddDoubleToObject(pJson, "preprocess_time", getMetricInt64(&pMetrics->preprocess_time));
  tjsonAddDoubleToObject(pJson, "wal_write_bytes", getMetricInt64(&pMetrics->wal_write_bytes));
  tjsonAddDoubleToObject(pJson, "wal_write_time", getMetricInt64(&pMetrics->wal_write_time));
  tjsonAddDoubleToObject(pJson, "apply_bytes", getMetricInt64(&pMetrics->apply_bytes));
  tjsonAddDoubleToObject(pJson, "apply_time", getMetricInt64(&pMetrics->apply_time));
  tjsonAddDoubleToObject(pJson, "commit_count", getMetricInt64(&pMetrics->commit_count));
  tjsonAddDoubleToObject(pJson, "commit_time", getMetricInt64(&pMetrics->commit_time));
  tjsonAddDoubleToObject(pJson, "memtable_wait_time", getMetricInt64(&pMetrics->memtable_wait_time));
  tjsonAddDoubleToObject(pJson, "blocked_commits", getMetricInt64(&pMetrics->blocked_commits));
  tjsonAddDoubleToObject(pJson, "merge_count", getMetricInt64(&pMetrics->merge_count));
  tjsonAddDoubleToObject(pJson, "merge_time", getMetricInt64(&pMetrics->merge_time));

  return pJson;
}

static SJson *dnodeMetricsToJson(SDnodeMetricsEx *pMetrics) {
  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return NULL;

  char    buf[40] = {0};
  int64_t curTime = taosGetTimestampMs();
  if (taosFormatUtcTime(buf, sizeof(buf), curTime, TSDB_TIME_PRECISION_MILLI) != 0) {
    tjsonDelete(pJson);
    return NULL;
  }

  tjsonAddStringToObject(pJson, "ts", buf);

  SJson *pDnodeMetricsJson = tjsonCreateObject();
  if (pDnodeMetricsJson == NULL) {
    tjsonDelete(pJson);
    return NULL;
  }

  tjsonAddDoubleToObject(pDnodeMetricsJson, "rpcQueueMemoryAllowed", getMetricInt64(&pMetrics->rpcQueueMemoryAllowed));
  tjsonAddDoubleToObject(pDnodeMetricsJson, "rpcQueueMemoryUsed", getMetricInt64(&pMetrics->rpcQueueMemoryUsed));
  tjsonAddDoubleToObject(pDnodeMetricsJson, "applyMemoryAllowed", getMetricInt64(&pMetrics->applyMemoryAllowed));
  tjsonAddDoubleToObject(pDnodeMetricsJson, "applyMemoryUsed", getMetricInt64(&pMetrics->applyMemoryUsed));

  tjsonAddItemToObject(pJson, "dnode_metrics", pDnodeMetricsJson);

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

static void sendDnodeMetricsReport(SJson *pJson) {
  if (!tsEnableMonitor || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;

  char *pCont = tjsonToString(pJson);
  if (pCont != NULL) {
    EHttpCompFlag flag = HTTP_FLAT;
    char          tmp[100] = {0};
    snprintf(tmp, 100, "0x%" PRIxLEAST64, taosGetTimestampUs());
    if (taosSendHttpReportWithQID(tsMonitorFqdn, "/dnode-metrics", tsMonitorPort, pCont, strlen(pCont), flag, tmp) !=
        0) {
      uError("failed to send dnode metrics msg");
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
          " "
          "FetchBatchMetaTime:%" PRId64 " FetchBatchMetaCount:%" PRId64 " Preproc:%" PRId64
          " "
          "WalWriteBytes:%" PRId64 " WalWriteTime:%" PRId64 " ApplyBytes:%" PRId64 " ApplyTime:%" PRId64
          " "
          "Commits:%" PRId64 " CommitTime:%" PRId64 " MemWait:%" PRId64 " Blocked:%" PRId64
          " "
          "Merges:%" PRId64 " MergeTime:%" PRId64,
          pMetrics->vgId, getMetricInt64(&pMetrics->total_requests), getMetricInt64(&pMetrics->total_rows),
          getMetricInt64(&pMetrics->total_bytes), getMetricInt64(&pMetrics->fetch_batch_meta_time),
          getMetricInt64(&pMetrics->fetch_batch_meta_count), getMetricInt64(&pMetrics->preprocess_time),
          getMetricInt64(&pMetrics->wal_write_bytes), getMetricInt64(&pMetrics->wal_write_time),
          getMetricInt64(&pMetrics->apply_bytes), getMetricInt64(&pMetrics->apply_time),
          getMetricInt64(&pMetrics->commit_count), getMetricInt64(&pMetrics->commit_time),
          getMetricInt64(&pMetrics->memtable_wait_time), getMetricInt64(&pMetrics->blocked_commits),
          getMetricInt64(&pMetrics->merge_count), getMetricInt64(&pMetrics->merge_time));

    SJson *pJson = writeMetricsToJson(pMetrics);
    if (pJson != NULL) {
      sendMetricsReport(pJson);
      tjsonDelete(pJson);
    }
  }
}

void reportDnodeMetrics() {
  if (gMetricsManager.pDnodeMetrics == NULL) {
    uError("Dnode metrics manager not initialized for reporting.");
    return;
  }

  SDnodeMetricsEx *pMetrics = (SDnodeMetricsEx *)gMetricsManager.pDnodeMetrics;

  uInfo("Dnode RpcQueueMemoryAllowed:%" PRId64 " RpcQueueMemoryUsed:%" PRId64 " ApplyMemoryAllowed:%" PRId64
        " ApplyMemoryUsed:%" PRId64,
        getMetricInt64(&pMetrics->rpcQueueMemoryAllowed), getMetricInt64(&pMetrics->rpcQueueMemoryUsed),
        getMetricInt64(&pMetrics->applyMemoryAllowed), getMetricInt64(&pMetrics->applyMemoryUsed));

  SJson *pJson = dnodeMetricsToJson(pMetrics);
  if (pJson != NULL) {
    sendDnodeMetricsReport(pJson);
    tjsonDelete(pJson);
  }
}
