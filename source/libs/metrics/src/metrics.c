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
#include "metricsInt.h"
#include "os.h"
#include "osTimezone.h"
#include "taoserror.h"
#include "tdef.h"
#include "tglobal.h"
#include "thash.h"
#include "tlog.h"
#include "tsched.h"
#include "tthread.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

static void freeWriteMetricsExData(SWriteMetricsEx *pMetric) {
  if (pMetric == NULL) return;

  taosMemoryFree(pMetric);
}

static void freeAllVnodesWriteMetrics(SHashObj *pHash) {
  if (pHash == NULL) return;
  void *cursor = NULL;
  void *pData = NULL;
  while ((pData = taosHashIterate(pHash, &cursor)) != NULL) {
    SWriteMetricsEx *pMetric = (SWriteMetricsEx *)pData;
    freeWriteMetricsExData(pMetric);
  }
}

static SMetricsManager gMetricsManager = {0};

int32_t initMetricsManager() {
  int32_t code = 0;

  gMetricsManager.pDnodeMetrics = taosArrayInit(8, sizeof(SMetric));
  if (gMetricsManager.pDnodeMetrics == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

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

  return 0;

_err:
  destroyMetricsManager();
  return code;
}

static void destroyMetricsManager() {
  freeAllVnodesWriteMetrics(gMetricsManager.pWriteMetrics);

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
  initMetric(&pMetrics->wal_write_rate, METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->sync_rate, METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->apply_rate, METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->memory_table_size, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->memory_table_rows, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->commit_count, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->auto_commit_count, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->forced_commit_count, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->stt_trigger_value, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->merge_count, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->avg_commit_time, METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->avg_merge_time, METRIC_TYPE_DOUBLE, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->blocked_commits, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
  initMetric(&pMetrics->memtable_wait_time, METRIC_TYPE_INT64, METRIC_LEVEL_LOW);
}

void cleanupMetrics() { destroyMetricsManager(); }
