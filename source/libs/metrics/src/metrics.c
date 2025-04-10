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
#include "tlog.h"
#include "tsched.h"
#include "tthread.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"

extern int32_t vnodeGetWriteMetricsEx(void *pVnode, SWriteMetricsEx *pMetrics);

// Free function for hash table values (SWriteMetricsBundle*)
static void freeWriteMetricsBundle(void *pData) {
  SWriteMetricsBundle *pBundle = (SWriteMetricsBundle *)pData;
  if (pBundle == NULL) return;
  // Free any dynamically allocated strings within formatted SMetric if necessary
  // Example:
  // if (pBundle->formatted.some_string.definition.type == METRIC_TYPE_STRING &&
  // pBundle->formatted.some_string.value.str_val) {
  //     taosMemoryFree(pBundle->formatted.some_string.value.str_val);
  // }
  taosMemoryFree(pBundle);  // Free the bundle itself
}

// Helper to iterate and free bundle data before clear/destroy
static void freeAllWriteMetricsBundles(SHashObj *pHash) {
  if (pHash == NULL) return;
  void *cursor = NULL;
  void *pData = NULL;
  while ((pData = taosHashIterate(pHash, &cursor)) != NULL) {
    freeWriteMetricsBundle(pData);
  }
}

// --- Global Manager ---

static SMetricsManager gMetricsManager = {0};

// --- Init/Destroy ---

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

  return 0;

_err:
  destroyMetricsManager();
  return code;
}

static void destroyMetricsManager() {
  freeAllWriteMetricsBundles(gMetricsManager.pWriteMetrics);
  taosArrayDestroy(gMetricsManager.pDnodeMetrics);
  taosHashClear(gMetricsManager.pWriteMetrics);
  taosHashClear(gMetricsManager.pQueryMetrics);
  taosHashClear(gMetricsManager.pStreamMetrics);
}

// --- Metric Update Helper ---

// Updates the 'formatted' SWriteMetricsEx from the 'raw' SRawWriteMetrics
static void updateFormattedMetrics(SWriteMetricsBundle *pBundle) {
  if (pBundle == NULL) return;

  SRawWriteMetrics *raw = &pBundle->raw;
  SWriteMetricsEx  *fmt = &pBundle->formatted;

  fmt->vgId = pBundle->vgId;  // Ensure vgId is set in formatted struct too

  // --- Update formatted SMetric fields from raw values ---
  // Integers
  setMetricInt64(&fmt->total_requests, raw->total_requests);
  setMetricInt64(&fmt->total_rows, raw->total_rows);
  setMetricInt64(&fmt->total_bytes, raw->total_bytes);
  setMetricInt64(&fmt->rpc_queue_wait, raw->rpc_queue_wait);
  setMetricInt64(&fmt->preprocess_time, raw->preprocess_time);
  setMetricInt64(&fmt->memory_table_size, raw->memory_table_size);
  setMetricInt64(&fmt->memory_table_rows, raw->memory_table_rows);
  setMetricInt64(&fmt->commit_count, raw->commit_count);
  setMetricInt64(&fmt->auto_commit_count, raw->auto_commit_count);
  setMetricInt64(&fmt->forced_commit_count, raw->forced_commit_count);
  setMetricInt64(&fmt->stt_trigger_value, raw->stt_trigger_value);
  setMetricInt64(&fmt->merge_count, raw->merge_count);
  setMetricInt64(&fmt->blocked_commits, raw->blocked_commits);
  setMetricInt64(&fmt->memtable_wait_time, raw->memtable_wait_time);

  // Doubles (Averages, Ratios, Rates - calculate if needed)
  // Example calculation for average commit time:
  double avg_commit_time = (raw->commit_count > 0) ? (raw->commit_time_sum / raw->commit_count) : 0.0;
  setMetricDouble(&fmt->avg_commit_time, avg_commit_time);

  double avg_merge_time = (raw->merge_count > 0) ? (raw->merge_time_sum / raw->merge_count) : 0.0;
  setMetricDouble(&fmt->avg_merge_time, avg_merge_time);

  // Example: Direct mapping if already calculated or stored as double
  setMetricDouble(&fmt->avg_write_size, raw->avg_write_size);
  setMetricDouble(&fmt->cache_hit_ratio, raw->cache_hit_ratio);

  // Rates need calculation based on time intervals and raw byte counts (e.g., wal_write_bytes)
  // These raw fields need to be added to SRawWriteMetrics first.
  // setMetricDouble(&fmt->wal_write_rate, calculated_wal_rate);
  // setMetricDouble(&fmt->sync_rate, calculated_sync_rate);
  // setMetricDouble(&fmt->apply_rate, calculated_apply_rate);

  // TODO: Add calculations for rates (wal_write_rate, sync_rate, apply_rate)
  // For now, set them to 0 or keep previous value if lazy update needed.
  // Assuming initWriteMetricsEx was called initially, they have default values.
  // setMetricDouble(&fmt->wal_write_rate, 0.0);
  // setMetricDouble(&fmt->sync_rate, 0.0);
  // setMetricDouble(&fmt->apply_rate, 0.0);

  // Mark as up-to-date if using lazy updates
  // pBundle->isFormattedUpToDate = true;
}

// --- Public API Functions ---

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

// Adds/Updates raw metrics for a vgId
int32_t addWriteMetrics(int32_t vgId, const SRawWriteMetrics *pRawMetrics) {
  if (pRawMetrics == NULL || gMetricsManager.pWriteMetrics == NULL) return TSDB_CODE_INVALID_PARA;

  // Try to find existing bundle
  SWriteMetricsBundle *pBundle = (SWriteMetricsBundle *)taosHashGet(gMetricsManager.pWriteMetrics, &vgId, sizeof(vgId));

  if (pBundle == NULL) {
    // Not found, create a new bundle
    pBundle = (SWriteMetricsBundle *)taosMemoryMalloc(sizeof(SWriteMetricsBundle));
    if (pBundle == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    memset(pBundle, 0, sizeof(SWriteMetricsBundle));  // Zero initialize
    pBundle->vgId = vgId;
    initWriteMetricsEx(&pBundle->formatted);  // Initialize formatted part once

    // Copy initial raw data
    memcpy(&pBundle->raw, pRawMetrics, sizeof(SRawWriteMetrics));

    // Add the new bundle to the hash table
    int32_t code = taosHashPut(gMetricsManager.pWriteMetrics, &vgId, sizeof(vgId), pBundle, sizeof(SWriteMetricsBundle));
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(pBundle);  // Free if put failed
      return code;
    }
  } else {
    // Found, just update the raw data part
    // For counters/sums, you might want to += instead of memcpy
    // Assuming pRawMetrics contains the *latest absolute* values or sums
    memcpy(&pBundle->raw, pRawMetrics, sizeof(SRawWriteMetrics));
    // Mark formatted data as stale if using lazy updates
    // pBundle->isFormattedUpToDate = false;
  }

  return TSDB_CODE_SUCCESS;
}

// Gets the formatted metrics, updating from raw if needed
SWriteMetricsEx *getWriteMetricsByVgId(int32_t vgId) {
  if (gMetricsManager.pWriteMetrics == NULL) return NULL;

  SWriteMetricsBundle *pBundle = (SWriteMetricsBundle *)taosHashGet(gMetricsManager.pWriteMetrics, &vgId, sizeof(vgId));
  if (pBundle == NULL) {
    return NULL;  // Not found
  }

  // Update the formatted part from raw data before returning
  // If using lazy update, check flag: if (!pBundle->isFormattedUpToDate)
  updateFormattedMetrics(pBundle);

  return &pBundle->formatted;
}

void cleanupMetrics() { destroyMetricsManager(); }
