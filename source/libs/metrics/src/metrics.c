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
#include "talloc.h"
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
#include "vnodeInt.h"

extern int32_t vnodeGetWriteMetricsEx(void *pVnode, SWriteMetricsEx *pMetrics);

// --- Hash Table Functions for SWriteMetricsBundle ---

// Hash function for SWriteMetricsBundle key (vgId)
static uint32_t writeMetricsHashFn(const void *key, uint32_t keyLen, uint32_t seed) {
  if (keyLen != sizeof(int32_t)) return 0;
  return taosHashDo(key, keyLen, seed);
}

// Key compare function (Key vs Bundle Data)
static int32_t writeMetricsKeyCompareFn(const void *key, int32_t keyLen, const void *pData) {
  if (keyLen != sizeof(int32_t)) return -1;
  int32_t                    keyVgId = *(const int32_t *)key;
  const SWriteMetricsBundle *pBundle = (const SWriteMetricsBundle *)pData;
  if (keyVgId < pBundle->vgId) return -1;
  if (keyVgId > pBundle->vgId) return 1;
  return 0;
}

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
  gMetricsManager.pWriteMetrics = taosHashInit(8, writeMetricsHashFn, writeMetricsKeyCompareFn, true, SHASH_LOCK_MUTEX);
  if (gMetricsManager.pWriteMetrics == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  gMetricsManager.pQueryMetrics = taosArrayInit(8, sizeof(SQueryMetricsEx));
  if (gMetricsManager.pQueryMetrics == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  gMetricsManager.pStreamMetrics = taosArrayInit(8, sizeof(SStreamMetricsEx));
  if (gMetricsManager.pStreamMetrics == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  return 0;

_err:
  if (gMetricsManager.pDnodeMetrics) {
    taosArrayDestroy(gMetricsManager.pDnodeMetrics);
    gMetricsManager.pDnodeMetrics = NULL;
  }

  if (gMetricsManager.pWriteMetrics) {
    freeAllWriteMetricsBundles(gMetricsManager.pWriteMetrics);
    taosHashClear(gMetricsManager.pWriteMetrics);
    taosHashDestroy(gMetricsManager.pWriteMetrics);
    gMetricsManager.pWriteMetrics = NULL;
  }

  if (gMetricsManager.pQueryMetrics) {
    taosArrayDestroy(gMetricsManager.pQueryMetrics);
    gMetricsManager.pQueryMetrics = NULL;
  }

  if (gMetricsManager.pStreamMetrics) {
    taosArrayDestroy(gMetricsManager.pStreamMetrics);
    gMetricsManager.pStreamMetrics = NULL;
  }

  return code;
}

static void destroyMetricsManager() {
  if (gMetricsManager.pDnodeMetrics) {
    taosArrayDestroy(gMetricsManager.pDnodeMetrics);
    gMetricsManager.pDnodeMetrics = NULL;
  }

  if (gMetricsManager.pWriteMetrics) {
    freeAllWriteMetricsBundles(gMetricsManager.pWriteMetrics);
    taosHashClear(gMetricsManager.pWriteMetrics);
    taosHashDestroy(gMetricsManager.pWriteMetrics);
    gMetricsManager.pWriteMetrics = NULL;
  }

  if (gMetricsManager.pQueryMetrics) {
    taosArrayDestroy(gMetricsManager.pQueryMetrics);
    gMetricsManager.pQueryMetrics = NULL;
  }

  if (gMetricsManager.pStreamMetrics) {
    taosArrayDestroy(gMetricsManager.pStreamMetrics);
    gMetricsManager.pStreamMetrics = NULL;
  }
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
    int32_t code = taosHashPut(gMetricsManager.pWriteMetrics, pBundle);
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

// Collects RAW metrics from all vnodes and updates the manager
int32_t logAllVnodeMetrics(SVnodeMetricsLogFn fnGetVnode, VnodeGetRawMetricsFn fnGetRawMetrics) {
  if (fnGetVnode == NULL || fnGetRawMetrics == NULL || gMetricsManager.pWriteMetrics == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Manual free and clear only if we want to remove non-existent vnodes
  // freeAllWriteMetricsBundles(gMetricsManager.pWriteMetrics);
  // taosHashClear(gMetricsManager.pWriteMetrics);

  void *pIter = NULL;
  void *pVnodeRaw = NULL;  // Use a generic pointer from iterator

  while ((pVnodeRaw = fnGetVnode(&pIter)) != NULL) {
    SRawWriteMetrics rawMetrics = {0};
    SVnode          *pVnode = (SVnode *)pVnodeRaw;  // Cast to SVnode to access config.vgId

    if (fnGetRawMetrics(pVnodeRaw, &rawMetrics) == 0) {
      addWriteMetrics(pVnode->config.vgId, &rawMetrics);
    }
  }
  return 0;
}

// Resets all metrics data
void resetAllVnodeMetrics() {
  if (gMetricsManager.pWriteMetrics == NULL) return;
  // Free existing bundle data first, then clear the hash table structure.
  freeAllWriteMetricsBundles(gMetricsManager.pWriteMetrics);
  taosHashClear(gMetricsManager.pWriteMetrics);
}

// Formats JSON from the *formatted* metrics
int32_t formatMetricsToJson(int32_t vgId, char *buffer, int32_t bufferSize) {
  if (buffer == NULL || bufferSize <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  // getWriteMetricsByVgId ensures the formatted data is up-to-date
  SWriteMetricsEx *pMetrics = getWriteMetricsByVgId(vgId);
  if (pMetrics == NULL) {
    return TSDB_CODE_NOT_FOUND;
  }

  // Formatting logic uses the SMetric fields from pMetrics
  int32_t len =
      snprintf(buffer, bufferSize,
               "{\"vgId\":%d,\"total_requests\":%" PRId64 ",\"total_rows\":%" PRId64 ",\"total_bytes\":%" PRId64
               ","\"avg_write_size\":%f,\"preprocess_time\":%" PRId64 ",\"memory_table_size\":%" PRId64
                   ","\"commit_count\":%" PRId64 ",\"merge_count\":%" PRId64
                       ",\"avg_commit_time\":%f,"
                       "\"avg_merge_time\":%f,\"blocked_commits\":%" PRId64 ",\"memtable_wait_time\":%" PRId64 "}",
               pMetrics->vgId, getMetricInt64(&pMetrics->total_requests), getMetricInt64(&pMetrics->total_rows),
               getMetricInt64(&pMetrics->total_bytes), getMetricDouble(&pMetrics->avg_write_size),
               getMetricInt64(&pMetrics->preprocess_time), getMetricInt64(&pMetrics->memory_table_size),
               getMetricInt64(&pMetrics->commit_count), getMetricInt64(&pMetrics->merge_count),
               getMetricDouble(&pMetrics->avg_commit_time), getMetricDouble(&pMetrics->avg_merge_time),
               getMetricInt64(&pMetrics->blocked_commits), getMetricInt64(&pMetrics->memtable_wait_time));

  return (len > 0) ? TSDB_CODE_SUCCESS : TSDB_CODE_APP_ERROR;
}

// Iterates through bundles, updates formatted data, and calls callback
int32_t forEachMetric(MetricsLogCallback callback, void *param) {
  if (callback == NULL || gMetricsManager.pWriteMetrics == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t              code = 0;
  void                *cursor = NULL;
  SWriteMetricsBundle *pBundle = NULL;

  while ((pBundle = (SWriteMetricsBundle *)taosHashIterate(gMetricsManager.pWriteMetrics, &cursor)) != NULL) {
    // Update formatted data just before using it
    // If using lazy update, check flag: if (!pBundle->isFormattedUpToDate)
    updateFormattedMetrics(pBundle);

    char jsonBuffer[2048] = {0};
    // Use the formatted vgId for safety
    if (pBundle->formatted.vgId > 0) {
      // formatMetricsToJson now implicitly uses the updated formatted data via getWriteMetricsByVgId
      // OR, we can call it directly using the pBundle->formatted we already have:
      // Direct formatting (avoids another hash lookup inside formatMetricsToJson)
      snprintf(jsonBuffer, sizeof(jsonBuffer), /* ... format string ... */, pBundle->formatted.vgId,
               getMetricInt64(&pBundle->formatted.total_requests),
               /* ... etc ... */);
      // For simplicity, stick with calling formatMetricsToJson which does the lookup again
      if (formatMetricsToJson(pBundle->formatted.vgId, jsonBuffer, sizeof(jsonBuffer)) == TSDB_CODE_SUCCESS) {
        code = callback(jsonBuffer, param);
        if (code != TSDB_CODE_SUCCESS) {
          break;
        }
      }
    }
  }
  // Assume no explicit iterator destroy needed

  return code;
}

// --- String Builder & getAllMetricsJson ---

// Structure for string building
typedef struct {
  char  *buf;
  size_t len;
  size_t cap;
} SStringBuilder;

// Initialize string builder
static int32_t sb_init(SStringBuilder *sb, size_t capacity) {
  sb->buf = taosMemoryCalloc(1, capacity);
  if (sb->buf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  sb->len = 0;
  sb->cap = capacity;
  return TSDB_CODE_SUCCESS;
}

// Append string to string builder
static int32_t sb_append(SStringBuilder *sb, const char *str) {
  size_t str_len = strlen(str);

  // Check if we need to resize
  if (sb->len + str_len + 1 > sb->cap) {
    size_t new_cap = sb->cap * 2;
    if (new_cap < sb->len + str_len + 1) {
      new_cap = sb->len + str_len + 1024;  // Add extra space
    }

    char *new_buf = taosMemoryRealloc(sb->buf, new_cap);
    if (new_buf == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    sb->buf = new_buf;
    sb->cap = new_cap;
  }

  // Append the string
  memcpy(sb->buf + sb->len, str, str_len);
  sb->len += str_len;
  sb->buf[sb->len] = '\0';

  return TSDB_CODE_SUCCESS;
}

// Free string builder
static void sb_free(SStringBuilder *sb) {
  if (sb->buf) {
    taosMemoryFree(sb->buf);
    sb->buf = NULL;
  }
  sb->len = 0;
  sb->cap = 0;
}

// Callback function to append metrics to a string builder
static int32_t appendMetricsCallback(const char *jsonMetrics, void *param) {
  SStringBuilder *sb = (SStringBuilder *)param;

  // Add comma if not the first item
  if (sb->len > 1) {  // > 1 because we start with "["
    int32_t code = sb_append(sb, ",");
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return sb_append(sb, jsonMetrics);
}

// Function to get all metrics as a JSON array
int32_t getAllMetricsJson(char **pJson) {
  if (pJson == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (gMetricsManager.pWriteMetrics == NULL) {
    *pJson = strdup("[]");
    return (*pJson != NULL) ? TSDB_CODE_SUCCESS : TSDB_CODE_OUT_OF_MEMORY;
  }

  int size = taosHashGetSize(gMetricsManager.pWriteMetrics);
  if (size == 0) {
    *pJson = strdup("[]");
    return (*pJson != NULL) ? TSDB_CODE_SUCCESS : TSDB_CODE_OUT_OF_MEMORY;
  }

  // Initialize string builder with estimated size
  int32_t        estSize = size * 2048 + 8;
  SStringBuilder sb = {0};
  int32_t        code = sb_init(&sb, estSize);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // Start with opening bracket
  code = sb_append(&sb, "[");
  if (code != TSDB_CODE_SUCCESS) {
    sb_free(&sb);
    return code;
  }

  // Use the forEachMetric function with our callback
  // forEachMetric now ensures formatted data is updated internally
  code = forEachMetric(appendMetricsCallback, &sb);

  if (code == TSDB_CODE_SUCCESS) {
    // Add closing bracket
    code = sb_append(&sb, "]");

    if (code == TSDB_CODE_SUCCESS) {
      // Ensure null termination before strdup
      // sb_append ensures this, but double-check for safety
      if (sb.len < sb.cap) {
        sb.buf[sb.len] = '\0';
        *pJson = strdup(sb.buf);
        if (*pJson == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
        }
      } else {
        // Buffer was exactly full, should not happen with sb_append logic
        code = TSDB_CODE_APP_ERROR;
      }
    }
  }

  sb_free(&sb);

  return code;
}
