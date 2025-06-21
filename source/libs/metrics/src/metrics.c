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
#include "metricsInt.h"

// Metric name definitions following monFramework.c pattern
#define WRITE_TABLE                   "taosd_write_metrics"
#define WRITE_TOTAL_REQUESTS          WRITE_TABLE ":total_requests"
#define WRITE_TOTAL_ROWS              WRITE_TABLE ":total_rows"
#define WRITE_TOTAL_BYTES             WRITE_TABLE ":total_bytes"
#define WRITE_FETCH_BATCH_META_TIME   WRITE_TABLE ":fetch_batch_meta_time"
#define WRITE_FETCH_BATCH_META_COUNT  WRITE_TABLE ":fetch_batch_meta_count"
#define WRITE_PREPROCESS_TIME         WRITE_TABLE ":preprocess_time"
#define WRITE_WAL_WRITE_BYTES         WRITE_TABLE ":wal_write_bytes"
#define WRITE_WAL_WRITE_TIME          WRITE_TABLE ":wal_write_time"
#define WRITE_APPLY_BYTES             WRITE_TABLE ":apply_bytes"
#define WRITE_APPLY_TIME              WRITE_TABLE ":apply_time"
#define WRITE_COMMIT_COUNT            WRITE_TABLE ":commit_count"
#define WRITE_COMMIT_TIME             WRITE_TABLE ":commit_time"
#define WRITE_MEMTABLE_WAIT_TIME      WRITE_TABLE ":memtable_wait_time"
#define WRITE_BLOCK_COMMIT_COUNT      WRITE_TABLE ":block_commit_count"
#define WRITE_BLOCKED_COMMIT_TIME     WRITE_TABLE ":blocked_commit_time"
#define WRITE_MERGE_COUNT             WRITE_TABLE ":merge_count"
#define WRITE_MERGE_TIME              WRITE_TABLE ":merge_time"
#define WRITE_LAST_CACHE_COMMIT_TIME  WRITE_TABLE ":last_cache_commit_time"
#define WRITE_LAST_CACHE_COMMIT_COUNT WRITE_TABLE ":last_cache_commit_count"

#define DNODE_TABLE                    "taosd_dnode_metrics"
#define DNODE_RPC_QUEUE_MEMORY_ALLOWED DNODE_TABLE ":rpc_queue_memory_allowed"
#define DNODE_RPC_QUEUE_MEMORY_USED    DNODE_TABLE ":rpc_queue_memory_used"
#define DNODE_APPLY_MEMORY_ALLOWED     DNODE_TABLE ":apply_memory_allowed"
#define DNODE_APPLY_MEMORY_USED        DNODE_TABLE ":apply_memory_used"

// Global variables definition
SMetricsManager gMetricsManager = {0};

taos_counter_t *dnode_rpc_queue_memory_allowed = NULL;
taos_counter_t *dnode_rpc_queue_memory_used = NULL;
taos_counter_t *dnode_apply_memory_allowed = NULL;
taos_counter_t *dnode_apply_memory_used = NULL;

void destroyVnodeMetricsCounters(SVnodeMetricsCounters *pCounters) {
  if (pCounters == NULL) return;

  // Note: We don't need to explicitly destroy taos_counter_t objects
  // as they are managed by the monitorfw registry
  taosMemoryFree(pCounters);
}

void destroyVnodeMetricsCountersWrapper(void *p) {
  SVnodeMetricsCounters **ppCounters = (SVnodeMetricsCounters **)p;
  if (ppCounters && *ppCounters) {
    destroyVnodeMetricsCounters(*ppCounters);
  }
}

SVnodeMetricsCounters *createVnodeMetricsCounters(int32_t vgId, int32_t dnodeId, int64_t clusterId,
                                                  const char *dbname) {
  SVnodeMetricsCounters *pCounters = taosMemoryCalloc(1, sizeof(SVnodeMetricsCounters));
  if (pCounters == NULL) {
    return NULL;
  }

  pCounters->vgId = vgId;
  pCounters->dnodeId = dnodeId;
  pCounters->clusterId = clusterId;
  tstrncpy(pCounters->dbname, dbname ? dbname : "", TSDB_DB_NAME_LEN);

  // Prepare label values for this vnode
  char clusterIdStr[32], dnodeIdStr[32], vgIdStr[32];
  snprintf(clusterIdStr, sizeof(clusterIdStr), "%" PRId64, clusterId);
  snprintf(dnodeIdStr, sizeof(dnodeIdStr), "%d", dnodeId);
  snprintf(vgIdStr, sizeof(vgIdStr), "%d", vgId);
  const char *write_labels[] = {"cluster_id", "dnode_id", "vg_id", "db_name"};
  const char *label_values[] = {clusterIdStr, dnodeIdStr, vgIdStr, pCounters->dbname};

  // Create unique metric names for this vnode
  char metricName[256];

#define CREATE_VNODE_COUNTER(field, base_name, desc)                                       \
  do {                                                                                     \
    snprintf(metricName, sizeof(metricName), "%s_vg%d", base_name, vgId);                  \
    pCounters->field = taos_counter_new(metricName, desc, 4, write_labels);                \
    if (pCounters->field == NULL) goto _error;                                             \
    if (taos_collector_registry_register_metric((taos_metric_t *)pCounters->field) == 1) { \
      taos_counter_destroy(pCounters->field);                                              \
      pCounters->field = NULL;                                                             \
      goto _error;                                                                         \
    }                                                                                      \
  } while (0)

  CREATE_VNODE_COUNTER(total_requests, WRITE_TOTAL_REQUESTS, "Total write requests");
  CREATE_VNODE_COUNTER(total_rows, WRITE_TOTAL_ROWS, "Total rows written");
  CREATE_VNODE_COUNTER(total_bytes, WRITE_TOTAL_BYTES, "Total bytes written");
  CREATE_VNODE_COUNTER(fetch_batch_meta_time, WRITE_FETCH_BATCH_META_TIME, "Fetch batch meta time");
  CREATE_VNODE_COUNTER(fetch_batch_meta_count, WRITE_FETCH_BATCH_META_COUNT, "Fetch batch meta count");
  CREATE_VNODE_COUNTER(preprocess_time, WRITE_PREPROCESS_TIME, "Preprocess time");
  CREATE_VNODE_COUNTER(wal_write_bytes, WRITE_WAL_WRITE_BYTES, "WAL write bytes");
  CREATE_VNODE_COUNTER(wal_write_time, WRITE_WAL_WRITE_TIME, "WAL write time");
  CREATE_VNODE_COUNTER(apply_bytes, WRITE_APPLY_BYTES, "Apply bytes");
  CREATE_VNODE_COUNTER(apply_time, WRITE_APPLY_TIME, "Apply time");
  CREATE_VNODE_COUNTER(commit_count, WRITE_COMMIT_COUNT, "Commit count");
  CREATE_VNODE_COUNTER(commit_time, WRITE_COMMIT_TIME, "Commit time");
  CREATE_VNODE_COUNTER(memtable_wait_time, WRITE_MEMTABLE_WAIT_TIME, "Memtable wait time");
  CREATE_VNODE_COUNTER(block_commit_count, WRITE_BLOCK_COMMIT_COUNT, "Block commit count");
  CREATE_VNODE_COUNTER(blocked_commit_time, WRITE_BLOCKED_COMMIT_TIME, "Blocked commit time");
  CREATE_VNODE_COUNTER(merge_count, WRITE_MERGE_COUNT, "Merge count");
  CREATE_VNODE_COUNTER(merge_time, WRITE_MERGE_TIME, "Merge time");
  CREATE_VNODE_COUNTER(last_cache_commit_time, WRITE_LAST_CACHE_COMMIT_TIME, "Last cache commit time");
  CREATE_VNODE_COUNTER(last_cache_commit_count, WRITE_LAST_CACHE_COMMIT_COUNT, "Last cache commit count");

#undef CREATE_VNODE_COUNTER

  return pCounters;

_error:
  destroyVnodeMetricsCounters(pCounters);
  return NULL;
}

int32_t initMetricsManager() {
  if (gMetricsManager.initialized) {
    return TSDB_CODE_SUCCESS;
  }

  // Initialize monitorfw framework
  taos_collector_registry_default_init();

  // Initialize write metrics hash table
  gMetricsManager.pWriteMetrics =
      taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (gMetricsManager.pWriteMetrics == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosHashSetFreeFp(gMetricsManager.pWriteMetrics, destroyVnodeMetricsCountersWrapper);

  // Initialize rwlock
  if (taosThreadRwlockInit(&gMetricsManager.lock, NULL) != 0) {
    taosHashCleanup(gMetricsManager.pWriteMetrics);
    gMetricsManager.pWriteMetrics = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // Initialize global dnode counters
  const char *dnode_labels[] = {"cluster_id", "dnode_id"};
  dnode_rpc_queue_memory_allowed = taos_collector_registry_must_register_metric(
      taos_counter_new(DNODE_RPC_QUEUE_MEMORY_ALLOWED, "RPC queue memory allowed", 2, dnode_labels));
  dnode_rpc_queue_memory_used = taos_collector_registry_must_register_metric(
      taos_counter_new(DNODE_RPC_QUEUE_MEMORY_USED, "RPC queue memory used", 2, dnode_labels));
  dnode_apply_memory_allowed = taos_collector_registry_must_register_metric(
      taos_counter_new(DNODE_APPLY_MEMORY_ALLOWED, "Apply memory allowed", 2, dnode_labels));
  dnode_apply_memory_used = taos_collector_registry_must_register_metric(
      taos_counter_new(DNODE_APPLY_MEMORY_USED, "Apply memory used", 2, dnode_labels));

  gMetricsManager.initialized = true;
  return TSDB_CODE_SUCCESS;
}

void cleanupMetrics() {
  if (!gMetricsManager.initialized) {
    return;
  }

  taosThreadRwlockWrlock(&gMetricsManager.lock);

  if (gMetricsManager.pWriteMetrics) {
    taosHashCleanup(gMetricsManager.pWriteMetrics);
    gMetricsManager.pWriteMetrics = NULL;
  }

  gMetricsManager.initialized = false;

  taosThreadRwlockUnlock(&gMetricsManager.lock);
  taosThreadRwlockDestroy(&gMetricsManager.lock);

  // monitorfw handles cleanup automatically
}

int32_t addWriteMetrics(int32_t vgId, int32_t dnodeId, int64_t clusterId, const char *dbname,
                        const SRawWriteMetrics *pRawMetrics) {
  if (pRawMetrics == NULL || !gMetricsManager.initialized) {
    return TSDB_CODE_INVALID_PARA;
  }

  taosThreadRwlockRdlock(&gMetricsManager.lock);

  // Get or create vnode metrics counters
  SVnodeMetricsCounters **ppCounters =
      (SVnodeMetricsCounters **)taosHashGet(gMetricsManager.pWriteMetrics, &vgId, sizeof(vgId));
  SVnodeMetricsCounters *pCounters = NULL;

  if (ppCounters == NULL) {
    // Need to create new counters - upgrade to write lock
    taosThreadRwlockUnlock(&gMetricsManager.lock);
    taosThreadRwlockWrlock(&gMetricsManager.lock);

    // Check again after getting write lock
    ppCounters = (SVnodeMetricsCounters **)taosHashGet(gMetricsManager.pWriteMetrics, &vgId, sizeof(vgId));
    if (ppCounters == NULL) {
      pCounters = createVnodeMetricsCounters(vgId, dnodeId, clusterId, dbname);
      if (pCounters == NULL) {
        taosThreadRwlockUnlock(&gMetricsManager.lock);
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      int32_t code =
          taosHashPut(gMetricsManager.pWriteMetrics, &vgId, sizeof(vgId), &pCounters, sizeof(SVnodeMetricsCounters *));
      if (code != TSDB_CODE_SUCCESS) {
        destroyVnodeMetricsCounters(pCounters);
        taosThreadRwlockUnlock(&gMetricsManager.lock);
        return code;
      }
    } else {
      pCounters = *ppCounters;
    }
  } else {
    pCounters = *ppCounters;
  }

  // Prepare label values
  char clusterIdStr[32], dnodeIdStr[32], vgIdStr[32];
  snprintf(clusterIdStr, sizeof(clusterIdStr), "%" PRId64, clusterId);
  snprintf(dnodeIdStr, sizeof(dnodeIdStr), "%d", dnodeId);
  snprintf(vgIdStr, sizeof(vgIdStr), "%d", vgId);
  const char *label_values[] = {clusterIdStr, dnodeIdStr, vgIdStr, dbname ? dbname : ""};

  // Update counters using monitorfw
  taos_counter_add(pCounters->total_requests, (double)pRawMetrics->total_requests, label_values);
  taos_counter_add(pCounters->total_rows, (double)pRawMetrics->total_rows, label_values);
  taos_counter_add(pCounters->total_bytes, (double)pRawMetrics->total_bytes, label_values);
  taos_counter_add(pCounters->fetch_batch_meta_time, (double)pRawMetrics->fetch_batch_meta_time, label_values);
  taos_counter_add(pCounters->fetch_batch_meta_count, (double)pRawMetrics->fetch_batch_meta_count, label_values);
  taos_counter_add(pCounters->preprocess_time, (double)pRawMetrics->preprocess_time, label_values);
  taos_counter_add(pCounters->wal_write_bytes, (double)pRawMetrics->wal_write_bytes, label_values);
  taos_counter_add(pCounters->wal_write_time, (double)pRawMetrics->wal_write_time, label_values);
  taos_counter_add(pCounters->apply_bytes, (double)pRawMetrics->apply_bytes, label_values);
  taos_counter_add(pCounters->apply_time, (double)pRawMetrics->apply_time, label_values);
  taos_counter_add(pCounters->commit_count, (double)pRawMetrics->commit_count, label_values);
  taos_counter_add(pCounters->commit_time, (double)pRawMetrics->commit_time, label_values);
  taos_counter_add(pCounters->memtable_wait_time, (double)pRawMetrics->memtable_wait_time, label_values);
  taos_counter_add(pCounters->block_commit_count, (double)pRawMetrics->block_commit_count, label_values);
  taos_counter_add(pCounters->blocked_commit_time, (double)pRawMetrics->blocked_commit_time, label_values);
  taos_counter_add(pCounters->merge_count, (double)pRawMetrics->merge_count, label_values);
  taos_counter_add(pCounters->merge_time, (double)pRawMetrics->merge_time, label_values);
  taos_counter_add(pCounters->last_cache_commit_time, (double)pRawMetrics->last_cache_commit_time, label_values);
  taos_counter_add(pCounters->last_cache_commit_count, (double)pRawMetrics->last_cache_commit_count, label_values);

  taosThreadRwlockUnlock(&gMetricsManager.lock);
  return TSDB_CODE_SUCCESS;
}

int32_t addDnodeMetrics(const SRawDnodeMetrics *pRawMetrics) {
  if (pRawMetrics == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Prepare label values
  const char *label_values[] = {"0", "0"};  // TODO: Get actual cluster_id and dnode_id

  // Update counters using monitorfw
  taos_counter_add(dnode_rpc_queue_memory_allowed, (double)pRawMetrics->rpcQueueMemoryAllowed, label_values);
  taos_counter_add(dnode_rpc_queue_memory_used, (double)pRawMetrics->rpcQueueMemoryUsed, label_values);
  taos_counter_add(dnode_apply_memory_allowed, (double)pRawMetrics->applyMemoryAllowed, label_values);
  taos_counter_add(dnode_apply_memory_used, (double)pRawMetrics->applyMemoryUsed, label_values);

  return TSDB_CODE_SUCCESS;
}

// Backward compatibility function to get vnode metrics by vgId
SVnodeMetricsCounters *getVnodeMetricsCounters(int32_t vgId) {
  if (!gMetricsManager.initialized) {
    return NULL;
  }

  taosThreadRwlockRdlock(&gMetricsManager.lock);
  SVnodeMetricsCounters **ppCounters =
      (SVnodeMetricsCounters **)taosHashGet(gMetricsManager.pWriteMetrics, &vgId, sizeof(vgId));
  SVnodeMetricsCounters *pCounters = ppCounters ? *ppCounters : NULL;
  taosThreadRwlockUnlock(&gMetricsManager.lock);

  return pCounters;
}

// Clean expired metrics for vnodes that no longer exist
int32_t cleanExpiredWriteMetrics(SHashObj *pValidVgroups) {
  if (!gMetricsManager.initialized || pValidVgroups == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SArray *pVgIdsToRemove = taosArrayInit(16, sizeof(int32_t));
  if (pVgIdsToRemove == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  taosThreadRwlockRdlock(&gMetricsManager.lock);

  void *pIter = taosHashIterate(gMetricsManager.pWriteMetrics, NULL);
  while (pIter != NULL) {
    SVnodeMetricsCounters **ppCounters = (SVnodeMetricsCounters **)pIter;
    if (ppCounters && *ppCounters) {
      int32_t vgId = (*ppCounters)->vgId;
      void *pFound = taosHashGet(pValidVgroups, &vgId, sizeof(int32_t));
      if (pFound == NULL) {
        if (taosArrayPush(pVgIdsToRemove, &vgId) == NULL) {
          uError("Failed to add vgId:%d for removal", vgId);
        }
      }
    }
    pIter = taosHashIterate(gMetricsManager.pWriteMetrics, pIter);
  }

  taosThreadRwlockUnlock(&gMetricsManager.lock);

  // Remove expired metrics
  if (taosArrayGetSize(pVgIdsToRemove) > 0) {
    taosThreadRwlockWrlock(&gMetricsManager.lock);

    for (int32_t i = 0; i < taosArrayGetSize(pVgIdsToRemove); i++) {
      int32_t *pVgId = taosArrayGet(pVgIdsToRemove, i);
      if (pVgId) {
        if (taosHashRemove(gMetricsManager.pWriteMetrics, pVgId, sizeof(int32_t)) == 0) {
          uTrace("Removed expired write metrics for vgId: %d", *pVgId);
        } else {
          uError("Failed to remove expired write metrics for vgId: %d", *pVgId);
        }
      }
    }

    taosThreadRwlockUnlock(&gMetricsManager.lock);
  }

  taosArrayDestroy(pVgIdsToRemove);
  return TSDB_CODE_SUCCESS;
}
