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

// Global shared write metrics counters
taos_counter_t *write_total_requests = NULL;
taos_counter_t *write_total_rows = NULL;
taos_counter_t *write_total_bytes = NULL;
taos_counter_t *write_fetch_batch_meta_time = NULL;
taos_counter_t *write_fetch_batch_meta_count = NULL;
taos_counter_t *write_preprocess_time = NULL;
taos_counter_t *write_wal_write_bytes = NULL;
taos_counter_t *write_wal_write_time = NULL;
taos_counter_t *write_apply_bytes = NULL;
taos_counter_t *write_apply_time = NULL;
taos_counter_t *write_commit_count = NULL;
taos_counter_t *write_commit_time = NULL;
taos_counter_t *write_memtable_wait_time = NULL;
taos_counter_t *write_blocked_commit_count = NULL;
taos_counter_t *write_blocked_commit_time = NULL;
taos_counter_t *write_merge_count = NULL;
taos_counter_t *write_merge_time = NULL;
taos_counter_t *write_last_cache_commit_time = NULL;
taos_counter_t *write_last_cache_commit_count = NULL;

// Global dnode metrics counters
taos_gauge_t *dnode_rpc_queue_memory_allowed = NULL;
taos_gauge_t *dnode_rpc_queue_memory_used = NULL;
taos_gauge_t *dnode_apply_memory_allowed = NULL;
taos_gauge_t *dnode_apply_memory_used = NULL;

// Helper function to clean expired metrics from a counter
static void cleanExpiredCounterMetrics(taos_counter_t *counter, SHashObj *pValidVgroups, const char *counterName) {
  if (counter == NULL || pValidVgroups == NULL) {
    return;
  }

  int list_size = taos_counter_get_keys_size(counter);
  if (list_size == 0) return;

  int32_t *vgroup_ids = NULL;
  char   **keys = NULL;
  int      r = taos_counter_get_vgroup_ids(counter, &keys, &vgroup_ids, &list_size);
  if (r) {
    if (keys) taosMemoryFree(keys);
    if (vgroup_ids) taosMemoryFree(vgroup_ids);
    uError("failed to get vgroup ids for counter %s", counterName);
    return;
  }

  for (int i = 0; i < list_size; i++) {
    int32_t vgroup_id = vgroup_ids[i];
    void   *vnode = taosHashGet(pValidVgroups, &vgroup_id, sizeof(int32_t));
    if (vnode == NULL) {
      // VGroup doesn't exist, delete the metrics for this vgroup
      METRICS_DELETE_COUNTER(counter, keys[i]);
    }
  }

  if (vgroup_ids) taosMemoryFree(vgroup_ids);
  if (keys) taosMemoryFree(keys);
}

int32_t initMetricsManager() {
  taos_collector_registry_default_init();

  // Initialize global shared write metrics counters
  const char *write_labels[] = {"metric_type", "cluster_id", "dnode_id", "dnode_ep", "vgroup_id", "database_name"};
  write_total_requests = taos_collector_registry_must_register_metric(
      taos_counter_new(WRITE_TOTAL_REQUESTS, "Total write requests", 6, write_labels));
  write_total_rows = taos_collector_registry_must_register_metric(
      taos_counter_new(WRITE_TOTAL_ROWS, "Total rows written", 6, write_labels));
  write_total_bytes = taos_collector_registry_must_register_metric(
      taos_counter_new(WRITE_TOTAL_BYTES, "Total bytes written", 6, write_labels));
  write_fetch_batch_meta_time = taos_collector_registry_must_register_metric(
      taos_counter_new(WRITE_FETCH_BATCH_META_TIME, "Fetch batch meta time", 6, write_labels));
  write_fetch_batch_meta_count = taos_collector_registry_must_register_metric(
      taos_counter_new(WRITE_FETCH_BATCH_META_COUNT, "Fetch batch meta count", 6, write_labels));
  write_preprocess_time = taos_collector_registry_must_register_metric(
      taos_counter_new(WRITE_PREPROCESS_TIME, "Preprocess time", 6, write_labels));
  write_wal_write_bytes = taos_collector_registry_must_register_metric(
      taos_counter_new(WRITE_WAL_WRITE_BYTES, "WAL write bytes", 6, write_labels));
  write_wal_write_time = taos_collector_registry_must_register_metric(
      taos_counter_new(WRITE_WAL_WRITE_TIME, "WAL write time", 6, write_labels));
  write_apply_bytes =
      taos_collector_registry_must_register_metric(taos_counter_new(WRITE_APPLY_BYTES, "Apply bytes", 6, write_labels));
  write_apply_time =
      taos_collector_registry_must_register_metric(taos_counter_new(WRITE_APPLY_TIME, "Apply time", 6, write_labels));
  write_commit_count = taos_collector_registry_must_register_metric(
      taos_counter_new(WRITE_COMMIT_COUNT, "Commit count", 6, write_labels));
  write_commit_time =
      taos_collector_registry_must_register_metric(taos_counter_new(WRITE_COMMIT_TIME, "Commit time", 6, write_labels));
  write_memtable_wait_time = taos_collector_registry_must_register_metric(
      taos_counter_new(WRITE_MEMTABLE_WAIT_TIME, "Memtable wait time", 6, write_labels));
  write_blocked_commit_count = taos_collector_registry_must_register_metric(
      taos_counter_new(WRITE_BLOCKED_COMMIT_COUNT, "Blocked commit count", 6, write_labels));
  write_blocked_commit_time = taos_collector_registry_must_register_metric(
      taos_counter_new(WRITE_BLOCKED_COMMIT_TIME, "Blocked commit time", 6, write_labels));
  write_merge_count =
      taos_collector_registry_must_register_metric(taos_counter_new(WRITE_MERGE_COUNT, "Merge count", 6, write_labels));
  write_merge_time =
      taos_collector_registry_must_register_metric(taos_counter_new(WRITE_MERGE_TIME, "Merge time", 6, write_labels));
  write_last_cache_commit_time = taos_collector_registry_must_register_metric(
      taos_counter_new(WRITE_LAST_CACHE_COMMIT_TIME, "Last cache commit time", 6, write_labels));
  write_last_cache_commit_count = taos_collector_registry_must_register_metric(
      taos_counter_new(WRITE_LAST_CACHE_COMMIT_COUNT, "Last cache commit count", 6, write_labels));

  // Initialize global dnode counters
  const char *dnode_labels[] = {"metric_type", "cluster_id", "dnode_id", "dnode_ep"};
  dnode_rpc_queue_memory_allowed = taos_collector_registry_must_register_metric(
      taos_gauge_new(DNODE_RPC_QUEUE_MEMORY_ALLOWED, "RPC queue memory allowed", 4, dnode_labels));
  dnode_rpc_queue_memory_used = taos_collector_registry_must_register_metric(
      taos_gauge_new(DNODE_RPC_QUEUE_MEMORY_USED, "RPC queue memory used", 4, dnode_labels));
  dnode_apply_memory_allowed = taos_collector_registry_must_register_metric(
      taos_gauge_new(DNODE_APPLY_MEMORY_ALLOWED, "Apply memory allowed", 4, dnode_labels));
  dnode_apply_memory_used = taos_collector_registry_must_register_metric(
      taos_gauge_new(DNODE_APPLY_MEMORY_USED, "Apply memory used", 4, dnode_labels));

  return TSDB_CODE_SUCCESS;
}

void cleanupMetrics() {
  // monitorfw handles cleanup automatically
}

int32_t addWriteMetrics(int32_t vgId, int32_t dnodeId, int64_t clusterId, const char *dnodeEp, const char *dbname,
                        const SRawWriteMetrics *pRawMetrics) {
  if (pRawMetrics == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Prepare label values
  char clusterIdStr[32], dnodeIdStr[32], vgIdStr[32];
  snprintf(clusterIdStr, sizeof(clusterIdStr), "%" PRId64, clusterId);
  snprintf(dnodeIdStr, sizeof(dnodeIdStr), "%d", dnodeId);
  snprintf(vgIdStr, sizeof(vgIdStr), "%d", vgId);
  const char *label_values[] = {VNODE_WRITE_METRIC,     clusterIdStr, dnodeIdStr,
                                dnodeEp ? dnodeEp : "", vgIdStr,      dbname ? dbname : ""};

  // Update global shared counters using monitorfw
  taos_counter_add(write_total_rows, (double)pRawMetrics->total_rows, label_values);
  taos_counter_add(write_wal_write_time, (double)pRawMetrics->wal_write_time, label_values);
  taos_counter_add(write_commit_count, (double)pRawMetrics->commit_count, label_values);
  taos_counter_add(write_commit_time, (double)pRawMetrics->commit_time, label_values);
  taos_counter_add(write_blocked_commit_count, (double)pRawMetrics->blocked_commit_count, label_values);
  taos_counter_add(write_blocked_commit_time, (double)pRawMetrics->blocked_commit_time, label_values);
  taos_counter_add(write_merge_count, (double)pRawMetrics->merge_count, label_values);
  taos_counter_add(write_merge_time, (double)pRawMetrics->merge_time, label_values);
  taos_counter_add(write_last_cache_commit_time, (double)pRawMetrics->last_cache_commit_time, label_values);
  taos_counter_add(write_last_cache_commit_count, (double)pRawMetrics->last_cache_commit_count, label_values);

  // Update low level metrics when tsMetricsFlag is 1
  if (tsMetricsLevel == 1) {
    taos_counter_add(write_total_requests, (double)pRawMetrics->total_requests, label_values);
    taos_counter_add(write_total_bytes, (double)pRawMetrics->total_bytes, label_values);
    taos_counter_add(write_fetch_batch_meta_time, (double)pRawMetrics->fetch_batch_meta_time, label_values);
    taos_counter_add(write_fetch_batch_meta_count, (double)pRawMetrics->fetch_batch_meta_count, label_values);
    taos_counter_add(write_preprocess_time, (double)pRawMetrics->preprocess_time, label_values);
    taos_counter_add(write_apply_bytes, (double)pRawMetrics->apply_bytes, label_values);
    taos_counter_add(write_apply_time, (double)pRawMetrics->apply_time, label_values);
    taos_counter_add(write_wal_write_bytes, (double)pRawMetrics->wal_write_bytes, label_values);
    taos_counter_add(write_memtable_wait_time, (double)pRawMetrics->memtable_wait_time, label_values);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t addDnodeMetrics(const SRawDnodeMetrics *pRawMetrics, int64_t clusterId, int32_t dnodeId, const char *dnodeEp) {
  if (pRawMetrics == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Prepare label values
  char clusterIdStr[32], dnodeIdStr[32];
  snprintf(clusterIdStr, sizeof(clusterIdStr), "%" PRId64, clusterId);
  snprintf(dnodeIdStr, sizeof(dnodeIdStr), "%d", dnodeId);
  const char *label_values[] = {DNODE_METRIC, clusterIdStr, dnodeIdStr, dnodeEp ? dnodeEp : ""};

  // Update counters using monitorfw
  taos_gauge_set(dnode_rpc_queue_memory_allowed, (double)pRawMetrics->rpcQueueMemoryAllowed, label_values);
  taos_gauge_set(dnode_rpc_queue_memory_used, (double)pRawMetrics->rpcQueueMemoryUsed, label_values);
  taos_gauge_set(dnode_apply_memory_allowed, (double)pRawMetrics->applyMemoryAllowed, label_values);
  taos_gauge_set(dnode_apply_memory_used, (double)pRawMetrics->applyMemoryUsed, label_values);

  return TSDB_CODE_SUCCESS;
}

// New function to clean expired metrics based on valid vgroups
int32_t cleanupExpiredMetrics(SHashObj *pValidVgroups) {
  // Clean expired metrics for all write metrics counters
  cleanExpiredCounterMetrics(write_total_requests, pValidVgroups, "write_total_requests");
  cleanExpiredCounterMetrics(write_total_rows, pValidVgroups, "write_total_rows");
  cleanExpiredCounterMetrics(write_total_bytes, pValidVgroups, "write_total_bytes");
  cleanExpiredCounterMetrics(write_fetch_batch_meta_time, pValidVgroups, "write_fetch_batch_meta_time");
  cleanExpiredCounterMetrics(write_fetch_batch_meta_count, pValidVgroups, "write_fetch_batch_meta_count");
  cleanExpiredCounterMetrics(write_preprocess_time, pValidVgroups, "write_preprocess_time");
  cleanExpiredCounterMetrics(write_wal_write_bytes, pValidVgroups, "write_wal_write_bytes");
  cleanExpiredCounterMetrics(write_wal_write_time, pValidVgroups, "write_wal_write_time");
  cleanExpiredCounterMetrics(write_apply_bytes, pValidVgroups, "write_apply_bytes");
  cleanExpiredCounterMetrics(write_apply_time, pValidVgroups, "write_apply_time");
  cleanExpiredCounterMetrics(write_commit_count, pValidVgroups, "write_commit_count");
  cleanExpiredCounterMetrics(write_commit_time, pValidVgroups, "write_commit_time");
  cleanExpiredCounterMetrics(write_memtable_wait_time, pValidVgroups, "write_memtable_wait_time");
  cleanExpiredCounterMetrics(write_blocked_commit_count, pValidVgroups, "write_blocked_commit_count");
  cleanExpiredCounterMetrics(write_blocked_commit_time, pValidVgroups, "write_blocked_commit_time");
  cleanExpiredCounterMetrics(write_merge_count, pValidVgroups, "write_merge_count");
  cleanExpiredCounterMetrics(write_merge_time, pValidVgroups, "write_merge_time");
  cleanExpiredCounterMetrics(write_last_cache_commit_time, pValidVgroups, "write_last_cache_commit_time");
  cleanExpiredCounterMetrics(write_last_cache_commit_count, pValidVgroups, "write_last_cache_commit_count");
  return TSDB_CODE_SUCCESS;
}