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

#ifndef _TD_METRICS_INT_H
#define _TD_METRICS_INT_H

#include "metrics.h"
#include "os.h"
#include "osThread.h"
#include "taos_collector_registry.h"
#include "taos_counter.h"
#include "taoserror.h"
#include "tglobal.h"
#include "thash.h"
#include "tlog.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

// monitorfw types are already included via headers above

// Write metrics counters for a single vnode
typedef struct SVnodeMetricsCounters {
  int32_t vgId;
  int32_t dnodeId;
  int64_t clusterId;
  char    dbname[TSDB_DB_NAME_LEN];

  // monitorfw counters
  taos_counter_t *total_requests;
  taos_counter_t *total_rows;
  taos_counter_t *total_bytes;
  taos_counter_t *fetch_batch_meta_time;
  taos_counter_t *fetch_batch_meta_count;
  taos_counter_t *preprocess_time;
  taos_counter_t *wal_write_bytes;
  taos_counter_t *wal_write_time;
  taos_counter_t *apply_bytes;
  taos_counter_t *apply_time;
  taos_counter_t *commit_count;
  taos_counter_t *commit_time;
  taos_counter_t *memtable_wait_time;
  taos_counter_t *block_commit_count;
  taos_counter_t *blocked_commit_time;
  taos_counter_t *merge_count;
  taos_counter_t *merge_time;
  taos_counter_t *last_cache_commit_time;
  taos_counter_t *last_cache_commit_count;
} SVnodeMetricsCounters;

// Metrics manager structure
typedef struct {
  SHashObj      *pWriteMetrics;  // vgId -> SVnodeMetricsCounters*
  bool           initialized;
  TdThreadRwlock lock;
} SMetricsManager;

// Global variables
extern SMetricsManager gMetricsManager;
extern taos_counter_t *dnode_rpc_queue_memory_allowed;
extern taos_counter_t *dnode_rpc_queue_memory_used;
extern taos_counter_t *dnode_apply_memory_allowed;
extern taos_counter_t *dnode_apply_memory_used;

// Internal functions declarations that are not exposed in public API
SVnodeMetricsCounters *getVnodeMetricsCounters(int32_t vgId);
SVnodeMetricsCounters *createVnodeMetricsCounters(int32_t vgId, int32_t dnodeId, int64_t clusterId, const char *dbname);
void                   destroyVnodeMetricsCounters(SVnodeMetricsCounters *pCounters);
void                   destroyVnodeMetricsCountersWrapper(void *p);

#ifdef __cplusplus
}
#endif

#endif /*_TD_METRICS_INT_H_*/