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
#include "taos_gauge.h"
#include "taoserror.h"
#include "tglobal.h"
#include "thash.h"
#include "tlog.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

#define VNODE_WRITE_METRIC "write_metrics"
#define DNODE_METRIC       "dnodes_metrics"

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
#define WRITE_BLOCKED_COMMIT_COUNT    WRITE_TABLE ":blocked_commit_count"
#define WRITE_BLOCKED_COMMIT_TIME     WRITE_TABLE ":blocked_commit_time"
#define WRITE_MERGE_COUNT             WRITE_TABLE ":merge_count"
#define WRITE_MERGE_TIME              WRITE_TABLE ":merge_time"
#define WRITE_LAST_CACHE_COMMIT_TIME  WRITE_TABLE ":last_cache_commit_time"
#define WRITE_LAST_CACHE_COMMIT_COUNT WRITE_TABLE ":last_cache_commit_count"

#define DNODE_TABLE                    "taosd_dnodes_metrics"
#define DNODE_RPC_QUEUE_MEMORY_ALLOWED DNODE_TABLE ":rpc_queue_memory_allowed"
#define DNODE_RPC_QUEUE_MEMORY_USED    DNODE_TABLE ":rpc_queue_memory_used"
#define DNODE_APPLY_MEMORY_ALLOWED     DNODE_TABLE ":apply_memory_allowed"
#define DNODE_APPLY_MEMORY_USED        DNODE_TABLE ":apply_memory_used"

extern taos_counter_t *write_total_requests;
extern taos_counter_t *write_total_rows;
extern taos_counter_t *write_total_bytes;
extern taos_counter_t *write_fetch_batch_meta_time;
extern taos_counter_t *write_fetch_batch_meta_count;
extern taos_counter_t *write_preprocess_time;
extern taos_counter_t *write_wal_write_bytes;
extern taos_counter_t *write_wal_write_time;
extern taos_counter_t *write_apply_bytes;
extern taos_counter_t *write_apply_time;
extern taos_counter_t *write_commit_count;
extern taos_counter_t *write_commit_time;
extern taos_counter_t *write_memtable_wait_time;
extern taos_counter_t *write_blocked_commit_count;
extern taos_counter_t *write_blocked_commit_time;
extern taos_counter_t *write_merge_count;
extern taos_counter_t *write_merge_time;
extern taos_counter_t *write_last_cache_commit_time;
extern taos_counter_t *write_last_cache_commit_count;

// Global dnode metrics counters
extern taos_gauge_t *dnode_rpc_queue_memory_allowed;
extern taos_gauge_t *dnode_rpc_queue_memory_used;
extern taos_gauge_t *dnode_apply_memory_allowed;
extern taos_gauge_t *dnode_apply_memory_used;

// Macro for deleting a counter key with error logging
#define METRICS_DELETE_COUNTER(counter, key)                                     \
  do {                                                                           \
    int _r = taos_counter_delete((counter), (key));                              \
    if (_r) {                                                                    \
      uError("failed to delete metrics counter key:%s", (key) ? (key) : "NULL"); \
    }                                                                            \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_METRICS_INT_H_*/