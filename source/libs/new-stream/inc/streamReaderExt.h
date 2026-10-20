/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

/*
 * External source stream reader (ETR) -- internal header.
 *
 * P0 declared the SStreamExtReaderInfo struct and three stub entry points.
 * P2 fills in the struct fields and the full implementation in streamReaderExt.c.
 * See DS section 6.2.7.
 */
#ifndef _TD_STREAM_READER_EXT_H_
#define _TD_STREAM_READER_EXT_H_

#include "extConnector.h"
#include "stream.h"
#include "streamMsg.h"
#include "tcommon.h"
#include "thash.h"
#include "tmsg.h"
#include "tsimplehash.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Value stored in pUidIndex: one entry per external-source "sub-table" (uid).
 * groupId is derived at init time from the PARTITION BY spec (DS section 6.2.7).
 * maxTs is the watermark: reader only fetches rows with ts > maxTs.
 */
/*
 * One entry in pUidIndex.  groupId and maxTs are always present.
 * tagsetKey is only populated for InfluxDB sources; it holds the
 * "col1=val1|col2=val2|..." string used to reconstruct the tag-based
 * WHERE clause in per-uid queries. partitionValues caches the typed
 * PARTITION BY tuple evaluated during DISTINCT block initialization so
 * GROUP_COL_VALUE_PULL does not evaluate the same expressions again.
 * Empty string/NULL respectively for MySQL/PG.
 */
typedef struct SUidIndexEntry {
  uint64_t groupId;
  char     tagsetKey[1024]; /* "col1=val1|col2=val2|..."; empty for MySQL/PG */
  SArray  *partitionValues; /* SArray<SStreamGroupValue>; owned */
} SUidIndexEntry;

/*
 * Per-ETR-task, in-memory-only reader state.
 * Built on first TDMT_STREAM_TRIGGER_PULL_EXT processing.
 * See DS section 6.2.7.
 *
 * pUidIndex   : SSHashObj<uint64_t uid, SUidIndexEntry{groupId, tagsetKey, partitionValues}>
 * pGroupIndex : SSHashObj<uint64_t groupId, SArray<uint64_t uid>*>
 * pTagsetIndex: SSHashObj<char[] tagset, uint64_t uid>  (InfluxDB only)
 */
typedef struct SStreamExtReaderInfo {
  /* Connection to the external source.  Opened in streamReaderExtOpen and
   * held for the reader's lifetime by the trigger reader only; NULL for calc
   * readers (see borrowConn). */
  SExtConnectorHandle *pConn;

  /* Calc readers borrow a pooled connection for each fetch and return it
   * right after, instead of pinning one per execId for the stream's lifetime
   * (execReplica instances per calc scan would otherwise exhaust the
   * per-source pool).  The pool also probes liveness on reuse, so a broken
   * connection is replaced instead of failing every later fetch. */
  bool borrowConn;

  /* Serializes use of pConn and the pull state of the trigger reader. */
  TdThreadMutex connLock;
  bool          connLockInited;

  /* Deploy-time spec snapshot (copy; owned by this struct). */
  SStreamExtTriggerSpec spec;

  /* Back-pointer to the owning task for log context (not owned; lifetime >= pInfo). */
  const SStreamTask *pTask;

  /* InfluxDB N=64 batch loop: index of the next uid to pull.
   * Reset to 0 after all uids have been covered in a batch round. */
  int32_t influxBatchOffset;

  /* InfluxDB tag column names (SArray<char[TSDB_COL_NAME_LEN]>).
   * Populated once by extInitInfluxTagPartition; NULL for MySQL/PG.
   * Used to build the tagset string from returned rows and to look up uid. */
  SArray *pInfluxTagCols;   /* SArray<char[TSDB_COL_NAME_LEN]>; InfluxDB only */

  /* Parsed/cached scalar expression templates for spec.partitionTagExprs, parallel
   * to spec.partitionTagCols (same length as spec.partitionTagCols when
   * non-NULL). A NULL entry means that slot's partitionTagExprs[i] is "" (no
   * expression to evaluate -- bare column or tbname sentinel). Parsed once
   * in streamReaderExtOpen, then bound once to the DISTINCT tag block slots
   * before vectorized evaluation. Freed in streamReaderExtClose. */
  SArray *pPartitionColExprNodes;  /* SArray<SNode*> */

  /* Three in-memory lookup tables (DS section 6.2.7). */
  SSHashObj *pUidIndex;    /* hash<uint64_t uid,    SUidIndexEntry> */
  SSHashObj *pGroupIndex;  /* hash<uint64_t groupId, SArray<uint64_t>*> */
  SSHashObj *pTagsetIndex; /* hash<char[] tagset,  uint64_t uid>; InfluxDB only */

  /* Calc scan time range (STimeRangeNode*, owned) extracted from the calc
   * reader's scan plan at deploy time.  Each execId owns a separate clone.
   * mnode deploys one calc reader task per calc scan plan, so each external
   * scan (including each side of a self-join) has its own replica array.
   * When the calc statement carries
   * time-returning expressions (e.g. _twstart/_twend or casted localtime
   * placeholders), the fetch bounds must be evaluated per request with the
   * window parameters - the same way vnodeStream.c processCalaTimeRange does
   * for table scans - instead of trusting the raw merged [wstart, wend]
   * range.  NULL when the plan has no time range (full scan). */
  SNode *pCalcTimeRange;
  /* InfluxDB empty-SERIES-match enumeration cache: timestamp (ms) of the last
   * DISTINCT enumeration that matched no tag combinations. Repeated idle pulls
   * skip the information_schema + DISTINCT round-trips within the re-check
   * window; new remote data that appears later still triggers a fresh
   * enumeration. Reset naturally when the reader is re-deployed for a patch. */
  int64_t influxEmptyEnumTsMs;
} SStreamExtReaderInfo;

/* ---------------------------------------------------------------------------
 * Lifecycle API
 *
 *   streamReaderExtOpen   - decrypt password, open extConnector, init 3 hashes
 *   streamReaderExtClose  - close extConnector, cleanup hashes, zero the struct
 * --------------------------------------------------------------------------- */
int32_t streamReaderExtOpen(void *pSpec, const SStreamTask *pTask, SNode *pCalcTimeRange,
                            SStreamExtReaderInfo **ppReaderInfo);
void    streamReaderExtClose(SStreamExtReaderInfo *pReaderInfo);
int32_t streamReaderExtGetCalcInfo(const SStreamReaderTask *pTask, int32_t execId,
                                  SStreamExtReaderInfo **ppReaderInfo);

/* ---------------------------------------------------------------------------
 * PULL dispatcher
 *
 *   pReaderInfo : reader state (must be non-NULL)
 *   pullType    : ESTriggerPullType (STRIGGER_PULL_*_EXT)
 *   pReq        : in-memory SSTriggerExtPullReq* (caller owns)
 *   ppRsp       : output SSTriggerExtPullRsp* (callee allocates; caller frees
 *                 with streamExtPullRspFree)
 *
 * Returns TSDB_CODE_SUCCESS on success.
 * --------------------------------------------------------------------------- */
int32_t streamReaderExtHandlePull(SStreamExtReaderInfo *pReaderInfo,
                                  int32_t pullType, const void *pReq,
                                  void **ppRsp);

/* Free helper for SSTriggerExtPullRsp allocated inside streamReaderExtHandlePull. */
void streamExtPullRspFree(SSTriggerExtPullRsp *pRsp);

/* ---------------------------------------------------------------------------
 * EXT fetch: called by handleExtFetchReq in snode.c to execute an ad-hoc
 * SELECT against the external source and return a data block.
 *
 *   pReaderInfo : ETR state opened by streamReaderExtOpen (must be non-NULL)
 *   skey        : fetch time-range start (inclusive), INT64_MIN = no lower bound
 *   ekey        : fetch time-range end   (inclusive), INT64_MAX = no upper bound
 *   ppOut       : output SSDataBlock*; callee allocates; caller destroys via blockDataDestroy.
 *                 Set to NULL when no rows are available (no error).
 *
 * Returns TSDB_CODE_SUCCESS on success.
 * --------------------------------------------------------------------------- */
int32_t streamReaderExtFetchData(SStreamExtReaderInfo *pReaderInfo,
                                 int64_t skey, int64_t ekey,
                                 SSDataBlock **ppOut);

/* ---------------------------------------------------------------------------
 * EXT fetch time-range evaluation  -  mirrors vnodeStream.c processCalaTimeRange.
 *
 *   pReaderInfo : ETR state (pCalcTimeRange may be NULL -> full scan)
 *   pRt         : stream runtime func info of the fetch request (borrowed)
 *   pSkey/pEkey : output inclusive fetch bounds.  INT64_MIN/INT64_MAX means
 *                 unbounded (no range to apply / no windows).  An empty range
 *                 is reported as skey > ekey (the remote query returns no rows).
 *
 * A raw merged [wstart, wend] range is only correct for the standard external
 * window shape.  When the calc statement carries time-returning expressions
 * (STimeRangeNode.needCalc), the bounds are evaluated with the current window
 * parameters, exactly like the non-external reader path.  Multi-group calc
 * requests are bounded by the union of the per-group ranges.
 *
 * When pReaderInfo->pCalcTimeRange is set the result is authoritative: the
 * caller must not fall back to the runner-published curWindow.
 * --------------------------------------------------------------------------- */
int32_t streamReaderExtCalcFetchRange(SStreamExtReaderInfo *pReaderInfo,
                                      const SStreamRuntimeFuncInfo *pRt,
                                      int64_t *pSkey, int64_t *pEkey);

#ifdef __cplusplus
}
#endif
#endif /* _TD_STREAM_READER_EXT_H_ */
